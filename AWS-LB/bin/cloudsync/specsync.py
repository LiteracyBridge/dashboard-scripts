#!/usr/bin/env python3
import argparse
import base64
import copy
import datetime
import json
import os
import time
from pathlib import Path
from typing import Any

import boto3
import pg8000
from botocore.exceptions import ClientError

from .helpers import send_ses, EMAIL_WITHOUT_ERRORS, EMAIL_WITH_ERRORS

DEFAULT_STAGING_DIRECTORY = '~/work/DeploymentsToS3Sync'
DEFAULT_DROPBOX_DIRECTORY = '~/Dropbox (Amplio)'

usage = '''Update latest Deployments from Dropbox to S3.'''

BUCKET_NAME = 'amplio-progspecs'
PROGSPEC_DIR = 'programspec'

RECIPIENTS = 'recipients'
RECIPIENTS_FILE = RECIPIENTS + '.csv'

RECIPIENTS_MAP = 'recipients_map'
RECIPIENTS_MAP_FILE = RECIPIENTS_MAP + '.csv'

TALKINGBOOK_MAP = 'talkingbook_map'
TALKINGBOOK_MAP_FILE = TALKINGBOOK_MAP + '.csv'

DEPLOYMENT_SPEC = 'deployment_spec'
DEPLOYMENT_SPEC_FILE = DEPLOYMENT_SPEC + '.csv'

VERSIONS_FILE = 'etags.properties'
PROGSPEC_FILES_TO_KEEP = [VERSIONS_FILE, 'deployments.csv']
PROGSPEC_EXTENSIONS_TO_KEEP = ['.xlsx']

RECIPIENTS_TABLE = RECIPIENTS
RECIPIENTS_TABLE_PKEY = 'recipientid'

RECIPIENTS_COLUMNS = []

TEMP_RECIPIENTS_TABLE_SQL = '''
CREATE TEMPORARY TABLE temp_table (
        recipientid char varying,
        project char varying,
        partner char varying,
        communityname char varying,
        groupname char varying,
        affiliate char varying,
        component char varying,
        country char varying,
        region char varying,
        district char varying,
        numhouseholds integer,
        numtbs integer,
        supportentity char varying,
        model char varying,
        language char varying,
        coordinates point,
        agent char varying,
        latitude double precision,
        longitude double precision,
        variant char varying
    );
'''

args:Any = None  # argparse object with parsed command line args

s3_client = boto3.client('s3')

dropbox:Any = None

found_errors = False

local_projects = None
server_projects = None
projects_to_check = None
projects_updated = set()

_report = ['Checking for new Program Specifications at {}'.format(datetime.datetime.now())]
_pending_lines = []

def error(line):
    report('ERROR: {}'.format(line), Print=True)


def report(line, Print=False, Hold=False, Reset=False):
    global args, _pending_lines, _report
    if Reset:
        _pending_lines = []
        if line is None:
            return
    if Hold:
        _pending_lines.append((line, Print))
    else:
        if len(_pending_lines) > 0:
            saved = _pending_lines
            _pending_lines = []
            for (_line, _print) in saved:
                report(_line, _print)
        _report.append(line)
        if Print or args.verbose >= 1:
            print(line)


def send_report_email():
    if len(projects_updated) > 0 or found_errors:
        send_ses(subject='Program Specifications Updated', body_text='\n'.join(_report),
                 recipient=EMAIL_WITH_ERRORS if found_errors else EMAIL_WITHOUT_ERRORS)


# List the objects with the given prefix.
# noinspection PyPep8Naming
def _list_objects(Bucket=BUCKET_NAME, Prefix='', **kwargs):
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {'Bucket': Bucket, 'Prefix': Prefix, **kwargs}
    for objects in paginator.paginate(**kwargs):
        for obj in objects.get('Contents', []):
            yield obj


# Given a project or acm name, return the project name.
def cannonical_project_name(acmdir):
    project = acmdir.upper()
    if project[0:4] == 'ACM-':
        project = project[4:]
    return project


# Given a project or acm name, return the acm name.
def cannonical_acm_name(project):
    acmdir = project.upper()
    if acmdir[0:4] != 'ACM-':
        acmdir = 'ACM-' + acmdir
    return acmdir


# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.
def get_secret() -> dict:
    result = ''
    secret_name = "lb_stats_access2"
    region_name = "us-west-2"

    if args.verbose >= 2:
        report('    Getting credentials for database connection. v2.')
    start = time.time()

    # Create a Secrets Manager client
    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
    except Exception as e:
        report('    Exception getting session client: {}, elapsed: {}'.format(str(e), time.time() - start))
        raise e

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if args.verbose >= 2:
            report('    Exception getting credentials: {}, elapsed: {}'.format(e.response['Error']['code'], time.time()-start))

        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        else:
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            result = json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            result = decoded_binary_secret

    # Your code goes here.
    return result


# lazy initialized db connection
db_connection = None


# Make a connection to the SQL database
def get_db_connection():
    global args, db_connection, RECIPIENTS_COLUMNS
    if db_connection is None:
        secret = get_secret()

        parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                 'host': secret['host'], 'port': secret['port']}
        if args.db_host:
            parms['host'] = args.db_host
        if args.db_port:
            parms['port'] = int(args.db_port)
        if args.db_user:
            parms['user'] = args.db_user
        if args.db_password:
            parms['password'] = args.db_password
        if args.db_name:
            parms['database'] = args.db_name

        db_connection = pg8000.connect(**parms)

        cur = db_connection.cursor()
        cur.execute('SELECT * FROM ' + RECIPIENTS_TABLE + ' LIMIT 1;')
        try:
            RECIPIENTS_COLUMNS = [x for x in [x[0].decode('ascii') for x in cur.description] if x != RECIPIENTS_TABLE_PKEY]
        except Exception as ex:
            # pg8000 made a breaking change, this is the new way to read the descriptions.
            RECIPIENTS_COLUMNS = [x for x in [x[0] for x in cur.description] if x != RECIPIENTS_TABLE_PKEY]

    return db_connection


# Get a list of projects with Program Specifications in S3
def get_server_project_list():
    global s3_client
    result = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    kwargs = {'Bucket': BUCKET_NAME, 'Delimiter': '/'}
    for objects in paginator.paginate(**kwargs):
        for pref in objects.get('CommonPrefixes', []):
            prj = pref['Prefix'].strip('/')
            result.add(prj)
    report('Server projects: {}'.format(', '.join(result)))
    return result


# Gets a list of projects in the local machine (ie, dropbox)
def get_local_project_list(given_projects):
    global dropbox
    if given_projects is not None:
        # translate project names 'acm-test' => 'TEST'
        result = [cannonical_project_name(x) for x in given_projects]
    else:
        # ACM-ish files
        acm_dirs = [x.name for x in os.scandir(dropbox) if x.name[0:4] == 'ACM-']
        # Limit to those with a "programspec" directory
        result = [cannonical_project_name(x) for x in acm_dirs if Path(dropbox, x, PROGSPEC_DIR).exists()]
    print(result)
    report('Local projects: {}'.format(', '.join(result)))
    return result


# Get the list of projects to check:
def get_projects(given_projects):
    global local_projects, server_projects, projects_to_check
    local_projects = set(get_local_project_list(given_projects))
    server_projects = get_server_project_list()
    projects_to_check = server_projects.intersection(local_projects)
    report('Projects to check: {}'.format(', '.join(projects_to_check)))
    return projects_to_check

class ProjectSynchronizer():
    def __init__(self, project:str, projspecdir:Path=None):
        global dropbox
        self._project = project
        self._server_etags = {}
        self._local_etags = {}
        self._needed_etags = {}
        self._progspecdir = projspecdir or Path(dropbox, cannonical_acm_name(project), PROGSPEC_DIR)

        self._files_updated = set()
        self._files_removed = set()
        self._recipients_changed = False
        self._deployments_changed = False
        self._success = False

    # Update sql database from recipients file.
    # noinspection SqlResolve,SqlNoDataSourceInspection
    def update_tables(self):
        global args, dropbox, found_errors
        recipients_csv = Path(self._progspecdir, RECIPIENTS_FILE)
        recipients_map_csv = Path(self._progspecdir, RECIPIENTS_MAP_FILE)
        talkingbook_map_csv = Path(self._progspecdir, TALKINGBOOK_MAP_FILE)
        deployment_spec_csv = Path(self._progspecdir, DEPLOYMENT_SPEC_FILE)

        conn = get_db_connection()
        cur = conn.cursor()

        if self._recipients_changed:
            skip_loading_recipients = False
            with open(recipients_csv, 'rb') as f:
                header_line:str = f.readline().decode('utf-8').strip()
                headers = header_line.split(',')
                if 'direct_beneficiaries' in headers:
                    skip_loading_recipients = True

            if skip_loading_recipients:
                report(f'    Not syncing database recipients for {self._project}; has "direct_beneficiaries" column.')
            else:
                report(f'   Syncing database recipients for {self._project}')

                # load temporary table with recipients
                cur.execute(TEMP_RECIPIENTS_TABLE_SQL)
                with open(recipients_csv, 'rb') as f:
                    cur.execute("COPY temp_table FROM stdin WITH CSV HEADER;", stream=f)
                num_loaded = cur.rowcount

                # add the new 'group_size' column.
                cur.execute('ALTER TABLE temp_table ADD COLUMN group_size INTEGER;')
                cur.execute('UPDATE temp_table SET group_size = 0;')
                # update into production table
                update = 'INSERT INTO {0} SELECT * FROM temp_table ON CONFLICT ({1}) DO UPDATE SET '.format(RECIPIENTS_TABLE,
                                                                                                            RECIPIENTS_TABLE_PKEY)
                update += ','.join([x + '=EXCLUDED.' + x for x in RECIPIENTS_COLUMNS])
                update += ';'
                cur.execute(update)
                num_updated = cur.rowcount
                found_errors |= num_updated != num_loaded
                report('   Recipients updated {} of {} loaded'.format(num_updated, num_loaded))
                cur.execute('DROP TABLE temp_table;')

                # load temporary table with recipients_map
                cur.execute('CREATE TEMPORARY TABLE temp_table AS SELECT * FROM recipients_map WHERE FALSE;')
                with open(recipients_map_csv, 'rb') as f:
                    cur.execute("COPY temp_table FROM stdin WITH CSV HEADER;", stream=f)
                num_loaded = cur.rowcount
                # update into production table
                update = 'INSERT INTO recipients_map SELECT * FROM temp_table ON CONFLICT DO NOTHING;'
                cur.execute(update)
                num_updated = cur.rowcount
                report('   Recipients_map updated {} of {} loaded'.format(num_updated, num_loaded))
                cur.execute('DROP TABLE temp_table;')

                if talkingbook_map_csv.exists():
                    # load temporary table with recipients_map
                    cur.execute('CREATE TEMPORARY TABLE temp_table AS SELECT * FROM ' + TALKINGBOOK_MAP + ' WHERE FALSE;')
                    with open(talkingbook_map_csv, 'rb') as f:
                        cur.execute("COPY temp_table FROM stdin WITH CSV HEADER;", stream=f)
                    num_loaded = cur.rowcount
                    # update into production table
                    update = 'INSERT INTO ' + TALKINGBOOK_MAP + ' SELECT * FROM temp_table ON CONFLICT DO NOTHING;'
                    cur.execute(update)
                    num_updated = cur.rowcount
                    report('   Talkingbook_map updated {} of {} loaded'.format(num_updated, num_loaded))
                    cur.execute('DROP TABLE temp_table;')

        if self._deployments_changed:
            report('   Syncing deployments for {}'.format(self._project))

            # deployment_spec -> deployments
            cur.execute(
                'CREATE TEMPORARY TABLE temp_table (project TEXT, deployment_num INTEGER, startdate DATE, enddate DATE, component TEXT, name TEXT);')
            with open(deployment_spec_csv, 'rb') as f:
                cur.execute("COPY temp_table FROM stdin WITH DELIMITER ',' CSV HEADER FORCE NOT NULL component;", stream=f)
            num_loaded = cur.rowcount
            # update into production table
            update = 'INSERT INTO deployments ' + \
                     'SELECT project, name AS deployment, name AS deploymentname, deployment_num, ' + \
                     '  startdate, enddate, NULL, NULL, component ' + \
                     'FROM temp_table ' + \
                     'ON CONFLICT (project, deployment) ' + \
                     '  DO UPDATE SET startdate=EXCLUDED.startdate, enddate=EXCLUDED.enddate, ' + \
                     '    deploymentnumber=EXCLUDED.deploymentnumber, ' + \
                     '    deployment=EXCLUDED.deployment, component=EXCLUDED.component;'
            cur.execute(update)
            num_updated = cur.rowcount
            report('   Deployments updated {} of {} loaded'.format(num_updated, num_loaded))
            cur.execute('DROP TABLE temp_table;')

        conn.commit()

    # Get the list of files, and their etags, from the server
    def get_server_etags(self):
        result = {}
        for obj in _list_objects(Bucket=BUCKET_NAME, Prefix=self._project + '/'):
            fn = obj['Key'][len(self._project) + 1:]
            result[fn] = obj['ETag'][1:-1]  # Amazon adds bogus quotes around value.
        return result


    # Read the etags properties from the last time we sync'd the project.
    def get_local_etags(self):
        result = {}
        fn = Path(self._progspecdir, VERSIONS_FILE)
        if fn.exists():
            with open(fn, 'r') as vf:
                for line in vf:
                    line = line.strip()
                    parts = line.split('=')
                    result[parts[0]] = parts[1]
        self._local_etags = result
        return result


    # Write the etags properties of the files just sync'd
    def write_local_etags(self):
        with open(Path(self._progspecdir, VERSIONS_FILE), 'w') as vf:
            for fn, local_ver in self._server_etags.items():
                line = '{}={}'.format(fn, local_ver)
                print(line, file=vf)

    def scan(self):
        self._server_etags = self.get_server_etags()
        self._local_etags = self.get_local_etags()
        # find which files, if any, are added or changed.
        for fn, server_etag in self._server_etags.items():
            local_etag = self._local_etags.get(fn)
            if local_etag != server_etag or not Path(self._progspecdir, fn).exists() or args.force:
                self._needed_etags[fn] = server_etag
        # work to do?
        return len(self._needed_etags) > 0

    def sync(self):
        report('Syncing {}'.format(self._project), Hold=True)

        projects_updated.add(self._project)

        # fetch stale and missing files
        items = {k:v for k,v in self._needed_etags.items()}
        for fn, etag in items.items():
            if args.verbose >= 2:
                report('   Syncing file {}'.format(fn))
            # This should be much easier. And maybe it is, but boto3 "documentation" is so thin that one
            # can see through it.
            key = self._project + '/' + fn
            download_path = str(Path(self._progspecdir, fn))
            # head_object lets us get the versionid
            obj_head = s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
            xtra = {'VersionId': obj_head['VersionId']}
            s3_client.download_file(Bucket=BUCKET_NAME, Key=key, Filename=download_path, ExtraArgs=xtra)
            self._recipients_changed |= fn == RECIPIENTS_FILE
            self._deployments_changed |= fn == DEPLOYMENT_SPEC_FILE
            self._files_updated.add(fn)
            # so we don't download it again unnecessarily, if something else triggers a retry
            self._needed_etags.pop(fn, None)

        # Clean extraneous files. Note: ignores directories
        with os.scandir(self._progspecdir) as it:
            for entry in it:
                if entry.is_file() and entry.name not in PROGSPEC_FILES_TO_KEEP and \
                        os.path.splitext(entry.name)[1] not in PROGSPEC_EXTENSIONS_TO_KEEP and \
                        entry.name not in self._server_etags:
                    self._files_removed.add(entry.name)
                    Path(entry).unlink()

        if self._recipients_changed or self._deployments_changed:
            if args.verbose >= 2:
                report('  Updating tables')
            self.update_tables()

        self.write_local_etags()
        
        self._success = True
        return True

    def report(self):
        if len(self._files_updated) > 0:
            report('   Files updated: {}'.format(', '.join(self._files_updated)))
        if len(self._files_removed) > 0:
            report('   Files removed: {}'.format(', '.join(self._files_removed)))
        if len(self._files_updated) == 0 and len(self._files_removed) == 0 and not self._recipients_changed:
            report('Project {}: no sync needed'.format(self._project), Reset=True)
        elif self._success:
            report('Project {} synced'.format(self._project))
        else:
            report('Project {} failed to fully sync; see previous errors'.format(self._project))

        report(line=None, Reset=True)

    # Check the given project to see if there are progspec files needing downloading.
    # def sync_project(self):
    #     global args, dropbox, s3_client, projects_updated, found_errors
    #     files_updated = []
    #     files_removed = []
    #     recipients_changed = False
    #     report('Syncing {}'.format(self._project), Hold=True)
    #
    #     server_etags = get_server_etags(project)
    #     progspecdir = Path(dropbox, cannonical_acm_name(project), PROGSPEC_DIR)
    #     local_etags = get_local_etags(progspecdir)
    #     # find which files, if any, are added or changed.
    #     needed_etags = {}
    #     for fn, server_etag in server_etags.items():
    #         local_etag = local_etags.get(fn)
    #         if local_etag != server_etag or not Path(progspecdir, fn).exists() or args.force:
    #             needed_etags[fn] = server_etag
    #
    #     # Anything to do?
    #     if len(needed_etags) > 0:
    #         projects_updated.add(self._project)
    #         for fn, etag in needed_etags.items():
    #             # noinspection PyBroadException
    #             try:
    #                 if args.verbose >= 2:
    #                     report('   syncing file {}'.format(fn))
    #                 # This should be much easier. And maybe it is, but boto3 "documentation" is so thin that one
    #                 # can see through it.
    #                 key = self._project + '/' + fn
    #                 download_path = str(Path(progspecdir, fn))
    #                 # head_object lets us get the versionid
    #                 obj_head = s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
    #                 xtra = {'VersionId': obj_head['VersionId']}
    #                 s3_client.download_file(Bucket=BUCKET_NAME, Key=key, Filename=download_path, ExtraArgs=xtra)
    #                 recipients_changed |= fn == RECIPIENTS_FILE
    #                 files_updated.append(fn)
    #             except Exception as ex:
    #                 found_errors = True
    #                 report('   Exception syncing {}: {}'.format(self._project, str(ex)), Print=True)
    #                 return False
    #
    #         # Clean extraneous files. Note: preserves any directories
    #         with os.scandir(progspecdir) as it:
    #             for entry in it:
    #                 if entry.is_file() and entry.name not in PROGSPEC_FILES_TO_KEEP and \
    #                         os.path.splitext(entry.name)[1] not in PROGSPEC_EXTENSIONS_TO_KEEP and \
    #                         entry.name not in server_etags:
    #                     files_removed.append(entry.name)
    #                     Path(entry).unlink()
    #
    #         if recipients_changed:
    #             update_recipients(self._project, progspecdir)
    #
    #         self.write_local_etags()
    #
    #         if len(files_updated) > 0:
    #             report('   Files updated: {}'.format(', '.join(files_updated)))
    #         if len(files_removed) > 0:
    #             report('   Files removed: {}'.format(', '.join(files_removed)))
    #         if len(files_updated) == 0 and len(files_removed) == 0 and not recipients_changed:
    #             report('Project {}: no sync needed'.format(self._project), Reset=True)
    #         else:
    #             report('Project {} synced'.format(self._project))
    #
    #     report(line=None, Reset=True)
    #     return True


# Iterate over projects, and sync them one by one.
def sync_projects(projects):
    global found_errors

    print('Synchronizing progspec directories.')

    for proj in projects:
        if args.verbose >= 2:
            report('   Checking sync state for {}'.format(proj))

        syncer = ProjectSynchronizer(proj)
        if syncer.scan():
            # Try up to three times, in case of races
            tries = 3
            while tries > 0:
                # noinspection PyBroadException
                try:
                    if syncer.sync():
                        break
                except Exception as ex:
                    report('   Exception syncing: {}'.format(str(ex)))
                found_errors = True
                tries -= 1
                report('  {} failed to sync, {} retries remaining'.format(proj, 'no' if tries == 0 else tries))
            syncer.report()


def run(_args):
    global args, dropbox
    args = _args

    if args.force:
        report('FORCE: All files considered missing or old.')
    if args.dryrun:
        report('DRYRUN: No files will be changed.')

    dropbox = Path(os.path.expanduser(args.dropbox))
    if args.verbose >= 2:
        print('Using dropbox in {}'.format(str(dropbox)))

    projects = get_projects(args.project)
    sync_projects(projects)

    send_report_email()


def main():
    global args, dropbox
    arg_parser = argparse.ArgumentParser(description="Synchronize published content to S3.", usage=usage)
    arg_parser.add_argument('--verbose', '-v', action='count', default=0, help='More verbose output.')
    arg_parser.add_argument('--project', nargs='*', help='Project(s) to update. Default: all projects in Dropbox.')
    arg_parser.add_argument('--dropbox', default=DEFAULT_DROPBOX_DIRECTORY,
                            help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('--dryrun', '--dry-run', '-n', default=False, action='store_true',
                            help='Do not copy or delete anything.')
    arg_parser.add_argument('--nos3', default=False, action='store_true', help='Do not upload to or delete from S3.')
    arg_parser.add_argument('--noemail', '--no-email', default=False, action='store_true',
                            help='Do not send email.')
    arg_parser.add_argument('--force', '-f', default=False, action='store_true',
                            help='Force updates, even if no changes detected.')

    arg_parser.add_argument('--db-host', default=None, metavar='HOST',
                            help='Optional host name, default from secrets store.')
    arg_parser.add_argument('--db-port', default=None, metavar='PORT',
                            help='Optional host port, default from secrets store.')
    arg_parser.add_argument('--db-user', default=None, metavar='USER',
                            help='Optional user name, default from secrets store.')
    arg_parser.add_argument('--db-password', default=None, metavar='PWD',
                            help='Optional password, default from secrets store.')
    arg_parser.add_argument('--db-name', default='dashboard', metavar='DB',
                            help='Optional database name, default "dashboard".')
    args = arg_parser.parse_args()

    return run(args)


if __name__ == "__main__":
    exit(main())
