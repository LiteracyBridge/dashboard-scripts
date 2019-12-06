#!/usr/bin/env python3
import argparse
import datetime
import os
from pathlib import Path

import boto3

DEFAULT_STAGING_DIRECTORY = '~/work/DeploymentsToS3Sync'
DEFAULT_DROPBOX_DIRECTORY = '~/Dropbox (Amplio)'

usage = '''Update latest Deployments from Dropbox to S3.'''

bucket_name = 'amplio-progspecs'
bucket = None  # s3 bucket
args = None  # argparse object with parsed command line args

s3_client = boto3.client('s3')

dropbox = None
staging = None

report = ['Checking for new deployments at {}'.format(datetime.datetime.now())]
found_deployments = {}

@property
def s3_client2():
    return None

def error(msg):
    report.append('ERROR: {}'.format(msg))


# Format and send an ses message. Options are
# html    - if true, send as html format
# dry_run - if true, do not actually send email
def send_ses(fromaddr='ictnotifications@amplio.org',
             subject='',
             body_text='',
             recipient='ictnotifications@amplio.org'):
    """Send an email via the Amazon SES service.

    Example:
      send_ses('me@example.com, 'greetings', "Hi!", 'you@example.com)

    Return:
      If 'ErrorResponse' appears in the return message from SES,
      return the message, otherwise return an empty '' string.
    """
    # If we ever want to send as html, here's how.
    html = False

    message = {'Subject': {'Data': subject}}
    if html:
        message['Body'] = {'Html': {'Data': body_text}}
    else:
        message['Body'] = {'Text': {'Data': body_text}}

    client = boto3.client('ses')
    response = client.send_email(Source=fromaddr, Destination={'ToAddresses': [recipient]}, Message=message)

    print(response)

# List the objects with the given prefix.
# noinspection PyPep8Naming
def _list_objects(Bucket=bucket, Prefix='', **kwargs):
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {'Bucket': Bucket, 'Prefix': Prefix, **kwargs}
    for objects in paginator.paginate(**kwargs):
        for obj in objects.get('Contents', []):
            yield obj


# Lazy get the s3 bucket; cache for future calls
def get_bucket():
    global bucket
    if bucket == None:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('acm-content-updates')
    return bucket


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


def get_s3_projects():
    global s3_client
    result = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    kwargs = {'Bucket': bucket_name, 'Delimiter': '/'}
    for objects in paginator.paginate(**kwargs):
        for pref in objects.get('CommonPrefixes', []):
            prj = pref['Prefix'].strip('/')
            result.add(prj)
    return result


def get_db_projects(given_projects):
    global dropbox
    if given_projects is not None:
        # translate project names 'acm-test' => 'TEST'
        result = map(cannonical_project_name, given_projects)
    else:
        # ACM-ish files
        acm_dirs = [x.name for x in os.scandir(dropbox) if x.name[0:4] == 'ACM-']
        # Limit to those with a "published" directory
        result = [cannonical_project_name(x) for x in acm_dirs if Path(dropbox, x, 'programspec').exists()]
    print(result)
    return result


def get_projects(given_projects):
    db_projects = set(get_db_projects(given_projects))
    s3_projects = get_s3_projects()
    return s3_projects.intersection(db_projects)


def get_server_etags(project):
    result = {}
    for obj in _list_objects(Bucket=bucket_name, Prefix=project+'/'):
        fn = obj['Key'][len(project)+1:]
        result[fn] = obj['ETag'][1:-1] # Amazon adds bogus quotes around value.
    return result

def get_local_etags(progspecdir):
    result = {}
    fn = Path(progspecdir, 'etags.properties')
    if fn.exists():
        with open(fn, 'r') as vf:
            for line in vf:
                line = line.strip()
                parts = line.split('=')
                result[parts[0]] = parts[1]
    return result

def write_local_etags(progspecdir, etags):
    with open(Path(progspecdir, 'etags.properties'), 'w') as vf:
        for fn,local_ver in etags.items():
            line = '{}={}'.format(fn, local_ver)
            print(line, file=vf)


def sync_project(project):
    global dropbox, s3_client
    server_etags = get_server_etags(project)
    progspecdir = Path(dropbox, cannonical_acm_name(project), 'programspec')
    local_etags = get_local_etags(progspecdir)
    needed_etags = {}
    for fn,server_etag in server_etags.items():
        local_etag = local_etags.get(fn)
        if local_etag != server_etag:
            needed_etags[fn] = server_etag
    if len(needed_etags) > 0 or True:
        for fn,etag in needed_etags.items():
            try:
                # This should be much easier. And maybe it is, but boto3 "documentation" is so thin that one
                # can see through it.
                key = project+'/'+fn
                download_path = str(Path(progspecdir, fn))
                # head_object lets us get the versionid
                obj_head = s3_client.head_object(Bucket=bucket_name, Key=key)
                xtra = {'VersionId': obj_head['VersionId']}
                rslt = s3_client.download_file(Bucket=bucket_name, Key=key, Filename=download_path, ExtraArgs=xtra)
            except Exception as ex:
                return False

        # Clean extraneous files. Note: preserves any directories
        with os.scandir(progspecdir) as it:
            for entry in it:
                if entry.is_file() and entry.name != 'etags.properties' and entry.name not in server_etags:
                    Path(entry).unlink()

        write_local_etags(progspecdir, server_etags) # now they're local as well
    return True

def sync_projects(projects):
    for proj in projects:
        # Retry twice in case of races
        tries = 3
        while tries > 0:
            if sync_project(proj):
                break
            tries -= 1

def main():
    global args, dropbox, staging
    arg_parser = argparse.ArgumentParser(description="Synchronize published content to S3.", usage=usage)
    arg_parser.add_argument('--project', nargs='*', help='Project(s) to update. Default: all projects in Dropbox.')
    arg_parser.add_argument('--user', nargs='*', help='Users(s) to update. Default: all users in staging directory.')
    arg_parser.add_argument('--dropbox', default=DEFAULT_DROPBOX_DIRECTORY,
                            help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('--staging', default=DEFAULT_STAGING_DIRECTORY,
                            help='Directory in which latest Deployments are staged.')
    arg_parser.add_argument('--dryrun', '--dry-run', '-n', default=False, action='store_true',
                            help='Do not copy or delete anything.')
    arg_parser.add_argument('--nos3', default=False, action='store_true', help='Do not upload to or delete from S3.')
    arg_parser.add_argument('--noemail', '--no-email', default=False, action='store_true',
                            help='Do not send email.')
    arg_parser.add_argument('--force', '-f', default=False, action='store_true',
                            help='Force updates, even if no changes detected.')
    args = arg_parser.parse_args()

    if args.dryrun:
        report.append('DRYRUN: No files will be changed.')
    if args.force:
        report.append('FORCE: All files considered missing or old.')

    dropbox = Path(os.path.expanduser(args.dropbox))
    staging = Path(os.path.expanduser(args.staging))

    projects = get_projects(args.project)
    sync_projects(projects)

    print(projects)


if __name__ == "__main__":
    exit(main())
