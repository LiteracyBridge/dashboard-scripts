#!/usr/bin/env python3
import argparse
import datetime
import filecmp
import os
import shutil
from pathlib import Path

import boto3

DEFAULT_STAGING_DIRECTORY = '~/work/DeploymentsToS3Sync'
DEFAULT_DROPBOX_DIRECTORY = '~/Dropbox (Literacy Bridge)'

usage = '''Update latest Deployments from Dropbox to S3.'''

bucket = None  # s3 bucket
args = None  # argparse object with parsed command line args

dropbox = None
staging = None

report = ['Checking for new deployments at {}'.format(datetime.datetime.now())]
found_deployments = {}

def error(msg):
    report.append('ERROR: {}'.format(msg))


# Format and send an ses message. Options are
# html    - if true, send as html format
# dry_run - if true, do not actually send email
def send_ses(fromaddr='ictnotifications@literacybridge.org',
             subject='',
             body_text='',
             recipient='ictnotifications@literacybridge.org'):
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


# Lazy get the s3 bucket; cache for future calls
def get_bucket():
    global bucket
    if bucket == None:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('acm-content-updates')
    return bucket


# Update files to s3. 'context' is a tuple of (project, staged-zip-file, staged-marker-file)
def sync_project_to_s3(context):
    global args
    project, staged_zip, staged_marker = context
    prefix = 'projects/{}/'.format(project)

    # Get the list of objects related to the given project.
    bucket = get_bucket()
    things = [x for x in bucket.objects.filter(Prefix=prefix).all()]

    # just the names, no directory
    desired_names = {staged_zip.name, staged_marker.name}
    # just the names, no prefix; map to s3 objects
    existing_names = {obj.key[len(prefix):]: obj for obj in things}
    # full keys of objects to be deleted
    keys_to_delete = [prefix + n for n in existing_names.keys() if n not in desired_names]

    # If zip different or args.force, upload zip
    if not staged_zip.exists():
        error('No staged .zip file for project {}'.format(project))
    elif args.force or staged_zip.name not in existing_names or \
            os.stat(staged_zip).st_size != existing_names[staged_zip.name].size:
        report.append('Updating .zip in s3: {}{}'.format(prefix, staged_zip.name))
        if not args.dryrun:
            bucket.upload_file(str(staged_zip), prefix + staged_zip.name)

    # If marker different or args.force, upload marker
    if not staged_marker.exists():
        error('No staged marker file for project {}'.format(project))
    elif args.force or staged_marker.name not in existing_names or \
            os.stat(staged_marker).st_size != existing_names[staged_marker.name].size:
        report.append('Updating marker in s3: {}{}'.format(prefix, staged_marker.name))
        if not args.dryrun:
            bucket.upload_file(str(staged_marker), prefix + staged_marker.name)

    # Clean up any old s3 objects. Everything except the .zip and marker files.
    if len(keys_to_delete) > 0:
        report.append('Deleting old s3 objects: {}'.format(keys_to_delete))
        if not args.dryrun:
            delete_args = {'Objects': [{'Key': key} for key in keys_to_delete]}
            delete_result = bucket.delete_objects(Delete=delete_args)
            print(delete_result)


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


# Checks the TB-Loaders / published directory for the latest .rev file (the latest
# published deployment). If that is different from the staging directory, copy
# to the staging directory, and return relevant information to update to s3.
def check_acm_for_update(acmdir):
    global dropbox, staging, report, found_deployments
    result = None
    project = cannonical_project_name(acmdir)
    published = Path(dropbox, acmdir, 'TB-Loaders', 'published')
    staged = Path(staging, 'projects', project)

    # Determine latest Deployment.
    published_files = [x for x in os.scandir(published)]
    # If there are no files at all in the published directory, then there is nothing to do.
    # probably a new ACM (or ACM-template)
    if len(published_files) == 0:
        return
    rev = [x for x in published_files if x.name[-4:] == '.rev']
    # If there isn't a .rev file, there's no update.
    # If there's more than one, we don't know which to pick.
    if len(rev) == 0:
        error('Project {} has no published .rev files.'.format(project))
        return
    if len(rev) > 1:
        error('Project {} has too many .rev files ({}).'.format(project, len(rev)))
        return
    deployment = rev[0].name[0:-4]
    found_deployments[project] = deployment

    content_dir = Path(published, deployment)
    if not (content_dir.exists() and content_dir.is_dir()):
        error('Project {} missing content directory for deployment {}'.format(project, deployment))
        return

    content_zip = Path(content_dir, 'content-{}.zip'.format(deployment))
    if (not (content_zip.exists() and content_zip.is_file())):
        error('Project {} missing content .zip for deployment {}'.format(project, deployment))
        return

    # We know the latest Deployment for the project. Does it match what's in staging?
    needs_update = False
    staged_zip = Path(staged, content_zip.name)
    staged_marker = Path(staged, '{}.current'.format(deployment))

    if not (staged_zip.exists() and staged_zip.is_file()):
        report.append('No staged content for project {}, deployment {}'.format(project, deployment))
        needs_update = True
    elif not filecmp.cmp(content_zip, staged_zip):
        report.append('Mismatched staged content for project {}, deployment {}'.format(project, deployment))
        needs_update = True

    if not (staged_marker.exists() and staged_marker.is_file()):
        report.append('No marker file for project {}, deployment {}'.format(project, deployment))
        needs_update = True

    if needs_update or args.force:
        report.append('Updating content for project {}, deployment {}'.format(project, deployment))
        result = (project, staged_zip, staged_marker)
        if not args.dryrun:
            shutil.rmtree(staged, ignore_errors=True)
            os.makedirs(staged, exist_ok=True)
            shutil.copy2(content_zip, staged_zip)
            with open(staged_marker, 'w') as marker:
                print(deployment, file=marker)

    return result


# Checks the given acm directories for unstaged updates, and copies any found to the
# staging directory. Then updates the s3 bucket for the projects contained in those acmdirs.
def check_for_updates(acmdirs):
    # check all projects
    updated_projects = [check_acm_for_update(acm) for acm in acmdirs]
    # keep only the ones with  updates
    updated_projects = [p for p in updated_projects if p is not None]

    if not args.nos3:
        for proj in updated_projects:
            sync_project_to_s3(proj)

    return len(updated_projects) > 0


# Builds a list of acms to check for updates. If no '--project ...' command line argument, will
# look in dropbox for the ACMs.
def get_acm_list(given_projects):
    global dropbox
    if given_projects is not None:
        # translate project names 'test' => 'ACM-TEST'
        acm_dirs = map(cannonical_acm_name, given_projects)
    else:
        # ACM-ish files
        acm_dirs = [x.name for x in os.scandir(dropbox) if x.name[0:4] == 'ACM-']
        # Limit to those with a "published" directory
        acm_dirs = [x for x in acm_dirs if Path(dropbox, x, 'TB-Loaders', 'published').exists()]
    print(acm_dirs)
    return acm_dirs

def status_check(need_report):
    global staging, args, found_deployments
    date_format = '%Y-%m-%d %H:%M:%S.%f'
    now = datetime.datetime.now()
    then_file = Path(staging, 'status_check.txt')
    then = now - datetime.timedelta(weeks=1000)
    if then_file.exists():
        with open(then_file, 'r') as tf:
            then_str = tf.readline().strip()
            then = datetime.datetime.strptime(then_str, date_format)
    # Has it been a day?
    if (now-then).days > 0:
        report.insert(0, 'Daily Deployment Sync report\n\n')
        # Todo: produce a full report of current deployments, versions, and timestamps
        need_report = True

    # If we're going to send a status email, update this file. Keeps us from being too spammy.
    if need_report and not args.noemail:
        os.makedirs(staging, exist_ok=True)
        with open(then_file, 'w') as tf:
            then_str = now.strftime(date_format)
            print(then_str, file=tf)

        report.append('')
        report.append('Current deployments:')
        proj_width = max([len(p) for p in found_deployments.keys()])
        depl_width = max([len(p) for p in found_deployments.values()])
        report.append('{:>{pw}} : {:<{dw}}'.format('Project', 'Deployment', pw=proj_width, dw=depl_width))
        report.append('{:->{pw}}-+-{:-<{dw}}'.format('', '', pw=proj_width, dw=depl_width))
        for p,d in found_deployments.items():
            report.append('{:>{pw}} : {:<{dw}}'.format(p, d, pw=proj_width, dw=depl_width))

    return need_report


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

    projects = get_acm_list(args.project)
    need_report = check_for_updates(projects)

    # Presently, only sends a status once a day, even if nothing to report.
    need_report = status_check(need_report)

    if args.noemail or not need_report:
        print('\n'.join(report))
    else:
        send_ses(subject='Content updates to S3', body_text='\n'.join(report))


if __name__ == "__main__":
    exit(main())
