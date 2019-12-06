import argparse
import sys

from .specsync import run as spec_run
from .cronjob import run as cron_run

usage = '''Update latest Deployments from Dropbox to S3.'''

DEFAULT_STAGING_DIRECTORY = '~/work/DeploymentsToS3Sync'
DEFAULT_DROPBOX_DIRECTORY = '~/Dropbox (Amplio)'

args = None


def main():
    global args, dropbox, staging
    arg_parser = argparse.ArgumentParser(description="Synchronize published content to S3.", usage=usage)
    arg_parser.add_argument('--verbose', '-v', action='count', default=0, help='More verbose output.')
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

    arg_parser.add_argument('--db-host', default=None, metavar='HOST',
                            help='Optional host name, default from secrets store.')
    arg_parser.add_argument('--db-port', default=None, metavar='PORT',
                            help='Optional host port, default from secrets store.')
    arg_parser.add_argument('--db-user', default=None, metavar='USER',
                            help='Optional user name, default from secrets store.')
    arg_parser.add_argument('--db-password', default=None, metavar='PWD',
                            help='Optional password, default from secrets store.')

    arg_parser.add_argument('command', choices=['deployments', 'progspecs', 'all'],
                            help='Command to run')

    args = arg_parser.parse_args()

    if args.command in ['deployments', 'all']:
        cron_run(args)
    if args.command in ['progspecs', 'all']:
        spec_run(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())