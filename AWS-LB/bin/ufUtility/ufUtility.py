import argparse
import csv
import time
from pathlib import Path
from typing import Tuple, Union, Any

from A18Processor import A18Processor
from ArgParseActions import StorePathAction, StoreFileExtension
from UfBundler import UfBundler
from UfMetadata import UfMetadata
from a18file import A18File
from dbutils import DbUtils
from filesprocessor import FilesProcessor

args: Any = None

dbUtils: DbUtils

propertiesProcessor: Union[None, UfMetadata] = None


def _do_bundle() -> Tuple[int, int, int, int, int]:
    global args
    if not args.program or not args.depl:
        raise (Exception('Must specify both --program and --depl'))
    only_zip: bool = args.only_zip
    rebundle = args.rebundle
    bundle_uuids = args.bundle_uuid or None
    if bundle_uuids and not only_zip:
        raise (Exception('Must specify --only-zip with --bundle-uuid'))
    kwargs = {
        'max_files': args.max_files or 1000,
        'max_bytes': args.max_bytes or 10_000_000,  # 10 MB
        'min_uf_duration': 5,
        'max_uf_duration': 300,
        'bucket': args.bucket or None
    }
    programid = args.program
    deployment_number = args.depl

    bundler = UfBundler(programid, deployment_number, **kwargs)
    return bundler.make_bundles(zip=not args.no_zip, only_zip=only_zip, bundle_uuid=bundle_uuids, rebundle=rebundle)


def _do_list_properties_files() -> Tuple[int, int, int, int, int]:
    global args

    def acceptor(p: Path) -> bool:
        return p.suffix.lower() == '.a18'

    def processor(p: Path) -> None:
        a18_file = A18File(p, verbose=args.verbose, dry_run=args.dry_run)
        metadata = a18_file.metadata
        if metadata is not None:
            key_width = max([len(k) for k in metadata.keys()])
            for k, v in metadata.items():
                print(f'{k:>{key_width}} = {v}')

    fp: FilesProcessor = FilesProcessor(args.files)
    ret = fp.process_files(acceptor, processor, limit=args.limit, verbose=args.verbose)
    return ret  # n_dirs, n_files, n_skipped, n_missing, n_errors


def _do_create_properties_files() -> Tuple[int, int, int, int, int]:
    """
    Try to create a .properties file from .a18 files.
    :return: file and directory counts
    """
    global args

    def a18_acceptor(p: Path) -> bool:
        return p.suffix.lower() == '.a18'

    def a18_processor(p: Path):
        community = p.parent.name
        recipientid = recipients_map.get(community.upper())
        a18_file = A18File(p, verbose=args.verbose, dry_run=args.dry_run)
        kwargs = {
            'recipientid': recipientid,
            'programid': args.program,
            'deploymentnumber': args.depl,
            'community': community
        }
        ret = a18_file.create_sidecar(**kwargs)
        return ret

    recipients_map = {}
    with open(args.map, 'r') as recipients_map_file:
        csvreader = csv.DictReader(recipients_map_file)
        for row in csvreader:
            if row.get('project') == args.program:
                recipients_map[row.get('directory')] = row.get('recipientid')

    processor: FilesProcessor = FilesProcessor(args.files)
    ret = processor.process_files(a18_acceptor, a18_processor, limit=args.limit, verbose=args.verbose)
    return ret  # n_dirs, n_files, n_skipped, n_missing, n_errors


def _do_convert_audio_format() -> Tuple[int, int, int, int, int]:
    global args
    processor: A18Processor = A18Processor(args.files)
    ret = processor.convert_a18_files(format=args.format, limit=args.limit, verbose=args.verbose)
    return ret  # n_dirs, n_files, n_skipped, n_missing, n_errors


def _do_extract_uf() -> Tuple[int, int, int, int, int]:
    global args
    processor: A18Processor = A18Processor(args.files)
    ret = processor.extract_uf_files(out_dir=args.out, no_db=args.no_db, format=args.format, limit=args.limit, verbose=args.verbose)
    propertiesProcessor.commit()
    return ret  # n_dirs, n_files, n_skipped, n_missing, n_errors


def _do_import_uf_metadata() -> Tuple[int, int, int, int, int]:
    """
    Imports the contents of .properties files to PostgreSQL, uf_metadata table.
    :return: counts of files & directories processed.
    """
    global args

    ufMetadata = UfMetadata()

    ret = ufMetadata.add_from_files(args.files)
    ufMetadata.commit()
    return ret  # n_dirs, n_files, n_skipped, n_missing, n_errors


def main():
    global args, dbUtils, propertiesProcessor
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--verbose', '-v', action='count', default=0, help="More verbose output.")
    arg_parser.add_argument('--dry-run', '-n', action='store_true', default=False, help='Don\'t update anything.')
    arg_parser.add_argument('--ffmpeg', action='store_true', help='Use locally installed ffmpeg.')
    arg_parser.add_argument('--limit', type=int, default=999999999,
                            help='Stop after N files. Default is (virtually) unlimited.')

    subparsers = arg_parser.add_subparsers(dest="'Sub-command.'", required=True, help='Command descriptions')

    # List the contents of .properties files ("sidecar" files).
    list_parser = subparsers.add_parser('list', help='List the metadata from .a18 files.')
    list_parser.set_defaults(func=_do_list_properties_files)
    list_parser.add_argument('files', nargs='+', action=StorePathAction, help='Files and directories to be listed.')

    # Convert audio from .a18 to another audio format.
    convert_parser = subparsers.add_parser('convert', help='Convert files to another format.')
    convert_parser.set_defaults(func=_do_convert_audio_format)
    convert_parser.add_argument('files', nargs='+', action=StorePathAction,
                                help='Files and directories to be converted.')
    convert_parser.add_argument('--out', action=StorePathAction,
                                help='Output directory for converted files (default is adjacent to original file')
    convert_parser.add_argument('--format', choices=['mp3', 'aac', 'wma', 'wav', 'ogg'], default='mp3',
                                action=StoreFileExtension,
                                help='Audio format desired for the convert option.')

    # Create .properties files, where possible, for UF .a18 files.
    create_properties_parser = subparsers.add_parser('create_properties',
                                                     help='Try to create a .properties file from a .a18 file.')
    create_properties_parser.set_defaults(func=_do_create_properties_files)
    create_properties_parser.add_argument('files', nargs='+', action=StorePathAction,
                                          help='Files and directories to be extracted.')
    create_properties_parser.add_argument('--map', required=True, action=StorePathAction,
                                          help='recipients_map.csv file to find recipientids.')
    create_properties_parser.add_argument('--program', required=True, type=str,
                                          help='Program (id) from which the files were derived.')
    create_properties_parser.add_argument('--depl', required=True, type=int,
                                          help='Deployment from which the files were derived.')

    # Extract UF from the statistics uploads.
    extract_uf_parser = subparsers.add_parser('extract_uf', help='Extract user feedback audio files and metadata.')
    extract_uf_parser.set_defaults(func=_do_extract_uf)
    extract_uf_parser.add_argument('files', nargs='+', action=StorePathAction,
                                   help='Files and directories to be extracted.')
    extract_uf_parser.add_argument('--no-db', action='store_true', default=False,
                                   help='Do not update the SQL database.')
    extract_uf_parser.add_argument('--out', action=StorePathAction, required=True,
                                   help='Output directory for extracted files.')
    extract_uf_parser.add_argument('--format', choices=['mp3', 'aac', 'wma', 'wav', 'ogg'], default='mp3',
                                   action=StoreFileExtension,
                                   help='Audio format desired for the extracted user feedback.')

    # Import extracted metadata into PostgreSQL.
    import_parser = subparsers.add_parser('import', help='Import extracted UF metadata into PostgreSQL.')
    import_parser.set_defaults(func=_do_import_uf_metadata)
    import_parser.add_argument('files', nargs='+', action=StorePathAction, help='Files and directories to be imported.')

    # bundle user feedback.
    bundle_parser = subparsers.add_parser('bundle', help='Bundle user feedback into manageable groups.')
    bundle_parser.set_defaults(func=_do_bundle)
    bundle_parser.add_argument('--program', type=str, help='Program for which to bundle uf.')
    bundle_parser.add_argument('--depl', type=int, help='Deployment in the program for which to bundle uf.')
    bundle_parser.add_argument('--max-bytes', '-mb', type=int, help='Maximum aggregate size of files to bundle.')
    bundle_parser.add_argument('--max-files', '-mf', type=int, help='Maximum number of files to bundle together.')
    bundle_parser.add_argument('--max-duration', '-md', type=int,
                               help='Maximum number of combined seconds to bundle together.')
    bundle_parser.add_argument('--no-zip', action='store_true', default=False,
                               help='Don\'t zip the bundled files together.')
    bundle_parser.add_argument('--only-zip', action='store_true', default=False, help='Only zip existing bundles.')
    bundle_parser.add_argument('--rebundle', action='store_true', default=False, help='Remove existing bundle ids, and re-bundle.')
    bundle_parser.add_argument('--bundle-uuid', type=str, nargs='+',
                               help='With --only-zip, limit zip to the given bundle uuids.')
    bundle_parser.add_argument('--bucket', type=str,
                               help='Optional bucket for .zip file objects, default downloads.amplio.org')

    # database overrides
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
    if args.verbose > 2:
        print(f'Verbose setting: {args.verbose}.')
    dbArgs = {
        'verbose': args.verbose,
        'db_host': args.db_host,
        'db_port': args.db_port,
        'db_user': args.db_user,
        'db_password': args.db_password,
        'db_name': args.db_name
    }
    # Instantiate the db interface very early so that the connection parameters are set before the
    # database connection is needed.
    dbUtils = DbUtils(**dbArgs)
    propertiesProcessor = UfMetadata()

    timer = -time.time_ns()
    n_dirs, n_files, n_skipped, n_missing, n_errors = args.func()
    timer += time.time_ns()

    propertiesProcessor.print()

    print(f'Finished in {timer:,}ns')
    print(
        f'Processed {n_files} files{f" (with {n_errors} reported errors)" if n_errors else ""} in {n_dirs} directories. Skipped {n_skipped} files. '
        f'{n_missing} files not found.')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
