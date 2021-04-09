import argparse
import csv
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple, Dict, Union, Any

from A18Processor import A18Processor
from ArgParseActions import StorePathAction, StoreFileExtension
from a18file import A18File
from dbutils import DbUtils
from UfPropertiesProcessor import UfPropertiesProcessor
from filesprocessor import FilesProcessor

args: Any = None

dbUtils: DbUtils

propertiesProcessor: Union[None, UfPropertiesProcessor] = None


def bundle_uf() -> Tuple[int, int, int, int]:
    def t(s):
        """
        Format time 'h:mm:ss', 'mm:ss', or 'ss seconds'
        :param s: time in seconds
        :return: time as formatted string
        """
        h, m = divmod(s, 3600)
        m, s = divmod(m, 60)
        if h:
            return f'{h}:{m:02}:{s:02}'
        if m:
            return f'{m}:{s:02}'
        return f'{s} seconds'

    @dataclass
    class BundleInfo:
        language: str = ''
        uf_list: List[DbUtils.UfRecord] = field(default_factory=list)
        seconds: int = 0
        bytes: int = 0

    global args
    if not args.program or not args.depl:
        raise (Exception('Must specify both --program and --depl'))
    max_files = args.max_files or 1000
    max_bytes = args.max_bytes or 10_000_000  # 10 MB
    min_duration = 5
    max_duration = 300
    db = DbUtils(args)
    rows: List[DbUtils.UfRecord] = db.get_uf_records(programid=args.program, deploymentnumber=args.depl)

    good_uf = [x for x in rows if
               x.bundleid is None and x.length_seconds >= min_duration and x.length_seconds <= max_duration]
    print(f'Received {len(rows)} rows, {len(good_uf)} meet length criteria.')

    partitions: List[BundleInfo] = []
    current_partitions: Dict[str, BundleInfo] = {}
    for ix in range(0, len(good_uf)):
        uf = good_uf[ix]
        language = uf.language
        current_partition = current_partitions.setdefault(language, BundleInfo(language=language))
        # Does this message fit?
        if current_partition.bytes + uf.length_bytes > max_bytes and len(current_partition.uf_list) > 0:
            # Didn't fit, save previous partition, create a new, empty one for this language
            partitions.append(current_partition)
            current_partition = BundleInfo(language=language)
            current_partitions[language] = current_partition
        # Add message to current partition.
        current_partition.uf_list.append(uf)
        current_partition.bytes += uf.length_bytes
        current_partition.seconds += uf.length_seconds
    # Capture the partitions that were "in progress"
    for p in current_partitions.values():
        partitions.append(p)

    print(f'{len(partitions)} partitions:')
    for p in partitions:
        print(f'   {p.language}: {len(p.uf_list)} files, {t(p.seconds)} total, {p.bytes:,} bytes.')

    # Call partitions "directories" and good_uf "files"
    return len(partitions), len(good_uf), 0, 0


def list_a18_metadata() -> Tuple[int, int, int, int, int]:
    global args

    def acceptor(p: Path) -> bool:
        return p.suffix.lower() == '.a18'

    def processor(p: Path) -> None:
        a18_file = A18File(p, args)
        metadata = a18_file.metadata
        if metadata is not None:
            key_width = max([len(k) for k in metadata.keys()])
            for k, v in metadata.items():
                print(f'{k:>{key_width}} = {v}')

    fp: FilesProcessor = FilesProcessor(args.files)
    ret = fp.process_files(acceptor, processor, limit=args.limit, verbose=args.verbose)
    return ret


def create_properties() -> Tuple[int, int, int, int, int]:
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
        a18_file = A18File(p, args)
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
    return ret


def convert_files() -> Tuple[int, int, int, int, int]:
    global args
    processor: A18Processor = A18Processor(args.files)
    ret = processor.convert_a18_files(format=args.format, limit=args.limit, verbose=args.verbose)
    # ret = processor.process('convert')
    return ret


def extract_uf_files() -> Tuple[int, int, int, int, int]:
    global args
    processor: A18Processor = A18Processor(args.files, args)
    ret = processor.extract_uf_files(no_db=args.no_db, format=args.format, limit=args.limit, verbose=args.verbose)
    propertiesProcessor.commit()
    return ret


def import_uf_metadata() -> Tuple[int, int, int, int, int]:
    """
    Imports the contents of .properties files to PostgreSQL, uf_metadata table.
    :return: counts of files & directories processed.
    """
    global args, propertiesProcessor

    ret = propertiesProcessor.add_from_files(args.files)
    propertiesProcessor.commit()
    return ret


def main():
    global args, propertiesProcessor
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--verbose', '-v', action='count', default=0, help="More verbose output.")
    arg_parser.add_argument('--dry-run', '-n', action='store_true', default=False, help='Don\'t update anything.')
    arg_parser.add_argument('--ffmpeg', action='store_true', help='Use locally installed ffmpeg.')
    arg_parser.add_argument('--limit', type=int, default=999999999,
                            help='Stop after N files. Default is (virtually) unlimited.')

    subparsers = arg_parser.add_subparsers(dest="'Sub-command.'", required=True, help='Command descriptions')

    list_parser = subparsers.add_parser('list', help='List the metadata from .a18 files.')
    list_parser.set_defaults(func=list_a18_metadata)
    list_parser.add_argument('files', nargs='+', action=StorePathAction, help='Files and directories to be listed.')

    convert_parser = subparsers.add_parser('convert', help='Convert files to another format.')
    convert_parser.set_defaults(func=convert_files)
    convert_parser.add_argument('files', nargs='+', action=StorePathAction,
                                help='Files and directories to be converted.')
    convert_parser.add_argument('--out', action=StorePathAction,
                                help='Output directory for converted files (default is adjacent to original file')
    convert_parser.add_argument('--format', choices=['mp3', 'aac', 'wma', 'wav', 'ogg'], default='mp3',
                                action=StoreFileExtension,
                                help='Audio format desired for the convert option.')

    create_properties_parser = subparsers.add_parser('create_properties',
                                                     help='Try to create a .properties file from a .a18 file.')
    create_properties_parser.set_defaults(func=create_properties)
    create_properties_parser.add_argument('files', nargs='+', action=StorePathAction,
                                          help='Files and directories to be extracted.')
    create_properties_parser.add_argument('--map', required=True, action=StorePathAction,
                                          help='recipients_map.csv file to find recipientids.')
    create_properties_parser.add_argument('--program', required=True, type=str,
                                          help='Program (id) from which the files were derived.')
    create_properties_parser.add_argument('--depl', required=True, type=int,
                                          help='Deployment from which the files were derived.')

    extract_uf_parser = subparsers.add_parser('extract_uf', help='Extract user feedback audio files and metadata.')
    extract_uf_parser.set_defaults(func=extract_uf_files)
    extract_uf_parser.add_argument('files', nargs='+', action=StorePathAction,
                                   help='Files and directories to be extracted.')
    extract_uf_parser.add_argument('--no-db', action='store_true', default=False,
                                   help='Do not update the SQL database.')
    extract_uf_parser.add_argument('--out', action=StorePathAction, required=True,
                                   help='Output directory for extracted files (default is a temporary directory).')
    extract_uf_parser.add_argument('--format', choices=['mp3', 'aac', 'wma', 'wav', 'ogg'], default='mp3',
                                   action=StoreFileExtension,
                                   help='Audio format desired for the extracted user feedback.')

    import_parser = subparsers.add_parser('import', help='Import extracted UF metadata into PostgreSQL.')
    import_parser.set_defaults(func=import_uf_metadata)
    import_parser.add_argument('files', nargs='+', action=StorePathAction, help='Files and directories to be imported.')

    bundle_parser = subparsers.add_parser('bundle', help='Bundle user feedback into manageable groups.')
    bundle_parser.set_defaults(func=bundle_uf)
    bundle_parser.add_argument('--program', type=str, help='Program for which to bundle uf.')
    bundle_parser.add_argument('--depl', type=int, help='Deployment in the program for which to bundle uf.')
    bundle_parser.add_argument('--max-bytes', '-mb', type=int, help='Maximum aggregate size of files to bundle.')
    bundle_parser.add_argument('--max-files', '-mf', type=int, help='Maximum number of files to bundle together.')
    bundle_parser.add_argument('--max-duration', '-md', type=int,
                               help='Maximum number of combined seconds to bundle together.')

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

    propertiesProcessor = UfPropertiesProcessor(args=args)

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
