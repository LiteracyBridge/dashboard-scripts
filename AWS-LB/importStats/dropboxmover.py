#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

usage = '''Move inbox/stats -> outbox/stats in Dropbox. Don\'t move files in the root of each tbcd0000 directory.'''

args = None
dropbox = None

files_moved = 0
files_failed = 0
dirs_processed = 0


# Move the (appropriate) contents from the device directory to dest. By appropriate, we mean
# the sub-directories of the device directory. Files in the root of the device directory are not
# moved.
def move_device(dev_source_root: Path, dev_dest_root: Path):
    global args, files_moved, files_failed
    for dev_subpath, directories, files in os.walk(dev_source_root, topdown=False):
        if dev_source_root.samefile(dev_subpath):
            if not (len(files)==1 and files[0]=='.dropbox'):
                print('Skipping files in device directory {}: {}'.format(dev_subpath, files))
        else:
            # The relative subdir, so we can construct the target
            subdir = Path(dev_subpath).relative_to(dev_source_root)
            for fname in files:
                if fname == '.dropbox':
                    continue
                source_file = Path(dev_subpath).joinpath(fname)
                dest_path = dev_dest_root.joinpath(subdir)
                dest_file = dest_path.joinpath(fname)
                try:
                    if not args.dryrun:
                        if not dest_path.exists():
                            dest_path.mkdir(parents=True, exist_ok=True)
                        source_file.rename(dest_file)
                        files_moved += 1
                    print('Move {} -> {}'.format(source_file, dest_file))
                except Exception as ex:
                    files_failed += 1
                    print('Exception moving file: {}'.format(ex))
            # Try, but not too hard, to remove the source directory.
            try:
                if not args.dryrun:
                    Path(dev_subpath).rmdir()
                    print('Removed {}'.format(dev_subpath))
            except OSError as ex:
                print('Exception removing directory: {}'.format(ex))


# Move the (appropriate) contents from source to dest. By appropriate, we mean the device directories,
# not any files that happen to be there.
def move_dirs(source: Path, dest: Path):
    global dirs_processed
    with os.scandir(source) as it:
        sourcedirs = [entry.name for entry in it if not entry.name.startswith('.') and entry.is_dir()]
    # sourcedirs not contains the names of the device dirctories within source, 'tbcd0000', ...
    for dev in sourcedirs:
        device_source = Path(source, dev)
        device_dest = Path(dest, dev)
        move_device(device_source, device_dest)
        dirs_processed += 1


def main():
    global args, files_moved, files_failed, dirs_processed
    arg_parser = argparse.ArgumentParser(description="Move UF files to .zips", usage=usage)
    arg_parser.add_argument('--dryrun', '-n', default=False, action='store_true', help='Dry run, do not move files.')
    arg_parser.add_argument('--dropbox', default='~/Dropbox', help='Dropbox directory (default is ~/Dropbox).')
    args = arg_parser.parse_args()
    dropbox = os.path.expanduser(args.dropbox)

    source = Path(dropbox, 'inbox', 'stats')
    dest = Path(dropbox, 'outbox', 'stats')

    if source.exists() and source.is_dir() and dest.exists() and dest.is_dir():
        move_dirs(source, dest)
        print(
            '\nMoved {} files from {} directories, {} files failed.'.format(files_moved, dirs_processed, files_failed))


if __name__ == "__main__":
    exit(main())
