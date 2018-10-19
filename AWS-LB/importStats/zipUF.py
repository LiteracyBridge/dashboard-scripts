#!/usr/bin/env python3
import argparse
import os
import shutil
import tempfile
from timeit import default_timer as timer
import zipfile
from pathlib import Path

usage = '''Move user feedback files into .zips, to eliminate huge numbers of files (there is no 
significant compression).'''

UF_DIR_NAMES = ['userrecordings', 'recordingsprocessed', 'recordingsskipped']
totals = {x: 0 for x in UF_DIR_NAMES}

# Given a year-month-day, return the path the the daily collected-data-processed file.
def collected_data_path(year=None, month=None, day=None):
    global dropbox
    path = dropbox + '/' + 'collected-data-processed'
    if year is not None:
        path = path + '/' + year
        if month is not None:
            path = path + '/' + month
            if day is not None:
                path = path + '/' + day
    return path


# Zip the files in the from_path, into a file named 'name.zip' in the to_path.
# Delete the from_path when done.
# We want somewhat more control than shutil.make_archive gives us.
def move_dir_to_zip_file(from_path, to_path, name):
    global temp_dir, all_zipped
    files_zipped = 0
    zip_file = Path(temp_dir, name + '.zip')
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_STORED) as zipf:
        for root, dirs, files in os.walk(from_path):
            for file in files:
                files_zipped += 1
                file_to_zip = os.path.join(root, file)
                name_in_zip = os.path.relpath(os.path.join(root, file), os.path.join(from_path, '..'))
                zipf.write(file_to_zip, name_in_zip)
    # Only keep the zip file if we put anything into it.
    if files_zipped > 0:
        shutil.move(str(zip_file), to_path)
    else:
        os.remove(zip_file)
    # But get rid of the tree even if it was empty. Especially if it was empty.
    shutil.rmtree(from_path)
    all_zipped += files_zipped
    return files_zipped

# Move any user recording files to .zip archives.
def move_to_zips(processed_day_path, from_paths):
    global temp_dir
    files_zipped = 0
    for uf_name in UF_DIR_NAMES:
        if uf_name in from_paths and from_paths[uf_name] is not None:
            files_zipped += move_dir_to_zip_file(from_paths[uf_name], processed_day_path, uf_name)
    return files_zipped

# This is really the main function -- look for user recordings, and zip them into archives.
def process_directory(year, month, day, processed_day_path):
    # Get any YYYYyMMmDDdHHhMMmSSs directories. There can be more than one, but at most one should ever have
    # user recordings in it.
    ts_path = '{}y{}m{}d'.format(year, month, day)
    ts_dirs = [f.name for f in os.scandir(processed_day_path) if f.is_dir() and f.name.startswith(ts_path)]

    have_conflicts = False
    have_files = False
    counts = {x: 0 for x in UF_DIR_NAMES}
    from_paths = {x: None for x in UF_DIR_NAMES}

    # For each of the dispositions of user recordings that we care about...
    for uf_name in UF_DIR_NAMES:
        num_found = 0
        zip_found = False
        # Is there a directory in the daily directory?
        path = Path(processed_day_path, uf_name)
        if path.exists():
            from_paths[uf_name] = path
            num_found += 1
        # also check for .zip file
        if Path(processed_day_path, uf_name + '.zip').exists():
            zip_found = True
            print('Found existing {}.zip file in {}'.format(uf_name, processed_day_path))
        # Are there any directories in any of the timestamp directories?
        for ts_dir in ts_dirs:
            path = Path(processed_day_path, ts_dir, uf_name)
            if path.exists():
                from_paths[uf_name] = path
                num_found += 1
        # Accounting and checking for conflicts (multiple sets of one flavor)
        counts[uf_name] += num_found
        totals[uf_name] += num_found
        have_files |= num_found > 0
        if num_found > 1 or num_found == 1 and zip_found:
            have_conflicts = True
            print('Conflicting {} files in {}'.format(uf_name, processed_day_path))

    # If we're good to go, go.
    if have_files and not have_conflicts:
        start = timer()
        files_zipped = move_to_zips(processed_day_path, from_paths)
        end = timer()
        print('Zipped {} files in {:.2f} seconds'.format(files_zipped, end-start))


# Given a year, month, and a list of days, process the days.
def process_days(year, month, days):
    for day in days:
        path = Path(collected_data_path(year, month, day))
        if path.exists() and path.is_dir():
            print('Processing {}-{}-{}'.format(year, month, day))
            process_directory(year, month, day, path)


# Returns the list of days for the given year and month. May have been specified as an argument, or we may need
# to scan the collected-data-processed directory.
def find_days(year, month):
    global args
    day_list = args.day
    if day_list is None or len(day_list) == 0:
        with os.scandir(collected_data_path(year, month)) as it:
            dayDirs = [entry.name for entry in it if not entry.name.startswith('.') and entry.is_dir()]
        day_list = sorted(dayDirs)
    else:
        day_list = sorted(['{:0>2s}'.format(x) for x in day_list])
    return day_list


# Given a year and a list of months, find the days for each month, and process those days.
def process_months(year, months):
    for month in months:
        path = Path(collected_data_path(year, month))
        if path.exists() and path.is_dir():
            days = find_days(year, month)
            process_days(year, month, days)


# Returns the list of months for the given year. May have been specified as an argument, or we may need to scan the
# collected-data-processed directory.
def find_months(year):
    global args
    month_list = args.month
    if month_list is None or len(month_list) == 0:
        with os.scandir(collected_data_path(year)) as it:
            monthdirs = [entry.name for entry in it if not entry.name.startswith('.') and entry.is_dir()]
        month_list = sorted(monthdirs)
    else:
        month_list = sorted(['{:0>2s}'.format(x) for x in month_list])
    return month_list


# Given a list of years, find the months for each, and process those months.
def process_years(years):
    for year in years:
        months = find_months(year)
        process_months(year, months)


# Returns the list of years. May have been specified as an argument, or we may need to scan the
# collected-data-processed directory.
def find_years():
    global args
    year_list = args.year
    if year_list is None or len(year_list) == 0:
        with os.scandir(collected_data_path()) as it:
            yeardirs = [entry.name for entry in it if not entry.name.startswith('.') and entry.is_dir()]
        year_list = sorted(yeardirs)
    return year_list


def main():
    global args, dropbox, temp_dir, all_zipped
    arg_parser = argparse.ArgumentParser(description="Move UF files to .zips", usage=usage)
    arg_parser.add_argument('--year', nargs='*', help='Year(s) to process. Default: all years found.')
    arg_parser.add_argument('--month', nargs='*',
                            help='Month(s) to process (applies to every year). Default: all months found.')
    arg_parser.add_argument('--day', nargs='*',
                            help='Day(s) to process (applies to every month). Default: all days found.')
    arg_parser.add_argument('--dropbox', default='~/Dropbox', help='Dropbox directory (default is ~/Dropbox).')
    args = arg_parser.parse_args()
    dropbox = os.path.expanduser(args.dropbox)
    all_zipped = 0

    years = find_years()

    # This will delete the temporary directory when we're done.
    with tempfile.TemporaryDirectory() as temp:
        temp_dir = temp
        start = timer()
        process_years(years)
        end = timer()
        print('Zipped {} total files in {:0.2f} seconds'.format(all_zipped, end-start))

    print(totals)


if __name__ == "__main__":
    exit(main())
