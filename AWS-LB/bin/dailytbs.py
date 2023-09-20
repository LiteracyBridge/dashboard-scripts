#!/usr/bin/env python3

import argparse
import csv
import json
import os
import sys

usage = '''
Partitions the daily tbsdebployed.csv files into corresponding project directories.

Input files are assumed to exist in a directory structure like
  Dropbox/collected-data-processed/year/month/day
and the output files are placed into a directory structure like
  Dropbox/DashboardReports/project/tbsdeployed/year/month/day

Unless the --nodates option is set, the year, month, and day are determined by the
directory structure.  

The --scan option takes 1 or more directories to scan for tbsdeployed.csv files. All such 
files found are partitioned.

As files are partitioned, a dailytbs.json file is maintained in the DashboardReports/project
directory. This file has and entry for every year with data, containing entries for every
month with data, containing a list of days with data.
'''

# If either of these already exists, use it. If neither, create the last one.
TBSDAILY_FILE = ['dailytbs.json', 'tbsdaily.json']

# Will be a {proj1, proj2, ...} if only some projects desired.
projects_limited_to = None
files_opened = {}
counters = {}

# Parse the the '--arg x @y z' argument. Expands any @filename args. Doesn't support nested '@'.
def expand_arg_list(arg_list):
    expanded_list = []
    for arg in arg_list:
        # If it starts with @, it is a file containing a list.
        if arg.startswith('@'):
            arg_file = open(arg[1:], 'r')
            for line in arg_file:
                line = line.strip()
                if line.startswith('#'):
                    continue
                expanded_list.append(line)
        else:
            expanded_list.append(arg)
    return expanded_list

# Adds 1 to the counter of TBs installed for the project, year, month, date
def count_row(project):
    global year, month, day, counters
    key = '{},{}-{}-{}'.format(project.upper(), year, month, day)
    if key not in counters:
        counters[key] = 0
    counters[key] = counters[key] + 1

def print_counts():
    global counters
    print('project,date,count')
    for key in sorted(counters.keys()):
        print('{},{}'.format(key, counters[key]))

# Adds the day to the dailytbs.json file. Creates the file, adds year and month as necessary.
def record_date_in_dailies_list(project):
    global year, month, day
    dailies = {}
    changed = False
    for fn in TBSDAILY_FILE:
        dailies_path = '{}/DashboardReports/{}/{}'.format(dropbox, project.upper(), fn)
        if os.path.exists(dailies_path):
            file = open(dailies_path, 'r')
            dailies = json.load(file)
            file.close()
            break

    if not year in dailies:
        dailies[year] = {}
        changed = True
    if not month in dailies[year]:
        dailies[year][month] = []
        changed = True
    if not day in dailies[year][month]:
        dailies[year][month].append(day)
        changed = True

    if changed:
        with open(dailies_path, 'w') as file:
            json.dump(dailies, file, indent=2, sort_keys=True)

# Closes all the open output files.
def close_outputs():
    global files_opened
    for _, file in files_opened.items():
        file.close()
    files_opened = {}

# Given a project, and assuming that the global year, month, and day variables are set, determine the
# file to receive lines pertaining to the project. Creates the file if it isn't already open.
# If the option args.header is set, writes a first line populated with column names.
def file_for_project(project):
    global args, dropbox, columns, year, month, day, files_opened
    if not args.output_by_dates:
        path = args.output
    else:
        path = '{}/DashboardReports/{}/{}/{}/{}/{}'.format(dropbox, project.upper(), year, month, day, args.output)
    if not path in files_opened:
        if args.output_by_dates:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            record_date_in_dailies_list(project)
        file = open(path, 'w')
        if args.header:
            file.write(','.join(columns)+'\n')
        files_opened[path] = file

    return files_opened[path]

# Distribute one row of the csv file, if the row's project is included in the output.
def distribute_row(project, row):
    if not projects_limited_to or project in projects_limited_to:
        file = file_for_project(project)
        file.write(','.join(row)+'\n')
        count_row(project)

# Read one csv file (likely a tbsdeployed.csv file), and distribute the lines according to the PROJECT column.
def read_input(filename):
    global columns, year, month, day

    absname = os.path.abspath(filename)
    daypath, fname = os.path.split(absname)
    monthpath, day = os.path.split(daypath)
    yearpath, month = os.path.split(monthpath)
    _, year = os.path.split(yearpath)

    tbsdeployed_file = open(filename, 'r')
    csvfile = csv.reader(tbsdeployed_file, delimiter=',')
    columns = {}
    project_ix = 0

    for row in csvfile:
        if csvfile.line_num == 1:
            # First line, extract the column indices.
            for i in range(len(row)):
                columns[row[i]] = i
            project_ix = columns['project']
        else:
            # Subsequent lines: partition by projects
            project = row[project_ix]
            distribute_row(project, row)

# Process a list of one or more tbsdeployed.csv files. Invoke read_input on each file.
def read_inputs(filenames):
    global dropbox
    for filename in filenames:
        if not filename.startswith('/'):
            filename = dropbox + '/' + filename
        read_input(filename)
        close_outputs()

# Scan a list of paths for files named 'tbsdeployed.csv'. Recurse into directories. Return a list
# of all such files found.
def scan_files(paths):
    global dropbox
    result = []
    dirs = []
    for path in paths:
        if not path.startswith('/'):
            path = dropbox + '/' + path
        files = os.listdir(path)
        for file in files:
            if os.path.isdir(path+'/'+file):
                dirs.append(path+'/'+file)
            elif file.lower()=='tbsdeployed.csv':
                result.append(path+'/'+file)
    if len(dirs) > 0:
        result.extend(scan_files(dirs))
    return result

def main():
    global projects_limited_to, dropbox, args
    arg_parser = argparse.ArgumentParser(description="Extract deployments data", usage=usage)
    arg_parser.add_argument('files', nargs='*', help='Input file names, containing the aggregated tbsdeployed.csv data.')
    arg_parser.add_argument('--no-header', '--noheader', action='store_false', dest='header', help='Do not write headers to output .csv files.')
    arg_parser.add_argument('--output', default='tbsdeployed.csv', help='Output file name (default is tbsdeployed.csv).')
    arg_parser.add_argument('--nodates', action='store_false', dest='output_by_dates', help="Do not assume year/month/day file structure.")
    arg_parser.add_argument('--scan', nargs='*', help="Scan the given paths for 'tbsdeployed.csv' files.")
    arg_parser.add_argument('--dropbox', default='~/Dropbox', help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('--projects', nargs='*',
                            help='List of projects or \'@filename\' containing list. Projects limited to named projects. By default process all.')
    args = arg_parser.parse_args()

    dropbox = os.path.expanduser(args.dropbox)
    if args.projects:
        projects_limited_to = set(expand_arg_list(args.projects))

    files = args.files
    if args.scan:
        files.extend(scan_files(args.scan))

    if len(files) == 0:
        print('No input files', file=sys.stderr)
        arg_parser.print_help()
        sys.exit(1)

    read_inputs(files)
    print_counts()

if __name__ == '__main__':
    sys.exit(main())
