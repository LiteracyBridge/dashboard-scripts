#!/usr/bin/env python3

import argparse
import csv
import sys

usage = '''Read one or more key:value files, and produce a CSV file.
  Useful options:
    --noheader      As it sounds, don't print a header.
    --2pass         Gather the names in one pass, convert the data in another.
                    Useful if the volume of data is huge.
    --columns       Specify the columns to be output. Use '+' to also discover
                    columns. The default is like 'timestamp operation +'.
    --output        Names the output file. Otherwise stdout.
    --map           Specify a recipient_map file, mapping project+community to recipientid. If
                    specified, will fill missing recipientid from existing project+community.
'''

input_data = []
column_names = []
discover_columns = True
recipient_map = None
proj_warnings = {}
comm_warnings = {}
overrides_used = {}

recipient_overrides = {

    "MEDA": {
        "KANDANBAMBO B - CHARINGU": "KANDANBAMBO B - CHARIGU",
        "KANYIRI - CHARINGU": "KANYIRI - CHARIGU",
        "KATIMEN-LINYE - FIAN": "KATIMEN - LINYE - FIAN",
        "MWINISUMBO - KULKPONG": "MWINISUMBU - KULKPONG",
        "POGBETIETAA - BUNAA": "POGBATIETAA - BUNAA",
        "POG-OLO - BULEN": "POG - OLO - BULEN",
        "SUNGTAAMAALITAA A - BULEN": "SUNTAAMAALITAA - A - BULEN",
        "TIETAA - IRI - TAFALI": "TIETAA-IRI - TAFALI"
    },
    "UWR": {
        "MOTHER TO MOTHER SUPPORT TAMPAALA": "TAMPAALA-JIRAPA",  # this is a bit of a stretch, going on Tampaala.
        "KABERE-YOUTH TAMPAALA": "TAMPAALA-JIRAPA"
    },
    "CARE": {
        "KPATUA NO": "KPATUA NO 1",
        "SONGO ANONGTAABA": "SONGO ANONGTAABA 1"
    }
}

def load_recipient_map(filename):
    global recipient_map
    map_file = open(filename, newline='')
    csvfile = csv.reader(map_file, delimiter=',')
    proj_ix = directory_ix = recip_ix = 0
    recipient_map = {}

    for row in csvfile:
        if csvfile.line_num == 1:
            proj_ix = row.index('project')
            directory_ix = row.index('directory')
            recip_ix = row.index('recipientid')
        else:
            proj = row[proj_ix].upper()
            directory = row[directory_ix].upper()
            recipientid = row[recip_ix]
            if proj not in recipient_map:
                recipient_map[proj] = {}
            recipient_map[proj][directory] = recipientid


def lookup_recipient(proj, directory):
    global recipient_map, recipient_overrides, proj_warnings, comm_warnings, overrides_used
    if not recipient_map:
        return None
    proj = proj.upper().strip('"')
    directory = directory.upper().strip('"')
    if proj in recipient_overrides and directory in recipient_overrides[proj]:
        override = recipient_overrides[proj][directory]
        if proj not in overrides_used or directory not in overrides_used[proj]:
            if proj not in overrides_used: overrides_used[proj] = {}
            overrides_used[proj][directory] = True
            sys.stdout.write('Using {} as override for {}.\n'.format(override, directory))
        directory = override
    if proj not in recipient_map:
        if proj not in proj_warnings:
            proj_warnings[proj] = True
            sys.stdout.write('Project {} is not in recipient map.\n'.format(proj))
        return None
    if directory not in recipient_map[proj]:
        if proj not in comm_warnings or directory not in comm_warnings[proj]:
            if proj not in comm_warnings: comm_warnings[proj] = {}
            comm_warnings[proj][directory] = True
            sys.stdout.write('directory {} is not in map for {}.\n'.format(directory, proj))
        return None
    return recipient_map[proj][directory]

# Read a key:value file; comma-separated key:value pairs.
def read_input(kv_file, gather=True, process=True):
    global column_names, input_data, discover_columns

    lines = [line.strip() for line in kv_file]
    for line in lines:
        if len(line) < 1:
            continue
        #  First two fields are timestamp,operation, then k:v pairs separated by commas.
        parts = line.split(',')
        if len(parts) < 3:
            continue

        if gather and discover_columns:
            for ix in range(2, len(parts)):
                bits = parts[ix].split(':', 1)
                if not bits[0] in column_names:
                    column_names.append(bits[0])

        if process:
            # Map into a csv line.
            data = {}
            data['timestamp'] = parts[0]
            data['operation'] = parts[1]
            for ix in range(2, len(parts)):
                bits = parts[ix].split(':', 1)
                data[bits[0]] = bits[1] if len(bits)>1 else 'True'

            if 'recipientid' not in data and 'project' in data and 'community' in data:
                recipientid = lookup_recipient(data['project'], data['community'])
                if recipientid:
                    data['recipientid'] = recipientid

            if gather:
                # If gathering and processing in one pass, accumulate data until the end.
                input_data.append(data)
            else:
                write_line(data)


def process_inputs_list(names, two_pass):
    if two_pass:
        for name in names:
            kv_file = open(name, 'r')
            read_input(kv_file, process=False)
        for name in names:
            kv_file = open(name, 'r')
            read_input(kv_file, gather=False)
    else:
        for name in names:
            kv_file = open(name, 'r')
            read_input(kv_file)
        write_all()

# Write one line of data to the output. Write the header if (still) required.
def write_line(data):
    global column_names, outfile, needHeader
    if needHeader:
        # csv header
        h = ",".join(column_names)
        outfile.write(h)
        outfile.write('\n')
        needHeader = False

    # The data; use an empty string for missing data.
    vals = [data[k] if k in data else '' for k in column_names]
    outfile.write(','.join(vals))
    outfile.write('\n')

# Writes the deployments data that has been collected, to a .csv file appropriate to import to the tbsdeployed table.
def write_all():
    global input_data
    for data in input_data:
        write_line(data)


# Parse the the '--arg x @y z' argument. Expands any @filename args.
def expand_arg_list(arg_list):
    expanded_list = []
    for arg in arg_list:
        # If it starts with @, it is a file containing a list.
        if arg.startswith('@'):
            arg_file = open(arg[1:], 'rb')
            for line in arg_file:
                line = line.strip()
                if line.startswith('#'):
                    continue
                expanded_list.append(line)
        else:
            expanded_list.append(arg)
    return expanded_list

# Set the column_names from the columns command line argument
def set_columns(columns):
    global column_names, discover_columns
    if columns:
        columns_spec = expand_arg_list(columns)
        if len(columns_spec) > 0:
            column_names = [col for col in columns_spec if col != '+']
            discover_columns = '+' in columns_spec
    else:
        column_names = ['timestamp', 'operation']

def main():
    global needHeader, outfile
    arg_parser = argparse.ArgumentParser(description="Extract deployments data", usage=usage)
    arg_parser.add_argument('data', nargs='*', help='File name(s), contain(s) the key:value data.')
    arg_parser.add_argument('--no-header', '--noheader', action='store_false', dest='header')
    arg_parser.add_argument('--columns', nargs='*', help='List of column names, in order, or @filename with list')
    arg_parser.add_argument('--output', help='Output file (default is stdout)')
    arg_parser.add_argument('--2pass', action='store_true', default=False, dest='two_pass', help='Two pass operation; don\'t load all data into memory.')
    arg_parser.add_argument('--map', help='Optional csv file of project+community => recipientid.')
    # arg_parser.add_argument('--tbdata', help='Parse data extracted from tbdataoperations table.')
    args = arg_parser.parse_args()

    set_columns(args.columns)

    if args.map:
        load_recipient_map(args.map)

    outfile = sys.stdout
    if args.output:
        outfile = open(args.output, 'w')

    needHeader = args.header

    process_inputs_list(args.data, args.two_pass)

    outfile.close()

if __name__ == '__main__':
    sys.exit(main())
