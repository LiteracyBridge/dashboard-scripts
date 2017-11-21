import argparse
import csv
from datetime import datetime
import os
import sys

usage = '''Extract deployments data from tbdataoperations.csv and multiple deploymentsAll.log.
Create a tbsdeployed.csv, suitable for insertion into a database.'''

# tbsdeployed table column names, in order
columns = ['talkingbookid', 'recipientid', 'deployedtimestamp', 'project', 'deployment', 'contentpackage', 'firmware',
           'location', 'coordinates', 'username', 'tbcdid', 'action', 'newsn', 'testing']

deployments = []
outFile = None
needHeader = True
UNKNOWN = 'Unknown'
non_specifics = {}

recipient_map = {}


def parse_map_file(filename):
    global recipient_map
    map_file = open(filename, 'rb')
    csvfile = csv.reader(map_file, delimiter=',')
    proj_ix = directory_ix = recip_ix = 0

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
    proj = proj.upper()
    directory = directory.upper()
    if proj not in recipient_map:
        sys.stdout.write('Project {} is not in recipient map.\n'.format(proj))
        return None
    if directory not in recipient_map[proj]:
        sys.stdout.write('directory {} is not in map for {}.\n'.format(directory, proj))
        return None
    return recipient_map[proj][directory]


# Read and process an extract of tbdataoperations,
def read_tbdataactions(filename):
    global deployments
    tbdata_file = open(filename, 'rb')
    tbdata_csv = csv.reader(tbdata_file, delimiter=',')
    # These two lines are stupid, but quiet lint.
    outsn_ix = updatedatetime_ix = project_ix = deployment_ix = package_ix = community_ix = 0
    firmware_ix = syncdir_ix = location_ix = action_ix = 0

    for row in tbdata_csv:
        if tbdata_csv.line_num == 1:
            # First line, extract the column indices. Column names as in tbdataoperations.
            outsn_ix = row.index("outsn")
            updatedatetime_ix = row.index("updatedatetime")
            project_ix = row.index("project")
            deployment_ix = row.index("outdeployment")
            package_ix = row.index("outimage")
            community_ix = row.index("outcommunity")
            firmware_ix = row.index("outfwrev")
            syncdir_ix = row.index("outsyncdir")
            location_ix = row.index("location")
            action_ix = row.index("action")
        else:
            # Subsequent lines: reformat the data as needed.
            sn = row[outsn_ix]
            if len(sn) == 0 or sn == '-- TO BE ASSIGNED --':
                sn = UNKNOWN
            community = row[community_ix]
            if community.upper() == 'NON-SPECIFIC':
                project = row[project_ix]
                if project not in non_specifics:
                    non_specifics[project] = 1
                else:
                    non_specifics[project] = non_specifics[project] + 1
                continue
            out_row = {'talkingbookid': sn}
            ts = row[updatedatetime_ix]
            year = int(ts[0:4])
            month = int(ts[5:7])
            day = int(ts[8:10])
            hour = int(ts[11:13])
            minute = int(ts[14:16])
            second = int(ts[17:19])
            deployment_ts = str(datetime(year, month, day, hour, minute, second))
            out_row['deployedtimestamp'] = deployment_ts
            out_row['project'] = row[project_ix]
            out_row['deployment'] = row[deployment_ix]
            out_row['contentpackage'] = row[package_ix]

            out_row['community'] = row[community_ix]
            fw = row[firmware_ix]
            if len(fw) == 0:
                fw = UNKNOWN
            out_row['firmware'] = fw
            out_row['location'] = row[location_ix]
            out_row['coordinates'] = ''
            out_row['username'] = UNKNOWN

            out_row['recipientid'] = lookup_recipient(row[project_ix], row[community_ix])

            syncdir = row[syncdir_ix]
            tbcdid = UNKNOWN
            if len(syncdir) > 0:
                tbcdid = syncdir[-4:]
            out_row['tbcdid'] = tbcdid

            out_row['action'] = row[action_ix]
            out_row['newsn'] = 'f'
            out_row['testing'] = 'f'

            deployments.append(out_row)


# Read an deploymentsAll.log file; comma-separated key:value pairs.
def read_deployments(filename):
    global deployments
    deployments_file = open(filename, 'rb')
    found = 0
    # Map of fields we want to keep to the names by which we wish to keep them.
    keepers = {'sn': 'talkingbookid', 'timestamp': 'deployedtimestamp', 'project': 'project',
               'deployment': 'deployment', 'package': 'contentpackage', 'recipientid': 'recipientid',
               'community': 'community',
               'firmware': 'firmware', 'location': 'location', 'coordinates': 'coordinates', 'username': 'username',
               'tbcdid': 'tbcdid', 'action': 'action', 'newsn': 'newsn', 'testing': 'testing'}
    optionals = {'newsn': 'f', 'testing': 'f', 'coordinates': ''}

    lines = [line.strip() for line in deployments_file]
    for line in lines:
        if len(line) < 1:
            continue
        parts = line.split(',')
        if len(parts) < 3:
            continue
        data = {}
        for ix in range(2, len(parts)):
            (k, v) = parts[ix].split(':', 1)
            if k in keepers:
                data[keepers[k]] = v
                found += 1
        for o in optionals:
            if o not in data:
                data[o] = optionals[o]
        if data['community'].upper() == 'NON-SPECIFIC':
            project = data['project']
            if project not in non_specifics:
                non_specifics[project] = 1
            else:
                non_specifics[project] = non_specifics[project] + 1
            continue
        if 'recipientid' not in data:
            data['recipientid'] = lookup_recipient(data['project'], data['community'])
        if all(x in data for x in columns):
            deployments.append(data)


def read_deployments_list(names):
    for name in names:
        read_deployments(name)


# Writes the deployments data that has been collected, to a .csv file appropriate to import to the tbsdeployed table.
def write_tbsdeployed(output_name):
    global needHeader, outFile
    if not outFile:
        outFile = open(output_name, 'wb')

    if needHeader:
        # csv header
        h = ",".join(columns)

        outFile.write(h)
        outFile.write('\n')
        needHeader = False

    for depl in deployments:
        vals = [depl[k] for k in columns]
        outFile.write(','.join(vals))
        outFile.write('\n')


# Given the command line args, determine the output file name
def make_output_name(args):
    if args.output:
        return args.output
    (root, ext) = os.path.splitext(args.data)
    return root + '-out.csv'


def main():
    global needHeader, outFile
    arg_parser = argparse.ArgumentParser(description="Extract deployments data", usage=usage)
    arg_parser.add_argument('data', nargs='*', help='File name, contains the deploymentsAll.log data.')
    arg_parser.add_argument('--no-header', '--noheader', action='store_false', dest='header')
    arg_parser.add_argument('--output', default='out.csv', help='Output file name (default is inputname-out.csv)')
    arg_parser.add_argument('--map', required=True, help='Required csv file of project+directory => recipientid.')
    arg_parser.add_argument('--tbdata', help='Parse data extracted from tbdataoperations table.')
    args = arg_parser.parse_args()

    needHeader = args.header

    parse_map_file(args.map)

    output_name = make_output_name(args)
    if args.tbdata:
        read_tbdataactions(args.tbdata)
    read_deployments_list(args.data)
    write_tbsdeployed(output_name)

    if len(non_specifics) > 0:
        for p in non_specifics.keys():
            sys.stdout.write('Project {} had {} non-specific TB deployments.\n'.format(p, non_specifics[p]))

    if outFile:
        outFile.close()


if __name__ == '__main__':
    sys.exit(main())
