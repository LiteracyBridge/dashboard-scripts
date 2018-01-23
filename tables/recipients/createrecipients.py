import argparse
import csv
import hashlib
import os
import sys

from os.path import expanduser

usage = '''
    Create and populate the recipients and recipients_map tables.
    
    Given a .csv file from the UNICEF-2 project specification spreadsheet, create the recipients data.
    And given a list of projects to scan (in Dropbox), look in the TB-Loaders/communities folder(s)
    to create recipients data for those projects.
    
    For every recipient entry created, create or update a recipient.id file in the community directory.
    
    Also create the recipients_map data, to map the old (and possibly multiple) "community" directories
    to recipientids.
    
    This is specialized to the state of the db on Nov 3, 2017. It skips the directory ACM-UNICEF-2, and
    it 'manually' adds aliases for 4 MEDA community/groups.
    
'''

# input recipients columns.
csv_columns_to_keep = ['Affiliate', 'Partner', 'Component', 'Country', 'Region', 'District', 'Community',
                       'Group Name', '# HH', '# TBs',
                       'Support Entity (i.e. community agent or group name)', 'Model', 'Language']
csv_columns_ix = {}

csv_columns_to_db_columns = {'Affiliate': 'affiliate', 'Partner': 'partner', 'Component': 'component',
                             'Country': 'country', 'Region': 'region', 'District': 'district',
                             'Community': 'communityname', 'Group Name': 'groupname', '# HH': 'numhouseholds',
                             '# TBs': 'numtbs', 'Support Entity (i.e. community agent or group name)': 'supportentity',
                             'Model': 'model', 'Language': 'language'}

# Columns to be exported to recipients.csv.
db_columns = ['recipientid', 'project', 'partner', 'communityname', 'groupname', 'affiliate', 'component', 'country', 'region',
              'district', 'numhouseholds', 'numtbs', 'supportentity', 'model', 'language', 'coordinates']
# Most columns are strings, but some are integers, and need to be formatted differently.
db_int_columns = ['numhouseholds', 'numtbs']

aliases_to_add = {'Nyemmawero-Suke': 'NYEMMAWERO -SUKE', 'POGBESONGTAA- PIINA#1': 'POG-BE SONTAA-PIINA NO 1',
                  'PRUDA - Male Gender Activist': 'PRUDA - MALE GENDA ARTERIES', 'SUNTAA-KOGRI': 'SUNTAA-KOGRI 1'
                  }

# validate column names against each other
for c2k in csv_columns_to_keep:
    if c2k not in csv_columns_to_db_columns:
        raise ValueError('Column \'{}\'in csv_columns_to_keep is missing from csv_columns_to_db_columns'.format(c2k))
for dbc in csv_columns_to_db_columns.values():
    if dbc not in db_columns:
        raise ValueError('Column \'{}\'in csv_columns_to_db_columns is missing from db_columns'.format(dbc))

communities_map = {}    # whatever information is in the communities table
recipients = []         # build list of recipients here
recipient_map = []      # correlation from project + directory => recipientid
ids = {}                # just to verify we don't get duplicates (< 1 in a million chance)

needHeader = True

dropbox = ''
project_name = ''
acm_name = ''
communities_dir = ''

# Given a string of hex digits, "fold" to the desired length by XORing the extra digits onto desired digits.
def fold(hex_digits, desired):
    hex_chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '0', 'a', 'b', 'c', 'd', 'e', 'f']
    digits = [int(ch, 16) for ch in hex_digits]
    for ix in range(desired, len(hex_digits)):
        digits[ix % desired] = digits[ix % desired] ^ digits[ix]
    hex_digits = [hex_chars[digit] for digit in digits]
    result = ''.join(hex_digits[0:desired])
    return result

# Given a string, compute a secure hash, and fold to a friendlier length.
def compute_id(string):
    str_hash = hashlib.sha1(string)
    digx = str_hash.hexdigest()
    id12 = fold(digx, 12)
    return id12

# If there is an existing recipient.id file in the directory, read and parse it.
def read_existing_id(directory):
    existing_id = {}
    existing_alias = []
    id_path = communities_dir + '/' + directory + '/recipient.id'
    if os.path.exists(id_path) and os.path.isfile(id_path):
        id_file = open(id_path, 'rb')
        for line in id_file:
            line = line.strip()
            if line.startswith('#'):
                continue
            (k, v) = line.split('=', 1)
            k = k.strip().lower()
            v = v.strip()
            if k == 'alias':
                existing_alias.append(v)
            else:
                existing_id[k] = v
        id_file.close()

    return existing_id, existing_alias


# Computes a recipientid from the communities directory (which contains the project name)
# and the community directory name (that is, the community directory name and full path of its parent).
# The only requirement of the id is that it is unique; a 16-digit hex number gives us a hash space
# of 2.8e14. There is 1 chance in a million of any hash collisions with 24000 communities, and a
# 0.1% chance of ANY collision with 750,000 communities. There's a 50% chance of ANY collision with
# 20,000,000 communities. See https://en.wikipedia.org/wiki/Birthday_problem.
def create_id(directory, community=None, group=None, language=None):
    if len(directory) == 0:
        return None
    community_path = communities_dir + '/' + directory
    id12 = compute_id(communities_dir + ' ' + directory)

    (existing_id, existing_alias) = read_existing_id(directory)

    new_id = {'project': project_name, 'recipientid': id12}
    if language and len(language) > 0:
        new_id['language'] = language.strip()
    if community and len(community) > 0:
        new_id['community'] = community.strip()
    if group and len(group) > 0:
        new_id['group'] = group.strip()
    # Copy any existing keys
    for k in existing_id.keys():
        if k not in new_id:
            sys.stdout.write('Adding alias {}={} for {}\n'.format(k, existing_id[k], directory))
            new_id[k] = existing_id[k]

    new_alias = [directory.upper()]
    if directory in aliases_to_add:
        sys.stdout.write('Adding alias {} for {} in project {}\n'.format(aliases_to_add[directory], directory, project_name))
        new_alias.append(aliases_to_add[directory])
    for a in existing_alias:
        a = a.upper()
        if a not in new_alias:
            sys.stdout.write('Adding alias {} for {}\n'.format(a, directory))
            new_alias.append(a)

    if 'recipientid' in existing_id and existing_id['recipientid'] != id12:
        sys.stdout.write(
            'Recipient id changed from {} to {} in {}!\n'.format(existing_id['recipientid'], id12, directory))

    for a in new_alias:
        recipient_map.append({'project': project_name, 'directory': a, 'recipientid': id12})

    same = True
    # Check whether anything removed or changed
    for k in existing_id.keys():
        if k not in new_id or existing_id[k] != new_id[k]:
            same = False
            break
    # Check whether anything added
    for k in new_id.keys():
        if k not in existing_id:
            same = False
            break
    if same:
        return id12
    sys.stdout.write('Creating new recipient.id file for {}\n'.format(directory))

    if os.path.exists(community_path):
        if os.path.exists(community_path + '/community.id'):
            os.remove(community_path + '/community.id')
        idpath = community_path + '/recipient.id'
        idfile = open(idpath, 'wb')
        for k in new_id:
            idfile.write('{}={}\n'.format(k, new_id[k]))
        for a in new_alias:
            idfile.write('alias={}\n'.format(a))
        idfile.close()

    return id12


# Looks for the language associated with a community. If the community has multiple languages, arbitrarily choose one.
def look_for_language(directory):
    if len(directory) == 0:
        return None
    languages_path = communities_dir + '/' + directory + '/languages'
    if os.path.exists(languages_path):
        languages = [l for l in os.listdir(languages_path) if
                     (not l.startswith('.')) and os.path.isdir(languages_path + '/' + l)]
        if len(languages) == 1:
            return languages[0]
        sys.stdout.write('Multiple languages found in {}, \'{}\': {}\n'.format(project_name, directory, languages))
        return languages[0]


# Read and process the UNICEF-2 project specification recipients csv file.
def read_unicef2_recipients(filename):
    set_current_project('unicef-2')
    recipients_file = open(filename, 'rU')
    csvfile = csv.reader(recipients_file, delimiter=',')
    directory_ix = group_ix = language_ix = 0  # stupid, but quiets lint.

    for row in csvfile:
        if csvfile.line_num == 1:
            # First line, extract the column indices. Column names as in tbdataoperations.
            directory_ix = row.index('Directory Name')
            community_ix = row.index('Community')
            group_ix = row.index('Group Name')
            language_ix = row.index('Language')
            for column_name in csv_columns_to_keep:
                csv_columns_ix[column_name] = row.index(column_name)
        else:
            # Subsequent lines: reformat the data as needed.
            out_row = {}

            recipientid = create_id(row[directory_ix], row[community_ix], row[group_ix], row[language_ix])
            if recipientid and len(recipientid) > 0:
                if recipientid in ids:
                    # Yikes! collision
                    sys.stdout.write('Collision in ids: {}, community: {}'.format(recipientid, row[community_ix]))
                    raise ValueError("oh noes!")
                ids[recipientid] = row[community_ix]
            else:
                continue
            out_row['recipientid'] = recipientid
            out_row['project'] = project_name
            out_row['coordinates'] = ''

            for column_name in csv_columns_to_keep:
                db_column = csv_columns_to_db_columns[column_name]
                out_row[db_column] = row[csv_columns_ix[column_name]].strip()

            recipients.append(out_row)


# Sets global variables for project name.
def set_current_project(name):
    global dropbox, communities_dir, acm_name, project_name
    acm_name = cannonical_acm_directory(name)
    project_name = cannonical_project_name(name)
    communities_dir = dropbox + '/' + acm_name + '/TB-Loaders/communities'


# Scans the communities directory for the project, and computes the id for every directory found.
def scan_project_for_recipients():
    dirnames = os.listdir(communities_dir)

    for dirname in dirnames:
        if os.path.isdir(communities_dir + '/' + dirname):
            out_row = {}
            recipientid = create_id(dirname)
            if recipientid and len(recipientid) > 0:
                if recipientid in ids:
                    # Yikes! collision
                    sys.stdout.write(
                        'Collision in ids: {}, directory: {}/{}'.format(recipientid, ids[recipientid].directory,
                                                                        dirname))
                    raise ValueError("oh noes!")
            else:
                continue
            for column_name in db_columns:
                out_row[column_name] = ''
            if dirname.upper() in communities_map:
                comm_info = communities_map[dirname.upper()]
                if comm_info['tbs'] != '' and comm_info['tbs'] != '0':
                    out_row['numtbs'] = comm_info['tbs']
                if comm_info['households'] != '' and comm_info['households'] != '0':
                    out_row['numhouseholds'] = comm_info['households']
                if comm_info['lat'] != '' and comm_info['long'] != '' and comm_info['lat'] != '0' and comm_info['long'] != '0':
                    out_row['coordinates']='"({},{})"'.format(comm_info['lat'], comm_info['long'])
                if comm_info['district'] != '':
                    out_row['district'] = comm_info['district']
            out_row['directory'] = dirname
            out_row['communityname'] = dirname
            out_row['recipientid'] = recipientid
            out_row['partner'] = project_name
            out_row['project'] = project_name
            out_row['language'] = look_for_language(dirname)
            ids[recipientid] = out_row
            recipients.append(out_row)

    sys.stdout.write('{} community directories in project {}\n'.format(len(dirnames), project_name))


# Looks in the dropbox directory for projects (to search for communities). Scans directories for projects
# in the accepts_list.
def scan_for_projects(accepts_list):
    global dropbox
    accepts_list = [cannonical_acm_directory(l) for l in accepts_list]
    dbx_names = os.listdir(dropbox)
    for dbx in dbx_names:
        if dbx in accepts_list:
            if dbx == 'ACM-UNICEF-2':
                sys.stdout.write('Skipping directory ACM-UNICEF-2\n')
                continue
            set_current_project(dbx)
            scan_project_for_recipients()


# Formats the values for an entry in the recipients.csv file
def format_recip(recip):
    vals = []
    for c in db_columns:
        v = str(recip[c])
        if c in db_int_columns:
            if v == '':
                v = '0'
            vals.append(v)
        elif c == 'coordinates':
            vals.append(v)
        else:
            vals.append('"' + v + '"')
    return vals


# Writes the deployments data that has been collected, to a .csv file appropriate to import to the tbsdeployed table.
def write_recipients_data(output_name, map_name):
    global needHeader
    columns = db_columns
    out_file = open(output_name, 'wb')
    map_columns = ['project', 'directory', 'recipientid']
    map_file = open(map_name, 'wb')

    if needHeader:
        # csv header
        h = ",".join(columns)
        out_file.write(h)
        out_file.write('\n')
        h = ','.join(map_columns)
        map_file.write(h)
        map_file.write('\n')
        needHeader = False

    for recip in recipients:
        vals = format_recip(recip)
        out_file.write(','.join(vals))
        out_file.write('\n')
    for recip in recipient_map:
        vals = [recip['project'], recip['directory'], recip['recipientid']]
        map_file.write(','.join(vals))
        map_file.write('\n')

    out_file.close()
    map_file.close()


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


# Given a project or acm name, return the project name (no ACM-)
def cannonical_project_name(acm):
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


# Given a project or acm name, return the acm name (with ACM-)
def cannonical_acm_directory(acm):
    acm = acm.upper()
    if not acm.startswith('ACM-'):
        acm = 'ACM-' + acm
    return acm


def parse_communities(filename):
    communities_file = open(filename, 'rb')
    csvfile = csv.reader(communities_file, delimiter=',')
    columns = {}
    name_ix = 0

    for row in csvfile:
        if csvfile.line_num == 1:
            # First line, extract the column indices. Column names as in communities.
            for i in range(len(row)):
                columns[row[i]] = i
            name_ix = columns['communityname']
        else:
            # Subsequent lines: reformat the data as needed.
            out_row = {}
            name = row[name_ix]
            for col in columns:
                out_row[col] = row[columns[col]]
            communities_map[name] = out_row


def main():
    global needHeader, dropbox
    arg_parser = argparse.ArgumentParser(description="Extract deployments data", usage=usage)
    arg_parser.add_argument('data', nargs='?', help='Optional file name, contains the input recipients.csv data.')
    arg_parser.add_argument('--no-header', '--noheader', action='store_false', dest='header')
    arg_parser.add_argument('--output', default='recipients.csv', help='Output file name (default is recipients.csv).')
    arg_parser.add_argument('--map', default='recipients_map.csv',
                            help='Map file name (default is recipients_map.csv).')
    arg_parser.add_argument('--dropbox', default='~/Dropbox', help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('--communities', nargs='?', help='Optional extract from communities table.')
    arg_parser.add_argument('--projects', nargs='*',
                            help='List of projects to scan, or \'@filename\' containing list of projects.')
    args = arg_parser.parse_args()

    needHeader = args.header
    dropbox = expanduser(args.dropbox)
    if args.communities:
        parse_communities(args.communities)

    if args.projects:
        projects = expand_arg_list(args.projects)
        scan_for_projects(projects)
    if args.data:
        read_unicef2_recipients(args.data)

    write_recipients_data(args.output, args.map)


if __name__ == '__main__':
    sys.exit(main())
