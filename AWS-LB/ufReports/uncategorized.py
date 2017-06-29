import argparse
import csv
import datetime
import os
import re
import sys


usage = '''
  Parse a .csv file and count "General Feedback" vs other categories.
'''

# Characters we want to quote, in a .csv file
quotable = re.compile(".*[- ]")

# Filter for uncategorized category codes.
uncategorized_filter = re.compile("^9-0|^9$")
# Filter for categorized category codes
category_filter = re.compile("90-.*")
# List of categories we care about
categories = []
# Metadata names of the columns we care about
#columns = ["DC_TITLE",              # unique id of the feedback
#                "DC_PUBLISHER",     # Talking Book "serial number"
#                "DC_SOURCE",        # Village / community
#                "DC_LANGUAGE",      # Language
#                "DC_RELATION",      # Content id to which the feedback applies
#                "LB_CORRELATION_ID" # Correlation id, a short, unique string of letters
#                ]
# Friendlier names for the columns we care about
column_label = {"DC_TITLE":"feedback_id",               # unique id of the feedback
                "DC_PUBLISHER":"tb_id",                 # Talking Book "serial number"
                "DC_SOURCE":"community",                # Village / community
                "DC_LANGUAGE":"language",               # Language
                "DC_RELATION":"content_id",             # Content id to which the feedback applies
                "LB_CORRELATION_ID":"correlation_id"    # Correlation id, a short, unique string of letters
                }
columns = column_label.keys()
# Column index of the columns we care about
column_index = {}

# Make sure theres 1:1 between columns and column_label, disregarding order
def validate_columns():
    for col in columns:
        if not col in column_label:
            exit(1)
    for col in column_label:
        if not col in columns:
            exit(2)
    return True
ok = validate_columns()

# If a string contains a quotable character, quote the string
def enquote(string):
    tmp = str(string)
    if quotable.match(tmp) >= 0:
        tmp = '"'+tmp+'"'
    return tmp

# If any new categories were added (in the categories.csv extracted from the db), we have added them to the end of
# the list of categories from summary.csv. But if there are now new categories, we need to re-write the summary.csv
# file with a new header, including the new categories. While we're at it, re-write the existing records with zeros
# for the counts in the new categories.
def extendSummary(summary):
    # Read the old summary.csv from here
    oldf = open(summary, 'rb')
    old_csv = csv.reader(oldf, delimiter=',')

    # Write the new summary.csv to here
    if os.path.exists(summary+'.new'):
        os.remove(summary+'.new')
    newf = open(summary + '.new', 'wb')
    newf.write('date,other,uncategorized,categorized,' + ",".join(enquote(cat) for cat in categories) + '\n')
    for row in old_csv:
        # Skip the header, then copy the contents of every line.
        if old_csv.line_num > 1:
            # date,other,uncategorized,categorized,categories...
            values = [0 for ix in range(4+len(categories))]
            for ix in range(len(row)):
                values[ix] = row[ix]
            valstr = ','.join([str(v) for v in values])
            newf.write(valstr + '\n')

    # Rename files so we use the new one going forward
    newf.close()
    oldf.close()
    if os.path.exists(summary+'.old'):
        os.remove(summary+'.old')
    os.rename(summary, summary+'.old')
    os.rename(summary+'.new', summary)
    


# Read the list of categories from a file. Assumes category-code is first column. Because the order could possibly
# have changed, or because categories could possibly have been deleted, first read the existing list and order
# of categories from any summary.csv that we have. (If we have no summary.csv, then we have no existing list and/or
# ordering to worry about.)
def readCategories(filename, summary=None):
    global categories

    # If we're appending to a summaries file, first get the categories (in the existing order) already in use.
    if summary:
        if os.path.exists(summary):
            summaryf = open(summary, 'rb')
            summarycsv = csv.reader(summaryf, delimiter=',')
            row = summarycsv.next()
            # skip date, other, uncategorized, categorized
            categories = row[4:]
            summaryf.close()

    any_added = False
    file = open(filename, 'rb')
    csvfile = csv.reader(file, delimiter=',')
    for row in csvfile:
        if csvfile.line_num > 1:
            category = row[0]
            # If we want only feedback categories, add this: and (category_filter.match(category) or uncategorized_filter.match(category))
            if not category in categories:
                categories.append(category)
                any_added = True
    file.close()

    if any_added:
        extendSummary(summary)

# Read and process the metadata
def readFile(filename, summary=None, details=None):
    global categories, column_label, column_index
    other = 0
    categorized = 0
    uncategorized = 0
    sums = [0 for ix in range(len(categories))]
    file = open(filename, 'rb')
    csvfile = csv.reader(file, delimiter=',')
    for row in csvfile:
        if csvfile.line_num == 1:
            # First line, extract the column indices.
            category_ix = row.index("CATEGORIES")           # comma-separated list of category codes
            for col in columns:
                column_index[col] = row.index(col)
            # Print heading for the data lines
            if details:
                detailsf = open(details, 'wb')
                detailsf.write( ",".join(column_label[col] for col in columns) + "," + ",".join(enquote(cat) for cat in categories)  + '\n')
        else:
            # Subsequent lines: count the category(ies) for the feedback.
            buckets = [0 for ix in range(len(categories))]
            cats = row[category_ix]
            for col in cats.split(','):
                if len(col) > 0:
                    # "categorized" or "uncategorized"?
                    if uncategorized_filter.match(col):
                        uncategorized += 1
                    elif category_filter.match(col):
                        categorized += 1
                    else:
                        other += 1
                    while len(col) > 0 and col in categories:
                        # Count the category, and all the parents, like 90-12-2-3, 90-12-2, 90-12, 90
                        catix = categories.index(col)
                        buckets[catix] += 1
                        # Remove if we also want to count in parent categories.
                        break
                        dash = col.rfind('-')
                        if dash >= 0:
                            col = col[:dash]
                        else:
                            col = ''
            # Accumulate
            for ix in range(len(categories)):
                sums[ix] += buckets[ix];
            # Turn array of ints into array of strings, and join 'em.
            bucketstr = ",".join([str(b) for b in buckets])
            if details:
                detailsf.write( ",".join(enquote(row[column_index[col]]) for col in columns) + "," + bucketstr + '\n' )

    # Summary requested? Append to any existing file.
    if summary:
        if os.path.exists(summary):
            summaryf = open(summary, 'ab')
        else:
            summaryf = open(summary, 'wb')
            summaryf.write('date,other,uncategorized,categorized,' + ",".join(enquote(cat) for cat in categories) + '\n')
        t = datetime.datetime.utcnow().isoformat()
        sumstr = ','.join([str(s) for s in sums])
        summaryf.write( "{},{},{},{},".format(t, other, uncategorized, categorized) + sumstr + '\n')

    return (uncategorized, categorized, other)

def main():
    global args
    arg_parser = argparse.ArgumentParser(description="Analyze user feedback metadata", usage=usage)
    arg_parser.add_argument('data', help='File name, contains the metadata')
    arg_parser.add_argument('--categories', help='File with list of categories to include in output')
    arg_parser.add_argument('--details', help='File to receive per-message statistics')
    arg_parser.add_argument('--summary', help='File to receive summary statistics')
    args = arg_parser.parse_args()

    readCategories(args.categories, summary=args.summary)

    (u,c,o) = readFile(args.data, details=args.details, summary=args.summary)
    print "{0} uncategorized, {1} categorized ({2}%), {3} other".format(u,c,c/(c+u),o)

if __name__ == '__main__':
    sys.exit(main())
