import argparse
import csv
import datetime
import os
import re
import sys

usage = '''
  Parse one or more files of CSVDatabaseExporter output, and count categorized feedback categories.
  The file consists of one line per message; the "CATEGORIES" column may contain multiple comma separated
  categories, each of which is counted (most have only one). For 'Categorized Feedback' messages, use the  
  leaf node as a bucket, ie, "Endorsement", "Question", etc. For other messages, those that have not been 
  categorized, use "General" for the bucket.
  
  Note that CSVDatabaseExporter should be run with the "--categoryfullnames" ("-n") flag so that we get
  more than just the leaf node name.
'''

# Characters we want to quote, in a .csv file
quotable = re.compile(".*[,\'\"]")

# bucketized counts of feedback
counted_feedback = {}

buckets = ['endorsement', 'suggestion', 'complaint', 'question', 'comment', 'general']

# If a string contains a quotable character, quote the string
def enquote(string):
    tmp = str(string)
    if quotable.match(tmp) >= 0:
        tmp = string.replace('"', '')
        tmp = '"' + tmp + '"'
    return tmp

# Read and process the one file of CSVDatabaseExporter output. Accumulate the results into counted_feedback
def countFeedback(filename):
    global categories, column_label, column_index
    file = open(filename, 'rb')
    csvfile = csv.reader(file, delimiter=',')

    for row in csvfile:
        if csvfile.line_num == 1:
            # First line, extract the column indices.
            category_ix = row.index("CATEGORIES")           # comma-separated list of category codes
        else:
            # Subsequent lines: count the category(ies) for the feedback.
            cat_list = row[category_ix]
            for cat in cat_list.split(','):
                cat = cat.strip()
                if len(cat) > 0:
                    catParts = cat.split(':')
                    if catParts[0].lower() == 'categorized feedback':
                        catParts = catParts[1:]
                    bucket = catParts[-1].lower()
                    if bucket in buckets:
                        counted_cat = ':'.join(catParts[0:-1])
                    else:
                        catParts.insert(0, 'XTRA')
                        counted_cat = ':'.join(catParts)
                        bucket = 'general'

                    if not counted_cat in counted_feedback:
                        counted_feedback[counted_cat] = {}
                    if not bucket in counted_feedback[counted_cat]:
                        counted_feedback[counted_cat][bucket] = 0
                    counted_feedback[counted_cat][bucket] += 1



def countFeedback_x(inputName):
    lines = [line.strip() for line in open(inputName, 'rb')]
    for line in lines:
        catParts = line.split(':')
        if catParts[0].lower() == 'categorized feedback':
            catParts = catParts[1:]
        bucket = catParts[-1].lower()
        if bucket in buckets:
            cat = ':'.join(catParts[0:-1])
        else:
            catParts.insert(0,'XTRA')
            cat = ':'.join(catParts)
            bucket = 'general'

        if not cat in counted_feedback:
            counted_feedback[cat] = {}
        if not bucket in counted_feedback[cat]:
            counted_feedback[cat][bucket] = 0
        counted_feedback[cat][bucket] += 1

# Produce the output report
def produceResult(outputName, makeHeader):
    outFile = open(outputName, 'wb')

    if makeHeader:
        # csv header
        outFile.write('category')
        for b in buckets:
            outFile.write(","+b)
        outFile.write('\n')

    # list of categories. sort, to group the sub-categories together
    cats = counted_feedback.keys()
    cats.sort()

    for cat in cats:
        outFile.write(enquote(cat))
        for b in buckets:
            if b in counted_feedback[cat]:
                outFile.write(',{}'.format(counted_feedback[cat][b]))
            else:
                outFile.write(',')
        outFile.write('\n')
    outFile.close()

# Iterate over a list of filenames, processing each one
def countFeedbacks(names):
    for name in names:
        countFeedback(name)

# Given the command line args, determine the output file name
def makeOutputName(args):
    if args.output != None:
        return args.output
    (root, ext) = os.path.splitext(args.data[0])
    return root + '-out.csv'

def main():
    global args
    arg_parser = argparse.ArgumentParser(description="Analyze user feedback", usage=usage)
    arg_parser.add_argument('data', nargs='+', help='File name, contains the message categories.')
    arg_parser.add_argument('--no-header', '--noheader', action='store_false', dest='header')
    arg_parser.add_argument('--output', help='Output file name (default is inputname-out.csv)')
    args = arg_parser.parse_args()

    outputName = makeOutputName(args)
    countFeedbacks(args.data)
    produceResult(outputName, args.header)

if __name__ == '__main__':
    sys.exit(main())
