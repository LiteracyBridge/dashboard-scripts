#!/usr/local/bin/python
import argparse
import sys
import re

usage = '''
    This is a system for generating deployment and usage statistics. It reads a
    simple description file and generates one or more .sql files to extract the
    statistics from the dashboard database. It also generates a simple .sh
    script to execute the .sql and generate the output.

    The format of the description file looks much like a classic .ini file. It
    consists of several sections, introduced by a name enclosed in square
    brackets.

    Initial and trailing whitespace is ignored. Lines beginning with # are
    comments and are ignored. Lines beginning with ! are commands, and cause
    some operation to be performed at that point in parsing the file. Note that
    a command implicitly terminates any section preceeding it.

    There are three kinds of sections: selectors, report generators, and
    configuration.

    SELECTOR
    ========
    The selector section(s) control which data is included in the reports. They
    are used to generate the WHERE clause of the SQL statements. Selectors are
    supported for project, deploymentnumber, languagecode, categoryid, and
    village.

    Selectors for project, deploymentnumber, and languagecode are followed by
    one or more values, one per line, like this:
      [project]
      UWR
      [languagecode]
      dga
      en

    Selectors for categoryid and village can use one value per line, but can
    also provide a SQL query for a set, like this:
      [village]
      query = SELECT communityname FROM communities WHERE survey2015
    Multiple queries may be provided, along with simple values, and all will be
    combined with OR.

    If a selector appears again, the previous definition is discarded and
    replaced with the new one. The new definition can be empty, to remove the
    selector. See the "!generate" command.

    It is not currently possible to directly specify values to exclude. To
    build an exclusion, use something like:
      [village]
      query = SELECT communityname FROM communities WHERE communityname != 'WA'

    Report Generator
    ================
    Any section not recognized as a selector is handled as a report generator.
    The lines of the section consist of key=value pairs, and columns by which to
    group the results. Two kinds of reports are supported, deployment and usage.
    To get a deployment report, use "type = deployment", otherwise the default
    is "type = usage".

    The output can be named via a "name = ..." line. This will be used as the
    filename part of the .sql and .txt or .csv created by the report. If no
    "name = ..." is given, the section name will be used for the name.

    Single word lines in the report section are treated as column names. If they
    are actual columns, they'll be used in the SELECT DISTINCT clause, the
    GROUP BY clause, and the ORDER BY clause, in the order given. Columns not
    mentioned will be aggregated.

    The columns known for usage reports are: project, deployment,
    deploymentnumber, languagecode, contentpackage, village, acm_categoryid, and
    acm_categoryname. Those for deployment are the same, except not village,
    and format is supported for deployment reports.

    Note that the order of the columns affects only the presentation, not the
    values.

    Config
    ======
    Actually, a section named config is not treated as either a selector or a
    report, but as a sequence of "key = value" pairs with global configuration
    values. There are currently no [config] settings.

    Commands
    ========
    Recall that a line starting with a ! is a command. The "!generate" command
    will produce reports as defined up to that point in the file. Subsequent
    reports are produced at the next "!generate" command, or when the entire
    file has been read.

    Method Of Operation
    ===================
    The description file is read, and selector and report sections are parsed.
    After the entire file has been read, the reports are generated, with the
    selectors that have been defined. Thus, it doesn't matter whether the
    selectors or the reports are first.

    But, what if you want to slightly tweak the selectors between two reports?
    This is where the "!generate" command comes in. Define your selectors and
    reports, and "!generate" the reports, then define the new selectors and
    reports and repeat.


'''

# columns available to report for usage
usage_columns = ['project', 'deployment', 'deploymentnumber', 'languagecode', 'contentpackage', 'village', 'acm_categoryid', 'acm_categoryname']
# columns available to report for deployment
deployment_columns = ['project', 'deployment', 'deploymentnumber', 'languagecode', 'contentpackage', 'acm_categoryid', 'acm_categoryname', 'format']
# names to be translated from friendly->internal
translations = {'categoryname':'acm_categoryname', 'categoryid':'acm_categoryid', 'language':'languagecode', 'update':'deploymentnumber'}
# the script file is written here
scriptf = None
# Regular expression for "key = value"
key_value_re = re.compile('(\w*)\s*=\s*(.*)')

# Takes elements of an array and surrounds them with single quotes. Elements that already start
# with a single quote are not touched.
def enquote(lines):
    la = []
    for l in lines:
        if l[0:1] != "'":
            l = "'" + l + "'"
        la.append(l)
    return la

# Translates between friendly names and internal names. Needed?
def translate_name(name):
    if name in translations:
        return translations[name]
    return name

# Manages the 'WHERE' part of the query. Parses selector sections, and
# generates appropriate '=', 'IN', clauses.
class Filters:
    def __init__(self):
        self.column_filters = {}
        self.column_filters['village'] = "village != 'UNKNOWN'"

    # Creates a simple filter, either a single line "name = 'value'" or multiple lines
    # "name in ('v1', 'v2')"
    def simple_parser(self, name, values):
        pred = name + ' '
        values = enquote(values)
        if len(values) == 0:
            return ''
        elif len(values) == 1:
            pred = pred + '= ' + values[0]
        else:
            pred = pred + 'IN (' + ', '.join(values) + ')'
        return pred

    def query_parser(self, name, values):
        rest = []
        pred = ''
        queries = ''
        num_queries = 0
        sep = ''

        # pull out the "query = values", gather the others for simple processing
        for value in values:
            m = key_value_re.match(value)
            if m and len(m.groups()) > 1:
                if m.group(1) == 'query':
                    # name IN (query) [OR name IN (query)...]
                    queries += "{0}{1} IN ({2})".format(sep, name, m.group(2))
                    sep = ' OR '
                    num_queries += 1
            else:
                rest.append(value)

        # parse any non- "query = ..." values
        if len(rest) > 0:
            pred = self.simple_parser(name, rest)
            # add in "query = ..." if any
            if len(queries) > 0:
                # (name IN value OR name IN (query))
                pred = "({0} OR {1})".format(pred, queries)
        else:
            # only "query = ..."; maybe wrap in "("...")", if multiples
            pred = queries if num_queries < 2 else "({0})".format(queries)

        return pred



    def project_parser(self, name, values):
        pred = self.simple_parser(name, values)
        return pred

    def languagecode_parser(self, name, values):
        pred = self.simple_parser(name, values)
        return pred

    def village_parser(self, name, values):
        pred = self.query_parser(name, values)
        if len(pred) == 0:
            pred = "village != 'UNKNOWN'"
        return pred

    def acm_categoryid_parser(self, name, values):
        pred = self.query_parser(name, values)
        return pred

    def deploymentnumber_parser(self, name, values):
        # TODO: Proper deploymentnumber filter
        pred = self.simple_parser(name, values)
        return pred

    # parses a filter for the given column name, values
    def add_filter(self, name, values):
        name = translate_name(name)
        # if there's a function foo_parser, call it.
        fn = getattr(self, name + '_parser', None);
        if fn != None:
            pred = fn(name, values)
            if len(pred) > 0:
                self.column_filters[name] = pred
            else:
                del self.column_filters[name]
            return True
        return False

    def get_filters(self, sel):
        list = [v for k,v in self.column_filters.iteritems() if sel(k)]
        return '\n    AND '.join(list)


# The Report object holds properties for a report, and generates the WHERE and SELECT parts of the SQL
# Two kinds of queries are supported, usage and deployment. Both consist of a filter part, to
# extract the rows of interest from the database, and a query part, to organize and summarize.
class Report:
    def __init__(self, name, lines):
        self.columns = []
        self.type = 'usage'
        self.name = name
        for line in lines:
            m =  key_value_re.match(line)
            if m != None and len(m.groups()) > 1:
                if m.group(1) == 'type':
                    self.type = m.group(2)
                elif m.group(1) == 'name':
                    self.name = m.group(2)
            else:
                line = translate_name(line)
                self.columns.append(line)

        # Captures the difference between the flavors of reports
        # TODO: sub-class?
        if self.type == 'usage':
            self.source_name = 'message_stats_detail_1'
            self.filter_name = 'filtered_stats'
            self.valid_columns = usage_columns
            self.query_core = ('count(distinct contentid) as msgs_available\n'
                          '  ,round(sum(played_seconds_max)/3600,0) as hrs_played\n'
                          '  ,sum(completed_max) as completed\n'
                          '  ,count(distinct talkingbook) as tbs\n')
        elif self.type == 'deployment':
            self.source_name = 'content_deployed_detail'
            self.filter_name = 'filtered_deployments'
            self.valid_columns = deployment_columns
            self.query_core = ('count(distinct contentid) as msgs\n'
                         '  ,round(sum(duration_sec)/60,1) as minutes\n')
        else:
            raise('Unknown report type: {0}'.format(self.type))

        # Keep just columns that make sense for this query. TODO: report on others
        self.columns = [c for c in self.columns if c in self.valid_columns]


    # Generate the WHERE part of the query
    def make_filter(self):
        global filters
        result = "WITH {0} AS (\n  SELECT * FROM {1}\n".format(self.filter_name, self.source_name)
        where = filters.get_filters(lambda c: c in self.valid_columns)
        if len(where) > 0:
            result += '  WHERE ' + where
        result += '\n)\n'
        return result

    # Generate the SELECT part of the query
    def make_query(self):
         # extract the data
         result = 'SELECT '

         # The non-summarized level(s) of the query (project, deployment, village, etc.)
         if len(self.columns) > 0:
             result += 'DISTINCT '
             sep = ''
             for l in self.columns:
                 result += sep + l
                 sep = '\n  ,'

         # The actual summary data, the core of the query
         result += sep + "{0}FROM {1}".format(self.query_core, self.filter_name)

         # Group and order by the non-summarized level(s)
         sep = '\nGROUP BY '
         for l in self.columns:
             result += sep + l
             sep = '\n  ,'
         sep = '\nORDER BY '
         for l in self.columns:
             result += sep + l
             sep = '\n  ,'

         return result

# Manages a list of reports, and generates the .sql and .sh files when desired.
class Reports:
    def __init__(self):
        self.reports = []

    def add_report(self, name, lines):
        report = Report(name, lines)
        self.reports.append(report)

    # Generates the SELECT part of the query. Or queries.
    def produce_reports(self):
        global args, scriptf
        for report in self.reports:
            name = report.name
            ext = '.txt'
            if args.csv:
                ext = '.csv'
            # lazy create .sh script file
            if scriptf == None:
                scriptf = open('querygen.sh', 'w')

            fn = name + '.sql'
            outf = open(fn, 'w')
            # creating a .csv?
            if args.csv:
                outf.write('COPY ( ')

            outf.write(report.make_filter())
            outf.write(report.make_query())

            # finish .csv
            if args.csv:
                outf.write("\n) TO STDOUT (FORMAT csv, HEADER true)")
            cmd = args.psql + ' ' + args.dbcxn + ' -A -f ' + fn + '>' + name + ext + '\n'
            scriptf.write(cmd)
        self.reports = []

# Responsible for parsing the report description file
class Parser:
    def __init__(self):
        return

    # Based on a section's name, call the proper section parser
    def process_section(self, name, contents):
        global filters, reports
        if name == 'config':
            pass
        elif not filters.add_filter(name, contents):
            reports.add_report(name, contents)

    # Perform an immediate command (a "!" line)
    def process_command(self, command):
        global reports
        if command == 'generate':
            reports.produce_reports()

    # Reads a file, looking for section headers, lines like '[foo]'. Collects non-blank
    # lines following a section header, and passes complete sections to parse_section()
    def parse(self, fn):
        global args
        f = open(fn, 'rU')
        section = []
        section_name = None
        for line in f:
            line = line.strip()
            if len(line) < 1 or line[0:1] == '#':
                continue
            if line[0:1] == '[':
                if section_name:
                    self.process_section(section_name, section)
                section_name = line[1:-1]
                section = []
                continue
            if line[0:1] == '!':
                if section_name:
                    self.process_section(section_name, section)
                section_name = None
                self.process_command(line[1:])
                continue
            section.append(line)
        if section_name:
            self.process_section(section_name, section)


def main():
    global args, filters, reports
    filters = Filters()
    reports = Reports()
    parser = Parser()
    arg_parser = argparse.ArgumentParser(description="Query generator for deployment and usage statistics", usage=usage)
    arg_parser.add_argument('reports', help='File name, contains the report specification')
    arg_parser.add_argument('--csv', help='Generated SQL will create a .csv file', action='store_true')
    arg_parser.add_argument('--psql', help='Path to psql utility', default='psql')
    arg_parser.add_argument('--dbcxn', help='PostgreSQL connection string', default='--host=localhost --port 5432 --username=lb_data_uploader --dbname=dashboard')
    args = arg_parser.parse_args()

    print "Starting..."
    parser.parse(args.reports)
    reports.produce_reports()

if __name__ == '__main__':
    sys.exit(main())
