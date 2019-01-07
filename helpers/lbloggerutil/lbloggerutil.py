import subprocess
import sys
import datetime
import time
import argparse
import os


def log_line(filehandle, logtext):
    """Adds a new line to a log file"""
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    filehandle.write(timestamp + '\t' + logtext + '\n')


def init_log_header(logfile, program, programdir, program_args,
                    logdir, filename_utility_log, filename_stdout,
                    filename_stderr):
    """Creates the header of the utility logfile and lists variables"""
    log_line(filehandle=logfile, logtext='------------------------------------------------------------------')
    log_line(filehandle=logfile, logtext='+++ Starting LBUtilityLogger +++')
    log_line(filehandle=logfile, logtext='program: ' + program)
    log_line(filehandle=logfile, logtext='arguments: ' + str(program_args))
    log_line(filehandle=logfile, logtext='program directory:' + programdir)
    log_line(filehandle=logfile, logtext='utility log location: ' + logdir + filename_utility_log)
    log_line(filehandle=logfile, logtext='stdout log location: ' + logdir + filename_stdout)
    log_line(filehandle=logfile, logtext='stderr log location: ' + logdir + filename_stderr)


def parse_arguments():
    """Parse argv arguments and return these in an object"""
    parser = argparse.ArgumentParser()
    parser.add_argument("executable", help="The executable to be run by this script")
    parser.add_argument("program_args", nargs="*", help="The arguments of the executable")
    parser.add_argument("--logdir", help="Specify logging directory")
    parser.add_argument("--timestamp", help="Add current timestamp in filenames of stdout and stderr logs",
                        action="store_true")
    args = parser.parse_args()

    return args


def init_log_writers(args, logdir):
    """Initializes the log writers for the utility log,
    stdout and stderr. This includes creating the respective
    log files and file writers.
    """
    # Set file names
    currenttime = time.strftime("%Y%m%d-%H%M%S")
    program = os.path.basename(args.executable)
    programdir = os.path.dirname(args.executable)
    filename_utility_log = program + '_utility.log'

    # Check if timestamp must be added to logs
    if args.timestamp:
        filename_stdout = program + '_stdout_' + currenttime + '.log'
        filename_stderr = program + '_stderr_' + currenttime + '.log'
    else:
        filename_stdout = program + '_stdout.log'
        filename_stderr = program + '_stderr.log'

    # Set utility log writer
    utilitylogfile = open(file=logdir + filename_utility_log, mode='a')

    # Create logfile header
    init_log_header(logfile=utilitylogfile, program=program, programdir=programdir, program_args=args.program_args,
                    logdir=logdir, filename_utility_log=filename_utility_log,
                    filename_stdout=filename_stdout, filename_stderr=filename_stderr)

    # Set redirection for stdout
    sys.stdout = open(file=logdir + filename_stdout, mode='a')

    # Set redirection for stderr
    sys.stderr = open(file=logdir + filename_stderr, mode='a')

    return utilitylogfile


def add_log_footer(logfile):
    """Add trailing log statement"""
    log_line(filehandle=logfile, logtext='+++ Ending LBUtilityLogger +++')


def run_sub_process(executable, arguments, logfile):
    """Run the executable with the passed arguments in
    a new subprocess. The generated logs are stored in the
    passed log writer.
    """
    try:
        log_line(filehandle=logfile, logtext='Starting subprocess...')
        pc = subprocess.run(executable + ' ' + str(arguments), check=True, shell=True,
                            stdout=sys.stdout, stderr=sys.stderr, encoding='UTF-8')
        log_line(filehandle=logfile, logtext='Subprocess completed with exit code ' + str(pc.returncode))
        print('Exit code: ' + str(pc.returncode))
    except subprocess.CalledProcessError as procErr:
        errortext = 'Process Error:' + str(procErr.output) + ', Exit code:' + str(procErr.returncode)
        log_line(filehandle=logfile, logtext=errortext)
        print(errortext)


def main():

    # Read program arguments
    args = parse_arguments()
    executable = args.executable
    program_args = args.program_args
    if args.logdir:
        logdir = args.logdir
    else:
        logdir = ""

    # Initialize utility log and log writers for stdout and stderr
    utilitylogfile = init_log_writers(args=args, logdir=logdir)

    # Run the program in a subprocess
    run_sub_process(executable=executable, arguments=program_args, logfile=utilitylogfile)

    # Add footer to log file
    add_log_footer(logfile=utilitylogfile)


if __name__ == "__main__":
    sys.exit(main())

