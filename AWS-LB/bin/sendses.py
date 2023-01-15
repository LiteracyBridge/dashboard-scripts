#!/usr/bin/env python3
'''
Small utility to send email via Amazon ses. From & to addresses, & subject are
specified via command line options. Body text provided in a file or stdin. If
filename is *.html, or --html switch is given, sent as html, otherwise as plain
text.
'''
import argparse
import os

import boto3
import sys

options = {}

# Format and send an ses message. Options are
# html    - if true, send as html format
# dry_run - if true, do not actually send email
def send_ses(fromaddr,
             subject,
             body_text,
             recipient,
             options):
    """Send an email via the Amazon SES service.

    Example:
      send_ses('me@example.com, 'greetings', "Hi!", 'you@example.com)

    Return:
      If 'ErrorResponse' appears in the return message from SES,
      return the message, otherwise return an empty '' string.
    """

    print('Sending "{}" from "{}" to "{}"'.format(subject, fromaddr, recipient))

    message = {'Subject': {'Data': subject}}
    if options.html:
        message['Body'] = {'Html': {'Data': body_text}}
    else:
        message['Body'] = {'Text': {'Data': body_text}}

    if options.dry_run:
        print(message)
        return

    client = boto3.client('ses')
    response = client.send_email(
        Source=fromaddr,
        Destination={
            'ToAddresses': [
                recipient
            ]
        },
        Message=message
    )

    print(response)

# Reads the file specified by --body, else stdin
def get_body_text():
    is_html = options.html
    if 'body' in options and options.body != None:
        f = open(options.body)
        body_text = f.read()
        if options.body.lower().endswith('.html'):
            is_html = True
        print("Filename is {}, is_html is {}".format(options.body, is_html))
    else:
        body_text = sys.stdin.read()

    return (body_text, is_html)


def main():
    global options
    arg_parser = argparse.ArgumentParser(description="Send email using AWS SES")
    arg_parser.add_argument('--subject', help='Subject line for the email.', required=True)
    arg_parser.add_argument('--html', help='Send the body as html', action='store_true')
    arg_parser.add_argument('--body', help='File containing the body of the email.')
    arg_parser.add_argument('--sender', help='Email address from. Must be verified with SES.',
                            default='ictnotifications@amplio.org')
    arg_parser.add_argument('--to', help='Email address to. Must be verified with SES.',
                            default='ictnotifications@amplio.org')
    arg_parser.add_argument('--dry-run', '-n', help='Do not actually send the email.', action='store_true')

    options = arg_parser.parse_args()
    (body_text, is_html) = get_body_text()
    if is_html:
        options.html = True

    send_ses(options.sender, options.subject, body_text, options.to, options)


if __name__ == '__main__':
    sys.exit(main())
