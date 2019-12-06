# Format and send an ses message. Options are
# html    - if true, send as html format
# dry_run - if true, do not actually send email
import boto3

EMAIL_WITHOUT_ERRORS = 'ictnotifications@amplio.org'
EMAIL_WITH_ERRORS = [EMAIL_WITHOUT_ERRORS, 'techsupport@amplio.org']


def send_ses(fromaddr='ictnotifications@amplio.org',
             subject='',
             body_text='',
             recipient='ictnotifications@amplio.org'):
    """Send an email via the Amazon SES service.

    Example:
      send_ses('me@example.com, 'greetings', "Hi!", 'you@example.com)

    Return:
      If 'ErrorResponse' appears in the return message from SES,
      return the message, otherwise return an empty '' string.
    """
    if isinstance(recipient, str):
        recipients = [recipient]
    else:
        try:
            recipients = [str(x) for x in recipient]
        except:
            recipients = [str(recipient)]

    # If we ever want to send as html, here's how.
    html = False

    message = {'Subject': {'Data': subject}}
    if html:
        message['Body'] = {'Html': {'Data': body_text}}
    else:
        message['Body'] = {'Text': {'Data': body_text}}

    client = boto3.client('ses')
    response = client.send_email(Source=fromaddr, Destination={'ToAddresses': recipients}, Message=message)

    print(response)
