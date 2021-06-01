#!/usr/bin/env python

import json
import os.path
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import boto3
import pg8000
from botocore.exceptions import ClientError

REGION_NAME = 'us-west-2'

# specify dynamoDB table for checkout records
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
PROGRAM_TABLE_NAME = 'programs'
program_table = dynamodb.Table(PROGRAM_TABLE_NAME)

# This will be a connection to the PostgreSQL database
db_connection = None


# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.
def get_postgresql_secret() -> dict:
    result = ''
    secret_name = "lb_stats_access2"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            result = json.loads(secret)

    # Your code goes here.
    return result


# Make a connection to the SQL database
def get_db_connection():
    global db_connection
    if db_connection is None:
        secret = get_postgresql_secret()

        parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                 'host': secret['host'], 'port': secret['port']}
        db_connection = pg8000.connect(**parms)

    return db_connection


# noinspection SqlResolve,SqlNoDataSourceInspection,SqlDialectInspection
def get_program_descriptions() -> Dict[str, str]:
    """
    Gets the programid and program descriptions from PostgreSQL.
    :return: a Dict of {programid: description}
    """
    print('Getting projects from PostgreSQL...')

    result = {}
    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute('SELECT projectcode, project FROM projects ORDER BY projectcode;')
    for row in cur:
        result[row[0]] = row[1]
    return result


def update_program_records(programs: Dict[str, str]) -> bool:
    """
    Given a list of program descriptions, update the DynamoDB "programs" table with those descriptions.
    :param programs: A Dict of {programid: description}, from PostgreSQL.
    :return: True if everything was updated OK, False if there were any errors.
    """
    update_expression = 'SET description = :d'
    condition_expression = 'program = :p'

    program_items = program_table.scan()['Items']
    for item in program_items:
        program = item.get('program')
        if program in programs and 'description' not in item:
            try:
                description = programs[program]
                expression_values = {
                    ':p': program,
                    ':d': description
                }
                program_table.update_item(
                    Key={'program': program},
                    UpdateExpression=update_expression,
                    ConditionExpression=condition_expression,
                    ExpressionAttributeValues=expression_values
                )
            except Exception as err:
                print(f'exception updating {program}: {err}')
                return False

    return True


def update_config_properties(programs):
    """
    Given a Dict of {programid: description}, look in Dropbox for any ACM-programid, and update the
    config.properties file with DESCRIPTION=description.
    :param programs: A Dict of {programid: description} from PostgreSQL.
    :return: None
    """
    dropbox: Path = Path(os.path.expanduser('~/Dropbox'))
    print(f'Searching for config.properties files in {dropbox.absolute()}.')
    for programid, description in programs.items():
        acm_name = f'ACM-{programid}'
        config = Path(dropbox, acm_name, 'config.properties')
        if config.exists() and config.is_file():
            print(f'  Found config file for programid {programid}.')
            need_description = True
            with open(config, 'r') as config_in:
                lines = []
                for line in config_in:
                    line = line.strip()
                    l = line.split('=', 1)
                    # If there is a DESCRIPTION= line...
                    if l[0].strip().upper() == 'DESCRIPTION':
                        # ...see if it is still correct, and if so, keep it.
                        if l[1].strip() == description:
                            lines.append(line)
                            need_description = False
                        else:
                            print('    Description has changed, will update.')
                    elif line:
                        # Keep other non-blank lines
                        lines.append(line)
            if need_description:
                print(f'    Updating config.properties with DESCRIPTION={description}.')
                config_bak = config.with_suffix('.bak')
                config.replace(config_bak)
                lines.append(f'DESCRIPTION={description}')
                # Get the current time, in the local time zone.
                time_now = datetime.now(timezone.utc).astimezone()
                # Format like a Java properties timestamp, #Thu Apr 29 15:10:27 PDT 2021
                lines[0] = time_now.strftime('#%a %b %d %H:%M:%S %Z %Y')
                lines[1:] = sorted(lines[1:])
                with open(config, 'w') as config_out:
                    for line in lines:
                        print(line, file=config_out)


def main():
    programs: Dict[str, str] = get_program_descriptions()
    # update_program_records(programs)
    update_config_properties(programs)


if __name__ == '__main__':
    sys.exit(main())
