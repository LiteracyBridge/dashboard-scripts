import base64
import json
import time
from typing import Dict, List, Union, Tuple, Any

import boto3 as boto3
import pg8000 as pg8000
from botocore.exceptions import ClientError
from pg8000 import Cursor, Connection

from UfRecord import uf_column_map, UfRecord

recipient_cache: Dict[str, Dict[str, str]] = {}

_db_connection: Union[Connection, None] = None


# noinspection SqlDialectInspection ,SqlNoDataSourceInspection
class DbUtils:
    _instance = None

    # This class is a singleton. 
    def __new__(cls, **kwargs):
        if cls._instance is None:
            print('Creating the DbUtils object')
            cls._instance = super(DbUtils, cls).__new__(cls)
            cls._props: List[Tuple] = []
            cls._verbose = kwargs.get('verbose', 0)
            cls._db_host = kwargs.get('db_host')
            cls._db_port = kwargs.get('db_port')
            cls._db_user = kwargs.get('db_user')
            cls._db_password = kwargs.get('db_password')
            cls._db_name = kwargs.get('db_name')

        return cls._instance

    def _get_secret(self) -> dict:
        secret_name = "lb_stats_access2"
        region_name = "us-west-2"

        if self._verbose >= 2:
            print('    Getting credentials for database connection. v2.')
        start = time.time()

        # Create a Secrets Manager client
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
        except Exception as e:
            print('    Exception getting session client: {}, elapsed: {}'.format(str(e), time.time() - start))
            raise e

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if self._verbose >= 2:
                print('    Exception getting credentials: {}, elapsed: {}'.format(e.response['Error']['code'],
                                                                                  time.time() - start))

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
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                result = json.loads(secret)
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                result = decoded_binary_secret

        # Your code goes here.
        return result

    def _get_db_connection(self) -> None:
        global _db_connection
        if _db_connection is None:
            secret = self._get_secret()

            parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                     'host': secret['host'], 'port': secret['port']}
            if self._db_host:
                parms['host'] = self._db_host
            if self._db_port:
                parms['port'] = int(self._db_port)
            if self._db_user:
                parms['user'] = self._db_user
            if self._db_password:
                parms['password'] = self._db_password
            if self._db_name:
                parms['database'] = self._db_name

            _db_connection = pg8000.connect(**parms)

    @property
    def db_connection(self) -> Connection:
        global _db_connection
        if not _db_connection:
            self._get_db_connection()
        return _db_connection

    def query_recipient_info(self, recipientid: str) -> Dict[str, str]:
        """
        Given a recipientid, return information about the recipient. Previously found recipients are
        cached. Non-cached recipients are looked up in the database.
        :param recipientid: to be found.
        :return: a Dict[str,str] of data about the recipient.
        """
        if recipientid in recipient_cache:
            return recipient_cache[recipientid]

        cursor: Cursor = self.db_connection.cursor()
        cursor.paramstyle = 'named'

        # { db column : dict key }
        columns = {'recipientid': 'recipientid', 'project': 'program', 'partner': 'customer', 'affiliate': 'affiliate',
                   'country': 'country', 'region': 'region',
                   'district': 'district', 'communityname': 'community', 'groupname': 'group', 'agent': 'agent',
                   'language': 'language', 'listening_model': 'listening_model'}
        # select recipientid, project, ... from recipients where recipientid = '0123abcd4567efgh';
        command = f'select {",".join(columns.keys())} from recipients where recipientid=:recipientid;'
        values = {'recipientid': recipientid}

        recipient_info: Dict[str, str] = {}
        try:
            result_keys: List[str] = list(columns.values())
            cursor.execute(command, values)
            for row in cursor:
                # Copy the recipient info, translating from the database names to the local names.
                for key in result_keys:
                    recipient_info[key] = row[result_keys.index(key)]
        except Exception:
            pass
        recipient_cache[recipientid] = recipient_info
        return recipient_info

    def query_deployment_number(self, program: str, deployment: str) -> str:
        cursor: Cursor = self.db_connection.cursor()
        cursor.paramstyle = 'named'

        command = 'select deploymentnumber from deployments where project=:program and deployment=:deployment limit 1;'
        values = {'program': program, 'deployment': deployment}

        cursor.execute(command, values)
        for row in cursor:
            return str(row[0])

    def insert_uf_records(self, uf_items: List[Tuple]) -> Any:
        cursor: Cursor = self.db_connection.cursor()
        cursor.paramstyle = 'numeric'
        if self._verbose >= 1:
            print(f'Adding {len(uf_items)} records to uf_messages')

        # It doesn't seem that this should be necessary, but it seems to be.
        self.db_connection.rollback()

        columns = list(uf_column_map.keys())
        column_numbers = [f':{ix + 1}' for ix in range(0, len(columns))]

        command = f"INSERT INTO uf_messages " \
                  f"({', '.join(columns)}) VALUES ({', '.join(column_numbers)})" \
                  f"ON CONFLICT(message_uuid) DO NOTHING;"
        for uf_item in uf_items:
            cursor.execute(command, uf_item)

        self.db_connection.commit()
        if self._verbose >= 2:
            print(f'Committed {len(uf_items)} records to uf_messages.')

    def get_uf_records(self, programid: str, deploymentnumber: int) -> List[UfRecord]:
        cursor: Cursor = self.db_connection.cursor()
        cursor.paramstyle = 'named'
        if self._verbose >= 1:
            print(f'Getting uf records for {programid} / {deploymentnumber}.')

        result = []
        command = f"SELECT " + ', '.join(uf_column_map.keys()) + \
                  f" FROM uf_messages WHERE programid=:programid AND deploymentnumber=:deploymentnumber ORDER BY message_uuid;"
        options = {'programid': programid, 'deploymentnumber': deploymentnumber}
        cursor.execute(command, options)
        for row in cursor:
            result.append(UfRecord(*row))
        return result

    def update_uf_bundles(self, programid: str, deploymentnumber: int, bundles: Dict[str,List[str]]) -> bool:
        """
        Updates the bundle_uuid column of the inidicated messages.
        :param programid: For an extra validation, the record must belong to this program.
        :param deploymentnumber: For an extra validation, the record must belong to this deployment.
        :param bundles: A map of bundle_uuid to list of message_uuid.
        :return: pass/fail
        """
        cursor: Cursor = self.db_connection.cursor()
        cursor.paramstyle = 'named'
        if self._verbose >= 1:
            print(f'Updating uf bundle_uuids for {sum([len(v) for v in bundles.values()])} messages in {programid} / {deploymentnumber}.')

        try:
            command = "UPDATE uf_messages SET bundle_uuid=:bundle_uuid WHERE message_uuid=:message_uuid AND programid=:programid AND deploymentnumber=:deploymentnumber;"
            options = {'programid': programid, 'deploymentnumber': deploymentnumber}
            for bundle_uuid, messages in bundles.items():
                options['bundle_uuid'] = bundle_uuid
                for message_uuid in messages:
                    options['message_uuid'] = message_uuid
                    cursor.execute(command, options)
            self.db_connection.commit()
            return True
        except Exception as ex:
            return False
