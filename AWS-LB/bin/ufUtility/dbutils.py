import base64
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Union, Tuple, Any, Callable

import boto3 as boto3
import pg8000 as pg8000
from botocore.exceptions import ClientError
from pg8000 import Cursor, Connection

MD_MESSAGE_UUID_TAG = 'metadata.MESSAGE_UUID'

recipient_cache: Dict[str, Dict[str, str]] = {}

_db_connection: Union[Connection, None] = None


def make_date_extractor(md_field: str) -> Callable:
    """
    Create and return a function that will extract a date, validate it, and return an ISO formatted
    date if it is valid, or an empty string if it is not.

    We need this because the "date recorded" field is directly from the Talking Book, and, as such,
    is very likely to contain garbage.
    :param md_field: The name of the field that may or may not contain a valid date.
    :return: a function that accepts a Dict[str,str] and returns a str containing an ISO date.
    """
    def extract(props:Dict[str,str]) -> str:
        ds=''
        v = props.get(md_field, '')
        try:
            d = datetime.strptime(v, '%Y/%m/%d')
            ds = d.strftime('%Y%m%d')
        except Exception:
            pass
        return ds
    return extract

def _make_early_date() -> datetime:
    return datetime(2018,1,1)

uf_column_map = {
    # column:name : property_name or [prop1, prop2, ...]
    'message_uuid': 'metadata.MESSAGE_UUID',

    #'deployment_uuid': 'DEPLOYEDUUID', # Timestamp is probably sufficient
    'programid': 'PROJECT',
    'deploymentnumber': 'DEPLOYMENT_NUMBER',
    'recipientid': 'RECIPIENTID',
    'talkingbookid': 'TALKINGBOOKID',
    'deployment_tbcdid': 'TBCDID',
    'deployment_timestamp': 'TIMESTAMP',
    'deployment_user': 'USERNAME',
    'test_deployment': 'TESTDEPLOYMENT',
    #'collection_uuid': 'collection.STATSUUID', # Timestamp is probably sufficient
    'collection_tbcdid': 'collection.TBCDID',
    'collection_timestamp': 'collection.TIMESTAMP',
    'collection_user': ['collection.USEREMAIL', 'collection.USERNAME'],
    'length_seconds': 'metadata.SECONDS',
    'length_bytes': 'metadata.BYTES',
    'language': 'metadata.LANGUAGE',
    'date_recorded': make_date_extractor('metadata.DATE_RECORDED'),
    'relation': 'metadata.RELATION',
}
uf_default_values_map = {
    'deployment_timestamp': '180101',
    'test_deployment': 'f',
    'collection_timestamp' : '180103',
    'date_recorded': '180102'
}

# noinspection SqlDialectInspection ,SqlNoDataSourceInspection
class DbUtils:
    _instance = None

    def __new__(cls, **kwargs):
        if cls._instance is None:
            print('Creating the DbUtils object')
            cls._instance = super(DbUtils, cls).__new__(cls)
            cls._props: List[Tuple] = []
            cls._args = kwargs.get('args')
        return cls._instance

    def _get_secret(self) -> dict:
        secret_name = "lb_stats_access2"
        region_name = "us-west-2"

        if self._args.verbose >= 2:
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
            if self._args.verbose >= 2:
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
            if self._args.db_host:
                parms['host'] = self._args.db_host
            if self._args.db_port:
                parms['port'] = int(self._args.db_port)
            if self._args.db_user:
                parms['user'] = self._args.db_user
            if self._args.db_password:
                parms['password'] = self._args.db_password
            if self._args.db_name:
                parms['database'] = self._args.db_name

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
                   'language': 'language', 'model': 'model'}
        # select recipientid, project, ... from recipients where recipientid = '0123abcd4567efgh';
        command = f'select {",".join(columns.keys())} from recipients where recipientid=:recipientid;'
        values = {'recipientid': recipientid}

        recipient_info: Dict[str, str] = {}
        try:
            cursor.execute(command, values)
            for row in cursor:
                cols: List[str] = list(columns.values())
                for col in cols:
                    recipient_info[col] = row[cols.index(col)]
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

    def insert_uf_records(self, uf_list: List[Tuple]) -> Any:
        cursor: Cursor = self.db_connection.cursor()
        cursor.paramstyle = 'numeric'
        if self._args.verbose >= 1:
            print(f'Adding {len(uf_list)} records to uf_messages')

        columns = list(uf_column_map.keys())
        column_numbers = [f':{ix+1}' for ix in range(0,len(columns))]

        command = f"INSERT INTO uf_messages " \
                  f"({', '.join(columns)}) VALUES ({', '.join(column_numbers)})" \
                  f"ON CONFLICT(message_uuid) DO NOTHING;"
        for uf in uf_list:
            uf_list = [x for x in uf]
            for k,v in uf_default_values_map.items():
                ix = columns.index(k)
                if not uf_list[ix]:
                    uf_list[ix] = v
            cursor.execute(command, uf_list)

        self.db_connection.commit()
        if self._args.verbose >= 2:
            print(f'Committed {len(uf_list)} records to uf_messages.')

    @dataclass
    class UfRecord:
        message_uuid: str
        programid: str
        deploymentnumber: int
        recipientid: str
        talkingbookid: str
        deployment_tbcdid: str
        deployment_timestamp: datetime = field(default_factory=_make_early_date)
        deployment_user: str = ''
        test_deployment: bool = False
        collection_tbcdid: str = ''
        collection_timestamp: datetime = field(default_factory=_make_early_date)
        collection_user: str = ''
        length_seconds: int = 0
        length_bytes: int = 0
        language: str = 'en'
        date_recorded: date = field(default_factory=_make_early_date)
        relation: str = ''
        bundleid: str = None


    def get_uf_records(self, programid:str, deploymentnumber:int) -> List[UfRecord]:
        cursor: Cursor = self.db_connection.cursor()
        cursor.paramstyle = 'named'
        if self._args.verbose >= 1:
            print(f'Getting uf records for {programid} / {deploymentnumber}.')

        result = []
        command = f"SELECT " + ', '.join(uf_column_map.keys()) + \
                  f" FROM uf_messages WHERE programid=:programid AND deploymentnumber=:deploymentnumber ORDER BY message_uuid;"
        options = {'programid': programid, 'deploymentnumber': deploymentnumber}
        cursor.execute(command, options)
        for row in cursor:
            result.append(DbUtils.UfRecord(*row))
        return result


