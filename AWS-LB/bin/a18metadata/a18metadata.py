import argparse
import base64
import json
import platform
import shutil
import struct
import subprocess
import tempfile
import time
import uuid as uuid
from os.path import expanduser
from pathlib import Path
from typing import List, Tuple, Dict, Union, Any

import boto3 as boto3
import pg8000 as pg8000
from botocore.exceptions import ClientError
from pg8000 import Cursor

# noinspection SqlDialectInspection,SqlNoDataSourceInspection
DEPLOYMENT_NUMBER_TAG = 'DEPLOYMENT_NUMBER'
DEPLOYMENT_TAG = 'DEPLOYMENT'
MD_MESSAGE_UUID_TAG = 'metadata.MESSAGE_UUID'
PROJECT_TAG = 'PROJECT'
RECIPIENTID_TAG = 'RECIPIENTID'
NAMESPACE_UF = uuid.UUID('677aba79-e672-4fe3-91d5-c69306fe025d')

cursor: Union[Cursor, None] = None
args: Any = None


# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.
def get_secret() -> dict:
    secret_name = "lb_stats_access2"
    region_name = "us-west-2"

    if args.verbose >= 2:
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
        if args.verbose >= 2:
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


# lazy initialized db connection
db_connection = None


# Make a connection to the SQL database
def get_db_connection():
    global args, db_connection, cursor
    if db_connection is None:
        secret = get_secret()

        parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                 'host': secret['host'], 'port': secret['port']}
        if args.db_host:
            parms['host'] = args.db_host
        if args.db_port:
            parms['port'] = int(args.db_port)
        if args.db_user:
            parms['user'] = args.db_user
        if args.db_password:
            parms['password'] = args.db_password
        if args.db_name:
            parms['database'] = args.db_name

        db_connection = pg8000.connect(**parms)

        cursor = db_connection.cursor()
        cursor.paramstyle = 'named'

    return db_connection


recipient_cache: Dict[str, Dict[str, str]] = {}


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
def query_recipient_info(recipientid: str) -> Dict[str, str]:
    """
    Given a recipientid, return information about the recipient. Previously found recipients are
    cached. Non-cached recipients are looked up in the database.
    :param recipientid: to be found.
    :return: a Dict[str,str] of data about the recipient.
    """
    if recipientid in recipient_cache:
        return recipient_cache[recipientid]

    get_db_connection()

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


# noinspection SqlDialectInspection ,SqlNoDataSourceInspection
def query_deployment_number(program: str, deployment: str) -> str:
    get_db_connection()

    command = 'select deploymentnumber from deployments where project=:program and deployment=:deployment limit 1;'
    values = {'program': program, 'deployment': deployment}

    cursor.execute(command, values)
    for row in cursor:
        return str(row[0])


class StorePathAction(argparse.Action):
    """
    An argparse.Action to store a Path object. A leading ~ is expanded to the user's home directory.
    If the option 'trailing_slash' is True, the string is forced to have a trailing slash character,
    but that doesn't seem to matter to the Path object.
    """

    def _expand(self, v: str) -> Union[None, str]:
        """
        Does the work of expanding.
        :param v: A string, possibly with a leading ~ to be expanded ot user's home directory.
        :return: A Path object that encapsulates the given path. Note that there is no guarantee of
            any actual file system object at that path.
        """
        if v is None:
            return None
        v = expanduser(v)
        if self._trailing_slash and v[-1:] != '/':
            v += '/'
        return Path(v)

    def __init__(self, option_strings, dest, nargs=None, trailing_slash=False, default=None, **kwargs):
        self._trailing_slash = trailing_slash
        super(StorePathAction, self).__init__(option_strings, dest, default=self._expand(default), nargs=nargs,
                                              **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        values = [self._expand(v) for v in values] if isinstance(values, list) else self._expand(values)
        setattr(namespace, self.dest, values)


# noinspection PyShadowingBuiltins
class BinaryReader:
    """
    Helper class for reading binary data, providing those functions (and only those)
    required to read TB metadata from an .a18 file.
    """

    def __init__(self, buffer: bytes):
        """
        Initialize with a bytes object; keep track of where we are in that object.
        :param buffer: bytes of metadata.
        """
        self._buffer = buffer
        self._offset = 0
        # adjust these if we ever need to support big-endian
        self._I32 = '<l'
        self._I16 = '<h'
        self._I8 = '<b'

    def _read_x(self, format: str) -> any:
        """
        Read value(s) per the format string, and advance the offset (consume the data read).
        :param format: specification of one or more values to read.
        :return: the raw result from struct.unpack_from(), a tuple of values
        """
        values = struct.unpack_from(format, self._buffer, self._offset)
        self._offset += struct.calcsize(format)
        return values

    def read_i32(self) -> int:
        """
        Read a 32-bit signed integer.
        :return: the integer.
        """
        return self._read_x(self._I32)[0]

    def read_i16(self) -> int:
        """
        Read a 16-bit signed integer.
        :return: the integer.
        """
        return self._read_x(self._I16)[0]

    def read_i8(self) -> int:
        """
        Read a 8-bit signed integer.
        :return: the integer.
        """
        return self._read_x(self._I8)[0]

    def read_utf8(self) -> str:
        """
        Read a UTF-8 encoded string. These are encoded as a 16-bit length, followed by ${length}
        bytes of encoded data.
        :return: the string.
        """
        str_len = self.read_i16()
        str_format: str = f'<{str_len}s'
        str_bytes: bytes = self._read_x(str_format)[0]
        # noinspection PyUnusedLocal
        try:
            return str_bytes.decode('utf-8')
        except Exception:
            # extract as much of an ASCII string as we can. Possibly corrupted on Talking Book.
            chars = [chr(b) for b in str_bytes if 32 <= b <= 0x7f]
            return ''.join(chars)


class MetadataReader:
    """
    Class to parse .a18 metadata.
    """

    def __init__(self, buffer: BinaryReader):
        self._buffer = buffer
        # The known metadata types. From LBMetadataIDs.java.
        self._md_parsers = {
            0: (self._string_md_parser, 'CATEGORY'),
            1: (self._string_md_parser, 'TITLE'),
            5: (self._string_md_parser, 'PUBLISHER'),
            10: (self._string_md_parser, 'IDENTIFIER'),
            11: (self._string_md_parser, 'SOURCE'),
            12: (self._string_md_parser, 'LANGUAGE'),
            13: (self._string_md_parser, 'RELATION'),
            16: (self._string_md_parser, 'REVISION'),
            22: (self._string_md_parser, 'DURATION'),
            23: (self._string_md_parser, 'MESSAGE_FORMAT'),
            24: (self._string_md_parser, 'TARGET_AUDIENCE'),
            25: (self._string_md_parser, 'DATE_RECORDED'),
            26: (self._string_md_parser, 'KEYWORDS'),
            27: (self._string_md_parser, 'TIMING'),
            28: (self._string_md_parser, 'PRIMARY_SPEAKER'),
            29: (self._string_md_parser, 'GOAL'),
            30: (self._string_md_parser, 'ENGLISH_TRANSCRIPTION'),
            31: (self._string_md_parser, 'NOTES'),
            32: (self._string_md_parser, 'BENEFICIARY'),
            33: (self._integer_md_parser, 'STATUS'),
            35: (self._string_md_parser, 'SDG_GOALS'),
            36: (self._string_md_parser, 'SDG_TARGETS'),
        }

    def _string_md_parser(self, joiner: str = ';'):
        """
        Parses a string-valued metadata entry.
        :param joiner: A delimiter with which to join multiple values.
        :return: The value(s) found.
        """
        results = []
        num_values = self._buffer.read_i8()
        for i in range(num_values):
            str_value = self._buffer.read_utf8()
            results.append(str_value)
        return joiner.join(results)

    def _integer_md_parser(self, joiner: str = ';'):
        """
        Parses an integer-valued metadata entry. Values are returned as their string representation.
        :param joiner: A delimiter with which to join multiple values.
        :return: The value(s) found.
        """
        results = []
        num_values = self._buffer.read_i8()
        for i in range(num_values):
            int_value = self._buffer.read_i32()
            results.append(str(int_value))
        return joiner.join(results)

    def parse(self) -> Dict[str, str]:
        """
        Parses the .a18 file's data and returns the metatdata.
        :return: A Dict[str,str] with the metadata.
        """
        version = self._buffer.read_i32()
        if version != 1:
            raise ValueError(f"Unknown metadata version. Expected '1', but found '{version}'.")
        num_fields = self._buffer.read_i32()
        metadata: Dict[str, str] = {}

        for i in range(num_fields):
            field_id = self._buffer.read_i16()
            # This isn't used because the fields all know how big they are.
            # noinspection PyUnusedLocal
            field_len = self._buffer.read_i32()

            if field_id in self._md_parsers:
                fn, name = self._md_parsers.get(field_id)
                result = fn()
                metadata[name] = result
            else:
                print(f'undecoded field {field_id}')
        return metadata

    @staticmethod
    def read_from_file(a18_path: Path) -> Dict[str, str]:
        """
        Extract the metadata from an .a18 file. The file consists of a 32-bit length-of-audio-data, length bytes of
        audio data, bytes-til-eof of metadata
        :param a18_path: path to the .a18 file.
        :return: a Dict[str,str] of the metadata
        """
        _t = lambda a,b,c: b if a else c
        file_len = a18_path.stat().st_size
        with open(a18_path, 'rb') as f:
            # First 4 bytes is unsigned long 'size of audio'. Skip the audio, load the binary metadata.
            buffer = f.read(4)
            audio_len = struct.unpack('<l', buffer)[0]
            buffer = f.read(2)
            audio_bps = struct.unpack('<h', buffer)[0]

            md_offset = audio_len + 4
            md_len = file_len - md_offset
            f.seek(md_offset)
            md_bytes = f.read(md_len)
        bytes_reader = BinaryReader(md_bytes)
        md_parser = MetadataReader(bytes_reader)
        md:Dict[str,str] = md_parser.parse()
        total_seconds = int(audio_len * 8 / audio_bps + 0.5)
        if 'DURATION' not in md:
            min, sec = divmod(total_seconds, 60)
            duration = f'{min:02}:{sec:02} {_t(audio_bps==16000,"l","h")}'
            md['DURATION'] = duration
        if 'SECONDS' not in md:
            md['SECONDS'] = str(total_seconds)

        return md


class A18File:
    """
    Encapsulates an audio file in .a18 format. Provides functions to:
    - Read any Talking Book metadata embedded in the file.
    - Read and/or update a "sidecar" associated with the file, containing metadata (in addition to the
      metadata embedded in the file.
    - Convert the audio file to another format, anything supported by ffmpeg.
    """

    def __init__(self, file_path: Path):
        self._file_path:Path = file_path
        self._metadata: Union[Dict[str, str], None] = None
        self._sidecar_needs_save = False
        self._sidecar_data: Dict[str, str] = {}
        self._sidecar_header: List[str] = []
        self._sidecar_loaded = False

    @property
    def path(self) -> Path:
        return self._file_path

    @property
    def metadata(self) -> Dict[str, str]:
        if self._metadata is None:
            self._metadata = MetadataReader.read_from_file(self._file_path)
        return self._metadata

    @property
    def sidecar_path(self) -> Path:
        return self._file_path.with_suffix('.properties')

    @property
    def has_sidecar(self) -> bool:
        return self.sidecar_path.exists()

    def property(self, name: str) -> Any:
        if not self._sidecar_loaded:
            self._load_sidecar()
        return self._sidecar_data.get(name)

    def update_sidecar(self) -> bool:
        # If there is a parallel "sidecar" .properties file, append the metadata to it.
        if self.has_sidecar:
            try:
                if not self._sidecar_loaded:
                    self._load_sidecar()

                # Add the filename to the metadata. It's little different from "IDENTIFIED", by having "_9-0_"
                # in the filename.
                metadata = self.metadata
                metadata['filename'] = self._file_path.stem

                # Ensure the a18 metadata is in the sidecar, tagged with 'metadata.' This operation is idempotent
                # because the metadata values are constant.
                self.add_to_sidecar(metadata, 'metadata.')

                # Compute a message UUID based on the collection's STATSUUID and all the metadata. If no STATSUUID
                # or no metadata, allocate a new UUID. (Note that the metadata will include the file name added
                # above.)
                if not self.property(MD_MESSAGE_UUID_TAG): # includes 'metadata.' tag.
                    self.add_to_sidecar({MD_MESSAGE_UUID_TAG: str(self._compute_message_uuid())})

                # Ensure deployment number is in the sidecar. This is performed at most one time.
                if not self.property(DEPLOYMENT_NUMBER_TAG):
                    deployment_number = query_deployment_number(self.property(PROJECT_TAG),
                                                                self.property(DEPLOYMENT_TAG))
                    self.add_to_sidecar({DEPLOYMENT_NUMBER_TAG: deployment_number})

                # Ensure the recipient info is in the sidecar, tagged with 'recipient.' This operation is not idempotent
                # because the recipient values on the server could have changed.
                recipient_info = query_recipient_info(self.property(RECIPIENTID_TAG))
                self.add_to_sidecar(recipient_info, 'recipient')

                self.save_sidecar()
                return True
            except Exception:
                pass
        return False

    def _load_sidecar(self) -> None:
        header: List[str] = []
        props: Dict[str, str] = {}
        with open(self.sidecar_path, "r") as sidecar_file:
            for line in sidecar_file:
                line = line.strip()
                if line[0] == '#':
                    header.append(line)
                else:
                    parts = line.split('=', maxsplit=1)
                    if len(parts) == 2:
                        props[parts[0].strip()] = parts[1].strip()
        self._sidecar_needs_save = False
        self._sidecar_loaded = True
        self._sidecar_header = header
        self._sidecar_data = props

    def save_sidecar(self):
        if self._sidecar_needs_save:
            if args.dry_run:
                print(f'Dry run, not saving sidecar \'{str(self.sidecar_path)}\'.')
            else:
                temp_path = self.sidecar_path.with_suffix('.new')
                with open(temp_path, "w") as properties_file:
                    for h in self._sidecar_header:
                        print(h, file=properties_file, end='\x0d\x0a')  # microsoft's original sin
                    for k in sorted(self._sidecar_data.keys()):
                        print(f'{k}={self._sidecar_data[k]}', file=properties_file, end='\x0d\x0a')
                temp_path.replace(self.sidecar_path)
            self._sidecar_needs_save = False

    def add_to_sidecar(self, data: Dict[str, str], tag: str = None) -> None:
        if not tag:
            tag = ''
        elif tag[-1] != '.':
            tag += '.'
        for k, v in data.items():
            tagged_key = f'{tag}{k}'
            if tagged_key not in self._sidecar_data or self._sidecar_data[tagged_key] != v:
                if args.verbose > 2:
                    print(f'Adding value to sidecar: "{tagged_key}"="{v}".')
                self._sidecar_data[tagged_key] = v
                self._sidecar_needs_save = True

    def export_audio(self, audio_format: str, output:Path = None) -> Union[Path, None]:
        """
        Export the .a18 file as the given format.
        :param audio_format: Anything that ffmpeg can produce
        :return: True if successful, False otherwise
        """

        if audio_format[0] != '.':
            audio_format = '.' + audio_format
        # if audio_format is ".mp3"
        # If _file_path is "/user/phil/uf/file1.a18"...
        audio_path = self._file_path.parent # /user/phil/uf
        source_name: str = self._file_path.name # file1.a18
        target_path = output
        target_path = output if target_path is not None else self._file_path
        target_dir = target_path.parent
        target_path = target_path.with_suffix(audio_format)
        target_name: str = target_path.name

        tdp = Path(target_dir, '.')
        print(f'Target dir: {target_dir}, exists:{target_dir.exists()}, is_dir:{target_dir.is_dir()}')
        print(f'tdp dir: {tdp}, exists:{tdp.exists()}, is_dir:{tdp.is_dir()}')

        if not target_dir.exists():
            print(f'Target directory does not exist: \'{str(target_dir)}\'.')
            return None
        elif target_dir.is_file():
            print(f'Target \'{str(target_dir)}\' is not a directory.')
            return None
        elif args.dry_run:
            print(f'Dry run, not exporting audio as \'{str(target_path)}\'.')
            return target_path

        elif args.ffmpeg:
            if args.verbose > 0:
                print(f'Exporting audio as \'{str(target_path)}\'.')
            # Run locally installed ffmpeg
            container = 'amplionetwork/abc:1.0'
            tmp_dir: tempfile.TemporaryDirectory = tempfile.TemporaryDirectory()
            abc_command = ['docker', 'run', '--rm', '--platform', 'linux/386',
                           '--mount', f'type=bind,source={audio_path}/.,target=/audio',
                           '--mount', f'type=bind,source={tmp_dir.name},target=/out',
                           container, '-o', '/out', source_name]
            abc_result = subprocess.run(abc_command, capture_output=True)
            if abc_result.returncode != 0:
                return None

            tempfile_pathname: str = f'{tmp_dir.name}/{source_name}.wav'  # ...tmp/foo.a18.wav
            target_pathname: str = str(self._file_path.with_suffix(audio_format))
            ff_command = ['ffmpeg', '-hide_banner', '-y', '-i', tempfile_pathname, target_pathname]
            if args.verbose > 1:
                print(' '.join(ff_command))
            ff_result = subprocess.run(ff_command, capture_output=True)
            return target_path if ff_result.returncode == 0 else None

        else:
            if args.verbose > 0:
                print(f'Exporting audio as \'{str(target_path)}\'.')
            # Run container provided ffmpeg
            platform_args = ['--platform', 'linux/386'] if platform.system().lower()=='darwin' else []
            container = 'amplionetwork/ac:1.0'
            ac_command = ['docker', 'run', '--rm'] + platform_args + \
                          ['--mount', f'type=bind,source={audio_path}/.,target=/audio', \
                            '--mount', f'type=bind,source={target_dir}/.,target=/out',
                            container, source_name, '/out/'+target_name]
            if args.verbose > 1:
                print(' '.join(ac_command))
            ac_result = subprocess.run(ac_command, capture_output=True)
            if ac_result.returncode != 0:
                if 'cannot connect to the docker daemon' in ac_result.stderr.decode('utf-8').lower():
                    print('It appears that Docker is not running.')
                    raise (Exception('It appears that Docker is not running.'))
            if args.verbose > 1:
                print(ac_result)
            return target_path if ac_result.returncode == 0 else None

    def _compute_message_uuid(self):
        """
        Computes or allocates a uuid for this message. If there is an existing uuid for the stats collection
        event, and an existing "IDENTIFIER" for the message, use that to compute a type 5 uuid (the IDENTIFIER
        should be unique, as it has the 
        :return:
        """
        metadata_string = ''.join(sorted(self._metadata.values()))
        collection_id = self.property('collection.STATSUUID')
        if collection_id and metadata_string:
            message_id = uuid.uuid5(NAMESPACE_UF, collection_id + metadata_string)
        else:
            print('Missing collection id or metadata; allocating uuid.')
            message_id = uuid.uuid4()
        return message_id


class A18Processor:
    def __init__(self, **kwargs):
        pass

    @staticmethod
    def process_file(a18_path: Path) -> None:
        global args
        if args.verbose > 0:
            print(f'Processing file \'{str(a18_path)}\'.')
        a18_file = A18File(a18_path)
        if a18_file.update_sidecar():
            audio_format = args.format
            if audio_format[0] != '.':
                audio_format = '.' + audio_format
            if 'feedback' in args:
                message_uuid = a18_file.property(MD_MESSAGE_UUID_TAG)
                fb_dir = Path(args.feedback, a18_file.property('PROJECT'), a18_file.property('DEPLOYMENT_NUMBER'))
                fb_path = Path(fb_dir, message_uuid).with_suffix(audio_format)
                md_path = fb_path.with_suffix('.properties')
                if args.dry_run:
                    print(f'Dry run, not exporting \'{str(fb_path)}\'.')
                else:
                    fb_dir.mkdir(parents=True, exist_ok=True)
                    # Converts the audio directly to the target location.
                    audio_path = a18_file.export_audio(audio_format, output=fb_path)
                    # Always copy a18_file.sidecar_path -> md_path, because we do not want to lose the metadata.
                    shutil.copyfile(a18_file.sidecar_path, md_path)
            elif 'convert' in args:
                a18_file.export_audio(args.format)

    def process_files(self, file_specs: List[Path]) -> Tuple[int, int, int, int]:
        """
        Given a Path to an a18 file, or a directory containing a18 files, process the file(s).
        :param file_specs: A path.
        :return: a tuple of the counts of directories and files processed, and the files skipped.
        """
        n_files: int = 0
        n_skipped: int = 0
        n_dirs: int = 0
        n_missing: int = 0
        remaining: List[Path] = [] + file_specs

        # for file_spec in file_specs:
        while len(remaining) > 0:
            file_spec: Path = remaining.pop()
            if not file_spec.exists():
                if file_spec in file_specs:
                    print(f'The given file \'{str(file_spec)}\' does not exist')
                n_missing += 1
            elif file_spec.is_file():
                if file_spec.suffix.lower() == '.a18':
                    n_files += 1
                    self.process_file(file_spec)
                    if n_files >= args.limit:
                        if args.verbose:
                            print(f'Limit reached, quitting. {n_files} files.')
                        break
                else:
                    n_skipped += 1
            else:
                n_dirs += 1
                if args.verbose > 1:
                    print(f'Adding files from directory \'{str(file_spec)}\'.')
                remaining.extend([f for f in file_spec.iterdir()])
        return n_dirs, n_files, n_skipped, n_missing


def main():
    global args
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--verbose', '-v', action='count', default=0, help="More verbose output.")
    arg_parser.add_argument('--dropbox', action=StorePathAction, trailing_slash=True, default='~/Dropbox',
                            help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('a18', metavar='a18', nargs='+', action=StorePathAction,
                            help='One or more a18 files and/or directories with .a18 files, '
                                 'from which to extract metadata.')
    arg_parser.add_argument('--format', choices=['mp3', 'aac', 'wma', 'wav', 'ogg'], default='mp3',
                            help='Audio format desired for the convert option.')

    arg_parser.add_argument('--ffmpeg', action='store_true', default=False, help='Use locally installed ffmpeg.')
    arg_parser.add_argument('--dry-run', '-n', action='store_true', default=False, help='Don\'t update anything.')
    arg_parser.add_argument('--limit', type=int, default=999999, help='Stop after N files. Default is unlimited.')

    command_group = arg_parser.add_mutually_exclusive_group()
    command_group.add_argument('--convert', action='store_true',
                            help='Convert .a18 files to another format, .mp3 by default.')
    command_group.add_argument('--feedback', action=StorePathAction,
                            help='Convert files as user feedback, into the given directory.')

    # database overrides
    arg_parser.add_argument('--db-host', default=None, metavar='HOST',
                            help='Optional host name, default from secrets store.')
    arg_parser.add_argument('--db-port', default=None, metavar='PORT',
                            help='Optional host port, default from secrets store.')
    arg_parser.add_argument('--db-user', default=None, metavar='USER',
                            help='Optional user name, default from secrets store.')
    arg_parser.add_argument('--db-password', default=None, metavar='PWD',
                            help='Optional password, default from secrets store.')
    arg_parser.add_argument('--db-name', default='dashboard', metavar='DB',
                            help='Optional database name, default "dashboard".')

    print(str(uuid.uuid4()))

    args = arg_parser.parse_args()
    if args.verbose > 2:
        print(f'Verbose setting: {args.verbose}.')

    a18_specs: List[Path] = args.a18
    processor = A18Processor()

    timer = -time.time_ns()
    n_dirs, n_files, n_skipped, n_missing = processor.process_files(a18_specs)
    timer += time.time_ns()

    print(f'Finished in {timer:,}ns')
    print(f'Processed {n_files} files in {n_dirs} directories. Skipped {n_skipped} non-a18 files. '
          f'{n_missing} files not found.')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
