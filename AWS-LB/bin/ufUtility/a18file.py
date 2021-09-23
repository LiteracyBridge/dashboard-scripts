import platform
import struct
import subprocess
import tempfile
import uuid as uuid
from pathlib import Path
from typing import Dict, Union, List, Any

import dbutils

PUBLISHER_TAG = 'PUBLISHER'
COMMUNITY_TAG = 'COMMUNITY'
TALKINGBOOKID_TAG = 'TALKINGBOOKID'
DEPLOYMENT_NUMBER_TAG = 'DEPLOYMENT_NUMBER'
DEPLOYMENT_TAG = 'DEPLOYMENT'
MD_MESSAGE_UUID_TAG = 'metadata.MESSAGE_UUID'
PROJECT_TAG = 'PROJECT'
RECIPIENTID_TAG = 'RECIPIENTID'
STATS_UUID_TAG = 'collection.STATSUUID'
NAMESPACE_UF = uuid.UUID('677aba79-e672-4fe3-91d5-c69306fe025d')


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

    # noinspection PyShadowingBuiltins
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

        def _t(a, b, c):
            """
            return a ? b : c
            :param a: The condition.
            :param b: Value if True.
            :param c: Value if False.
            :return: the value.
            """
            return b if a else c

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
        md: Dict[str, str] = md_parser.parse()
        total_seconds = int(audio_len * 8 / audio_bps + 0.5)
        if 'DURATION' not in md:
            minutes, seconds = divmod(total_seconds, 60)
            duration = f'{minutes:02}:{seconds:02} {_t(audio_bps == 16000, "l", "h")}'
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

    def __init__(self, file_path: Path, **kwargs):
        self._verbose = kwargs.get('verbose', 0)
        self._dry_run = kwargs.get('dry_run', False)
        self._local_ffmpeg = kwargs.get('ffmpeg', False)
        self._db_utils = dbutils.DbUtils()
        self._file_path: Path = file_path
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

    def property(self, name: str, default: str = None) -> Any:
        if not self._sidecar_loaded:
            self._load_sidecar()
        return self._sidecar_data.get(name, default)

    def update_sidecar(self, creating: bool = False, save: bool = True) -> bool:
        # If there is a parallel "sidecar" .properties file, append the metadata to it.
        if self.has_sidecar or creating:
            try:
                if not self._sidecar_loaded and not creating:
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
                if not self.property(MD_MESSAGE_UUID_TAG):  # includes 'metadata.' tag.
                    self.add_to_sidecar({MD_MESSAGE_UUID_TAG: str(self._compute_message_uuid())})

                # Ensure deployment number is in the sidecar. This is performed at most one time.
                if not self.property(DEPLOYMENT_NUMBER_TAG):
                    deployment_number = self._db_utils.query_deployment_number(self.property(PROJECT_TAG),
                                                                               self.property(DEPLOYMENT_TAG))
                    self.add_to_sidecar({DEPLOYMENT_NUMBER_TAG: deployment_number})

                # Ensure the recipient info is in the sidecar, tagged with 'recipient.' This operation is not idempotent
                # because the recipient values on the server could have changed.
                recipient_info = self._db_utils.query_recipient_info(self.property(RECIPIENTID_TAG))
                self.add_to_sidecar(recipient_info, 'recipient')

                if save:
                    self.save_sidecar()
                return True
            except Exception as ex:
                print(f'Exception updating sidecar for \'{str(self._file_path)}\': {str(ex)}')
        return False

    def create_sidecar(self, recipientid: str, programid: str, deploymentnumber: int, community: str) -> bool:
        """
        Creates a sidecar "out of whole cloth". Given a few arguments, and the contents of the .a18 file's
        metadata, we can create a useful sidecar. This is targeted pretty closely at user feedback processing.
        :param recipientid: The recipeientid of the "community" where the user feedback was recorded.
        :param programid: The programid to which the feedback applies.
        :param deploymentnumber: The deployemnt number to which the feedback applies.
        :param community: The "community" of the recipient. This was a random mis-mash of actual community
                        name and group name. It was, however, constant over the life of a project.
        :return:
        """
        # Add enough that update_sidecar() can do its thing.
        try:
            md = {
                RECIPIENTID_TAG: recipientid,
                PROJECT_TAG: programid,
                DEPLOYMENT_NUMBER_TAG: str(deploymentnumber),
                TALKINGBOOKID_TAG: self.metadata.get(PUBLISHER_TAG),
                COMMUNITY_TAG: community.upper()
            }
            self._sidecar_loaded = True  # in a manner of speaking
            self._sidecar_needs_save = True
            self.add_to_sidecar(md)
            self.update_sidecar(creating=True, save=False)
            self.save_sidecar()
        except Exception as ex:
            return False
        return True

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

    def save_sidecar(self, save_as: Union[Path, None] = None, extra_data: Union[None, Dict[str, str]] = None) \
            -> Dict[str, str]:
        """
        Saves the side car. May be optionally saved to another location; if so, may optionally have additional
        key=value pairs added.
        :param save_as: Optional file Path to which to save the sidecar data.
        :param extra_data: Optional extra key=value pair(s) to be added when using save_as.
        :return: The net data saved.
        """
        if save_as is None and extra_data is not None:
            raise (ValueError('extra_data without save_as'))
        to_write = {k: v for k, v in self._sidecar_data.items()}
        if self._sidecar_needs_save or save_as:
            if extra_data:
                for k, v in extra_data.items():
                    to_write[k] = v
            save_path: Path = save_as or self.sidecar_path
            if self._dry_run:
                print(f'Dry run, not saving sidecar \'{str(save_path)}\'.')
            else:
                temp_path = save_path.with_suffix('.new')
                with open(temp_path, "w") as properties_file:
                    for h in self._sidecar_header:
                        print(h, file=properties_file, end='\x0d\x0a')  # microsoft's original sin
                    for k in sorted(to_write.keys()):
                        print(f'{k}={to_write[k]}', file=properties_file, end='\x0d\x0a')
                temp_path.replace(save_path)
            # If we saved to the default location, the metadata is no longer "dirty".
            if save_as is not None:
                self._sidecar_needs_save = False
        return to_write

    def add_to_sidecar(self, data: Dict[str, str], tag: str = None) -> None:
        if not tag:
            tag = ''
        elif tag[-1] != '.':
            tag += '.'
        for k, v in data.items():
            tagged_key = f'{tag}{k}'
            if tagged_key not in self._sidecar_data or self._sidecar_data[tagged_key] != v:
                if self._verbose > 2:
                    print(f'Adding value to sidecar: "{tagged_key}"="{v}".')
                self._sidecar_data[tagged_key] = v
                self._sidecar_needs_save = True

    def export_audio(self, audio_format: str, output: Path = None, mk_dirs: bool = True) -> Union[Path, None]:
        """
        Export the .a18 file as the given format.
        :param mk_dirs: if True, create parent directories as needed.
        :param output: if provided, write the output file here.
        :param audio_format: Anything that ffmpeg can produce
        :return: True if successful, False otherwise
        """

        if audio_format[0] != '.':
            audio_format = '.' + audio_format
        # if audio_format is ".mp3"
        # If _file_path is "/user/phil/uf/file1.a18"...
        audio_path = self._file_path.parent  # /user/phil/uf
        source_name: str = self._file_path.name  # file1.a18
        target_path = output
        target_path = output if target_path is not None else self._file_path
        target_dir = target_path.parent
        target_path = target_path.with_suffix(audio_format)
        target_name: str = target_path.name

        tdp = Path(target_dir, '.')
        print(f'Target dir: {target_dir}, exists:{target_dir.exists()}, is_dir:{target_dir.is_dir()}')
        print(f'tdp dir: {tdp}, exists:{tdp.exists()}, is_dir:{tdp.is_dir()}')

        if not target_dir.exists() and not mk_dirs:
            print(f'Target directory does not exist: \'{str(target_dir)}\'.')
            return None
        elif target_dir.is_file():
            print(f'Target \'{str(target_dir)}\' is not a directory.')
            return None
        elif self._dry_run:
            print(f'Dry run, not exporting audio as \'{str(target_path)}\'.')
            return target_path

        if not target_dir.exists():
            target_dir.mkdir(parents=True, exist_ok=True)

        if self._local_ffmpeg:
            if self._verbose > 0:
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
            if self._verbose > 1:
                print(' '.join(ff_command))
            ff_result = subprocess.run(ff_command, capture_output=True)
            return target_path if ff_result.returncode == 0 else None

        else:
            if self._verbose > 0:
                print(f'Exporting audio as \'{str(target_path)}\'.')
            # Run container provided ffmpeg
            platform_args = ['--platform', 'linux/386'] if platform.system().lower() == 'darwin' else []
            container = 'amplionetwork/ac:1.0'
            ac_command = ['docker', 'run', '--rm'] + platform_args + \
                         ['--mount', f'type=bind,source={audio_path}/.,target=/audio', \
                          '--mount', f'type=bind,source={target_dir}/.,target=/out',
                          container, source_name, '/out/' + target_name]
            if self._verbose > 1:
                print(' '.join(ac_command))
            ac_result = subprocess.run(ac_command, capture_output=True)
            if ac_result.returncode != 0:
                if 'cannot connect to the docker daemon' in ac_result.stderr.decode('utf-8').lower():
                    print('It appears that Docker is not running.')
                    raise (Exception('It appears that Docker is not running.'))
            if self._verbose > 1:
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
        collection_id = self.property(STATS_UUID_TAG, '')
        if metadata_string:
            message_id = uuid.uuid5(NAMESPACE_UF, collection_id + metadata_string)
        else:
            print('Missing collection id or metadata; allocating uuid.')
            message_id = uuid.uuid4()
        return message_id
