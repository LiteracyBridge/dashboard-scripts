from _testcapi import INT_MAX
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Tuple

from UfRecord import uf_column_map, uf_column_tweaks_map
from dbutils import DbUtils
from filesprocessor import FilesProcessor

MD_MESSAGE_UUID_TAG = 'metadata.MESSAGE_UUID'



class UfMetadata():
    _instance = None

    # This class implements a singleton object.
    def __new__(cls):
        if cls._instance is None:
            print('Creating the UfPropertiesProcessor object')
            cls._instance = super(UfMetadata, cls).__new__(cls)
            cls._props: List[Tuple] = []
        return cls._instance

    def print(self):
        print(','.join(uf_column_map.keys()))
        for line in self._props:
            print(','.join(line))

    def commit(self):
        db = DbUtils()
        db.insert_uf_records(self._props)

    def _add_props(self, props: Dict[str, str]) -> None:
        """
        Adds a Dict[str,str] of pairs to the list of UF metadata. The metadata must contain
        a property for metadata.MESSAGE_UUID.
        :param props: The props to be imported.
        """
        columns = {}
        if MD_MESSAGE_UUID_TAG in props:
            # for each column that we want...
            for column_name in uf_column_map.keys():
                prop_name = uf_column_map[column_name]
                val = ''
                if isinstance(prop_name, str):
                    # The property name from which to get the column's value, or ...
                    val = props.get(prop_name, '')
                elif callable(prop_name):
                    # a function to call to get the column's value, or ...
                    v = prop_name(props)
                    columns[column_name] = v
                else:
                    # List of properties, in priority order. Take the first one found.
                    for pn in prop_name:
                        if pn in props:
                            val = props[pn]
                            break
                if column_name in uf_column_tweaks_map:
                    val = uf_column_tweaks_map[column_name](val)
                columns[column_name] = val
            self._props.append(tuple([columns[k] for k in uf_column_map.keys()]))

    def _process_file(self, path: Path) -> None:
        """
        Processes one .properties file.
        :param path: Path to the file to be read and processed.
        """
        props: Dict[str, str] = {}
        with open(path, 'r') as props_file:
            for prop_line in props_file:
                parts: List[str] = prop_line.strip().split('=', maxsplit=1)
                if len(parts) != 2 or parts[0][0] == '#':
                    continue
                props[parts[0]] = parts[1]
        self._add_props(props)

    def add_from_files(self, files: List[Path] = None, **kwargs) -> Tuple[int, int, int, int, int]:
        """
        Given a Path to an a18 file, or a directory containing a18 files, process the file(s).
        :param files: An optional list of files to process.
        :return: a tuple of the counts of directories and files processed, and the files skipped.
        """

        def file_acceptor(p: Path) -> bool:
            return p.suffix.lower() == '.properties'

        def file_processor(p: Path) -> None:
            self._process_file(p)

        processor: FilesProcessor = FilesProcessor(files)

        return processor.process_files(file_acceptor, file_processor, limit=kwargs.get('limit', INT_MAX),
                                       verbose=kwargs.get('verbose', 0), files=files)

    def add_from_dict(self, props: Dict[str, str]) -> None:
        self._add_props(props)