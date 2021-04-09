from pathlib import Path
from typing import List, Tuple, Dict

from dbutils import uf_column_map, DbUtils, MD_MESSAGE_UUID_TAG
from filesprocessor import FilesProcessor


class UfPropertiesProcessor():
    _instance = None

    def __new__(cls, **kwargs):
        if cls._instance is None:
            print('Creating the UfPropertiesProcessor object')
            cls._instance = super(UfPropertiesProcessor, cls).__new__(cls)
            cls._props: List[Tuple] = []
            cls._args = kwargs.get('args')
        return cls._instance

    def print(self):
        print(','.join(uf_column_map.keys()))
        for line in self._props:
            print(','.join(line))

    def commit(self):
        db = DbUtils(args=self._args)
        db.insert_uf_records(self._props)

    def _add_props(self, props: Dict[str, str]) -> None:
        """
        Adds a Dict[str,str] of pairs to the list of UF metadata. The metadata must contain
        a property for metadata.MESSAGE_UUID.
        :param props: The props to be imported.
        """
        columns = {}
        if MD_MESSAGE_UUID_TAG in props:
            for column_name in uf_column_map.keys():
                prop_name = uf_column_map[column_name]
                if isinstance(prop_name, str):
                    columns[column_name] = props.get(prop_name, '')
                elif callable(prop_name):
                    v = prop_name(props)
                    columns[column_name] = v
                else:
                    # List of properties, in priority order. Take the first one found.
                    columns[column_name] = ''
                    for pn in prop_name:
                        if pn in props:
                            columns[column_name] = props[pn]
                            break
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

    def add_from_files(self, files: List[Path] = None) -> Tuple[int, int, int, int, int]:
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

        return processor.process_files(file_acceptor, file_processor, limit=self._args.limit, verbose=self._args.verbose,
                                       files=files)

    def add_from_dict(self, props: Dict[str, str]) -> None:
        self._add_props(props)