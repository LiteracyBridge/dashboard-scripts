"""
filesprocessing.py

A simple helper to walk a directory tree and process every file within the tree.

Given a list of files and/or directories, examine all the files in the list, and
in all the directories, and in all of their sub-directories, etc.

For each file, call a predicate to determine whether to process or skip the file,
and if "process", call a passed function to perform the processing.
"""
from pathlib import Path
from typing import List, Callable, Tuple, Any


class FilesProcessor:
    def __init__(self, files: List[Path]):
        self._files = files

    def process_files(self, acceptor: Callable[[Path], bool] = lambda x: True,
                      processor: Callable[[Path], Any] = lambda x: None,
                      **kwargs) -> Tuple[int, int, int, int, int]:
        """
        Given a list Paths to a file or directory containing, process the file(s).
        :param file_specs: A list of Pathss
        :param acceptor: a callback to determine if a file should be processed. Default returns true.
        :return: a tuple of the counts of directories and files processed, and the files skipped.
        """
        verbose = kwargs.get('verbose', 0)
        limit = kwargs.get('limit', 1_000_000_000)
        remaining = kwargs.get('files', self._files)
        n_files: int = 0
        n_skipped: int = 0
        n_dirs: int = 0
        n_missing: int = 0
        n_errors: int = 0

        while len(remaining) > 0:
            file_spec: Path = remaining.pop()
            if not file_spec.exists():
                if file_spec in self._files:
                    print(f'The given file \'{str(file_spec)}\' does not exist')
                n_missing += 1
            elif file_spec.is_file():
                if acceptor(file_spec):
                    n_files += 1
                    process_result = processor(file_spec)
                    if process_result is False:
                        n_errors += 1
                    if n_files >= limit:
                        if verbose:
                            print(f'Limit reached, quitting. {n_files} files.')
                        break
                else:
                    n_skipped += 1
            else:
                n_dirs += 1
                if verbose > 1:
                    print(f'Adding files from directory \'{str(file_spec)}\'.')
                remaining.extend([f for f in file_spec.iterdir()])
        return n_dirs, n_files, n_skipped, n_missing, n_errors
