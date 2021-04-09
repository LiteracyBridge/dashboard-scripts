import os
from pathlib import Path
from typing import List, Union, Any, Tuple, Dict

from UfPropertiesProcessor import UfPropertiesProcessor
from a18file import A18File, MD_MESSAGE_UUID_TAG
from filesprocessor import FilesProcessor


class A18Processor(FilesProcessor):
    def __init__(self, files: List[Path], args: None):
        super().__init__(files)
        self._function = None
        self._args = args

    def process_file(self, a18_path: Path) -> None:
        """
        Process one User Feedback .a18 file. There must be a sidecar present for the .a18 file.

        The file is converted to the desired audio format, .mp3 by default.

        If the --feedback DIR command line argument was specified, the file is exported as the
        desired audio format and placed into the directory specified with the --feedback DIR
        command line argument. The metadata sidecar is updated with whatever can be extracted
        from the file, and an entry is added to the metadata file with the size of the
        resulting audio file, and the updated metadata sidecar is added to the same directory
        as the output audio file.

        If the --convert command line argument was specified, the file is exported as the
        desired audio format and placed next to the original file. Additionally, the metadata
        sidecar is updated with whatever can be extracted from the file.
        :param a18_path:
        :return:
        """
        if self._args.verbose > 0:
            print(f'Processing file \'{str(a18_path)}\'.')
        propertiesProcessor = UfPropertiesProcessor(args=self._args)
        a18_file = A18File(a18_path, self._args)
        if a18_file.update_sidecar():
            audio_format = self._args.format
            if audio_format[0] != '.':
                audio_format = '.' + audio_format
            if self._function == 'extract':
                message_uuid = a18_file.property(MD_MESSAGE_UUID_TAG)
                fb_dir = Path(self._args.out, a18_file.property('PROJECT'), a18_file.property('DEPLOYMENT_NUMBER'))
                fb_path = Path(fb_dir, message_uuid).with_suffix(audio_format)
                md_path = fb_path.with_suffix('.properties')
                if self._args.dry_run:
                    print(f'Dry run, not exporting \'{str(fb_path)}\'.')
                    print(f'Dry run, not saving metadata \'{str(md_path)}\'.')
                else:
                    # Converts the audio directly to the target location.
                    audio_path: Union[Path, Any] = a18_file.export_audio(audio_format, output=fb_path)
                    # Save the size of the file, to be used when assembling bundles of uf files.
                    if audio_path and audio_path.exists():
                        # Save a copy of the metadata, augmented with the audio file size.
                        metadata = a18_file.save_sidecar(save_as=md_path, extra_data={
                            'metadata.BYTES': str(os.path.getsize(audio_path))})
                        propertiesProcessor.add_from_dict(metadata)
            elif self._function == 'convert':
                a18_file.export_audio(self._args.format)

    @staticmethod
    def _a18_acceptor(p: Path) -> bool:
        return p.suffix.lower() == '.a18'

    def process(self, function: str, a18_acceptor=None, a18_processor=None) -> Tuple[int, int, int, int, int]:
        """
        Given a Path to an a18 file, or a directory containing a18 files, process the file(s).
        :return: a tuple of the counts of directories and files processed, and the files skipped.
        """

        def _a18_processor(a18_path: Path) -> None:
            self.process_file(a18_path)

        a18_acceptor = a18_acceptor or A18Processor._a18_acceptor
        a18_processor = a18_processor or _a18_processor
        self._function = function
        return self.process_files(a18_acceptor, a18_processor, limit=self._args.limit, verbose=self._args.verbose)

    def extract_uf_files(self, **kwargs) -> Tuple[int, int, int, int, int]:
        def _a18_processor(a18_path: Path) -> Union[None,bool]:
            if verbose > 0:
                print(f'Processing file \'{str(a18_path)}\'.')
            a18_file = A18File(a18_path, self._args)
            if a18_file.update_sidecar():
                message_uuid = a18_file.property(MD_MESSAGE_UUID_TAG)
                programid = a18_file.property('PROJECT')
                deploymentnumber = a18_file.property('DEPLOYMENT_NUMBER')
                if not (programid and deploymentnumber):
                    print(f'Missing value for "PROJECT" or "DEPLOYMENT_NUMBER" in .properties for {a18_path.name}')
                    return False
                fb_dir = Path(self._args.out, programid, deploymentnumber)
                fb_path = Path(fb_dir, message_uuid).with_suffix(audio_format)
                md_path = fb_path.with_suffix('.properties')
                if self._args.dry_run:
                    print(f'Dry run, not exporting \'{str(fb_path)}\'.')
                    print(f'Dry run, not saving metadata \'{str(md_path)}\'.')
                else:
                    # Converts the audio directly to the target location.
                    audio_path: Union[Path, Any] = a18_file.export_audio(audio_format, output=fb_path)
                    # Save the size of the file, to be used when assembling bundles of uf files.
                    if audio_path and audio_path.exists():
                        # Save a copy of the metadata, augmented with the audio file size.
                        metadata = a18_file.save_sidecar(save_as=md_path, extra_data={
                            'metadata.BYTES': str(os.path.getsize(audio_path))})
                        if not no_db:
                            propertiesProcessor.add_from_dict(metadata)

        propertiesProcessor = UfPropertiesProcessor(args=self._args)
        no_db = kwargs.get('no_db', False)
        audio_format = kwargs.get('format')
        verbose = kwargs.get('verbose', 0)
        kw: Dict[str, str] = {k: v for k, v in kwargs.items() if k in ['limit', 'verbose', 'files']}

        return self.process_files(A18Processor._a18_acceptor, _a18_processor, **kw)

    def convert_a18_files(self, **kwargs) -> Tuple[int, int, int, int, int]:
        def _a18_processor(a18_path: Path) -> None:
            if verbose > 0:
                print(f'Processing file \'{str(a18_path)}\'.')
            a18_file = A18File(a18_path, self._args)
            if a18_file.update_sidecar():
                a18_file.export_audio(audio_format)

        audio_format = kwargs.get('format')
        verbose = kwargs.get('verbose', 0)
        kw: Dict[str, str] = {k: v for k, v in kwargs.items() if k in ['limit', 'verbose', 'files']}

        return self.process_files(A18Processor._a18_acceptor, _a18_processor, **kw)
