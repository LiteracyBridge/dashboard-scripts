import os
from pathlib import Path
from typing import List, Union, Any, Tuple, Dict

from UfMetadata import UfMetadata
from a18file import A18File, MD_MESSAGE_UUID_TAG
from filesprocessor import FilesProcessor


class A18Processor(FilesProcessor):
    def __init__(self, files: List[Path]):
        super().__init__(files)

    @staticmethod
    def _a18_acceptor(p: Path) -> bool:
        return p.suffix.lower() == '.a18'

    def extract_uf_files(self, out_dir:Path, **kwargs) -> Tuple[int, int, int, int, int]:
        def _a18_processor(a18_path: Path) -> Union[None,bool]:
            if verbose > 0:
                print(f'Processing file \'{str(a18_path)}\'.')
            a18_file = A18File(a18_path, verbose=verbose, dry_run=dry_run)
            if a18_file.update_sidecar():
                message_uuid = a18_file.property(MD_MESSAGE_UUID_TAG)
                programid = a18_file.property('PROJECT')
                deploymentnumber = a18_file.property('DEPLOYMENT_NUMBER')
                if not (programid and deploymentnumber):
                    print(f'Missing value for "PROJECT" or "DEPLOYMENT_NUMBER" in .properties for {a18_path.name}')
                    return False
                fb_dir = Path(out_dir, programid, deploymentnumber)
                fb_path = Path(fb_dir, message_uuid).with_suffix(audio_format)
                md_path = fb_path.with_suffix('.properties')
                if dry_run:
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
                            if verbose > 1:
                                print(f'Adding metadata properties for {str(a18_path)}.')
                            propertiesProcessor.add_from_dict(metadata)
            else:
                print(f'Couldn\'t update sidecar for \'{str(a18_path)}\'.')
        propertiesProcessor = UfMetadata()
        no_db = kwargs.get('no_db', False)
        audio_format = kwargs.get('format')
        verbose = kwargs.get('verbose', 0)
        dry_run = kwargs.get('dry_run', False)
        kw: Dict[str, str] = {k: v for k, v in kwargs.items() if k in ['limit', 'verbose', 'files']}

        return self.process_files(A18Processor._a18_acceptor, _a18_processor, **kw)

    def convert_a18_files(self, **kwargs) -> Tuple[int, int, int, int, int]:
        def _a18_processor(a18_path: Path) -> None:
            if verbose > 0:
                print(f'Processing file \'{str(a18_path)}\'.')
            a18_file = A18File(a18_path, verbose=verbose, dry_run=dry_run)
            # TODO: Why do we need to update the sidecar to export the audio in a new format?
            if a18_file.update_sidecar():
                a18_file.export_audio(audio_format)

        audio_format = kwargs.get('format')
        verbose = kwargs.get('verbose', 0)
        dry_run = kwargs.get('dry_run', False)
        kw: Dict[str, str] = {k: v for k, v in kwargs.items() if k in ['limit', 'verbose', 'files']}

        return self.process_files(A18Processor._a18_acceptor, _a18_processor, **kw)
