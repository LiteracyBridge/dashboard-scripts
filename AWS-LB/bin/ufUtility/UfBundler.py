import io
import uuid
import zipfile
from dataclasses import field, dataclass
from io import StringIO
from typing import List, Dict

import boto3

from UfRecord import UfRecord
from dbutils import DbUtils

s3 = boto3.client('s3')


def t(s):
    """
    Format time 'h:mm:ss', 'mm:ss', or 'ss seconds'
    :param s: time in seconds
    :return: time as formatted string
    """
    h, m = divmod(s, 3600)
    m, s = divmod(m, 60)
    if h:
        return f'{h}:{m:02}:{s:02}'
    if m:
        return f'{m}:{s:02}'
    return f'{s} seconds'


@dataclass
class BundleInfo:
    language: str = ''
    uf_list: List[UfRecord] = field(default_factory=list)
    seconds: int = 0
    bytes: int = 0
    bundle_uuid: str = ''


class UfBundler():
    def __init__(self, programid: str, deployment_number: int, max_files: int, max_bytes: int, min_uf_duration: int = 5,
                 max_uf_duration: int = 300, bucket: str = None):
        self._programid = programid
        self._deployment_number = deployment_number
        self._max_files = max_files
        self._max_bytes = max_bytes
        self._min_uf_duration = min_uf_duration
        self._max_uf_duration = max_uf_duration
        self._out_bucket = bucket

        self._db = DbUtils(args=None)

    def _make_zipped_bundle(self, bundle: BundleInfo) -> bytes:
        # get the output bucket and prefix
        input_bucket = 'amplio-uf'
        input_prefix = f'collected/{self._programid}/{self._deployment_number}/'
        out_buffer = io.BytesIO()
        with zipfile.ZipFile(out_buffer, "w", compression=zipfile.ZIP_STORED, allowZip64=False) as out_zip:
            for uf in bundle.uf_list:
                filename = uf.message_uuid + '.mp3'
                input_key = input_prefix + filename
                input_obj = s3.get_object(Bucket=input_bucket, Key=input_key)
                input_data = input_obj.get('Body').read()
                out_zip.writestr(filename, input_data)
            out_zip.close()
        return out_buffer.getvalue()

    def _make_zipped_bundles(self, bundles: List[BundleInfo]) -> str:
        all_ok = True
        # get the output bucket and prefix
        bucket = self._out_bucket or 'downloads.amplio.org'
        root = str(uuid.uuid4())
        for bundle in bundles:
            key = f'{root}/deployment-{self._deployment_number}/{bundle.language}/{bundle.bundle_uuid}.zip'
            bundle_data = self._make_zipped_bundle(bundle)
            put_result = s3.put_object(Bucket=bucket, Key=key, Body=bundle_data)
        return root

    def _get_uf_records(self, rebundle=False) -> List[UfRecord]:
        def keep(x: UfRecord) -> bool:
            return (x.bundle_uuid is None or rebundle) \
                   and x.length_seconds >= self._min_uf_duration and x.length_seconds <= self._max_uf_duration

        rows: List[UfRecord] = self._db.get_uf_records(programid=self._programid,
                                                       deploymentnumber=self._deployment_number)

        good_uf: List[UfRecord] = [x for x in rows if keep(x)]
        print(f'Received {len(rows)} rows, {len(good_uf)} meet length criteria.')
        return good_uf

    def _get_partitioned_records(self, bundle_uuids=None, rebundle=False) -> List[BundleInfo]:
        rows: List[UfRecord] = self._db.get_uf_records(programid=self._programid,
                                                       deploymentnumber=self._deployment_number)

        # previously bundled messages, limited to a set of existing uuids, if desired
        good_uf: List[UfRecord] = [x for x in rows if x.bundle_uuid is not None and (
                    bundle_uuids is None or x.bundle_uuid in bundle_uuids)]

        print(f'Received {len(rows)} rows, {len(good_uf)} already bundled.')
        # put uf records into their respective bundles
        bundles: Dict[str, BundleInfo] = {}
        for uf in good_uf:
            bundle = bundles.setdefault(uf.bundle_uuid, BundleInfo(language=uf.language, bundle_uuid=uf.bundle_uuid))
            bundle.uf_list.append(uf)
            bundle.bytes += uf.length_bytes
            bundle.seconds += uf.length_seconds
        return list(bundles.values())

    def _partition(self, good_uf: List[UfRecord]) -> List[BundleInfo]:
        partitions: List[BundleInfo] = []
        current_partitions: Dict[str, BundleInfo] = {}
        for ix in range(0, len(good_uf)):
            uf = good_uf[ix]
            language = uf.language
            current_partition = current_partitions.setdefault(language, BundleInfo(language=language))
            # Does this message fit?
            if current_partition.bytes + uf.length_bytes > self._max_bytes and len(current_partition.uf_list) > 0:
                # Didn't fit, save previous partition, create a new, empty one for this language
                partitions.append(current_partition)
                current_partition = BundleInfo(language=language)
                current_partitions[language] = current_partition
            # Add message to current partition.
            current_partition.uf_list.append(uf)
            current_partition.bytes += uf.length_bytes
            current_partition.seconds += uf.length_seconds
        # Capture the partitions that were "in progress"
        for p in current_partitions.values():
            partitions.append(p)

        return partitions

    def _update_with_bundle_uuid(self, bundles: List[BundleInfo]):
        # allocate a bundle id for the partitions
        for b in bundles:
            b.bundle_uuid = uuid.uuid4()
        # and update the database
        bundles = {b.bundle_uuid: [m.message_uuid for m in b.uf_list] for b in bundles}
        result = self._db.update_uf_bundles(programid=self._programid, deploymentnumber=self._deployment_number,
                                            bundles=bundles)
        return result

    def make_bundles(self, **kwargs):
        zip = kwargs.get('zip', True)
        only_zip = kwargs.get('only_zip', False)
        rebundle= kwargs.get('rebundle', False)
        if only_zip:
            bundles: List[BundleInfo] = self._get_partitioned_records(bundle_uuids=kwargs.get('bundle_uuid', []), rebundle=rebundle)
        else:
            good_uf: List[UfRecord] = self._get_uf_records(rebundle=rebundle)
            bundles: List[BundleInfo] = self._partition(good_uf)
            self._update_with_bundle_uuid(bundles)

        if zip:
            path = self._make_zipped_bundles(bundles)
            print(f'path: {path}')

        print(f'{len(bundles)} bundles:')
        for p in bundles:
            print(f'   {p.language}: {len(p.uf_list)} files, {t(p.seconds)} total, {p.bytes:,} bytes.')

        # Call partitions "directories" and good_uf "files"
        return len(bundles), sum([len(b.uf_list) for b in bundles]), 0, 0, 0  # n_dirs, n_files, n_skipped, n_missing, n_errors
