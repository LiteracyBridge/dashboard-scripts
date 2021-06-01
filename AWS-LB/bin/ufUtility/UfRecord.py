from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Callable, Dict


def make_date_extractor(md_field: str) -> Callable:
    """
    Create and return a function that will extract a date, validate it, and return an ISO formatted
    date if it is valid, or an empty string if it is not.

    We need this because the "date recorded" field is directly from the Talking Book, and, as such,
    is very likely to contain garbage.
    :param md_field: The name of the field that may or may not contain a valid date.
    :return: a function that accepts a Dict[str,str] and returns a str containing an ISO date.
    """

    def extract(props: Dict[str, str]) -> str:
        ds = ''
        v = props.get(md_field, '')
        try:
            d = datetime.strptime(v, '%Y/%m/%d')
            ds = d.strftime('%Y%m%d')
        except Exception:
            pass
        return ds

    return extract


def _make_early_date() -> datetime:
    return datetime(2018, 1, 1)


uf_column_map = {
    # column:name : property_name or [prop1, prop2, ...]
    'message_uuid': 'metadata.MESSAGE_UUID',

    # 'deployment_uuid': 'DEPLOYEDUUID', # Timestamp is probably sufficient
    'programid': 'PROJECT',
    'deploymentnumber': 'DEPLOYMENT_NUMBER',
    'recipientid': 'RECIPIENTID',
    'talkingbookid': 'TALKINGBOOKID',
    'deployment_tbcdid': 'TBCDID',
    'deployment_timestamp': 'TIMESTAMP',
    'deployment_user': 'USERNAME',
    'test_deployment': 'TESTDEPLOYMENT',
    # 'collection_uuid': 'collection.STATSUUID', # Timestamp is probably sufficient
    'collection_tbcdid': 'collection.TBCDID',
    'collection_timestamp': 'collection.TIMESTAMP',
    'collection_user': ['collection.USEREMAIL', 'collection.USERNAME'],
    'length_seconds': 'metadata.SECONDS',
    'length_bytes': 'metadata.BYTES',
    'language': 'metadata.LANGUAGE',
    'date_recorded': make_date_extractor('metadata.DATE_RECORDED'),
    'relation': 'metadata.RELATION',
    'bundle_uuid': lambda x:None
}
# @formatter:off
# for any column, provide code to tweak the value as needed.
uf_column_tweaks_map = {
    'deployment_timestamp': lambda x: x or '180101',
    'test_deployment':      lambda x: x or 'f',
    'collection_timestamp': lambda x: x or '180103',
    'language':             lambda x: (x or '').lower(),
    'date_recorded':        lambda x: x or '180102'
}
# @formatter:on

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
    bundle_uuid: str = None