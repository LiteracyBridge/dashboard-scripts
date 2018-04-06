select * from
(select count(timestamp) as nulls
from playstatistics
where recipientid is null) as foo,

(
select count(timestamp) as nons
from playstatistics
where recipientid is not null
) nulls
