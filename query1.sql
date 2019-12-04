--COMPUTE STATS ON ALL
-- CREATE PARQUET VERSIONS OF ALL

--select count(*) from (
use cms;
select arzt, 
sum(auszahlung_euro) num
from auszahlungen 
group by arzt
order by num desc
limit 50
