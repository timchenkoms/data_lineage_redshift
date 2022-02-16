SET search_path TO {{currentschema}};
/*
drop table if exists dk_table_names_log;
create table dk_table_names_log(
table_id bigint,
table_name text,
table_schema text,
createtime timestamp
);
*/

-- save new table IDs
insert into dk_table_names_log(table_id, table_name, table_schema, createtime)
select
	table_id, table_name, table_schema,
	CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) as createtime
from (
	select table_id, "table" table_name, schema table_schema
	from svv_table_info
	minus
	select table_id, table_name, table_schema
	from dk_table_names_log
);

-- delete old log in case we rename tables and schemas
delete from dk_table_names_log
where createtime <> (select max(createtime) from dk_table_names_log last
					 where last.table_id = dk_table_names_log.table_id);
					 
-- delete log order two months
delete from dk_table_names_log
where createtime < add_months(getdate(),-2);

select count(1)
from (
	select table_id
	from dk_table_names_log
	group by table_id
	having count(1)>1
);
