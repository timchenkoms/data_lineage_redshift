SET search_path TO {{currentschema}};
/*
drop table if exists dk_view_names_log;
create table dk_view_names_log(
view_name text,
view_schema text,
refreshtime timestamp
);
*/
truncate dk_view_names_log;

insert into dk_view_names_log(view_name, view_schema, refreshtime)
select viewname, schemaname, 
       CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp))
from PG_views;