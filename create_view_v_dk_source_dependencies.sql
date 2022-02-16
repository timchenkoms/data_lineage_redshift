SET search_path TO {{redshift.data_sources_schema}};

drop view if exists v_dk_source_dependencies cascade;
create or replace view v_dk_source_dependencies
as
select
  	d_main.runtime,
	d_main.xid,
	d_main.userid,
	d_main.querytype,
	d_main.querymd5,
	d_main.querytext,
	d_main.target_table,
    decode(d_child.querytype, 'create temp table',d_child.source_table, d_main.source_table) as source_table,
	d_main.target_schema,
	d_main.source_schema,
	d_main.username
from {{redshift.data_sources_schema}}.dk_source_dependencies d_main
  left join (select distinct target_table, source_table, querytype
             from {{redshift.data_sources_schema}}.dk_source_dependencies
             where querytype = 'create temp table'
			   and source_table not ilike 'volt_tt%') d_child
  on d_main.source_table = d_child.target_table
  where d_main.querytype <> 'create temp table'
    and decode(d_child.querytype, 'create temp table',d_child.source_table, d_main.source_table) not ilike 'volt_tt%';
  
select count(*)
from v_dk_source_dependencies;