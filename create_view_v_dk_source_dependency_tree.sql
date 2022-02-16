SET search_path TO {{redshift.data_sources_schema}};

drop view if exists v_dk_source_dependency_tree cascade;
create or replace view v_dk_source_dependency_tree(
    root_page_name, table_order, root_table, root_schema, root_page_id, id, level, runtime, target_table, source_table, status, page_id, page_name)
as
WITH RECURSIVE dependecies(root_page_name, table_order, root_table, root_schema, root_page_id, id, level, runtime, target_table, source_table, status, page_id, page_name) AS (
    SELECT sdt_groups.page_name                                                 AS root_page_name,
           sdt_groups.table_order,
           sdt.table_name                                                        AS root_table,
	       sdt.table_schema                                                      AS root_schema,
	       sdt_groups.page_id                                                    AS root_page_id,
           cast(left(sdt.table_name, 4000) as varchar(4000))                     AS id,
           cast(0 as int)                                                        AS level,
           max(sd.runtime)                                                       AS runtime,
           cast(NULL as varchar(256))                                            AS target_table,
           sdt.table_name                                                        AS source_table,
           cast('root' as varchar(256))                                          AS status,
	       sdt_groups.page_id                                                    AS page_id,
	       sdt_groups.page_name                                                  AS page_name
    FROM (select schema table_schema, "table" table_name 
		  from svv_table_info
          where schema in ('hem_daily_current','car_t', 'car_t_rpt')
		  --schema not in ('pg_catalog','information_schema')
            and "table" not ilike 'raw_%'
            and "table" not ilike 'tmp_%'
            and "table" not ilike 'temp_%'
            and "table" not ilike 'qa_%'
            and "table" not ilike 'qc_%'
          union
          select view_schema, view_name 
		  from {{redshift.data_sources_schema}}.dk_view_names_log
          where view_schema not in ('pg_catalog','information_schema')
		    and view_name not ilike 'qa_%'
            and view_name not ilike 'qc_%'
          union
          select table_schema, table_name 
		  from {{redshift.data_sources_schema}}.dk_source_dependency_tables) sdt
    LEFT JOIN {{redshift.data_sources_schema}}.dk_source_dependency_tables sdt_groups
      on sdt.table_schema = sdt_groups.table_schema and sdt.table_name = sdt_groups.table_name
    LEFT JOIN {{redshift.data_sources_schema}}.dk_source_dependencies sd
      on sdt.table_name = sd.target_table
	WHERE (sd.target_table <> sd.source_table or sd.target_table is null)
	group by sdt.table_name, sdt.table_schema, 
	  sdt_groups.page_name, sdt_groups.table_order, sdt_groups.page_id
    UNION ALL
    SELECT rd.root_page_name,
           rd.table_order,
           rd.root_table,
	       rd.root_schema,
	       rd.root_page_id,
           left(rd.id||','||sd.source_table,4000) AS id,
           rd.level + 1 as level,
           sd.runtime,
           sd.target_table,
           sd.source_table,
           case
             when sd.source_table ilike 's3://%' then 'end'
             when ','||rd.id||',' ilike '%,'||sd.source_table||',%' then 'loop'
             when nvl(sdt.page_id,0) <> nvl(rd.root_page_id,0) and 
	              sdt.table_name is not null then 'url'
             when nvl(sdt.page_id,0) = nvl(rd.root_page_id,0) and 
	              sdt.table_name is not null then 'anchor'
             else 'next'
           end as status,
	       sdt.page_id,
	       sdt.page_name
    FROM (select sd.target_table, sd.source_table,
		  max(nvl(sd_runtime.runtime,sd.runtime)) as runtime
		  from build_admin.v_dk_source_dependencies sd
		    left join (select * from build_admin.v_dk_source_dependencies 
					   where runtime > getdate()-8) sd_runtime
              on sd.source_table = sd_runtime.target_table
		  where sd.runtime > getdate()-8
		    and sd.target_table <> sd.source_table
		  group by sd.target_table, sd.source_table) sd
    JOIN dependecies rd ON rd.source_table = sd.target_table
	LEFT JOIN build_admin.dk_source_dependency_tables sdt 
	  on sdt.table_name = sd.source_table and sdt.table_schema = rd.root_schema
    where rd.status in ('root','next')
)
SELECT root_page_name, table_order, root_table, root_schema, 
       root_page_id, id, level, runtime,
       target_table, source_table, status, page_id, page_name
FROM dependecies
;

select count(*)
from v_dk_source_dependency_tree
where level = 0;