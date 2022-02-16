SET search_path TO {{redshift.data_sources_schema}};

create or replace procedure proc_source_dependency_log(
    p_xid bigint,
    p_starttime timestamp
)
    language plpgsql
as
$$
DECLARE
    lv_target_table varchar(512) = '';
	lv_source_tables varchar(4000);
    query_info RECORD;
    index RECORD;
    lv_max_sequence int;
    lv_max_index int;
    lv_query_text varchar(4000);
	lv_target_query varchar(4000);
    lv_proc_starttime timestamp;
    lv_proc_endtime timestamp;
BEGIN
	SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) into lv_proc_starttime;
	RAISE INFO 'procedure start time: %', lv_proc_starttime;

	insert into {{redshift.data_sources_schema}}.dk_source_dependencies(
		runtime, xid, userid, username, querytype, querymd5,
		querytext, target_table, source_table, target_schema, source_schema)
	select	
	        runtime, xid, userid, username, querytype, querymd5,
			querytext,
			decode(position('.' in target_table), 0, target_table,
              substring(target_table from position('.' in target_table) + 1)) target_table,
			source_table,
			decode(position('.' in target_table), 0, null,
              substring(target_table from 1 for position('.' in target_table) - 1)) target_schema,
			source_schema
	from (
         select distinct
		        stl_query.starttime as runtime,
                stl_query.xid,
                stl_query.userid,
				null username,
                case
                    when ' ' || querytxt ilike '% create table %select %' then 'create table'
		            when ' ' || querytxt ilike '% create temp table %select %' then 'create temp table'
                    when ' ' || querytxt ilike '% insert into %select % from %' then 'insert'
                    when ' ' || querytxt ilike '% select % into % from %' then 'select into'
                    when ' ' || querytxt ilike '% delete % select % from %' then 'delete'
                    when ' ' || querytxt ilike '% update % set %select % from %' then 'update'
		end as querytype,
                md5(trim(querytxt)) as querymd5,
                trim(querytxt) as querytext,
lower({{redshift.data_sources_schema}}.get_target_table_in_query(svl_statementtext.text)) as target_table,
                --svv_table_info."table" as source_table,
		        nvl(lower(dk_table_names_log.table_name),stl_scan.perm_table_name) as source_table,
				--svv_table_info.schema source_schema
		        lower(dk_table_names_log.table_schema) as source_schema
         from stl_query
                  join (select distinct query, tbl, 
						       lower(replace(perm_table_name,'$','')) as perm_table_name
					    from stl_scan
						where stl_scan.perm_table_name <> 'Internal Worktable' 
						  and stl_scan.perm_table_name <> 'Runtime Filter'
		                  --and stl_scan.perm_table_name not ilike 'volt_tt_%'
					   ) stl_scan 
		            on stl_scan.query = stl_query.query
		          left join {{redshift.data_sources_schema}}.dk_table_names_log 
		            on stl_scan.tbl = dk_table_names_log.table_id
		          join (select xid, pid, starttime,
                               listagg(REPLACE(REPLACE(REPLACE(REPLACE(text,'\\n','\n'),' ',chr(17)||chr(18)),chr(18)||chr(17),''), chr(17)||chr(18),' '))
                               within group (order by sequence) as text
                        from svl_statementtext
                        where sequence between 0 and 19
                        group by xid, pid, starttime) svl_statementtext
                    on svl_statementtext.xid = stl_query.xid
                      and svl_statementtext.pid = stl_query.pid
                      and svl_statementtext.starttime = stl_query.starttime
         where ' ' || querytxt not ilike '% create % procedure % language % begin %'
           and ' ' || querytxt not ilike '% create % function % returns % language %'
           and (' ' || querytxt ilike '% create table %select %'
		     or ' ' || querytxt ilike '% create temp table %select %'
             or ' ' || querytxt ilike '% insert into %select % from %'
             or ' ' || querytxt ilike '% select % into % from %'
             or ' ' || querytxt ilike '% delete % select % from %'
             or ' ' || querytxt ilike '% update % set %select % from %'
             )
     ) sd_new
	where target_table != ''
	  -- copy all new log and then drop old duplicates at the end of procedure
	  -- I decided not to use update/insert. I use insert all, then delete
	minus
	-- exclude same log because run the procedure more often then redshift cleans system log
	select runtime, xid, userid, null username, querytype, querymd5,
		   querytext, target_table, source_table, target_schema, source_schema
	from {{redshift.data_sources_schema}}.dk_source_dependencies;

	-- save copy log
	insert into {{redshift.data_sources_schema}}.dk_source_dependencies(
                            runtime, querytype, querymd5, querytext,
                            xid, userid, username, target_table, source_table,
							target_schema, source_schema)
	select
	runtime, querytype, querymd5, querytext, xid, userid, username,
	decode(position('.' in target_table), 0, target_table,
	substring(target_table from position('.' in target_table) + 1)) as target_table,
	source_table,
	decode(position('.' in target_table), 0, null,
	substring(target_table from 1 for position('.' in target_table) - 1)) as target_schema,
	'S3' as source_schema
	from (
         select stl_query.starttime                                               as runtime,
                'copy'                                                            as querytype,
                md5(trim(querytxt))                                               as querymd5,
                trim(querytxt)                                                    as querytext,
                stl_query.xid,
                stl_query.userid,
                null username,
trim(replace(regexp_substr(lower({{redshift.data_sources_schema}}.clean_comments_in_query(text)), 
										 'copy [^ ]*'), 'copy ','')) as target_table,
trim(replace(regexp_substr(lower({{redshift.data_sources_schema}}.clean_comments_in_query(text)), 
									'from ''[^'']*'), 'from ''','')) as source_table
         from stl_query
          join     (select xid, pid, starttime,
                    listagg(REPLACE(REPLACE(REPLACE(REPLACE(text,'\\n','\n'),' ',chr(17)||chr(18)),chr(18)||chr(17),''), chr(17)||chr(18),' '))
                    within group (order by sequence) as text
                    from svl_statementtext
                    where sequence between 0 and 19
                    group by xid, pid, starttime) svl_statementtext
           on svl_statementtext.xid = stl_query.xid
           and svl_statementtext.pid = stl_query.pid
           and svl_statementtext.starttime = stl_query.starttime
		 where ' '||querytxt ilike '% copy % from % credentials %'
    )
	minus
	-- exclude same log because run the procedure more often then redshift cleans system log
	select runtime, querytype, querymd5, querytext,
           xid, userid, null username, target_table, source_table,
		   target_schema, source_schema
	from {{redshift.data_sources_schema}}.dk_source_dependencies;
	
	-- rename table/view log
	insert into {{redshift.data_sources_schema}}.dk_source_dependencies(
      runtime, querytype, querymd5, querytext, xid, userid, username, 
	  target_table, source_table, target_schema, source_schema)
	select starttime, querytype, querymd5, text, xid, userid, null username,
      substring(target_table from position('.' in target_table) + 1) as target_table,
      substring(source_table from position('.' in source_table) + 1) as source_table,
      decode(position('.' in target_table), 0, null,
      substring(target_table from 1 for position('.' in target_table) - 1)) as target_schema,
      decode(position('.' in source_table), 0, null,
      substring(source_table from 1 for position('.' in source_table) - 1)) as source_schema
    from (
       select starttime, 'rename' querytype, md5(text) querymd5, text, xid, userid,
         trim(replace(regexp_substr(text, 'alter table [^ ]*'), 'alter table ','')) +
		 trim(replace(regexp_substr(text, 'alter view [^ ]*'), 'alter view ','')) as source_table,
         trim(replace(regexp_substr(text, 'rename to [^ ]*'), 'rename to ','')) as target_table
       from (
         select REPLACE(REPLACE(REPLACE(REPLACE(text, ' ', chr(17) || chr(18)), chr(18) || chr(17), ''), chr(17) || chr(18), ' '),'if exists ','') text,
           starttime, xid, userid
         from (
           select xid, pid, starttime, userid,
lower({{redshift.data_sources_schema}}.clean_comments_in_query(listagg(REPLACE(text, '\\n', '\n'))))
             within group (order by sequence) as text
           from svl_statementtext
           where sequence between 0 and 19
           group by xid, pid, userid, starttime)
         where ' '||text ilike '% alter table % rename %to %'
		    or ' '||text ilike '% alter view % rename %to %'
	   )
    )
	minus
	select runtime, querytype, querymd5, querytext,
           xid, userid, null username, target_table, source_table,
		   target_schema, source_schema
	from {{redshift.data_sources_schema}}.dk_source_dependencies;
	
    -- clean trash after rename table/view log script
	delete from {{redshift.data_sources_schema}}.dk_source_dependencies
	where querytype = 'rename'
	  and username is null
	  and not exists (select 1 from
	                     (select table_name 
						  from {{redshift.data_sources_schema}}.dk_table_names_log
	                      union all
	                      select view_name 
						  from {{redshift.data_sources_schema}}.dk_view_names_log)
	                 where table_name = target_table);
	
	-- at the same process time there are same queryes with different runtime
	-- I clean dublicated queries and leave only recent log
	delete from {{redshift.data_sources_schema}}.dk_source_dependencies
    where runtime <> (select max(runtime) 
					  from {{redshift.data_sources_schema}}.dk_source_dependencies d
                      where d.querymd5 = dk_source_dependencies.querymd5);
					  
	-- delete old copy log for raw tables
	delete from {{redshift.data_sources_schema}}.dk_source_dependencies
	where querytype = 'copy'
	and runtime < (select dateadd(hours, -12, max(runtime))
				   from {{redshift.data_sources_schema}}.dk_source_dependencies d
                   where dk_source_dependencies.querytype = d.querytype 
				     and dk_source_dependencies.target_table = d.target_table);

	-- If I join SVL_USER_INFO, it kills entire queries
	update {{redshift.data_sources_schema}}.dk_source_dependencies
	set username = usename
	from SVL_USER_INFO
	where usesysid = userid
	  and username is null;

	SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) into lv_proc_endtime;
	RAISE INFO 'procedure end time: %', lv_proc_endtime;
END;
$$;

CALL proc_source_dependency_log(
    cast(null as bigint),
    cast(null as timestamp)
);

/*
-- cleaning dublicates
create table build_admin.dk_source_dependencies_old as select * from build_admin.dk_source_dependencies;
select count(1) from build_admin.dk_source_dependencies_old;
truncate build_admin.dk_source_dependencies;
insert into build_admin.dk_source_dependencies select distinct * from build_admin.dk_source_dependencies_old;
select count(1) from build_admin.dk_source_dependencies;
drop table build_admin.dk_source_dependencies_old;
*/