SET search_path TO {{redshift.data_sources_schema}};

create or replace procedure proc_view_dependency_log()
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

	-- save log for views
    -- select first part of each new query
	for query_info in (
        select xid, starttime, userid, null username, type, replace(text,'\\n','\n') text,
			'create view' as querytype
        from (select listagg(text) WITHIN GROUP (ORDER BY sequence) as text,
                     xid, starttime, userid, type
			from svl_statementtext
            where type in ('DDL')
              and sequence <= 19
			group by xid, starttime, userid, type)
        where ' '||replace(text,'\\n',' ') ilike '% create view %'
	    order by starttime
	    )
	loop
		-- select first 4000 chars and look for a target table
		lv_target_query := REPLACE(REPLACE(REPLACE(query_info.text,' ',chr(17)||chr(18)),chr(18)||chr(17),''),chr(17)||chr(18),' ');

		-- update runtime for existing query
		if exists (select 1 
				   from {{redshift.data_sources_schema}}.dk_source_dependencies 
				   where querymd5 = md5(lv_target_query)) then
			update {{redshift.data_sources_schema}}.dk_source_dependencies
			set runtime = query_info.starttime, 
			    xid = query_info.xid,
				userid = query_info.userid,
				username = null
			where querymd5 = md5(lv_target_query);
		else
			-- creating log for a new query
            lv_target_table := lower({{redshift.data_sources_schema}}.get_target_table_in_query(lv_target_query));
            -- if no target_table, move to the next query
            if lv_target_table != '' then
                RAISE INFO 'target table: %', lv_target_table;
                RAISE INFO 'target query: %', lv_target_query;
                RAISE INFO 'xid: %', query_info.xid;
                RAISE INFO 'starttime: %', query_info.starttime;

                -- amount of parts
                -- to calculate slises and indicate the truth of this procedure
                lv_max_sequence := null;
                select max(sequence)
                into lv_max_sequence
                from svl_statementtext
                where xid = query_info.xid and starttime = query_info.starttime;
                RAISE INFO 'max_sequence: %', lv_max_sequence;

                -- generate slices with overlap
                lv_max_index := case when lv_max_sequence in (0,1) then 1 
				                else ceil((lv_max_sequence - 1.0) / 18.0) end;
                RAISE INFO 'max_index: %', lv_max_index;
                for index in 1..lv_max_index
                loop
                    -- get new 4000 chars of the query with overlap
                    lv_query_text := '';
                    lv_source_tables := '';
                    with query_text as
                        (select listagg(text) WITHIN GROUP (ORDER BY sequence) as text
                        from svl_statementtext
                        where xid = query_info.xid and starttime = query_info.starttime
                        and sequence between (index-1)*18 and index*18 + 1
                        )
                    select {{redshift.data_sources_schema}}.get_tables_in_query(replace(text,'\\n','\n')),
                           REPLACE(REPLACE(REPLACE(replace(text,'\\n',' '),' ',chr(17)||chr(18)),chr(18)||chr(17),''),chr(17)||chr(18),' ')
                    into lv_source_tables, lv_query_text
                    from query_text;

                    if lv_source_tables <> '' then
					    lv_source_tables := lower(lv_source_tables);
                        RAISE INFO 'source tables: %', lv_source_tables;
                        RAISE INFO 'source query: %', lv_query_text;

                        -- save sourch tables
                        insert into {{redshift.data_sources_schema}}.dk_source_dependencies(
                            runtime, querytype, querymd5, querytext,
                            xid, userid, username, target_table, source_table,
							target_schema, source_schema)
                        -- parse list of tables and turn them into new rows
                        with recursive rows (old_num, new_num, source_table) as (
                            select regexp_count(lv_source_tables, ';')+1 old_num,
                                   regexp_count(lv_source_tables, ';') new_num,
                                   trim(ltrim(regexp_substr(';' || lv_source_tables, ';[^;]*',1,regexp_count(lv_source_tables, ';')+1),';')) as source_table
                            union all
                            select new_num old_num,
                                   new_num-1 new_num,
                                   trim(ltrim(regexp_substr(';' || lv_source_tables, ';[^;]*',1,new_num),';')) as source_table
                            from rows
                            where new_num > 0
                        )
                        select distinct
                            query_info.starttime, query_info.querytype,
                            md5(lv_target_query) querymd5, lv_target_query querytext,
                            query_info.xid, query_info.userid, query_info.username,
-- target_table
decode(position('.' in lv_target_table), 0, lv_target_table,
substring(lv_target_table from position('.' in lv_target_table) + 1)) as target_table,
-- source_table
decode(position('.' in source_table), 0, source_table,
substring(source_table from position('.' in source_table) + 1)) as source_table,
-- target_schema
decode(position('.' in lv_target_table), 0, null,
substring(lv_target_table from 1 for position('.' in lv_target_table) - 1)) as target_schema,
-- source_schema
decode(position('.' in source_table), 0, null,
substring(source_table from 1 for position('.' in source_table) - 1)) as source_schema
						from rows
                        where not exists (select 1 
								from {{redshift.data_sources_schema}}.dk_source_dependencies
                                         where querymd5 = md5(lv_target_query)
                                            and xid != query_info.xid
                                            and runtime != query_info.starttime)
                        -- apply distinct to source_tables because of slices
                        minus
                        select runtime, querytype, querymd5, querytext,
                            xid, userid, null username, target_table,  source_table,
							target_schema,  source_schema
                        from {{redshift.data_sources_schema}}.dk_source_dependencies
                        where xid = query_info.xid
                          and runtime = query_info.starttime
                          and target_table = decode(position('.' in lv_target_table), 0, lv_target_table, substring(lv_target_table from position('.' in lv_target_table) + 1));
                    end if;
                end loop;

                -- I wanna track the progress and be able to restart at the stop spot
                --commit;
            end if;
        end if;
	end loop;
	
    delete from {{redshift.data_sources_schema}}.dk_source_dependencies
	where querytype = 'create view'
	  and username is null
	  and not exists (select 1 from
	                     (select table_name 
						  from {{redshift.data_sources_schema}}.dk_table_names_log
	                      union
	                      select view_name 
						  from {{redshift.data_sources_schema}}.dk_view_names_log)
	                 where table_name = source_table);

	delete from {{redshift.data_sources_schema}}.dk_source_dependencies
	where querytype = 'create view'
	  and username is null
	  and not exists (select 1 from
	                     (select table_name 
						  from {{redshift.data_sources_schema}}.dk_table_names_log
	                      union
	                      select view_name 
						  from {{redshift.data_sources_schema}}.dk_view_names_log)
	                 where table_name = target_table);
	  
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

CALL proc_view_dependency_log();