SET search_path TO {{redshift.data_sources_schema}};
/*
DROP TABLE IF EXISTS dk_source_dependencies;
CREATE TABLE dk_source_dependencies (
	runtime timestamp,
	xid bigint,
	userid int,
	querytype varchar(50),
	querymd5 varchar(32),
	querytext varchar(4000),
	target_table varchar(256),
	source_table varchar(256),
	target_schema varchar(256),
	source_schema varchar(256),
	username varchar(256)
);
*/