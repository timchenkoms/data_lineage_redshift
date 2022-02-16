SET search_path TO {{redshift.data_sources_schema}};

drop view if exists v_dk_source_dependency_tree_wiki;
create or replace view v_dk_source_dependency_tree_wiki(
    page_name, table_order, root_table, root_schema, id, level, runday,
	target_table, source_table, source_table_limit, status, root_page_id)
as
SELECT
    page_name, table_order, root_table, root_schema, id, level, 
	to_char(runtime,'mm/dd/yyyy') runday,
    target_table,
	case when status = 'root' then 
           '<ac:structured-macro ac:name=\"anchor\">'
		   + '<ac:parameter ac:name=\"\">'
		   + source_table
		   + '</ac:parameter>'
           + '</ac:structured-macro>'
		   + '<b>' + source_table + '</b>'
	     when status = 'anchor' then
		   repeat('<BLOCKQUOTE style=\"padding-top:0; padding-bottom:0;\">', level)
		   + '<a href = \"#' + root_page_name + '-' + source_table + '\" style=\"color:blue;\">' 
		   + source_table + '</a>'
		   + repeat('</BLOCKQUOTE>', level)
		 when status = 'url' then
		   repeat('<BLOCKQUOTE style=\"padding-top:0; padding-bottom:0;\">', level)
		   + '<a href = \"https://hm.datakitchen.io/wiki/pages/viewpage.action?pageId='  
		   + page_id + '#' +  page_name + '-' + source_table
		   + '\" style=\"color:blue;\">' + source_table + '</a>'
		   + repeat('</BLOCKQUOTE>', level)
	     when status = 'limit' then
		   repeat('<BLOCKQUOTE style=\"padding-top:0; padding-bottom:0;\">', level) 
		   + '<span style=\"color:red;\">' + source_table + ' <i>(detail shown separately)</i></span>'
		   + repeat('</BLOCKQUOTE>', level)
		 when status = 'loop' then
		   repeat('<BLOCKQUOTE style=\"padding-top:0; padding-bottom:0;\">', level) 
		   + '<span style=\"color:blue;\">' + source_table + ' <i>(circular reference)</i></span>'
		   + repeat('</BLOCKQUOTE>', level)
		 else
		   repeat('<BLOCKQUOTE style=\"padding-top:0; padding-bottom:0;\">', level)
		   + '<span style=\"color:black;\">'
		   + source_table 
		   + '</span>'
		   + repeat('</BLOCKQUOTE>', level)
    end as source_table,
	repeat('<BLOCKQUOTE style=\"padding-top:0; padding-bottom:0;\">', level) 
    + '<span style=\"color:red;\">' + source_table + ' <i>(detail shown separately)</i></span>'
	+ repeat('</BLOCKQUOTE>', level) source_table_limit,
    status,
	root_page_id
FROM {{redshift.data_sources_schema}}.v_dk_source_dependency_tree;

select count(*)
from v_dk_source_dependency_tree_wiki
where level = 0;