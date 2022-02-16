SET search_path TO {{redshift.data_sources_schema}};

create or replace function get_tables_in_query(
  sql_str varchar(4000)
) returns varchar(4000)
stable
language plpythonu
as $$
# help https://grisha.org/blog/2016/11/14/table-names-from-sql/
# https://stackoverflow.com/questions/49773059/how-to-extract-tables-names-in-a-sql-script

#import logging
#select * from SVL_UDF_LOG where funcname = 'tables_in_query2' order by created desc
#logger = logging.getLogger()
#logger.setLevel(logging.INFO)
#logger.info("my comment ")

import re
# remove the /* */ comments
q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

# remove whole line -- and # comments
lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

# remove trailing -- and # comments
q = " ".join([re.split("--|#", line)[0] for line in lines])

replace_list = ['\n', '*', '=']
for i in replace_list:
    q = q.replace(i, ' ')

# split on blanks, parens and semicolons
tokens = re.split(r"[\s)(,;]+", q)

# scan the tokens. if we see a FROM or JOIN, we set the get_next
# flag, and grab the next one (unless its SELECT).
result = set()
get_next = False
for tok in tokens:
    if get_next:
        if tok.lower() not in ["", "select"]:
            # Added support for recovering quoted names with spaces
            if tok[0:1] == '"' and tok[-1:] != '"':
                quoted = sql_str[sql_str.find(tok) + 1:]
                quoted = quoted[:quoted.find('"')]
                result.add(quoted)
            else:
                result.add(tok)
        get_next = False
    get_next = tok.lower() in ["from", "join"]
return ';'.join(result)
$$;