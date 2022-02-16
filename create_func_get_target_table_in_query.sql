SET search_path TO {{redshift.data_sources_schema}};

create or replace function get_target_table_in_query(
  sql_str varchar(4000)
) returns varchar(200)
stable
language plpythonu
as $$

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

# CREATE [ [ LOCAL ] { TEMPORARY | TEMP } ]
# TABLE table_name
# [ ( column_name [, ... ] ) ]
# [ BACKUP { YES | NO } ]
# [ DISTSTYLE { EVEN | ALL | KEY } ]
# [ DISTKEY( distkey_identifier ) ]
# [ [ COMPOUND | INTERLEAVED ] SORTKEY( column_name [, ...] ) ]
# AS query

# INSERT INTO table_name [ ( column [, ...] ) ]
# {DEFAULT VALUES |
# VALUES ( { expression | DEFAULT } [, ...] )
# [, ( { expression | DEFAULT } [, ...] )
# [, ...] ] |
# query }

# [ WITH [RECURSIVE] common_table_expression [, common_table_expression , ...] ]
#             UPDATE table_name [ [ AS ] alias ] SET column = { expression | DEFAULT } [,...]
# [ FROM fromlist ]
# [ WHERE condition ]

# [ WITH with_subquery [, ...] ]
# SELECT
# [ TOP number ] [ ALL | DISTINCT ]
# * | expression [ AS output_name ] [, ...]
# INTO [ TEMPORARY | TEMP ] [ TABLE ] new_table
# [ FROM table_reference [, ...] ]
# [ WHERE condition ]
# [ GROUP BY expression [, ...] ]
# [ HAVING condition [, ...] ]
# [ { UNION | INTERSECT | { EXCEPT | MINUS } } [ ALL ] query ]
# [ ORDER BY expression
# [ ASC | DESC ]
# [ LIMIT { number | ALL } ]
# [ OFFSET start ]

# [ WITH [RECURSIVE] common_table_expression [, common_table_expression , ...] ]
# DELETE [ FROM ] table_name
# [ {USING } table_name, ... ]
# [ WHERE condition ]
   
# COPY table-name 
# [ column-list ]
# FROM data_source
# authorization
# [ [ FORMAT ] [ AS ] data_format ] 
# [ parameter [ argument ] [, ... ] ]


# scan the tokens. if we see a FROM or JOIN, we set the get_next
# flag, and grab the next one (unless its SELECT).
result = set()
get_next = False
target_table = ''
for tok in tokens:
   if get_next:
      if tok.lower() in ["temporary", "temp", "table", "from"]:
         continue
      elif tok.find('"') != -1:
         if target_table == '':
            target_table = tok
         else:
            target_table = target_table + ' ' + tok
         if target_table.count('"') in [2, 4]:
            result.add(target_table)
            break
         else:
            continue
      else:
         result.add(tok)
         break
   get_next = tok.lower() in ["table", "into", "update", "delete", "copy", "view"]
return ';'.join(result)
$$;