#! /usr/bin/python3
#
## imports
#
import datetime
import os
#
## Clear the screen and save start time
#
os.system('clear')
start = datetime.datetime.now()
print('Script started at ', start)
#
## Variables
#
sql_in        = ' '
in_ctr        = 0
curr_key      = ' '
prev_key      = ' '
prev_sql_stmt = ' '
line_out      = ''
first_read    = 'y'
char_types    = ['CHAR','VARCHAR','DATE','TIME','TIMESTAMP']
#
## Main logic
#
#
## Open output file for sql with psuedo key
#
file_sql_with_key = open('./sql-with-key.txt', 'w')
#
## Add a psuedo key to all reords to facilitate processing of multi line sqls
#
with open('./sql-raw-data.txt') as rawsql:
  for sql_in in rawsql:
      in_ctr = in_ctr + 1
      sql_in = sql_in.replace("\n", "")
      sql_in = sql_in.replace('\t', '        ')
      sql_in = sql_in.replace('""','"')
      sql_in_bits = sql_in.split('^')
      if  len(sql_in_bits) == 8:
          #
          ## The entire sql is in one input record
          #
          activity_id      = sql_in_bits[0]
          uow_id           = sql_in_bits[1]
          sql_time_stamp   = sql_in_bits[2]
          sql_time_stamp   = sql_time_stamp.replace('"','')
          sql_stmt         = sql_in_bits[3].strip() + ';'
          sql_stmt         = sql_stmt.replace('"','',1)
          sql_stmt         = sql_stmt.replace('";',';',1)
          key_sql_stmt     = sql_stmt
          psuedo_key       = activity_id + '-' + uow_id + '-' + sql_time_stamp + '-' + key_sql_stmt
          parm_value       = sql_in_bits[4].strip()
          parm_value       = parm_value.replace('"','')
          parm_index       = sql_in_bits[5].strip()
          parm_type        = sql_in_bits[6].replace('"','')
          parm_type        = parm_type.strip()
          line_out         = 'single-sql' + "#$#" + psuedo_key + "#$#" + sql_stmt + "#$#" + parm_value + "#$#" + parm_index + "#$#" + parm_type
          file_sql_with_key.write(line_out + "\n")
      elif len(sql_in_bits) == 4:
          #
          ## Start of a multi line sql
          #
          activity_id      = sql_in_bits[0]
          uow_id           = sql_in_bits[1]
          sql_time_stamp   = sql_in_bits[2]
          sql_time_stamp   = sql_time_stamp.replace('"','')
          sql_stmt         = sql_in_bits[3].strip()
          sql_stmt         = sql_stmt.replace('"','',1)
          key_sql_stmt     = sql_stmt
          psuedo_key       = activity_id + '-' + uow_id + '-' + sql_time_stamp + '-' + key_sql_stmt
          parm_value       = ''
          parm_index       = ''
          parm_type        = ''
          line_out         = 'multi-line-start' + "#$#" + psuedo_key + "#$#" + sql_stmt + "#$#" + parm_value + "#$#" + parm_index + "#$#" + parm_type
          file_sql_with_key.write(line_out + "\n")
      elif len(sql_in_bits) == 1:
          #
          ## Middle lines (i.e. neither the first or the last line) of a multi line sql
          #
          sql_stmt         = sql_in_bits[0].strip()
          sql_stmt         = sql_stmt.replace('"','',1)
          parm_value       = ''
          parm_index       = ''
          parm_type        = ''
          line_out         = 'multi-line-middle' + "#$#" + psuedo_key + "#$#" + sql_stmt + "#$#" + parm_value + "#$#" + parm_index + "#$#" + parm_type
          file_sql_with_key.write(line_out + "\n")
      elif len(sql_in_bits) == 5:
          #
          ## Last line of a multi line sql
          #
          sql_stmt         = sql_in_bits[0].strip() + ';'
          sql_stmt         = sql_stmt.replace('"','',1)
          parm_value       = sql_in_bits[1].strip()
          parm_value       = parm_value.replace('"','')
          parm_index       = sql_in_bits[2].strip()
          parm_type        = sql_in_bits[3].replace('"','')    
          parm_type        = parm_type.strip()
          line_out         = 'multi-line-end' + "#$#" + psuedo_key + "#$#" + sql_stmt + "#$#" + parm_value + "#$#" + parm_index + "#$#" + parm_type
          file_sql_with_key.write(line_out + "\n")
#
## Close files
#
file_sql_with_key.close()
#
## Process the data
#
## Open output files that will contain the SQL statements and parmameters
#
file_parms        = open('./sql-parms.txt', 'w')
file_sql          = open('./sql-statement.txt', 'w')
line_out = ''
with open('./sql-with-key.txt') as keysql:
  for sql_in in keysql:
      sql_in      = sql_in.replace("\n", "")
      sql_in_bits = sql_in.split('#$#')
      sql_type    = sql_in_bits[0]
      curr_key    = sql_in_bits[1]
      sql_stmt    = sql_in_bits[2]
      parm_value  = sql_in_bits[3]
      parm_index  = sql_in_bits[4]
      parm_type   = sql_in_bits[5]
      
      if  first_read == 'y':
          prev_key       = curr_key
          prev_sql_stmt  = sql_stmt
          first_read     = 'n'

      if  curr_key == prev_key:
          if  parm_type in char_types:
              line_out = line_out + "'" + parm_value + "'" + ' '
          else:
              line_out = line_out + parm_value + ' '
          if  not sql_stmt == prev_sql_stmt:
              file_sql.write(prev_sql_stmt + "\n")
              prev_sql_stmt  = sql_stmt
      else:
          file_parms.write(line_out + "\n")
          line_out = ''
          file_sql.write(prev_sql_stmt + "\n")

          if  parm_type in char_types:
              line_out = line_out + "'" + parm_value + "'" + ' '
          else:
              line_out = line_out + parm_value + ' '

          prev_key       = curr_key
          prev_sql_stmt  = sql_stmt

file_parms.write(line_out + "\n")
file_sql.write(prev_sql_stmt + "\n")
#
## close files
#
file_parms.close()
file_sql.close()
#
## end time
#
end = datetime.datetime.now()
print(' ')
print('Number of SQLs    ', in_ctr)
print(' ')
print('Script ended at   ', start)
print(' ')
print('Execution time    ', end-start)
print(' ')
