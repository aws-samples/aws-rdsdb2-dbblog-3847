## Replay Db2 LUW workload in Workload in RDS for Db2

The python will convert db2 event monitor into SQL and paremater files that can used by db2batch

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

Key Processing Steps
The key processing steps are as follows:

•	Imports necessary modules: datetime for handling time and os for file operations.

•	Clears the screen and records the script's start time.

•	Defines variables for processing.

•	Opens an output file named ./sql-with-key.txt to temporarily store processed SQL statements with pseudo keys. This is needed as some of the SQL statements span multiple lines in the input file and do not have a key.

•	Reads the raw SQL data file line by line

   o	Splits each line into components based on delimiters.
 
   o	Handles different record types (single-line SQLs, multi-line SQLs, and parts of multi-line SQLs).
 
   o	Generates a pseudo key for each SQL statement using activity ID, unit of work ID, timestamp, and SQL text for multi-line SQL statements
 
   o	Writes the processed SQL statements, parameters, and pseudo keys to the ./sql-with-key.txt file.
 
   o	Closes the file.
 
•	Opens two output files: ./sql-statement.txt and ./sql-parms.txt.

•	Reads the ./sql-with-key.txt file line by line:

   o	Separates SQL statements, parameters, and pseudo keys.
 
   o	Groups parameters based on pseudo keys.
 
   o	Writes SQL statements to ./sql-statement.txt.
 
   o	Writes parameters to ./sql-parms.txt, enclosing character-type values in single quotes.
•	Closes the output files.
•	Records the script's end time and calculates execution time.
•	Prints summary information, including the number of processed SQL statements and execution time.

