# SQL-File-Moves
A Script which will move SQL Data and log files from one instance to another, it will detach the files from the first instance and then attach it to a second instance
The script fully logs its actions and allows you to set variables for test and live enviroments and will rollback the actions 
To configure this script use the variables:
**$is_test**: Y/N variable to set if this instance is running as a test or not 
**$is_rollback**: Y/N variable to set if this run is a rollback or not 
**$source_server**: the source instance for the SQL files - it has 2 variables to complete Y for test and N for live 
**$destination_server**: the Destination instance for the SQL files - it has 2 variables to complete Y for test and N for live 
**$database**: Database for initial connection
**$username**: User name for SQL Authentication
**$password**: password for SQL Authentication 
**$new_file_location**: new location to move the data files to 
**$new_log_location**: new location to move the log files to 
**$rollback_file_location**: rollback location to move the data files to 
**$attached_script **: if a copy of the DB being attached already exists on the target instance, the attach process will cease - a script is generated to attach the files after confirming if the process should be completed or not 
**$rollback_attached_script** : if a copy of the DB being attached already exists on the target instance, the attach process will cease - a script is generated to attach the files after confirming if the process should be completed or not 
**$logfile**: Location for the logfile 
**$rollback_logfile**: rollback location to move the log files to 

