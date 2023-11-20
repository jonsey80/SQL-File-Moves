############################################################### 
#Date: 13/11/2020                                             #
#Version: 1.0                                                 #
#Author: Mark Jones                                           #
#Details: Script to move databases from one server to another #
# requierments: account running script needs sa to both DB    #
#  and full access to the database folders for source and     #
#  destination                                                #
#date       version    author        details                  #
#13/11/20    1.0       M Jones        Initial Script          #
#19/02/21    1.1       M Jones    Add logging, fix rollback bug
###############################################################

##create timestamp for log 

function Get-TimeStamp {
    
    return "[{0:MM/dd/yy} {0:HH:mm:ss}]" -f (Get-Date)
    
}


##are we running a test, or live change##
$is_test = 'Y'

#are we doing a rollback

$is_rollback = 'Y'
##INIT VARIABLES##

$source_server = switch($is_test) {
                    "Y" {"<INSTANCE>"}
                    "N" {"<INSTANCE>"}
                    }
$destination_server = switch($is_test) {
                        "Y" {"<INSTANCE>"}
                        "N" {"<INSTANCE>"}
                        }

$database ="master"

$username = ""
$password = ""

$new_file_location = "<FILE_LOCATION>"
$new_log_location = "<FILE_LOCATION>"

$rollback_file_location = "<FILE_LOCATION>"
 $attached_script = "<FILE_LOCATION>"
  $rollback_attached_script = "<FILE_LOCATION>"
  $logfile = "<FILE_LOCATION>"
  $rollback_logfile = "<FILE_LOCATION>"

###here are queries we will be running###
##this query provides details on the d
$table_list = "
select df.name,data_file,data_file_name,log_file,log_file_name,data_location,log_location from(
																		select name,data_file,physical_name 'Data_location',substring(physical_name,(data_data_length-data_space)+1,data_space) 'data_file_name' from (
																		select b.name,c.name 'data_file',c.physical_name,len(c.physical_name) 'data_data_length', charindex(reverse('\'),reverse(c.physical_name))-1 'data_space' from sys.databases b
																		INNER JOIN SYS.master_files C ON B.database_id = C.database_id and c.type_desc = 'ROWS') a ) df
																	inner join 
																		(select name,log_file,physical_name'log_location',substring(physical_name,(data_data_length-data_space)+1,data_space) 'log_file_name' from (
																		select b.name,c.name'log_file',c.physical_name,len(c.physical_name) 'data_data_length', charindex(reverse('\'),reverse(c.physical_name))-1 'data_space' from sys.databases b
																		INNER JOIN SYS.master_files C ON B.database_id = C.database_id and c.type_desc = 'log') b ) lf on df.name = lf.name		
	where df.name in (
<DBLIST>



)
"
#we write the contents of the table details to a file in case we need to rollback later

if ($is_rollback -eq "N") {
Write-Output "Running"
## stores each of the databases we are working in within an array which we will step through database by database
#[array]$table_details = $null
Write-Output "$(Get-TimeStamp) running details query"|Tee-object -filepath $logfile 
$table_details = Invoke-Sqlcmd -ServerInstance $source_server -database $database -query $table_list
write-output "$(Get-TimeStamp) populating rollback file"|Tee-object -filepath $logfile -Append 
$table_details|Export-CSV $rollback_file_location
[array] $detach = $null


foreach ($db_process in $table_details) {
                    Write-Output "Processing databases"
                    $dbname1= $db_process.name
                    write-output $dbname1|Tee-object -filepath $logfile -Append
                    $working_list = New-Object PSObject 
                    $working_list| add-Member NoteProperty -Name "DBName" -Value $db_process.name
                    $working_list| add-Member NoteProperty -Name "Datafile_logical" -Value $db_process.data_file
                    $working_list| add-Member NoteProperty -Name "Datafile" -Value $db_process.data_file_name
                    $working_list| add-Member NoteProperty -Name "logfile_logical" -Value $db_process.log_file
                    $working_list| add-Member NoteProperty -Name "logfile" -Value $db_process.log_file_name
                    $working_list| add-Member NoteProperty -Name "datafile_location" -Value $db_process.data_location
                    $working_list| add-Member NoteProperty -Name "logfile_location" -Value $db_process.log_location
                    #$detach += $working_list
                    foreach ($to_detach in $working_list) { 
                    $dbname2 = $to_detach.DBNAME
                        Write-output "$(Get-TimeStamp) moving $dbname2"|Tee-object -filepath $logfile -append
                    ##detach the database
                    
                    
                        $cut_connection = "ALTER DATABASE [" + $to_detach.DBNAME + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                        
                        
                        $detach_sql = "exec dbo.sp_detach_db '" + $to_detach.DBName + "','TRUE'"
                    write-output " $(Get-TimeStamp) Setting $dbname2 to single user"|Tee-object -filepath $logfile -append
                    try{
                    Invoke-Sqlcmd -ServerInstance $source_server -Database $database  -Query $cut_connection -erroraction Stop
                    }
                    catch {

                    $errormsg = $_.Exception.Message
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $logfile -append 
                     }
                     write-output " $(Get-TimeStamp) Detatching Database $dbname2"|Tee-object -filepath $logfile -Append
                     try{
                    Invoke-Sqlcmd -ServerInstance $source_server -Database $database  -Query $detach_sql -erroraction Stop
                    }
                    catch {
                    $errormsg = $_.Exception.Message
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $logfile -append 
                    }
                    ##move the data and log file to the new location
                    
                    $destfile = $new_file_location #+ "\" + $to_detach.datafile 
                    $destlog = $new_log_location #+ "\" + $to_detach.logfile
                     $datafile_detatch = $to_detach.datafile_location
                    $logfile_detach = $to_detach.logfile_location
                    Write-Output " $(Get-TimeStamp) moving files from $datafile_detatch to $destfile"|Tee-object -filepath $logfile -append
                    try{
                    move-item -path $datafile_detatch -Destination $destfile 2>&1 |Tee-object -filepath $logfile -append
                    }
                    catch{
                      $errormsg = $error
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $logfile -append 
                     }
                   
                    Write-Output " $(Get-TimeStamp) Moving Log from $logfile_detach to $destlog"|Tee-object -filepath $logfile -append
                    try {
                    move-item -path $logfile_detach -destination $destlog 2>&1 |Tee-object -filepath $logfile -append
                    
                    }
                    catch {
                      $errormsg = $error
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $logfile -append 
                     }
                    ##now we need to attach the database in the new location
                    Write-Output " $(Get-TimeStamp) Attaching Database $dbname2"|Tee-object -filepath $logfile -append
                    $attach_sql = "exec dbo.sp_attach_db '" + $to_detach.DBName + "',
                    @filename1 = '" + $new_file_location + "\" + $to_detach.datafile  + "',
                    @filename2 = '" + $new_log_location + "\" + $to_detach.logfile + "'"

                   

                    $check_query = " Select count(*) from sys.databases where name = '" + $to_detach.DBName + "'"

                    $count_db = Invoke-Sqlcmd -ServerInstance $destination_server -Database $database -Query $check_query
                    
                    Write-Output $count_db.Column1
                    if ($count_db.Column1 -gt 0) {
                    write-output "$(get-timestamp) Database is attached to the server, it is unsafe to attach, Attach script is being 
                    outputed to $attached_script please run it after the destination server is clear"|Tee-object $logfile -append
                    $attach_sql| Tee-object -FilePath $attached_script -append


                    }
                    else {
                    write-output "attach DB"
                    try{
                    Invoke-Sqlcmd -ServerInstance $destination_server -Database $database -Query $attach_sql -erroraction Stop
                    }
                    catch {
                      $errormsg = $_.Exception.Message
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $logfile -append 
                     }
                    }

                    }

                    Write-Output "$(Get-TimeStamp) $dbname2 is now moved"|Tee-object -filepath $logfile -append

    }
            write-output "Complete"




}

if ($is_rollback -eq "Y") {

Write-Output "Rollingback" 

[array]$rollback_details = $null

Write-Output "$(Get-TimeStamp) Readng Rollback details"|Tee-object -filepath $rollback_logfile 
 $rollback_details = import-csv $rollback_file_location


 foreach ($db_process in $rollback_details) {
                    Write-Output " $(get-timestamp) Processing databases"|Tee-object -filepath $rollback_logfile -append
                    $dbname1= $db_process.name
                    write-output "$(Get-TimeStamp) Rollingback $dbname1"|Tee-object -filepath $rollback_logfile -append
                    $working_list = New-Object PSObject 
                    $working_list| add-Member NoteProperty -Name "DBName" -Value $db_process.name
                    $working_list| add-Member NoteProperty -Name "Datafile_logical" -Value $db_process.data_file
                    $working_list| add-Member NoteProperty -Name "Datafile" -Value $db_process.data_file_name
                    $working_list| add-Member NoteProperty -Name "logfile_logical" -Value $db_process.log_file
                    $working_list| add-Member NoteProperty -Name "logfile" -Value $db_process.log_file_name
                    $working_list| add-Member NoteProperty -Name "datafile_location" -Value $db_process.data_location
                    $working_list| add-Member NoteProperty -Name "logfile_location" -Value $db_process.log_location
                    #$detach += $working_list
                    foreach ($to_detach in $working_list) { 
                    $dbname2 = $to_detach.DBNAME
                        Write-output "$(get-timestamp) moving $dbname2"|Tee-object -filepath $rollback_logfile -Append
                    ##detach the database
                        $cut_connection = "ALTER DATABASE [" + $to_detach.DBNAME + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                        
                        $detach_sql = "exec dbo.sp_detach_db '" + $to_detach.DBName + "','TRUE'"
                        write-output "$(get-timestamp) Setting $dbname2 to single user"|Tee-object -filepath $rollback_logfile -Append
                    try{
                    Invoke-Sqlcmd -ServerInstance $destination_server -Database $database  -Query $cut_connection -ErrorAction stop
                    }
                    catch{
                    $errormsg = $_.Exception.Message
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $rollback_logfile -append 
                    }
                    write-output "$(Get-TimeStamp) detaching $dbname2"|Tee-object -filepath $rollback_logfile -append
                    try{
                    Invoke-Sqlcmd -ServerInstance $destination_server -Database $database  -Query $detach_sql -ErrorAction stop
                    }
                    catch{
                    $errormsg = $_.Exception.Message
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $rollback_logfile -append 
                    }
                    ##move the data and log file to the new location
                    $destfile = $new_file_location + "\" + $to_detach.datafile 
                    $destlog = $new_log_location + "\" + $to_detach.logfile
                    $datafile_loc = $to_detach.datafile_location
                    $datafile_ed = $datafile_loc.Substring(0,$datafile_loc.LastIndexOf('\'))
                    $logfile_loc = $to_detach.logfile_location
                    $logfile_ed = $logfile_loc.Substring(0,$logfile_loc.LastIndexOf('\'))
                    Write-Output "$(get-timestamp) Moving $destfile to $datafile_ed" |Tee-object -filepath $rollback_logfile -append
                    try{
                    move-item -path $destfile -Destination $datafile_ed  2>&1 |Tee-object -filepath $rollback_logfile -append
                    }
                    catch{
                    $errormsg = $_.Exception.Message
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $rollback_logfile -append 
                    }
                    write-output "$(get-timestamp) moving $destlog to $logfile_ed" |Tee-object -filepath $rollback_logfile -Append
                    try{
                    move-item -path $destlog -destination $logfile_ed   2>&1 |Tee-object -filepath $rollback_logfile -append
                    }
                    catch{
                    $errormsg = $_.Exception.Message
                     Write-Output "$(get-timestamp) $errormsg " |Tee-object -filepath $rollback_logfile -append 
                     }
                    ##now we need to attach the database in the new location

                    $attach_sql = "exec dbo.sp_attach_db '" + $to_detach.DBName + "',
                    @filename1 = '" + $to_detach.datafile_location + "',
                    @filename2 = '" + $to_detach.logfile_location + "'"

                   

                    $check_query = " Select count(*) from sys.databases where name = '" + $to_detach.DBName + "'"

                    $count_db = Invoke-Sqlcmd -ServerInstance $source_server -Database $database -Query $check_query
                    
                    Write-Output $count_db.Column1
                    if ($count_db.Column1 -gt 0) {
                    write-output "Database is attached to the server, it is unsafe to attach, Attach script is being 
                    outputed to $attached_script please run it after the destination server is clear" |Tee-Object -filepath $rollback_logfile -append
                    $attach_sql| Tee-object -filepath $rollback_attached_script -append


                    }
                    else {
                    try{
                    write-output "attach DB"|Tee-object -filepath $rollback_logfile -append

                    Invoke-Sqlcmd -ServerInstance $source_server -Database $database -Query $attach_sql -ErrorAction stop
                    }
                    catch{
                     $errormsg = $_.Exception.Message
                     Write-Output "$(get-timestamp) $errormsg " |Tee-Object -filepath $rollback_logfile -append 
                    }
                    }

}
}
Write-Output "complete"
}   