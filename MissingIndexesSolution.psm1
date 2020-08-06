function MissingIndexes-Collect
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [System.IO.FileInfo]
        $ServerList,
 [Parameter(Mandatory = $false)]
        [int]
        $TopN=20
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

    if ($ServerList)
        { MissingIndexes-Check-CollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ServerList $ServerList 
        }
        else
        { MissingIndexes-Check-CollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase 
        }



if ($TopN -eq 0){
   $par1=" "}
   else{
   $par1=" top {0} " -f $TopN }

$Query="
Declare @ServerStart DateTime
select @ServerStart=sqlserver_start_time from sys.dm_os_sys_info

Select {0}
    @@ServerName as ServerName
	,SERVERPROPERTY('Edition') as [ServerEdition]
	,SERVERPROPERTY('EngineEdition') as [EngineEdition]
    ,CAST(CURRENT_TIMESTAMP AS [smalldatetime]) AS [Collection_Time]
	,@ServerStart as Sqlserver_start_time 
    ,db.[name] AS [DatabaseName]
	,id.[statement] AS [FullyQualifiedObjectName]
    ,id.[object_id] AS [ObjectID]
	,OBJECT_SCHEMA_NAME (id.[object_id], db.[database_id]) AS [SchemaName]
	,OBJECT_NAME(id.[object_id], db.[database_id]) AS [TableName]
    ,id.[equality_columns] AS [EqualityColumns]
    ,id.[inequality_columns] AS [InEqualityColumns]
    ,id.[included_columns] AS [IncludedColumns]
	,((gs.[user_seeks]+gs.[user_scans]) * gs.[avg_total_user_cost] * gs.[avg_user_impact]) AS [Impact]
    ,gs.[user_seeks] AS [UserSeeks]
    ,gs.[user_scans] AS [UserScans]
	,gs.[unique_compiles] AS [UniqueCompiles]
    ,gs.[avg_total_user_cost] AS [AvgTotalUserCost]  
    ,gs.[avg_user_impact] AS [AvgUserImpact] 
	,gs.[last_user_seek] AS [LastUserSeekTime]
    ,gs.[last_user_scan] AS [LastUserScanTime]
    ,gs.[system_seeks] AS [SystemSeeks]
    ,gs.[system_scans] AS [SystemScans]
    ,gs.[last_system_seek] AS [LastSystemSeekTime]
    ,gs.[last_system_scan] AS [LastSystemScanTime]
    ,gs.[avg_total_system_cost] AS [AvgTotalSystemCost]
    ,gs.[avg_system_impact] AS [AvgSystemImpact]
	,IIF(CharIndex(',',IsNull(id.[included_columns],''),1)=0,0, (Len(IsNull(id.[included_columns],'')) - Len(replace(IsNull(id.[included_columns],''), ',', '')))+1)  AS numberofIncludedFields
	,CONVERT(NVARCHAR(32),HASHBYTES('MD5', CONCAT(id.[statement],'=', IsNull(id.[equality_columns],'NULL'),'-',IsNULL(id.[inequality_columns], 'NULL'),'-',ISNULL(id.[included_columns], 'NULL'))),2)  as ProposedIndex_Hash
	,'CREATE INDEX [IX_' + OBJECT_NAME(id.[object_id], db.[database_id]) + '_' + REPLACE(REPLACE(REPLACE(ISNULL(id.[equality_columns], ''), ', ', '_'), '[', ''), ']', '') + CASE
        WHEN id.[equality_columns] IS NOT NULL
            AND id.[inequality_columns] IS NOT NULL
            THEN '_'
        ELSE ''
        END + REPLACE(REPLACE(REPLACE(ISNULL(id.[inequality_columns], ''), ', ', '_'), '[', ''), ']', '') + '_' + LEFT(CAST(NEWID() AS [nvarchar](64)), 5) + ']' + ' ON ' + id.[statement] + ' (' + ISNULL(id.[equality_columns], '') + CASE
        WHEN id.[equality_columns] IS NOT NULL
            AND id.[inequality_columns] IS NOT NULL
            THEN ','
        ELSE ''
        END + ISNULL(id.[inequality_columns], '') + ')' + ISNULL(' INCLUDE (' + id.[included_columns] + ')', '') AS [ProposedIndex]
FROM [sys].[dm_db_missing_index_group_stats] gs WITH (NOLOCK)
INNER JOIN [sys].[dm_db_missing_index_groups] ig WITH (NOLOCK) ON gs.[group_handle] = ig.[index_group_handle]
INNER JOIN [sys].[dm_db_missing_index_details] id WITH (NOLOCK) ON ig.[index_handle] = id.[index_handle]
INNER JOIN [sys].[databases] db WITH (NOLOCK) ON db.[database_id] = id.[database_id]
WHERE db.[database_id] > 4  
ORDER BY ((gs.[user_seeks]+gs.[user_scans]) * gs.[avg_total_user_cost] * gs.[avg_user_impact]) DESC" -f $Par1 

   # $srvrs=get-content -Path $ServerList

    $scon = New-Object System.Data.SqlClient.SqlConnection
    $scon.ConnectionString = "Data Source=$DataWarehouseServer;Initial Catalog=$DataWarehouseDatabase;Integrated Security=true"
        
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $scon
    $cmd.CommandTimeout = 0
    $scon.Open()
         
    $srvrs= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Select ServerName from [Target_Servers] where Is_Enabled=1"
    
    foreach($row in $srvrs)
    {
        $srv =$row.Item('ServerName')

        try
        {
            $mi= Invoke-Sqlcmd -Query $Query -ServerInstance $srv -ErrorAction Stop
            Write-Host "[$srv] connected.. " -ForegroundColor Green

        $rowCount=0
        foreach ($row in $mi)
        {

            $P_01=    $row.Item('ServerName')
            $P_02=    $row.Item('ServerEdition')
            $P_03=    $row.Item('EngineEdition')
            $P_04=    $row.Item('Collection_Time')
            $P_05=    $row.Item('Sqlserver_start_time')
            $P_06=    $row.Item('DatabaseName')  
            $P_07=    $row.Item('FullyQualifiedObjectName')
            $P_08=    $row.Item('ObjectID')
            $P_09=    $row.Item('SchemaName')
            $P_10=    $row.Item('TableName')
            $P_11=    $row.Item('EqualityColumns')
            $P_12=    $row.Item('InEqualityColumns')
            $P_13=    $row.Item('IncludedColumns')
            $P_14=    $row.Item('Impact')
            $P_15=    $row.Item('UserSeeks')
            $P_16=    $row.Item('UserScans')
            $P_17=    $row.Item('UniqueCompiles')
            $P_18=    $row.Item('AvgTotalUserCost')
            $P_19=    $row.Item('AvgUserImpact')
            $P_20=    $row.Item('LastUserSeekTime')
            $P_21=    $row.Item('LastUserScanTime')
            $P_22=    $row.Item('SystemSeeks')
            $P_23=    $row.Item('SystemScans')
            $P_24=    $row.Item('LastSystemSeekTime')
            $P_25=    $row.Item('LastSystemScanTime')
            $P_26=    $row.Item('AvgTotalSystemCost')
            $P_27=    $row.Item('AvgSystemImpact')
            $P_28=    $row.Item('numberofIncludedFields')
            $P_29=    $row.Item('ProposedIndex_Hash')
            $P_30=    $row.Item('ProposedIndex')

            try
            {
                $cmd.CommandText = "EXEC dbo.Missing_Index_Manage '$P_01','$P_02',$P_03,'$P_04','$P_05','$P_06','$P_07',$P_08,'$P_09','$P_10','$P_11','$P_12','$P_13',$P_14,$P_15,$P_16,$P_17,$P_18,$P_19,'$P_20','$P_21',$P_22,$P_23,'$P_24','$P_25',$P_26,$P_27,$P_28,'$P_29','$P_30'"
                $cmd.ExecuteNonQuery() | Out-Null
                $rowCount=$rowCount+1
            }
            catch [Exception] #exec each row
            {
                Write-Warning $_.Exception.Message
                $cmd.CommandText = "INSERT INTO Missing_Index_Collection_Errors (ServerName,Error_Message) VALUES ('$Srv','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null                  
            }
        }
        
        Write-Host "[$srv] wrote $rowCount row(s)" -ForegroundColor Green
        }
        Catch   #exec each srvr
        {
                Write-Warning "Problem connecting $srv"
                #$error[0]
                $cmd.CommandText = "INSERT INTO Missing_Index_Collection_Errors (ServerName,Error_Message) VALUES ('$Srv','Error Connecting $srv')"
                $cmd.ExecuteNonQuery() | Out-Null
        }

        Write-Host ""
    } 

    $scon.Close()
    $scon.Dispose()
    $cmd.Dispose()

    Make_LogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'User scan!'

}

function MissingIndexes-Check-CollectionDB
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [System.IO.FileInfo]
        $ServerList
)

#check if Colection database exists 
    $sqlDB = "SELECT Count(*) as value_data FROM Sys.Databases where Name = N'$DataWarehouseDatabase'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database 'Master' -Query $sqlDB | select -expand value_data
    
    if ( $res -ne 1)
    {
      $sqlDBCreate = "Create database [$DataWarehouseDatabase]"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database 'Master' -Query $sqlDBCreate
      Write-host "Database $DataWarehouseDatabase created!" -ForegroundColor Yellow
    } 


#check if Colection Table exists
    $obj_Name = 'Missing_Indexes'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[Missing_Indexes](
	[ServerName] [nvarchar](256) NOT NULL,
    [ServerEdition] [nvarchar](64) NULL,
    [EngineEdition] [int] NOT NULL,
	[Collection_Time] [datetime2](7) NULL,
	[First_Detected_date] [datetime2](7) NULL,
    [Last_Detected_date] [datetime2](7) NULL,
	[Number_of_Detections] [int] NULL,
	[Sqlserver_start_time] [datetime2](7) NULL,
	[DatabaseName] [nvarchar](256) NULL,
	[FullyQualifiedObjectName] [nvarchar](256) NULL,
	[ObjectID] [int] NULL,
	[SchemaName] [nvarchar](256) NULL,
	[TableName] [nvarchar](256) NOT NULL,
	[EqualityColumns] [nvarchar](max) NULL,
	[InEqualityColumns] [nvarchar](max) NULL,
	[IncludedColumns] [nvarchar](max) NULL,
	[Impact] [float] NULL,
	[UserSeeks] [bigint] NULL,
	[UserScans] [bigint] NULL,
	[UniqueCompiles] [bigint] NULL,
	[AvgTotalUserCost] [float] NULL,
	[AvgUserImpact] [float] NULL,
	[LastUserSeekTime] [datetime2](7) NULL,
	[LastUserScanTime] [datetime2](7) NULL,
	[SystemSeeks] [bigint] NULL,
	[SystemScans] [bigint] NULL,
	[LastSystemSeekTime] [datetime2](7) NULL,
	[LastSystemScanTime] [datetime2](7) NULL,
	[AvgTotalSystemCost] [float] NULL,
	[AvgSystemImpact] [float] NULL,
	[numberofIncludedFields] [int] NULL,
	[ProposedIndex_Hash] [nvarchar](32) NULL,
	[ProposedIndex] [nvarchar](max) NULL
) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [IX_Hash] ON [dbo].[Missing_Indexes] ([ProposedIndex_Hash] ASC )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    } 


#check if error Colection Table exists
    $obj_Name = 'Missing_Index_Collection_Errors'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[Missing_Index_Collection_Errors](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [nvarchar](128) NULL,
	[DateCollected] [datetime2](7) NOT NULL,
	[Error_Message] [varchar](max) NULL
) ON [PRIMARY] 
GO
ALTER TABLE [dbo].[Missing_Index_Collection_Errors] ADD  CONSTRAINT [DF_Missing_Index_Collection_Errors_DateCollected]  DEFAULT (getdate()) FOR [DateCollected]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    } 


#check if Target_Servers Table exists
    $obj_Name = 'Target_Servers'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[Target_Servers](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [nvarchar](128) NULL,
	[is_Enabled] Bit CONSTRAINT [df_Enabled] DEFAULT 1,
	[DateAdded] [datetime2](7) NOT NULL CONSTRAINT [df_DateAdded] DEFAULT GETDATE()
) ON [PRIMARY] 
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }


#check if Log_Table Table exists
    $obj_Name = 'Log_Table'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[Log_Table](
	[Log_ID] [int] IDENTITY(1,1) NOT NULL,
	[DateAdded] [datetime2](7) NOT NULL CONSTRAINT [df_DateAdded1] DEFAULT GETDATE(),
	[UserName] [nvarchar](128) NULL,
	[Comment] [nvarchar](max) NULL 
) ON [PRIMARY] 
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }


#check if Colection Table exists
    $obj_Name = 'Missing_Index_Manage'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE Procedure [dbo].[Missing_Index_Manage]
(
 @ServerName nvarchar(256),					--P01 , because I am lazy
 @ServerEdition nvarchar(50),				--P02
 @EngineEdition int,						--P03
 @Collection_Time datetime2(7),  			--P04
 @Sqlserver_start_time datetime2(7),		--P05
 @DatabaseName nvarchar(256),				--P06
 @FullyQualifiedObjectName nvarchar(256),	--P07
 @ObjectID int,								--P08
 @SchemaName nvarchar(256),					--P09
 @TableName nvarchar(256),					--P10
 @EqualityColumns nvarchar(max), 			--P11
 @InEqualityColumns nvarchar(max), 			--P12
 @IncludedColumns nvarchar(max), 			--P13
 @Impact float, 							--P14
 @UserSeeks bigint, 						--P15
 @UserScans bigint, 						--P16
 @UniqueCompiles bigint, 					--P17
 @AvgTotalUserCost float, 					--P18
 @AvgUserImpact float, 						--P19
 @LastUserSeekTime datetime2(7), 			--P20
 @LastUserScanTime datetime2(7), 			--P21
 @SystemSeeks bigint, 						--P22
 @SystemScans bigint, 						--P23
 @LastSystemSeekTime datetime2(7), 			--P24
 @LastSystemScanTime datetime2(7), 			--P25
 @AvgTotalSystemCost float, 				--P26
 @AvgSystemImpact float, 					--P27
 @numberofIncludedFields int, 				--P28
 @ProposedIndex_Hash nvarchar(32), 			--P29
 @ProposedIndex nvarchar(max)  				--P30
)					
AS
SET NOCOUNT ON;
IF EXISTS(select * from [dbo].[Missing_Indexes] where ProposedIndex_Hash =@ProposedIndex_Hash)
	UPDATE [dbo].[Missing_Indexes]
	SET [Collection_Time] = @Collection_Time , 
       [ServerName] = @ServerName,
	   [Last_Detected_Date]=@Collection_Time,
		[Number_of_Detections]=[Number_of_Detections]+1,
       --[Sqlserver_start_time] = @Sqlserver_start_time,
	   [Sqlserver_start_time]=IIF([Sqlserver_start_time] <= @Sqlserver_start_time,[Sqlserver_start_time] , @Sqlserver_start_time),
       --[DatabaseName] = @DatabaseName,
       --[ObjectID] = @ObjectID,
       --[ObjectName] = @ObjectName,
       --[FullyQualifiedObjectName] = @FullyQualifiedObjectName,
       --[EqualityColumns] = @EqualityColumns,
       --[InEqualityColumns] = @InEqualityColumns,
       --[IncludedColumns] = @IncludedColumns,
       [Impact] = @Impact,
       --[UserSeeks] = @UserSeeks,
	   --[UserScans] = @UserScans,
	   [UserSeeks] = IIF([Sqlserver_start_time] <= @Sqlserver_start_time , @UserSeeks,@UserSeeks+[UserSeeks]),
	   [UserScans] = IIF([Sqlserver_start_time] <= @Sqlserver_start_time , @UserScans,@UserScans+[UserScans]),
       [UniqueCompiles] = @UniqueCompiles,
       [AvgTotalUserCost] = @AvgTotalUserCost,
       [AvgUserImpact] = @AvgUserImpact,
       [LastUserSeekTime] = @LastUserSeekTime,
       [LastUserScanTime] = @LastUserScanTime,
       [SystemSeeks] = @SystemSeeks,
       [SystemScans] = @SystemScans,
       [LastSystemSeekTime] = @LastSystemSeekTime,
       [LastSystemScanTime] = @LastSystemScanTime,
       [AvgTotalSystemCost] = @AvgTotalSystemCost,
       [AvgSystemImpact] = @AvgSystemImpact
       --[numberofIncludedFields] = @numberofIncludedFields,
       --[ProposedIndex] = @ProposedIndex 
 WHERE  [ProposedIndex_Hash] = @ProposedIndex_Hash
ELSE
   insert into [dbo].[Missing_Indexes]
           ([ServerName]
		   ,[ServerEdition]
		   ,[EngineEdition]
           ,[Collection_Time]
		   ,[First_Detected_date]
		   ,[Last_Detected_date]
		   ,[Number_of_Detections]
           ,[Sqlserver_start_time]
           ,[DatabaseName]
           ,[FullyQualifiedObjectName]
           ,[ObjectID]
           ,[SchemaName]
           ,[TableName]
           ,[EqualityColumns]
           ,[InEqualityColumns]
           ,[IncludedColumns]
           ,[Impact]
           ,[UserSeeks]
           ,[UserScans]
           ,[UniqueCompiles]
           ,[AvgTotalUserCost]
           ,[AvgUserImpact]
           ,[LastUserSeekTime]
           ,[LastUserScanTime]
           ,[SystemSeeks]
           ,[SystemScans]
           ,[LastSystemSeekTime]
           ,[LastSystemScanTime]
           ,[AvgTotalSystemCost]
           ,[AvgSystemImpact]
           ,[numberofIncludedFields]
           ,[ProposedIndex_Hash]
           ,[ProposedIndex])
     VALUES
           (@ServerName,
		    @ServerEdition,
			@EngineEdition,
            @Collection_Time,
			@Collection_Time,
			@Collection_Time,
			1,
            @Sqlserver_start_time,
            @DatabaseName,
            @FullyQualifiedObjectName,
            @ObjectID,
			@SchemaName,
            @TableName,
            @EqualityColumns,
            @InEqualityColumns,
            @IncludedColumns,
            @Impact,
            @UserSeeks,
            @UserScans,
            @UniqueCompiles,
            @AvgTotalUserCost,
            @AvgUserImpact,
            @LastUserSeekTime,
            @LastUserScanTime,
            @SystemSeeks,
            @SystemScans,
            @LastSystemSeekTime,
            @LastSystemScanTime,
            @AvgTotalSystemCost,
            @AvgSystemImpact,
            @numberofIncludedFields, 
            @ProposedIndex_Hash, 
            @ProposedIndex)
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 

    if ($ServerList)
    { 
    Try{
        $srvrs=get-content -Path $ServerList -ErrorAction Stop

        foreach($srv in $srvrs)
            {
                $SQL_Add="IF NOT EXISTS(select * from [dbo].[Target_Servers] where ServerName ='$srv')
                          insert into [dbo].[Target_Servers] ([ServerName]) VALUES ('$srv')"

            $mi= Invoke-Sqlcmd -Query  $SQL_Add -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -ErrorAction Stop
            }
            Write-host "Server list initialized from file!" -ForegroundColor Yellow
       }
       catch
       { 
           Write-Host "Problem initializing servers list from file" 
       }
    }

}

Function Make_LogEntry
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $true)]
        [String]
        $Message
  
)
try
            {
                $UsrIns=[System.Security.Principal.WindowsIdentity]::GetCurrent().Name
                $cmdIns= "INSERT INTO [dbo].[Log_Table] ([UserName],[Comment])
                    VALUES ('$UsrIns','$Message')"
                 Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $cmdIns
            }
            catch [Exception] 
            {
                Write-Warning "Error Logging message : $_.Exception.Message" 
                
            }

}

Function MissingIndexes-Index-Valid
{
param(
[Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $true)]
        [String]
        $ServerName,
 [Parameter(Mandatory = $true)]
        [String]
        $DBName,
 [Parameter(Mandatory = $true)]
        [String]
        $TableName,
 [Parameter(Mandatory = $true)]
        [String]
        $Object_ID,
 [Parameter(Mandatory = $false)]
        [String]
        $EqualityColumns,
 [Parameter(Mandatory = $false)]
        [String]
        $InEqualityColumns,
 [Parameter(Mandatory = $false)]
        [String]
        $IncludedColumns,
 [Parameter(Mandatory = $false)]
        [String]
        $Ind_Hash,
 [Parameter(Mandatory = $false)]
        [String]
        $CreateQuery
                      
)

        $TotFound=0
        $TotMI=0

    $Indexes_Found_Tbl = New-Object System.Data.DataTable
   
        $Indexes_Found_Tbl.Columns.Add(“table_name”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“index_name”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“index_description”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“indexed_columns”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“included_columns”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“usage_scans”, "System.Int32") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“usage_seeks”, "System.Int32") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“usage_lookups”, "System.Int32") | Out-Null


##Collect existing indexes with usage

    $sqlDB = "SELECT '[' + sch.NAME + '].[' + obj.NAME + ']' AS 'table_name'
    ,+ ind.NAME AS 'index_name'
    ,LOWER(ind.type_desc) + CASE 
        WHEN ind.is_unique = 1
            THEN ', unique'
        ELSE ''
        END + CASE 
        WHEN ind.is_primary_key = 1
            THEN ', primary key'
        ELSE ''
        END AS 'index_description'
    ,STUFF((
            SELECT ', [' + sc.NAME + ']' AS ""text()""
            FROM sys.columns AS sc
            INNER JOIN sys.index_columns AS ic ON ic.object_id = sc.object_id
                AND ic.column_id = sc.column_id
            WHERE sc.object_id = obj.object_id
                AND ic.index_id = ind.index_id
                AND ic.is_included_column = 0
            ORDER BY key_ordinal
            FOR XML PATH('')
            ), 1, 2, '') AS 'indexed_columns'
    ,STUFF((
            SELECT ', [' + sc.NAME + ']' AS ""text()""
            FROM sys.columns AS sc
            INNER JOIN sys.index_columns AS ic ON ic.object_id = sc.object_id
                AND ic.column_id = sc.column_id
            WHERE sc.object_id = obj.object_id
                AND ic.index_id = ind.index_id
                AND ic.is_included_column = 1
            FOR XML PATH('')
            ), 1, 2, '') AS 'included_columns',
            ISNULL(usage.user_seeks,0) as seeks_count,
			ISNULL(usage.user_scans,0) as scans_count,
			ISNULL(usage.user_lookups,0) as lookups_count
FROM sys.indexes AS ind 
INNER JOIN sys.objects AS obj ON ind.object_id = obj.object_id
    AND obj.is_ms_shipped = 0
INNER JOIN sys.schemas AS sch ON sch.schema_id = obj.schema_id
left outer JOIN sys.dm_db_index_usage_stats AS usage ON ind.object_id = usage.object_id and ind.index_id=usage.index_id
WHERE obj.type = 'U'
    AND ind.type in (1,2)
    AND obj.NAME <> 'sysdiagrams'
	and ind.object_id=$Object_ID
ORDER BY ind.index_id"

    $IndexesFound = Invoke-Sqlcmd -ServerInstance $ServerName -Database $DBName -Query $sqlDB 
    

    
    if ($InEqualityColumns -eq '')
        {$cols=$EqualityColumns}
    elseif ($EqualityColumns -eq '')
        {$cols=$InEqualityColumns}    
    else
        {$cols=$EqualityColumns + ',' +$InEqualityColumns }
    

    foreach($indRow in $IndexesFound)
    {

        $iRow = $Indexes_Found_Tbl.NewRow()
            $iRow[“table_name”] = $indRow.Item('table_name')
            $iRow[“index_name”] = $indRow.Item('index_name')
            $iRow[“index_description”] = $indRow.Item('index_description')
            $iRow[“indexed_columns”] = $indRow.Item('indexed_columns')
            $iRow[“included_columns”] = $indRow.Item('included_columns')
            $iRow[“usage_scans”] = $indRow.Item('scans_count')
            $iRow[“usage_seeks”] = $indRow.Item('seeks_count')
            $iRow[“usage_lookups”] = $indRow.Item('lookups_count')

        $Indexes_Found_Tbl.rows.Add($iRow)


 
        if ($indRow.Item('indexed_columns') -eq  $cols)
        {
            $msg="identical index " + $indRow.Item('index_name') + " Found!"
            Write-Host $msg -ForegroundColor DarkRed
            Return
        } 

 

       $TotFound=$TotFound+1
    }

    
    
##Collect missing indexes from catalog
 
    $sqlMI = "select 
 SUBSTRING(FullyQualifiedObjectName, CHARINDEX ( '.' , FullyQualifiedObjectName  )+1,Len(FullyQualifiedObjectName)-CHARINDEX ( '.' , FullyQualifiedObjectName  )) as Table_Name,
'*' as index_name,
'Missing Index' as index_description,
case When ([InEqualityColumns]='') then [EqualityColumns]
	 When ([EqualityColumns]='') then [InEqualityColumns]
     ELSE [EqualityColumns] +'.' + [InEqualityColumns]
   END as indexed_columns,   
IncludedColumns as  included_columns
from [dbo].[Missing_Indexes]
where ServerName='$ServerName'
and DatabaseName = '$DBName'
and OBJECTID = '$Object_ID'
and [ProposedIndex_Hash] <> '$Ind_Hash'
ORDER BY indexed_columns"

    $IndexesFound2 = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlMI 
    
    
    foreach($indRow2 in $IndexesFound2)
    {

        $iRow = $Indexes_Found_Tbl.NewRow()
            $iRow[“table_name”] = $indRow2.Item('table_name')
            $iRow[“index_name”] = $indRow2.Item('index_name')
            $iRow[“index_description”] = $indRow2.Item('index_description')
            $iRow[“indexed_columns”] = $indRow2.Item('indexed_columns')
            $iRow[“included_columns”] = $indRow2.Item('included_columns')
            $iRow[“usage_scans”] = 0
            $iRow[“usage_seeks”] = 0
            $iRow[“usage_lookups”] = 0
        $Indexes_Found_Tbl.rows.Add($iRow)

       $TotMI=$TotMI+1
    }

    

    If (($TotFound + $TotMI) -gt 0)
    {
        Write-Host ($TotFound + $TotMI) -ForegroundColor Green 
         $Indexes_Found_Tbl | Format-Table
    }
    else
    {
       Write-Host "No existing and missing indexes found on table" -ForegroundColor Green 
    }
   

    Write-host "Would you like to create Missing index? (Default is No)" -ForegroundColor Yellow 
    
    $Readhost = Read-Host " ( y / n ) " 
    Switch ($ReadHost) 
     { 
       Y {Write-host "Yes, create index"; $Answ=$true} 
       N {Write-Host "No, skip this missing index"; $Answ=$false} 
       Default {Write-Host "Default, skip this missing index"; $Answ=$false}  
     } 

    IF ($Answ)
        {
            try
            {
                Invoke-Sqlcmd -ServerInstance $ServerName -Database $DBName -Query $CreateQuery
                Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "delete from [dbo].[Missing_Indexes] where [ProposedIndex_Hash]='$Ind_Hash'"
                $LogMsg0='User added ' + $iRow[“index_name”] + ' to ' + $DBName + ' database on ' + $ServerName + ' Server'
                Make_LogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message $LogMsg0
            }
            catch [Exception] #exec each row
            {
                Write-Warning $_.Exception.Message
                $cmd1 = "INSERT INTO Missing_Index_Collection_Errors (ServerName,Error_Message) VALUES ('$Srv','$_.Exception.Message')"
                Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $cmd1                  
            }
        }
} 

function MissingIndexes-Create
{
param(
[Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [int]
        $TopN=20,
 [Parameter(Mandatory = $false)]
        [int]
        $Minimal_Impact=0 
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

if ($TopN -eq 0){
   $par1=" "}
   else{
   $par1=" top {0} " -f $TopN }


$Query="
Select {0}
* from [dbo].[Missing_Indexes]
where Impact > {1} 
order by Impact DESC" -f $Par1, $Minimal_Impact



  $IndexesFound = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $Query

  foreach ($rowFound in $IndexesFound)
  {
    
    $rowFound
    MissingIndexes-Index-Valid -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase   -ServerName $rowFound.Item('ServerName') -DBName $rowFound.Item('DatabaseName') -TableName $rowFound.Item('TableName') -Object_ID $rowFound.Item('ObjectID') -EqualityColumns $rowFound.Item('EqualityColumns') -InEqualityColumns $rowFound.Item('InEqualityColumns') -IncludedColumns $rowFound.Item('IncludedColumns') -CreateQuery $rowFound.Item('ProposedIndex') -Ind_Hash $rowFound.Item('ProposedIndex_Hash') 

   }
}


Export-ModuleMember -Function MissingIndexes-Collect                 #v1.0
Export-ModuleMember -Function MissingIndexes-Check-CollectionDB      #v1.0
Export-ModuleMember -Function MissingIndexes-Index-Valid             #v1.0
Export-ModuleMember -Function MissingIndexes-Create                  #v1.0


<# Samples:
#MissingIndexes-Check-CollectionDB -DataWarehouseServer Petar_T -DataWarehouseDatabase 'SQL_Datawarehouse' -ServerList 'C:\Deploy\Query_Repository\SQLServerList.txt'

#MissingIndexes-Collect -DataWarehouseServer Petar_T -DataWarehouseDatabase Tempdb -ServerList 'C:\Deploy\Query_Repository\SQLServerList.txt'

#MissingIndexes-Create -DataWarehouseServer Petar_T 

#MissingIndexes-Create -DataWarehouseServer Petar_T -DataWarehouseDatabase TempDB 
#>