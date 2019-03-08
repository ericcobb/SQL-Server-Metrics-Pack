/**************************************************************************
	INDEX METRICS
	Author: Eric Cobb - http://www.sqlnuggets.com/
	Supported Versions: SQL Server 2008 R2, SQL Server 2012, SQL Server 2014, and SQL Server 2016
	License:
			MIT License

			Portions of this code (as noted in the comments) were adapted from 
			Ola Hallengren's SQL Server Maintenance Solution (https://ola.hallengren.com/), 
			and are provided under the MIT license, Copyright (c) 2017 Ola Hallengren.

			Copyright (c) 2017 Eric Cobb

			Permission is hereby granted, free of charge, to any person obtaining a copy
			of this software and associated documentation files (the "Software"), to deal
			in the Software without restriction, including without limitation the rights
			to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
			copies of the Software, and to permit persons to whom the Software is
			furnished to do so, subject to the following conditions:

			The above copyright notice and this permission notice shall be included in all
			copies or substantial portions of the Software.

			THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
			IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
			FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
			AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
			LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
			OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
			SOFTWARE.
***************************************************************************/

--Change this to whatever database you want to create the Index Metrics objects in.
USE [SQLMetrics]
GO


/**************************************************************************
	Create Table(s)
***************************************************************************/

--If our IndexMetrics table doesn't already exist, create it.
IF OBJECT_ID('dbo.IndexMetrics') IS NULL
BEGIN
	CREATE TABLE [dbo].[IndexMetrics](
		[ID] [bigint] IDENTITY(1,1) NOT NULL CONSTRAINT [PK_IndexMetrics] PRIMARY KEY CLUSTERED,
		[DatabaseID] [smallint] NOT NULL,
		[DatabaseName] [nvarchar](128) NOT NULL,
		[SchemaName] [nvarchar](128) NOT NULL,
		[TableName] [nvarchar](128) NULL,
		[IndexName] [nvarchar](128) NULL,
		[IndexID] [int] NOT NULL,
		[IndexType] [nvarchar](60) NULL,
		[PartitionNumber] [int] NULL,
		[Rows] [bigint] NULL,
		[UserSeeks] [bigint] NULL,
		[UserScans] [bigint] NULL,
		[UserLookups] [bigint] NULL,
		[UserUpdates] [bigint] NULL,
		[IndexSizeMB] [decimal](18, 2) NULL,
		[IndexMetricsChecks] [int] NOT NULL CONSTRAINT [DF_IndexMetrics_IndexMetricsCheck]  DEFAULT ((0)),
		[LastUserSeek] [datetime] NULL,
		[LastUserScan] [datetime] NULL,
		[LastUserLookup] [datetime] NULL,
		[LastUserUpdate] [datetime] NULL,
		[SystemSeeks] [bigint] NULL,
		[SystemScans] [bigint] NULL,
		[SystemLookups] [bigint] NULL,
		[SystemUpdates] [bigint] NULL,
		[LastSystemSeek] [datetime] NULL,
		[LastSystemScan] [datetime] NULL,
		[LastSystemLookup] [datetime] NULL,
		[LastSystemUpdate] [datetime] NULL,
		[isUnique] [bit] NULL,
		[isUniqueConstraint] [bit] NULL,
		[isPrimaryKey] [bit] NULL,
		[isDisabled] [bit] NULL,
		[isHypothetical] [bit] NULL,
		[allowRowLocks] [bit] NULL,
		[allowPageLocks] [bit] NULL,
		[FillFactor] [tinyint] NOT NULL,
		[hasFilter] [bit] NULL,
		[Filter] [nvarchar](max) NULL,
		[DateInitiallyChecked] [datetime] NOT NULL CONSTRAINT [DF_IndexMetrics_DateInitiallyChecked]  DEFAULT (getdate()),
		[DateLastChecked] [datetime] NOT NULL CONSTRAINT [DF_IndexMetrics_DateLastChecked]  DEFAULT (getdate()),
		[SQLServerStartTime] [datetime] NOT NULL,
		[DropStatement] [nvarchar](1000) NULL,
		[Hash] [varbinary](256) NULL
	);
END
GO



/**************************************************************************
	Create Stored Procedure(s)
***************************************************************************/

--If our procedure doesn't already exist, create one with a dummy query to be overwritten.
IF OBJECT_ID('dbo.loadIndexMetrics') IS NULL
  EXEC sp_executesql N'CREATE PROCEDURE dbo.loadIndexMetrics AS	SELECT 1;';
GO

ALTER PROCEDURE [dbo].[loadIndexMetrics]
	@DBName sysname
	,@IndexTypes NVARCHAR(256) = 'ALL'
AS 

/**************************************************************************
	Author: Eric Cobb - http://www.sqlnuggets.com/
		License:
			MIT License
			Copyright (c) 2017 Eric Cobb
			View full license disclosure: https://github.com/ericcobb/SQL-Server-Metrics-Pack/blob/master/LICENSE
			
			Portions of this code (as noted in the comments) were adapted from 
			Ola Hallengren's SQL Server Maintenance Solution (https://ola.hallengren.com/), 
			and are provided under the MIT license, Copyright (c) 2017 Ola Hallengren.

	Purpose: 
			This stored procedure is used to collect Index metrics from multiple DMVs,
			this data is then persisted in the dbo.IndexMetrics table.

	Parameters:
			@DBName - Name of the Database you want to collect Index Metrics for.
			@IndexTypes - Type of indexes you want to collect Index Metrics for. 
						- Supported IndexTypes: ALL,HEAP,CLUSTERED,NONCLUSTERED,XML,SPATIAL,CLUSTERED_COLUMNSTORE,NONCLUSTERED_COLUMNSTORE

	Usage:	
			--Collect ALL index metrics for the MyDB database
			EXEC [dbo].[loadIndexMetrics] @DBName='MyDB';

			--Collect only CLUSTERED and NONCLUSTERED index metrics for the MyDB database
			--Mulitple IndexTypes can be can be combined with a comma (,)
			EXEC [dbo].[loadIndexMetrics] @DBName='MyDB', @IndexTypes=N'CLUSTERED,NONCLUSTERED';

			--Collect ALL index metrics except for HEAPs for the MyDB database
			--The hyphen character (-) can be used to exclude IndexTypes
			EXEC [dbo].[loadIndexMetrics] @DBName='MyDB', @IndexTypes=N'ALL,-HEAP';

	Knows Issues:
			@DBName currently only supports a single database, so the loadIndexMetrics procedure will have to be run individually for 
			each database that you want to gather Index Metrics for.  Future enhancements will allow for muliple databases.
***************************************************************************/

BEGIN
	SET NOCOUNT ON

	DECLARE @sql NVARCHAR(MAX)
	DECLARE @crlf NCHAR(2) = NCHAR(13)+NCHAR(10) 
	DECLARE @IndexTypeList TABLE ([IndexType] NVARCHAR(60), [Selected] BIT DEFAULT 0);

	INSERT INTO @IndexTypeList([IndexType]) VALUES(N'HEAP'),(N'CLUSTERED'),(N'NONCLUSTERED');

	/* 
	This section of code was adapted from Ola Hallengren's SQL Server Maintenance Solution (https://ola.hallengren.com/).
	If you are not using Ola's Solution, stop what you are doing and go get it!
	*/
	SET @IndexTypes = REPLACE(@IndexTypes, CHAR(10), '');
	SET @IndexTypes = REPLACE(@IndexTypes, CHAR(13), '');

	WHILE CHARINDEX(', ',@IndexTypes) > 0 SET @IndexTypes = REPLACE(@IndexTypes,', ',',');
	WHILE CHARINDEX(' ,',@IndexTypes) > 0 SET @IndexTypes = REPLACE(@IndexTypes,' ,',',');

	SET @IndexTypes = LTRIM(RTRIM(@IndexTypes));

	IF (CHARINDEX('ALL', @IndexTypes)) > 0
		UPDATE @IndexTypeList SET [Selected] = 1;

	WITH idx1 (StartPosition, EndPosition, IndexType) AS
		(SELECT 1 AS StartPosition,
				ISNULL(NULLIF(CHARINDEX(',', @IndexTypes, 1), 0), LEN(@IndexTypes) + 1) AS EndPosition,
				SUBSTRING(@IndexTypes, 1, ISNULL(NULLIF(CHARINDEX(',', @IndexTypes, 1), 0), LEN(@IndexTypes) + 1) - 1) AS IndexType
		WHERE @IndexTypes IS NOT NULL
		UNION ALL
		SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
				ISNULL(NULLIF(CHARINDEX(',', @IndexTypes, EndPosition + 1), 0), LEN(@IndexTypes) + 1) AS EndPosition,
				SUBSTRING(@IndexTypes, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @IndexTypes, EndPosition + 1), 0), LEN(@IndexTypes) + 1) - EndPosition - 1) AS IndexType
		FROM idx1
		WHERE EndPosition < LEN(@IndexTypes) + 1
	),
		idx2 (IndexType, Selected) AS
		(SELECT CASE WHEN IndexType LIKE '-%' THEN RIGHT(IndexType,LEN(IndexType) - 1) ELSE IndexType END AS IndexType,
				CASE WHEN IndexType LIKE '-%' THEN 0 ELSE 1 END AS Selected
		FROM idx1
	)
	
	UPDATE itl
	SET itl.Selected = idx2.Selected
		,itl.IndexType = REPLACE(itl.IndexType,'_',' ')
	FROM @IndexTypeList AS itl
	INNER JOIN idx2 ON itl.IndexType = idx2.IndexType;
	/*
	End adaptation of Ola Hallengren's code.
	*/

	DELETE FROM @IndexTypeList WHERE Selected = 0;

	CREATE TABLE #IndexMetrics(
		[DatabaseID] [int]
		,[DatabaseName] [nvarchar](128) NULL
		,[SchemaName] [sysname] NOT NULL
		,[TableName] [nvarchar](128) NULL
		,[IndexName] [sysname] NULL
		,[IndexID] [int] NOT NULL
		,[IndexType] [nvarchar](60) NULL
		,[PartitionNumber] [int] NULL
		,[Rows] [bigint] NULL
		,[UserSeeks] [bigint] NULL
		,[UserScans] [bigint] NULL
		,[UserLookups] [bigint] NULL
		,[UserUpdates] [bigint] NULL
		,[IndexSizeMB] [decimal](18, 2) NULL
		,[IndexMetricsChecks] [int] NOT NULL
		,[LastUserSeek] [datetime] NULL
		,[LastUserScan] [datetime] NULL
		,[LastUserLookup] [datetime] NULL
		,[LastUserUpdate] [datetime] NULL
		,[SystemSeeks] [bigint] NULL
		,[SystemScans] [bigint] NULL
		,[SystemLookups] [bigint] NULL
		,[SystemUpdates] [bigint] NULL
		,[LastSystemSeek] [datetime] NULL
		,[LastSystemScan] [datetime] NULL
		,[LastSystemLookup] [datetime] NULL
		,[LastSystemUpdate] [datetime] NULL
		,[isUnique] [bit] NULL
		,[isUniqueConstraint] [bit] NULL
		,[isPrimaryKey] [bit] NULL
		,[isDisabled] [bit] NULL
		,[isHypothetical] [bit] NULL
		,[allowRowLocks] [bit] NULL
		,[allowPageLocks] [bit] NULL
		,[FillFactor] [tinyint] NOT NULL
		,[hasFilter] [bit] NULL
		,[Filter] [nvarchar](max) NULL
		,[DateLastChecked] [datetime] NOT NULL
		,[SQLServerStartTime] [datetime] NULL
		,[DropStatement] [nvarchar](1000) NULL
	)

	SET @sql = '
	USE '+ @DBName +' 
	
	DECLARE @sqlserver_start_time datetime, @date_last_checked datetime = GETDATE()

	SELECT @sqlserver_start_time = sqlserver_start_time from sys.dm_os_sys_info' + @crlf

	--Load the Index Metrics into our Temp table
	SET @sql = @sql +  'INSERT INTO #IndexMetrics([DatabaseID],[DatabaseName],[SchemaName],[TableName],[IndexName],[IndexID],[IndexType],[PartitionNumber],[Rows],[UserSeeks],[UserScans],[UserLookups],[UserUpdates],[IndexSizeMB],[IndexMetricsChecks],[LastUserSeek],[LastUserScan],[LastUserLookup],[LastUserUpdate],[SystemSeeks],[SystemScans],[SystemLookups],[SystemUpdates],[LastSystemSeek],[LastSystemScan],[LastSystemLookup],[LastSystemUpdate],[isUnique],[isUniqueConstraint],[isPrimaryKey],[isDisabled],[isHypothetical],[allowRowLocks],[allowPageLocks],[FillFactor],[hasFilter],[Filter],[DateLastChecked],[SQLServerStartTime],[DropStatement])
	SELECT  [DatabaseID] = DB_ID()
			,[DatabaseName] = DB_NAME()
			,[SchemaName] = s.name
			,[TableName] = OBJECT_NAME(i.OBJECT_ID)
			,[IndexName] = i.name
			,[IndexID] = i.index_id
			,[IndexType] = i.type_desc
			,[PartitionNumber] = ps.partition_number
			,[Rows] = p.TableRows
			,[UserSeeks] = COALESCE(ius.user_seeks,0)
			,[UserScans] = COALESCE(ius.user_scans,0)
			,[UserLookups] = COALESCE(ius.user_lookups,0)
			,[UserUpdates] = COALESCE(ius.user_updates,0)
			,[IndexSizeMB] = CASE WHEN ps.usedpages > ps.pages 
									THEN (ps.usedpages - ps.pages) 
									ELSE 0 
							END * 8 / 1024.0 
			,[IndexMetricsChecks] = 1
			,[LastUserSeek] = ius.last_user_seek
			,[LastUserScan] = ius.last_user_scan
			,[LastUserLookup] = ius.last_user_lookup
			,[LastUserUpdate] = ius.last_user_update
			,[SystemSeeks] = COALESCE(ius.system_seeks,0)
			,[SystemScans] = COALESCE(ius.system_scans,0)
			,[SystemLookups] = COALESCE(ius.system_lookups,0)
			,[SystemUpdates] = COALESCE(ius.system_updates,0)
			,[LastSystemSeek] = ius.last_system_seek
			,[LastSystemScan] = ius.last_system_scan
			,[LastSystemLookup] = ius.last_system_lookup
			,[LastSystemUpdate] = ius.last_system_update
			,[isUnique] = i.is_unique
			,[isUniqueConstraint] = i.is_unique_constraint
			,[isPrimaryKey] = i.is_primary_key
			,[isDisabled] = i.is_disabled
			,[isHypothetical] = i.is_hypothetical
			,[allowRowLocks] = i.allow_row_locks
			,[allowPageLocks] = i.allow_page_locks
			,[FillFactor] = i.fill_factor
			,[hasFilter] = i.has_filter
			,[Filter] = i.filter_definition
			,[DateLastChecked] = @date_last_checked
			,[SQLServerStartTime] = @sqlserver_start_time
			,[DropStatement] = CASE WHEN i.index_id >1 THEN ''DROP INDEX '' + QUOTENAME(i.name) + '' ON ['' +DB_NAME()+''].''+ QUOTENAME(s.name) + ''.'' + QUOTENAME(OBJECT_NAME(i.OBJECT_ID)) ELSE NULL END
	FROM sys.indexes i WITH (NOLOCK)
	LEFT JOIN sys.dm_db_index_usage_stats ius WITH (NOLOCK) ON ius.index_id = i.index_id AND ius.OBJECT_ID = i.OBJECT_ID
	INNER JOIN (SELECT sch.name, sch.schema_id, o.OBJECT_ID, o.create_date FROM sys.schemas sch  WITH (NOLOCK)
				INNER JOIN sys.objects o ON o.schema_id = sch.schema_id) s ON s.OBJECT_ID = i.OBJECT_ID
	LEFT JOIN (SELECT SUM(p.rows) TableRows, p.index_id, p.OBJECT_ID FROM sys.partitions p  WITH (NOLOCK)
				GROUP BY p.index_id, p.OBJECT_ID) p ON p.index_id = i.index_id AND i.OBJECT_ID = p.OBJECT_ID
	LEFT JOIN (SELECT OBJECT_ID, index_id, partition_number, SUM(used_page_count) AS usedpages,
						SUM(CASE WHEN (index_id < 2) 
								THEN (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count) 
								ELSE lob_used_page_count + row_overflow_used_page_count 
							END) AS pages
					FROM sys.dm_db_partition_stats WITH (NOLOCK)
					GROUP BY object_id, index_id, partition_number) AS ps ON i.object_id = ps.object_id AND i.index_id = ps.index_id
	WHERE OBJECTPROPERTY(i.OBJECT_ID,''IsUserTable'') = 1
	AND (ius.database_id = DB_ID() OR ius.database_id IS NULL)
	OPTION (MAXDOP 2)'

	--run the generated T-SQL
	EXECUTE sp_executesql @sql

	--Merge our temp data set into our existing table
	MERGE INTO dbo.IndexMetrics AS Target
			USING (select [DatabaseID]
						,[DatabaseName]
						,[SchemaName]
						,[TableName]
						,[IndexName]
						,[IndexID]
						,i.[IndexType]
						,[PartitionNumber]
						,[Rows]
						,[UserSeeks]
						,[UserScans]
						,[UserLookups]
						,[UserUpdates]
						,[IndexSizeMB]
						,[IndexMetricsChecks]
						,[LastUserSeek]
						,[LastUserScan]
						,[LastUserLookup]
						,[LastUserUpdate]
						,[SystemSeeks]
						,[SystemScans]
						,[SystemLookups]
						,[SystemUpdates]
						,[LastSystemSeek]
						,[LastSystemScan]
						,[LastSystemLookup]
						,[LastSystemUpdate]
						,[isUnique]
						,[isUniqueConstraint]
						,[isPrimaryKey]
						,[isDisabled]
						,[isHypothetical]
						,[allowRowLocks]
						,[allowPageLocks]
						,[FillFactor]
						,[hasFilter]
						,[Filter]
						,[DateLastChecked]
						,[SQLServerStartTime]
						,[DropStatement]
						--Generate hash to compare records with; have to use a SHA1 hash here so that we're compatible with SQL Server 2008
						,[Hash] = HASHBYTES('SHA1', CAST(i.[DatabaseID] AS NVARCHAR)
										+ i.[DatabaseName]
										+ CAST(i.[SchemaName] AS NVARCHAR(128))
										+ CAST(i.[TableName] AS NVARCHAR(128))
										+ CAST(COALESCE(i.[IndexName],'NA') AS NVARCHAR(128))
										+ CAST(i.[IndexID] AS NVARCHAR)
										+ CAST(i.[IndexType] AS NVARCHAR)
										+ CAST(i.[PartitionNumber] AS NVARCHAR))
				FROM #IndexMetrics i
				--Filter on the specified Index Types; It's faster to do it here than when loading the #IndexMetrics temp table 
				INNER JOIN @IndexTypeList itl ON itl.[IndexType] = i.[IndexType] 
			) AS Source ([DatabaseID],[DatabaseName],[SchemaName],[TableName],[IndexName],[IndexID],[IndexType],[PartitionNumber],[Rows],[UserSeeks],[UserScans],[UserLookups],[UserUpdates],[IndexSizeMB],[IndexMetricsChecks],[LastUserSeek],[LastUserScan],[LastUserLookup],[LastUserUpdate],[SystemSeeks],[SystemScans],[SystemLookups],[SystemUpdates],[LastSystemSeek],[LastSystemScan],[LastSystemLookup],[LastSystemUpdate],[isUnique],[isUniqueConstraint],[isPrimaryKey],[isDisabled],[isHypothetical],[allowRowLocks],[allowPageLocks],[FillFactor],[hasFilter],[Filter],[DateLastChecked],[SQLServerStartTime],[DropStatement],[Hash])
			ON ( Target.[Hash] = Source.[Hash] AND Target.SQLServerStartTime = Source.SQLServerStartTime)
			WHEN NOT MATCHED THEN 
				INSERT ([DatabaseID],[DatabaseName],[SchemaName],[TableName],[IndexName],[IndexID],[IndexType],[PartitionNumber],[Rows],[UserSeeks],[UserScans],[UserLookups],[UserUpdates],[IndexSizeMB],[IndexMetricsChecks],[LastUserSeek],[LastUserScan],[LastUserLookup],[LastUserUpdate],[SystemSeeks],[SystemScans],[SystemLookups],[SystemUpdates],[LastSystemSeek],[LastSystemScan],[LastSystemLookup],[LastSystemUpdate],[isUnique],[isUniqueConstraint],[isPrimaryKey],[isDisabled],[isHypothetical],[allowRowLocks],[allowPageLocks],[FillFactor],[hasFilter],[Filter],[DateLastChecked],[SQLServerStartTime],[DropStatement],[Hash])
				VALUES ([DatabaseID],[DatabaseName],[SchemaName],[TableName],[IndexName],[IndexID],[IndexType],[PartitionNumber],[Rows],[UserSeeks],[UserScans],[UserLookups],[UserUpdates],[IndexSizeMB],[IndexMetricsChecks],[LastUserSeek],[LastUserScan],[LastUserLookup],[LastUserUpdate],[SystemSeeks],[SystemScans],[SystemLookups],[SystemUpdates],[LastSystemSeek],[LastSystemScan],[LastSystemLookup],[LastSystemUpdate],[isUnique],[isUniqueConstraint],[isPrimaryKey],[isDisabled],[isHypothetical],[allowRowLocks],[allowPageLocks],[FillFactor],[hasFilter],[Filter],[DateLastChecked],[SQLServerStartTime],[DropStatement],[Hash])
			WHEN MATCHED THEN 
				UPDATE SET
						target.[Rows] = source.[Rows]
						,target.[UserSeeks] = source.[UserSeeks]
						,target.[UserScans] = source.[UserScans]
						,target.[UserLookups] = source.[UserLookups]
						,target.[UserUpdates] = source.[UserUpdates]
						,target.[IndexSizeMB] = source.[IndexSizeMB]
						,target.[IndexMetricsChecks] = target.IndexMetricsChecks + 1
						,target.[LastUserSeek] = source.[LastUserSeek]
						,target.[LastUserScan] = source.[LastUserScan]
						,target.[LastUserLookup] = source.[LastUserLookup]
						,target.[LastUserUpdate] = source.[LastUserUpdate]
						,target.[SystemSeeks] = source.[SystemSeeks]
						,target.[SystemScans] = source.[SystemScans]
						,target.[SystemLookups] = source.[SystemLookups]
						,target.[SystemUpdates] = source.[SystemUpdates]
						,target.[LastSystemSeek] = source.[LastSystemSeek]
						,target.[LastSystemScan] = source.[LastSystemScan]
						,target.[LastSystemLookup] = source.[LastSystemLookup]
						,target.[LastSystemUpdate] = source.[LastSystemUpdate]
						,target.[isUnique] = source.[isUnique]
						,target.[isUniqueConstraint] = source.[isUniqueConstraint]
						,target.[isPrimaryKey] = source.[isPrimaryKey]
						,target.[isDisabled] = source.[isDisabled]
						,target.[isHypothetical] = source.[isHypothetical]
						,target.[allowRowLocks] = source.[allowRowLocks]
						,target.[allowPageLocks] = source.[allowPageLocks]
						,target.[FillFactor] = source.[FillFactor]
						,target.[hasFilter] = source.[hasFilter]
						,target.[Filter] = source.[Filter]
						,target.[DateLastChecked] = source.[DateLastChecked]
		;

	DROP TABLE #IndexMetrics;

END
GO

/**************************************************************************
	Create View(s)
***************************************************************************/
GO

--If our view doesn't already exist, create one with a dummy query to be overwritten.
IF OBJECT_ID('dbo.vwIndexMetrics_CurrentMetricsWithTotals') IS NULL
  EXEC sp_executesql N'CREATE VIEW [dbo].[vwIndexMetrics_CurrentMetricsWithTotals] AS SELECT [DatabaseName] FROM [dbo].[IndexMetrics];';
GO

ALTER VIEW [dbo].[vwIndexMetrics_CurrentMetricsWithTotals]
AS
	/**************************************************************************
		Author: Eric Cobb - http://www.sqlnuggets.com/
		License:
				MIT License
				Copyright (c) 2017 Eric Cobb
				View full license disclosure: https://github.com/ericcobb/SQL-Server-Metrics-Pack/blob/master/LICENSE
		Purpose: 
				This view queries the IndexMetrics table to return both the current (since last SQL Server restart) and 
				total (historical aggregations across all available index data) Index metrics.		
	***************************************************************************/

	SELECT [DatabaseName]
		,[SchemaName]
		,[TableName]
		,[IndexName]
		,[IndexType]
		,[Rows]
		,[IndexSizeMB]
		,[UserSeeks]
		,[UserScans]
		,[UserLookups]
		,[UserUpdates]
		,[IndexMetricsChecks]
		,[SQLServerStartTime]
		,[totalUserSeek]
		,[totalUserScans]
		,[totalUserLookups]
		,[totalUserUpdates]
		,[TotalIndexMetricsChecks]
		,[DateInitiallyChecked]
		,[DateLastChecked]
		,[isDisabled]
		,[isHypothetical]			
		,[DropStatement]
	FROM (SELECT ixm.[DatabaseName],ixm.[SchemaName],ixm.[TableName],ixm.[IndexName],[IndexType],[Rows],ixm.[IndexSizeMB],ixm.[UserSeeks],ixm.[UserScans],
			ixm.[UserLookups],ixm.[UserUpdates],ixm.[IndexMetricsChecks],ixm.[SQLServerStartTime],t.[totalUserSeek],t.[totalUserScans],t.[totalUserLookups],t.[totalUserUpdates],
			[TotalIndexMetricsChecks] = t.[totalCount],	t.[DateInitiallyChecked], t.[DateLastChecked], ixm.[isDisabled], ixm.[isHypothetical], ixm.[DropStatement],
			ROW_NUMBER() OVER (PARTITION BY ixm.[Hash] ORDER BY ixm.SQLServerStartTime DESC) AS rn
		FROM [dbo].[IndexMetrics] ixm
		INNER JOIN (SELECT [Hash], [totaluserseek] = SUM(UserSeeks), [totalUserScans] = SUM(UserScans), [totalUserLookups] = SUM(UserLookups), 
					[totalUserUpdates] = SUM(UserUpdates), [totalcount] = SUM([IndexMetricsChecks]),[DateInitiallyChecked] = MIN([DateInitiallyChecked]),
					[DateLastChecked] = MAX([DateLastChecked])
				FROM [dbo].[IndexMetrics]
				GROUP BY [Hash]
				) t ON t.[Hash] = ixm.[Hash]
	) ix
	WHERE ix.rn = 1
GO

--If our view doesn't already exist, create one with a dummy query to be overwritten.
IF OBJECT_ID('dbo.vwIndexMetrics_CurrentActiveIndexMetrics') IS NULL
  EXEC sp_executesql N'CREATE VIEW [dbo].[vwIndexMetrics_CurrentActiveIndexMetrics] AS SELECT [DatabaseName] FROM [dbo].[IndexMetrics];';
GO

ALTER VIEW [dbo].[vwIndexMetrics_CurrentActiveIndexMetrics]
AS
	/**************************************************************************
		Author: Eric Cobb - http://www.sqlnuggets.com/
		License:
				MIT License
				Copyright (c) 2017 Eric Cobb
				View full license disclosure: https://github.com/ericcobb/SQL-Server-Metrics-Pack/blob/master/LICENSE
		Purpose: 
				This view queries the IndexMetrics table to return the metrics gathered since the last SQL Server restart,
				as determined by the sys.dm_os_sys_info DMV. Excludes Disabled and Hypothetical indexes.		
	***************************************************************************/

	SELECT [DatabaseID]
		  ,[DatabaseName]
		  ,[SchemaName]
		  ,[TableName]
		  ,[IndexName]
		  ,[IndexType]
		  ,[UserSeeks]
		  ,[UserScans]
		  ,[UserLookups]
		  ,[UserUpdates]
		  ,[LastUserSeek]
		  ,[LastUserScan]
		  ,[LastUserLookup]
		  ,[LastUserUpdate]
		  ,[SystemSeeks]
		  ,[SystemScans]
		  ,[SystemLookups]
		  ,[SystemUpdates]
		  ,[LastSystemSeek]
		  ,[LastSystemScan]
		  ,[LastSystemLookup]
		  ,[LastSystemUpdate]
		  ,[IndexMetricsChecks]
		  ,[DateInitiallyChecked]
		  ,[DateLastChecked]
		  ,[SQLServerStartTime]
  FROM [dbo].[IndexMetrics] ixm
  INNER JOIN sys.dm_os_sys_info info ON ixm.SQLServerStartTime = info.sqlserver_start_time
  WHERE ixm.isDisabled = 0
  AND	ixm.isHypothetical = 0

GO

--If our view doesn't already exist, create one with a dummy query to be overwritten.
IF OBJECT_ID('dbo.vwIndexMetrics_CurrentActiveIndexDetails') IS NULL
  EXEC sp_executesql N'CREATE VIEW [dbo].[vwIndexMetrics_CurrentActiveIndexDetails] AS SELECT [DatabaseName] FROM [dbo].[IndexMetrics];';
GO

ALTER VIEW [dbo].[vwIndexMetrics_CurrentActiveIndexDetails]
AS
	/**************************************************************************
		Author: Eric Cobb - http://www.sqlnuggets.com/
		License:
				MIT License
				Copyright (c) 2017 Eric Cobb
				View full license disclosure: https://github.com/ericcobb/SQL-Server-Metrics-Pack/blob/master/LICENSE
		Purpose: 
				This view queries the IndexMetrics table to return the index details gathered since the last SQL Server restart,
				as determined by the sys.dm_os_sys_info DMV. Excludes Disabled and Hypothetical indexes.		
	***************************************************************************/

	SELECT [DatabaseID]
		  ,[DatabaseName]
		  ,[SchemaName]
		  ,[TableName]
		  ,[IndexName]
		  ,[IndexID]
		  ,[IndexType]
		  ,[PartitionNumber]
		  ,[Rows]
		  ,[IndexSizeMB]
		  ,[isUnique]
		  ,[isUniqueConstraint]
		  ,[isPrimaryKey]
		  ,[isDisabled]
		  ,[isHypothetical]
		  ,[allowRowLocks]
		  ,[allowPageLocks]
		  ,[FillFactor]
		  ,[hasFilter]
		  ,[Filter]
		  ,[IndexMetricsChecks]
		  ,[DateInitiallyChecked]
		  ,[DateLastChecked]
		  ,[SQLServerStartTime]
		  ,[DropStatement]
  FROM [dbo].[IndexMetrics] ixm
  INNER JOIN sys.dm_os_sys_info info ON ixm.SQLServerStartTime = info.sqlserver_start_time
  WHERE ixm.isDisabled = 0
  AND	ixm.isHypothetical = 0

GO

--If our view doesn't already exist, create one with a dummy query to be overwritten.
IF OBJECT_ID('dbo.vwIndexMetrics_RarelyUsedIndexes') IS NULL
  EXEC sp_executesql N'CREATE VIEW [dbo].[vwIndexMetrics_RarelyUsedIndexes] AS SELECT [DatabaseName] FROM [dbo].[IndexMetrics];';
GO

ALTER VIEW [dbo].[vwIndexMetrics_RarelyUsedIndexes]
AS
	/**************************************************************************
		Author: Eric Cobb - http://www.sqlnuggets.com/
		License:
				MIT License
				Copyright (c) 2017 Eric Cobb
				View full license disclosure: https://github.com/ericcobb/SQL-Server-Metrics-Pack/blob/master/LICENSE
		Purpose: 
				This view queries the IndexMetrics table to return rarely used since the last SQL Server restart, as determined by the sys.dm_os_sys_info DMV. 	
				This view considers an index "rarely" used when the summed total of UserSeeks, UserScans, and UserLookups is less than 25% of number of upates to the index.
				Excludes Heaps, Clustered Indexes, Primary Keys, Disabled and Hypothetical indexes.
	***************************************************************************/

	SELECT [DatabaseID]
		  ,[DatabaseName]
		  ,[SchemaName]
		  ,[TableName]
		  ,[IndexName]
		  ,[IndexType]
		  ,[UserSeeks]
		  ,[UserScans]
		  ,[UserLookups]
		  ,[UserUpdates]
		  ,[LastUserSeek]
		  ,[LastUserScan]
		  ,[LastUserLookup]
		  ,[LastUserUpdate]
		  ,[Rows]
		  ,[IndexSizeMB]
		  ,[IndexMetricsChecks]
		  ,[DateInitiallyChecked]
		  ,[DateLastChecked]
		  ,[SQLServerStartTime]
		  ,[DropStatement]
  FROM [dbo].[IndexMetrics] ixm
  INNER JOIN sys.dm_os_sys_info info ON ixm.[SQLServerStartTime] = info.[sqlserver_start_time]
  WHERE ixm.[IndexID] > 1
  AND	(ixm.[isDisabled] = 0 AND ixm.[isHypothetical] = 0 AND ixm.[isPrimaryKey] = 0)
  AND   ((ixm.[UserSeeks] + ixm.[UserScans] + ixm.[UserLookups]) < (ixm.[UserUpdates] * 0.25)
		OR (ixm.[UserSeeks] + ixm.[UserScans] + ixm.[UserLookups]) = 0)

GO
