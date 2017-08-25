/**************************************************************************
	DATABASE FILE METRICS
	Author: Eric Cobb - http://www.sqlnuggets.com/
	Supported Versions: SQL Server 2008 R2, SQL Server 2012, SQL Server 2014, and SQL Server 2016
	License:
			MIT License

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

--Change this to whatever database you want to create the Metrics objects in.
USE [SQLMetrics]
GO


/**************************************************************************
	Create Table(s)
***************************************************************************/
GO
--If our DatabaseFileMetrics table doesn't already exist, create it.
IF OBJECT_ID('dbo.DatabaseFileMetrics') IS NULL
BEGIN
	CREATE TABLE [dbo].[DatabaseFileMetrics](
		[ID] [bigint] IDENTITY(1,1) NOT NULL CONSTRAINT [PK_DatabaseFileMetrics] PRIMARY KEY CLUSTERED,
		[DatabaseID] [smallint] NULL,
		[DatabaseName] [nvarchar](128) NULL,
		[FileID] [int] NOT NULL,
		[FileName] [nvarchar](128) NOT NULL,
		[FileType] [nvarchar](60) NULL,
		[FileLocation] [nvarchar](260) NOT NULL,
		[CurrentState] [nvarchar](60) NULL,
		[isReadOnly] [bit] NOT NULL,
		[CurrentSizeMB] [decimal](10, 2) NULL,
		[SpaceUsedMB] [decimal](10, 2) NULL,
		[PercentUsed] [decimal](10, 2) NULL,
		[FreeSpaceMB] [decimal](10, 2) NULL,
		[PercentFree] [decimal](10, 2) NULL,
		[AutoGrowth] [nvarchar](128) NULL,
		[CaptureDate] [datetime] NOT NULL CONSTRAINT [DF_DatabaseFileMetrics_CaptureDate]  DEFAULT (getdate())
	);

END
GO


/**************************************************************************
	Create Stored Procedure(s)
***************************************************************************/
GO
--If our procedure doesn't already exist, create one with a dummy query to be overwritten.
IF OBJECT_ID('dbo.loadDatabaseFileMetrics') IS NULL
  EXEC sp_executesql N'CREATE PROCEDURE dbo.loadDatabaseFileMetrics AS	SELECT 1;';
GO

ALTER PROCEDURE [dbo].[loadDatabaseFileMetrics]
	@DBName sysname --Name of the Database you want to collect Database File Metrics for.
AS 

/**************************************************************************
	Author: Eric Cobb - http://www.sqlnuggets.com/
		License:
			MIT License
			Copyright (c) 2017 Eric Cobb
			View full license disclosure: https://github.com/ericcobb/SQL-Server-Metrics-Pack/blob/master/LICENSE
			
	Purpose: 
			This stored procedure is used to collect Database File Metrics from multiple DMVs,
		    this data is persisted in the dbo.DatabaseFileMetrics table. 

	Parameters:
			@DBName - Name of the Database you want to collect Index Metrics for.

	Usage:	
			--Collect Database File Metrics for the MyDB database
			EXEC [dbo].[loadDatabaseFileMetrics] @DBName='MyDB';

	Knows Issues:
			@DBName currently only supports a single database, so the loadDatabaseFileMetrics procedure will have to be run individually for 
			each database that you want to gather Index Metrics for.  Future enhancements will allow for muliple databases.
***************************************************************************/

BEGIN

	SET NOCOUNT ON
	
	DECLARE @sql NVARCHAR(MAX)
	DECLARE @crlf NCHAR(2) = NCHAR(13)+NCHAR(10) 
	DECLARE @CaptureDate [datetime] = SYSDATETIME()

	CREATE TABLE #DBFileInfo(
		[DatabaseID] [smallint] NULL,
		[DatabaseName] [nvarchar](128) NULL,
		[FileID] [int] NOT NULL,
		[FileName] [nvarchar](128) NOT NULL,
		[FileType] [nvarchar](60) NULL,
		[FileLocation] [nvarchar](260) NOT NULL,
		[CurrentState] [nvarchar](60) NULL,
		[isReadOnly] [bit] NOT NULL,
		[CurrentSizeMB] [decimal](10, 2) NULL,
		[SpaceUsedMB] [decimal](10, 2) NULL,
		[PercentUsed] [decimal](10, 2) NULL,
		[FreeSpaceMB] [decimal](10, 2) NULL,
		[PercentFree] [decimal](10, 2) NULL,
		[AutoGrowth] [varchar](128) NULL
	) ON [PRIMARY]


	SET @sql = '
	USE '+ @DBName +' 
	' + @crlf

	--Load the Index Metrics into our Temp table
	SET @sql = @sql +  'INSERT INTO #DBFileInfo ([DatabaseID],[DatabaseName],[FileID],[FileName],[FileType],[FileLocation],[CurrentState],[isReadOnly],[CurrentSizeMB],[SpaceUsedMB],[PercentUsed],[FreeSpaceMB],[PercentFree],[AutoGrowth])
	SELECT [DatabaseID] = DB_ID()
		,[DatabaseName] = DB_NAME()
		,[FileID] = f.file_id
		,[FileName] = f.name
		,[FileType] = f.type_desc
		,[FileLocation] = f.physical_name
		,[CurrentState] = f.state_desc
		,[isReadOnly] = f.is_read_only
		,[CurrentSizeMB] = CONVERT(DECIMAL(10,2),f.SIZE/128.0)
		,[SpaceUsedMB] = CONVERT(DECIMAL(10,2),CAST(FILEPROPERTY(f.name, ''SPACEUSED'') AS INT)/128.0)
		,[PercentUsed] = CAST((CAST(FILEPROPERTY(f.name, ''SPACEUSED'')/128.0 AS DECIMAL(10,2))/CAST(f.SIZE/128.0 AS DECIMAL(10,2)))*100 AS DECIMAL(10,2))
		,[FreeSpaceMB] = CONVERT(DECIMAL(10,2),f.SIZE/128.0 - CAST(FILEPROPERTY(f.name, ''SPACEUSED'') AS INT)/128.0)
		,[PercentFree] = CONVERT(DECIMAL(10,2),((f.SIZE/128.0 - CAST(FILEPROPERTY(f.name, ''SPACEUSED'') AS INT)/128.0)/(f.SIZE/128.0))*100)
		,[AutoGrowth] = ''By '' + CASE is_percent_growth 
								WHEN 0 THEN CAST(f.GROWTH/128 AS VARCHAR(10)) + '' MB -'' 
								WHEN 1 THEN CAST(f.GROWTH AS VARCHAR(10)) + ''% -'' ELSE '''' END 
							+ CASE max_size 
								WHEN 0 THEN ''DISABLED'' 
								WHEN -1 THEN '' Unrestricted'' 
								ELSE '' Restricted to '' + CAST(max_size/(128*1024) AS VARCHAR(10)) + '' GB'' END  
	FROM sys.master_files f --use sys.master_files instead of sys.database_files, because if the database is hosted by an AlwaysOn readable secondary replica, sys.database_files.physical_name indicates the file location of the primary replica database instead.
	WHERE f.database_id = DB_ID()
	OPTION (MAXDOP 2);'

	--run the generated T-SQL
	EXECUTE sp_executesql @sql

	INSERT INTO [dbo].[DatabaseFileMetrics]
			   ([DatabaseID]
			   ,[DatabaseName]
			   ,[FileID]
			   ,[FileName]
			   ,[FileType]
			   ,[FileLocation]
			   ,[CurrentState]
			   ,[isReadOnly]
			   ,[CurrentSizeMB]
			   ,[SpaceUsedMB]
			   ,[PercentUsed]
			   ,[FreeSpaceMB]
			   ,[PercentFree]
			   ,[AutoGrowth]
			   ,[CaptureDate])
	SELECT [DatabaseID]
		  ,[DatabaseName]
		  ,[FileID]
		  ,[FileName]
		  ,[FileType]
		  ,[FileLocation]
		  ,[CurrentState]
		  ,[isReadOnly]
		  ,[CurrentSizeMB]
		  ,[SpaceUsedMB]
		  ,[PercentUsed]
		  ,[FreeSpaceMB]
		  ,[PercentFree]
		  ,[AutoGrowth]
		  ,@CaptureDate
	  FROM #DBFileInfo

	DROP TABLE #DBFileInfo
END
GO


/**************************************************************************
	Create View(s)
***************************************************************************/
GO
--If our view doesn't already exist, create one with a dummy query to be overwritten.
IF OBJECT_ID('dbo.vwDBFileMetrics_CurrentFileSizes') IS NULL
  EXEC sp_executesql N'CREATE VIEW [dbo].[vwDBFileMetrics_CurrentFileSizes] AS SELECT [DatabaseName] FROM [dbo].[DatabaseFileMetrics];';
GO

ALTER VIEW dbo.vwDBFileMetrics_CurrentFileSizes
AS

	/**************************************************************************
		Author: Eric Cobb - http://www.sqlnuggets.com/
		License:
				MIT License
				Copyright (c) 2017 Eric Cobb
				View full license disclosure: https://github.com/ericcobb/SQL-Server-Metrics-Pack/blob/master/LICENSE
		Purpose: 
				This view queries the DatabaseFileMetrics table to return the most recently recorded
				data and log file metrics.
	***************************************************************************/

	SELECT [DatabaseName]
		  ,[FileName]
		  ,[FileType]
		  ,[CurrentSize]
		  ,[SpaceUsed]
		  ,[PercentUsed]
		  ,[FreeSpace]
		  ,[PercentFree]
		  ,[AutoGrowth]
		  ,[CaptureDate]
	FROM (SELECT [DatabaseName]
			  ,[FileName]
			  ,[FileType]
			  ,[CurrentSize] = Cast([CurrentSizeMB] AS varchar(25))+' MB'
			  ,[SpaceUsed] = Cast([SpaceUsedMB] AS varchar(25))+' MB'
			  ,[PercentUsed] = Cast([PercentUsed] AS varchar(25))+'%'
			  ,[FreeSpace] = Cast([FreeSpaceMB] AS varchar(25))+' MB'
			  ,[PercentFree] = Cast([PercentFree] AS varchar(25))+'%'
			  ,[AutoGrowth]
			  ,[CaptureDate]
			  ,ROW_NUMBER() OVER (PARTITION BY [DatabaseID],[FileID] ORDER BY [CaptureDate] DESC) AS rn
		  FROM [dbo].[DatabaseFileMetrics]
		) fm
	WHERE fm.rn = 1;

GO