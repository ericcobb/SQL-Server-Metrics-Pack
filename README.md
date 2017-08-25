# SQL Server Metrics Pack
SQL Server Metrics Pack is a collection of scripts for gathering metrics from SQL Server's underlying Dynamic Management Objects (DMOs).  Since the data gathered by SQL Server's DMOs gets cleared out with every SQL Server restart, the scripts in SQL Server Metrics Pack provide a way for you to retain that data across server restarts, allowing for historical views and long term analysis.

## Who Is  SQL Server Metrics Pack Designed For?
SQL Server Metrics Pack was originally written as tool for those who do not have a true Database Administrator on staff, giving Developers, System Administrators, and others a way to easily gain in-depth insight into items such as indexes and data file usage.  However, SQL Server Metrics Pack is also a valuable tool for Database Administrators, allowing them to gather and retain these metrics over long periods of time, and analyze them to help plan future storage & growth needs.


## Index Metrics
The Index Metrics scripts provide a way for you to see and track index usage over time.  By storing this information in a table, it allows you to continuously collect and aggregate this data across SQL Server restarts, without having to have knowledge of (or access to) SQL Server's internal DMOs.

### Installation:
Simply run the *index-metrics-install.sql* file, specifying which database you want the objects to be created in, and then schedule the *loadIndexMetrics* Stored Procedure to run on a regular basis (I recommend nightly).  On each scheduled run, the Procedure will populate and/or aggregate the current index metrics for the designated database.  The install will also create Views that are ready to calculate and report on your index metrics.

### Features:
- See how much or how little an index is being used
- See if an index's usage has increased or decreased over time
- Identify unused and rarely used indexes

### Benefits:
- Database Administrators can give Developers permissions to read the Index Metrics Views without having to grant the elevated access sometimes required to run SQL Server's DMOs.  This allows Developers to gain deep insight into index usage in Production systems that they may not have access to otherwise.


## Database File Metrics
The Database File Metrics scripts provide insight into the data and log files of your database

### Installation:
Simply run the *database-file-metrics-install.sql* file, specifying which database you want the objects to be created in, and then schedule the *loadDatabaseFileMetrics* Stored Procedure to run on a regular basis (I recommend nightly).  On each scheduled run, the Procedure will populate the current database file metrics for the designated database.  The install will also create Views that are ready to report on your database file metrics.

### Features:
- Keep check on the amount of used and free space inside of your data files
- Easily see autogrowth settings for each database file

### Benefits:
- Track the amount of free space in your database files, and determine when to grow your files to avoid costly auto-growth events.
- Track database file growths over time to plan future storage & growth needs
