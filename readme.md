### Welcome!
This is a fork of the most used tool provided by the SQL CAT team to manage partitions in SQL Server ([https://sqlpartitionmgmt.codeplex.com/](https://sqlpartitionmgmt.codeplex.com/)).

It provides a set of commands (at the Command Line or via Powershell) to create a staging table on-demand (including all appropriate indexes and constraints) based on a specific partitioned table and a particular partition of interest.
By calling this executable, with parameters, from maintenance scripts or SSIS packages, DBAs can avoid having to "hard code" table and index definition scripts for staging tables. This solves the problem of keeping staging table scripts in synch when a permanent partitioned table evolves to contain new boundary values or column attributes. It also provides a fast, single-command shortcut for the operation of quickly deleting all data from a partition.

I decided to fork the project because it seems that the owner of the project Stuart Ozer abandoned it (the last update was on November 28, 2012). New feature are added and some issues are fixed as you can see in the [Releases Page](https://github.com/lucazav/sqlserverpartitionmanager/releases), where you can find the executables too.

If you want to collaborate, you're welcome! :) Just let me know and I'll add you as a contributor.

### Authors and Contributors
The original project creator is [Stuart Ozer](https://social.msdn.microsoft.com/Profile/stuart%20ozer%20msft)
