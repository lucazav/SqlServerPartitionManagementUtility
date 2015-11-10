-- Testing scenarios for ManagePartition 2.0 on SQL2005
--
-- Stuart Ozer
-- February 2008
---
-- Relies on Adventureworks2008 database installed on the test instance
--
-- NOTE:  Commented lines invoking ManagePartition.exe must be executed 
--        from the command line in the bin/release directory of the tool

-- Create the DB

USE [master]
GO

DROP DATABASE PartitionTest
go 

CREATE DATABASE [PartitionTest] ON  PRIMARY 
( NAME = N'PartitionTest', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL.1\MSSQL\Data\PartitionTest.mdf' , SIZE = 21504KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB ), 
 FILEGROUP [FG1] 
( NAME = N'FG1_1', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL.1\MSSQL\Data\FG1_1.ndf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB ), 
 FILEGROUP [FG2] 
( NAME = N'FG2_1', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL.1\MSSQL\Data\FG2_1.ndf' , SIZE = 6144KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB ), 
 FILEGROUP [FG3] 
( NAME = N'FG3_1', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL.1\MSSQL\Data\FG3_1.ndf' , SIZE = 3072KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'PartitionTest_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL.1\MSSQL\Data\PartitionTest_log.ldf' , SIZE = 39296KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
GO

ALTER DATABASE [PartitionTest] SET ANSI_NULL_DEFAULT OFF 
GO

ALTER DATABASE [PartitionTest] SET ANSI_NULLS OFF 
GO

ALTER DATABASE [PartitionTest] SET ANSI_PADDING OFF 
GO

ALTER DATABASE [PartitionTest] SET ANSI_WARNINGS OFF 
GO

ALTER DATABASE [PartitionTest] SET ARITHABORT OFF 
GO

ALTER DATABASE [PartitionTest] SET AUTO_CLOSE OFF 
GO

ALTER DATABASE [PartitionTest] SET AUTO_CREATE_STATISTICS ON 
GO

ALTER DATABASE [PartitionTest] SET AUTO_SHRINK OFF 
GO

ALTER DATABASE [PartitionTest] SET AUTO_UPDATE_STATISTICS ON 
GO

ALTER DATABASE [PartitionTest] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO

ALTER DATABASE [PartitionTest] SET CURSOR_DEFAULT  GLOBAL 
GO

ALTER DATABASE [PartitionTest] SET CONCAT_NULL_YIELDS_NULL OFF 
GO

ALTER DATABASE [PartitionTest] SET NUMERIC_ROUNDABORT OFF 
GO

ALTER DATABASE [PartitionTest] SET QUOTED_IDENTIFIER OFF 
GO

ALTER DATABASE [PartitionTest] SET RECURSIVE_TRIGGERS OFF 
GO

ALTER DATABASE [PartitionTest] SET  DISABLE_BROKER 
GO

ALTER DATABASE [PartitionTest] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO

ALTER DATABASE [PartitionTest] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO

ALTER DATABASE [PartitionTest] SET TRUSTWORTHY OFF 
GO

ALTER DATABASE [PartitionTest] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO

ALTER DATABASE [PartitionTest] SET PARAMETERIZATION SIMPLE 
GO

ALTER DATABASE [PartitionTest] SET READ_COMMITTED_SNAPSHOT OFF 
GO

ALTER DATABASE [PartitionTest] SET  READ_WRITE 
GO

ALTER DATABASE [PartitionTest] SET RECOVERY FULL 
GO

ALTER DATABASE [PartitionTest] SET  MULTI_USER 
GO

ALTER DATABASE [PartitionTest] SET PAGE_VERIFY CHECKSUM  
GO

ALTER DATABASE [PartitionTest] SET DB_CHAINING OFF 
GO

------------------------------------------------------------
-- Copy some reference tables

USE [AdventureWorks]

-- select distinct DATEPART(year,OrderDate) from Sales.SalesOrderHeader
-- select distinct salespersonid from Sales.SalesOrderHeader order by 1
-- select distinct status from Sales.SalesOrderHeader order by 1

Select * into PartitionTest.dbo.Address from Person.Address;

Select * into PartitionTest.dbo.CurrencyRate from Sales.CurrencyRate;

------------------------------------------------------
-- Create the partitioned table and indexes

USE [PartitionTest]
GO

ALTER TABLE [Address] ADD  CONSTRAINT [PK_Address_AddressID] PRIMARY KEY CLUSTERED 
(
	[AddressID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]


ALTER TABLE [CurrencyRate] ADD  CONSTRAINT [PK_CurrencyRate_CurrencyRateID] PRIMARY KEY CLUSTERED 
(
	[CurrencyRateID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

Create Partition Function pf_l_date (datetime) as RANGE LEFT  for values
('01/01/2001', '01/01/2002', '01/01/2003', '01/01/2004', '01/01/2005');

Create Partition Scheme ps_l_date as partition pf_l_date to (fg1, fg2, fg3, fg1, fg2, fg3);

Create Partition Function pf_r_date (datetime) as RANGE RIGHT  for values
('01/01/2001', '01/01/2002', '01/01/2003', '01/01/2004', '01/01/2005');
Create Partition Scheme ps_r_date as partition pf_r_date to (fg1, fg2, fg3, fg1, fg2, fg3);

Create Partition Function pf_l_salespers (int) as RANGE LEFT for values
(275, 280, 285, 290);
Create Partition Scheme ps_l_salespers as partition pf_l_salespers to (fg1, fg2, fg3, fg1, fg2);

Create Partition Function pf_r_salespers (int) as RANGE RIGHT for values
(275, 280, 285, 290);
Create Partition Scheme ps_r_salespers as partition pf_r_salespers all to ([PRIMARY]);

CREATE TYPE [dbo].[Flag] FROM [bit] NOT NULL
GO
CREATE TYPE [dbo].[OrderNumber] FROM [nvarchar](25) NULL
GO
CREATE TYPE [dbo].[AccountNumber] FROM [nvarchar](15) NULL
GO

-------------------------------
-- Range Left date (quotable) Key
DROP VIEW v_SOH_CC_SHIP;
go
drop view [v_SOH_SALESP_TOTALS]
go
DROP VIEW t1_v_SOH_CC_SHIP
go
DROP VIEW t2_v_SOH_CC_SHIP
go
DROP VIEW t3_v_SOH_CC_SHIP
go
DROP VIEW t1_v_SOH_SALESP_TOTALS
go
DROP VIEW t2_v_SOH_SALESP_TOTALS
go
DROP VIEW t3_v_SOH_SALESP_TOTALS
go
DROP TABLE SalesOrderHeader;
go
drop table t1;
go
drop table t2;
go
drop table t3;
go

CREATE TABLE [SalesOrderHeader](
	[SalesOrderID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[RevisionNumber] [tinyint] NOT NULL,
	[OrderDate] [datetime] NOT NULL,
	[DueDate] [datetime] NOT NULL,
	[ShipDate] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[OnlineOrderFlag] [dbo].[Flag] NOT NULL,
	[SalesOrderNumber]  AS (isnull(N'SO'+CONVERT([nvarchar](23),[SalesOrderID],0),N'*** ERROR ***')),
	[PurchaseOrderNumber] [dbo].[OrderNumber] NULL,
	[AccountNumber] [dbo].[AccountNumber] NULL,
	[CustomerID] [int] NOT NULL,
	[SalesPersonID] [int] NULL,
	[TerritoryID] [int] NULL,
	[BillToAddressID] [int] NOT NULL,
	[ShipToAddressID] [int] NOT NULL,
	[ShipMethodID] [int] NOT NULL,
	[CreditCardID] [int] NULL,
	[CreditCardApprovalCode] [varchar](15) NULL,
	[CurrencyRateID] [int] NULL,
	[SubTotal] [money] NOT NULL,
	[TaxAmt] [money] NOT NULL,
	[Freight] [money] NOT NULL,
	[TotalDue]  AS (isnull(([SubTotal]+[TaxAmt])+[Freight],(0))) PERSISTED,
	[Comment] [nvarchar](128) NULL,
	[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_SalesOrderHeader_SalesOrderID] PRIMARY KEY CLUSTERED 
(
	[SalesOrderID] ASC, [OrderDate] ASC
)WITH (PAD_INDEX  = OFF, 
	STATISTICS_NORECOMPUTE  = OFF, 
	IGNORE_DUP_KEY = OFF, 
	ALLOW_ROW_LOCKS  = ON, 
	ALLOW_PAGE_LOCKS  = ON) ON ps_l_date(OrderDate)
) ON ps_l_date(OrderDate)
go

Create nonclustered index NC_SOH_CUST on SalesOrderHeader(CustomerID)
on ps_l_date(OrderDate)
go

Create nonclustered index NC_SOH_ACCT on SalesOrderHeader(AccountNumber)
on ps_l_date(OrderDate)
go

Create nonclustered index NC_SOH_SONBR on SalesOrderHeader(SalesOrderNumber)
on ps_l_date(OrderDate)
go

Create nonclustered index NC_SOH_TERR on SalesOrderHeader(TerritoryID, AccountNumber)
INCLUDE(ShipDate, DueDate)
on ps_l_date(OrderDate)
go

Create unique nonclustered index NC_SOH_SOID on SalesOrderHeader([SalesOrderID], [OrderDate])
on ps_l_date(OrderDate)
go

set identity_insert SalesOrderHeader ON

insert into SalesOrderHeader with (TABLOCK) 
(	   [SalesOrderID]
      ,[RevisionNumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
      ,[Status]
      ,[OnlineOrderFlag]
--      ,[SalesOrderNumber]
      ,[PurchaseOrderNumber]
      ,[AccountNumber]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[TerritoryID]
      ,[BillToAddressID]
      ,[ShipToAddressID]
      ,[ShipMethodID]
      ,[CreditCardID]
      ,[CreditCardApprovalCode]
      ,[CurrencyRateID]
      ,[SubTotal]
      ,[TaxAmt]
      ,[Freight]
--      ,[TotalDue]
      ,[Comment]
      ,[rowguid]
      ,[ModifiedDate])
SELECT [SalesOrderID]
      ,[RevisionNumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
      ,[Status]
      ,[OnlineOrderFlag]
--      ,[SalesOrderNumber]
      ,[PurchaseOrderNumber]
      ,[AccountNumber]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[TerritoryID]
      ,[BillToAddressID]
      ,[ShipToAddressID]
      ,[ShipMethodID]
      ,[CreditCardID]
      ,[CreditCardApprovalCode]
      ,[CurrencyRateID]
      ,[SubTotal]
      ,[TaxAmt]
      ,[Freight]
--      ,[TotalDue]
      ,[Comment]
      ,[rowguid]
      ,[ModifiedDate]
 from AdventureWorks.Sales.SalesOrderHeader;

SET ANSI_PADDING ON
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_BillToAddressID] FOREIGN KEY([BillToAddressID])
REFERENCES [Address] ([AddressID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_BillToAddressID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_ShipToAddressID] FOREIGN KEY([ShipToAddressID])
REFERENCES [Address] ([AddressID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_ShipToAddressID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_CurrencyRate_CurrencyRateID] FOREIGN KEY([CurrencyRateID])
REFERENCES [CurrencyRate] ([CurrencyRateID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_CurrencyRate_CurrencyRateID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_DueDate] CHECK  (([DueDate]>=[OrderDate]))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_DueDate]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_Freight] CHECK  (([Freight]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_Freight]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_ShipDate] CHECK  (([ShipDate]>=[OrderDate] OR [ShipDate] IS NULL))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_ShipDate]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_Status] CHECK  (([Status]>=(0) AND [Status]<=(8)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_Status]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_SubTotal] CHECK  (([SubTotal]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_SubTotal]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_TaxAmt] CHECK  (([TaxAmt]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_TaxAmt]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_RevisionNumber]  DEFAULT ((0)) FOR [RevisionNumber]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_OrderDate]  DEFAULT (getdate()) FOR [OrderDate]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_Status]  DEFAULT ((1)) FOR [Status]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_OnlineOrderFlag]  DEFAULT ((1)) FOR [OnlineOrderFlag]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_SubTotal]  DEFAULT ((0.00)) FOR [SubTotal]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_TaxAmt]  DEFAULT ((0.00)) FOR [TaxAmt]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_Freight]  DEFAULT ((0.00)) FOR [Freight]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_rowguid]  DEFAULT (newid()) FOR [rowguid]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO

ALTER INDEX NC_SOH_SONBR on SalesOrderHeader DISABLE
go

----------------------------------------------------

/* ManagePartition tests:

ManagePartition  /C:CreateStagingFull /PartitionRangeValue:2002-01-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t1

ManagePartition  /C:CreateStagingClusteredIndex /PartitionRangeValue:2003-01-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2

ManagePartition  /C:CreateStagingNoIndex /PartitionRangeValue:2001-01-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3

Errs:
Object reference not set to an instance of an object:  Invalid Table Name
NOTE:  Switch out works even when non-clustered index is not available!
*/
-------------------------------------------------------

Alter table [SalesOrderHeader] switch partition 2 to t1;
go
Alter table t1 switch to [SalesOrderHeader] partition 2;
go

-- ManagePartition  /C:IndexStaging /PartitionRangeValue:2003-01-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2

Alter table [SalesOrderHeader] switch partition 3 to t2;
go
Alter table t2 switch to [SalesOrderHeader] partition 3;
go
-- ManagePartition  /C:IndexStaging /PartitionRangeValue:2001-01-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3

Alter table [SalesOrderHeader] switch partition 1 to t3;
go
Alter table t3 switch to [SalesOrderHeader] partition 1;
go
---------------------------------------

-- Range Right date (quotable) Key

DROP TABLE SalesOrderHeader;
go
drop table t1;
go
drop table t2;
go
drop table t3;
go

CREATE TABLE [SalesOrderHeader](
	[SalesOrderID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[RevisionNumber] [tinyint] NOT NULL,
	[OrderDate] [datetime] NOT NULL,
	[DueDate] [datetime] NOT NULL,
	[ShipDate] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[OnlineOrderFlag] [dbo].[Flag] NOT NULL,
	[SalesOrderNumber]  AS (isnull(N'SO'+CONVERT([nvarchar](23),[SalesOrderID],0),N'*** ERROR ***')),
	[PurchaseOrderNumber] [dbo].[OrderNumber] NULL,
	[AccountNumber] [dbo].[AccountNumber] NULL,
	[CustomerID] [int] NOT NULL,
	[SalesPersonID] [int] NULL,
	[TerritoryID] [int] NULL,
	[BillToAddressID] [int] NOT NULL,
	[ShipToAddressID] [int] NOT NULL,
	[ShipMethodID] [int] NOT NULL,
	[CreditCardID] [int] NULL,
	[CreditCardApprovalCode] [varchar](15) NULL,
	[CurrencyRateID] [int] NULL,
	[SubTotal] [money] NOT NULL,
	[TaxAmt] [money] NOT NULL,
	[Freight] [money] NOT NULL,
	[TotalDue]  AS (isnull(([SubTotal]+[TaxAmt])+[Freight],(0))) PERSISTED,
	[Comment] [nvarchar](128)  NULL,
	[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_SalesOrderHeader_SalesOrderID] PRIMARY KEY CLUSTERED 
(
	[SalesOrderID] ASC, [OrderDate] ASC
)WITH (PAD_INDEX  = OFF, 
	STATISTICS_NORECOMPUTE  = OFF, 
	IGNORE_DUP_KEY = OFF, 
	ALLOW_ROW_LOCKS  = ON, 
	ALLOW_PAGE_LOCKS  = ON) ON ps_r_date(OrderDate)
) ON ps_r_date(OrderDate)
go

Create nonclustered index NC_SOH_CUST on SalesOrderHeader(CustomerID)
on ps_r_date(OrderDate)
go

Create nonclustered index NC_SOH_ACCT on SalesOrderHeader(AccountNumber)
on ps_r_date(OrderDate)
go

Create nonclustered index NC_SOH_SONBR on SalesOrderHeader(SalesOrderNumber)
on ps_r_date(OrderDate)
go

Create nonclustered index NC_SOH_TERR on SalesOrderHeader(TerritoryID, AccountNumber)
INCLUDE(ShipDate, DueDate)
on ps_r_date(OrderDate)
go

Create unique nonclustered index NC_SOH_SOID on SalesOrderHeader([SalesOrderID], [OrderDate])
on ps_r_date(OrderDate)
go

set identity_insert SalesOrderHeader ON

insert into SalesOrderHeader with (TABLOCK) 
(	   [SalesOrderID]
      ,[RevisionNumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
      ,[Status]
      ,[OnlineOrderFlag]
--      ,[SalesOrderNumber]
      ,[PurchaseOrderNumber]
      ,[AccountNumber]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[TerritoryID]
      ,[BillToAddressID]
      ,[ShipToAddressID]
      ,[ShipMethodID]
      ,[CreditCardID]
      ,[CreditCardApprovalCode]
      ,[CurrencyRateID]
      ,[SubTotal]
      ,[TaxAmt]
      ,[Freight]
--      ,[TotalDue]
      ,[Comment]
      ,[rowguid]
      ,[ModifiedDate])
SELECT [SalesOrderID]
      ,[RevisionNumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
      ,[Status]
      ,[OnlineOrderFlag]
--      ,[SalesOrderNumber]
      ,[PurchaseOrderNumber]
      ,[AccountNumber]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[TerritoryID]
      ,[BillToAddressID]
      ,[ShipToAddressID]
      ,[ShipMethodID]
      ,[CreditCardID]
      ,[CreditCardApprovalCode]
      ,[CurrencyRateID]
      ,[SubTotal]
      ,[TaxAmt]
      ,[Freight]
--      ,[TotalDue]
      ,[Comment]
      ,[rowguid]
      ,[ModifiedDate]
 from AdventureWorks.Sales.SalesOrderHeader;

SET ANSI_PADDING ON
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_BillToAddressID] FOREIGN KEY([BillToAddressID])
REFERENCES [Address] ([AddressID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_BillToAddressID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_ShipToAddressID] FOREIGN KEY([ShipToAddressID])
REFERENCES [Address] ([AddressID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_ShipToAddressID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_CurrencyRate_CurrencyRateID] FOREIGN KEY([CurrencyRateID])
REFERENCES [CurrencyRate] ([CurrencyRateID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_CurrencyRate_CurrencyRateID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_DueDate] CHECK  (([DueDate]>=[OrderDate]))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_DueDate]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_Freight] CHECK  (([Freight]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_Freight]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_ShipDate] CHECK  (([ShipDate]>=[OrderDate] OR [ShipDate] IS NULL))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_ShipDate]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_Status] CHECK  (([Status]>=(0) AND [Status]<=(8)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_Status]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_SubTotal] CHECK  (([SubTotal]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_SubTotal]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_TaxAmt] CHECK  (([TaxAmt]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_TaxAmt]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_RevisionNumber]  DEFAULT ((0)) FOR [RevisionNumber]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_OrderDate]  DEFAULT (getdate()) FOR [OrderDate]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_Status]  DEFAULT ((1)) FOR [Status]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_OnlineOrderFlag]  DEFAULT ((1)) FOR [OnlineOrderFlag]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_SubTotal]  DEFAULT ((0.00)) FOR [SubTotal]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_TaxAmt]  DEFAULT ((0.00)) FOR [TaxAmt]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_Freight]  DEFAULT ((0.00)) FOR [Freight]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_rowguid]  DEFAULT (newid()) FOR [rowguid]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO

ALTER INDEX NC_SOH_SONBR on SalesOrderHeader DISABLE
go

----------------------------------------------------

/* ManagePartition tests:

ManagePartition  /C:CreateStagingFull /PartitionRangeValue:2002-01-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t1

ManagePartition  /C:CreateStagingClusteredIndex /PartitionRangeValue:2003-01-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2

ManagePartition  /C:CreateStagingNoIndex /PartitionRangeValue:2000-12-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3

*/
-------------------------------------------------------

Alter table [SalesOrderHeader] switch partition 3 to t1;
go
Alter table t1 switch to [SalesOrderHeader] partition 3;
go

-- ManagePartition  /C:IndexStaging /PartitionRangeValue:2003-01-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2

Alter table [SalesOrderHeader] switch partition 4 to t2;
go
Alter table t2 switch to [SalesOrderHeader] partition 4;
go
-- ManagePartition  /C:IndexStaging /PartitionRangeValue:2000-12-01 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3

Alter table [SalesOrderHeader] switch partition 1 to t3;
go
Alter table t3 switch to [SalesOrderHeader] partition 1;
go

-------------------------------------------------

-- Range left nullable int Key; 
DROP TABLE SalesOrderHeader;
go
drop table t1;
go
drop table t2;
go
drop table t3;
go

CREATE TABLE [SalesOrderHeader](
	[SalesOrderID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[RevisionNumber] [tinyint] NOT NULL,
	[OrderDate] [datetime] NOT NULL,
	[DueDate] [datetime] NOT NULL,
	[ShipDate] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[OnlineOrderFlag] [dbo].[Flag] NOT NULL,
	[SalesOrderNumber]  AS (isnull(N'SO'+CONVERT([nvarchar](23),[SalesOrderID],0),N'*** ERROR ***')),
	[PurchaseOrderNumber] [dbo].[OrderNumber] NULL,
	[AccountNumber] [dbo].[AccountNumber] NULL,
	[CustomerID] [int] NOT NULL,
	[SalesPersonID] [int] NULL,
	[TerritoryID] [int] NULL,
	[BillToAddressID] [int] NOT NULL,
	[ShipToAddressID] [int] NOT NULL,
	[ShipMethodID] [int] NOT NULL,
	[CreditCardID] [int] NULL,
	[CreditCardApprovalCode] [varchar](15) NULL,
	[CurrencyRateID] [int] NULL,
	[SubTotal] [money] NOT NULL,
	[TaxAmt] [money] NOT NULL,
	[Freight] [money] NOT NULL,
	[TotalDue]  AS (isnull(([SubTotal]+[TaxAmt])+[Freight],(0))) PERSISTED,
	[Comment] [nvarchar](128) NULL,
	[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL,
	[ModifiedDate] [datetime] NOT NULL
)
 ON ps_l_salespers(SalesPersonID)
go

Create Clustered Index CI_SOH_Spers on SalesOrderHeader (SalesPersonID, TerritoryID)
ON ps_l_salespers(SalesPersonID)


Create nonclustered index NC_SOH_CUST on SalesOrderHeader(CustomerID)
on ps_l_salespers(SalesPersonID)
go

Create nonclustered index NC_SOH_ACCT on SalesOrderHeader(AccountNumber)
on ps_l_salespers(SalesPersonID)
go

Create nonclustered index NC_SOH_SONBR on SalesOrderHeader(SalesOrderNumber)
on ps_l_salespers(SalesPersonID)
go

Create nonclustered index NC_SOH_TERR on SalesOrderHeader(TerritoryID, AccountNumber)
INCLUDE(ShipDate, DueDate)
on ps_l_salespers(SalesPersonID)
go

Create unique nonclustered index NC_SOH_SOID on SalesOrderHeader([SalesOrderID], [OrderDate], [SalesPersonID])
on ps_l_salespers(SalesPersonID)
go

set identity_insert SalesOrderHeader ON

insert into SalesOrderHeader with (TABLOCK) 
(	   [SalesOrderID]
      ,[RevisionNumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
      ,[Status]
      ,[OnlineOrderFlag]
--      ,[SalesOrderNumber]
      ,[PurchaseOrderNumber]
      ,[AccountNumber]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[TerritoryID]
      ,[BillToAddressID]
      ,[ShipToAddressID]
      ,[ShipMethodID]
      ,[CreditCardID]
      ,[CreditCardApprovalCode]
      ,[CurrencyRateID]
      ,[SubTotal]
      ,[TaxAmt]
      ,[Freight]
--      ,[TotalDue]
      ,[Comment]
      ,[rowguid]
      ,[ModifiedDate])
SELECT [SalesOrderID]
      ,[RevisionNumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
      ,[Status]
      ,[OnlineOrderFlag]
--      ,[SalesOrderNumber]
      ,[PurchaseOrderNumber]
      ,[AccountNumber]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[TerritoryID]
      ,[BillToAddressID]
      ,[ShipToAddressID]
      ,[ShipMethodID]
      ,[CreditCardID]
      ,[CreditCardApprovalCode]
      ,[CurrencyRateID]
      ,[SubTotal]
      ,[TaxAmt]
      ,[Freight]
--      ,[TotalDue]
      ,[Comment]
      ,[rowguid]
      ,[ModifiedDate]
 from AdventureWorks.Sales.SalesOrderHeader;

SET ANSI_PADDING ON
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_BillToAddressID] FOREIGN KEY([BillToAddressID])
REFERENCES [Address] ([AddressID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_BillToAddressID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_ShipToAddressID] FOREIGN KEY([ShipToAddressID])
REFERENCES [Address] ([AddressID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_ShipToAddressID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_CurrencyRate_CurrencyRateID] FOREIGN KEY([CurrencyRateID])
REFERENCES [CurrencyRate] ([CurrencyRateID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_CurrencyRate_CurrencyRateID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_DueDate] CHECK  (([DueDate]>=[OrderDate]))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_DueDate]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_Freight] CHECK  (([Freight]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_Freight]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_ShipDate] CHECK  (([ShipDate]>=[OrderDate] OR [ShipDate] IS NULL))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_ShipDate]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_Status] CHECK  (([Status]>=(0) AND [Status]<=(8)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_Status]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_SubTotal] CHECK  (([SubTotal]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_SubTotal]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_TaxAmt] CHECK  (([TaxAmt]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_TaxAmt]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_RevisionNumber]  DEFAULT ((0)) FOR [RevisionNumber]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_OrderDate]  DEFAULT (getdate()) FOR [OrderDate]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_Status]  DEFAULT ((1)) FOR [Status]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_OnlineOrderFlag]  DEFAULT ((1)) FOR [OnlineOrderFlag]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_SubTotal]  DEFAULT ((0.00)) FOR [SubTotal]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_TaxAmt]  DEFAULT ((0.00)) FOR [TaxAmt]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_Freight]  DEFAULT ((0.00)) FOR [Freight]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_rowguid]  DEFAULT (newid()) FOR [rowguid]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO

ALTER INDEX NC_SOH_SONBR on SalesOrderHeader DISABLE
go

-------------------------------------------------------
/* ManagePartition tests:

ManagePartition  /C:CreateStagingFull /PartitionRangeValue:280 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t1

ManagePartition  /C:CreateStagingClusteredIndex /PartitionRangeValue:290 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2

ManagePartition  /C:CreateStagingNoIndex /PartitionRangeValue:270 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3

*/
-------------------------------------------------------
Alter table [SalesOrderHeader] switch partition 2 to t1;
go
Alter table t1 switch to [SalesOrderHeader] partition 2;
go

-- ManagePartition  /C:IndexStaging /PartitionRangeValue:290 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2

Alter table [SalesOrderHeader] switch partition 4 to t2;
go
Alter table t2 switch to [SalesOrderHeader] partition 4;
go
-- ManagePartition  /C:IndexStaging /PartitionRangeValue:270 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3

Alter table [SalesOrderHeader] switch partition 1 to t3;
go
Alter table t3 switch to [SalesOrderHeader] partition 1;
go

---------------------------------------------------------

-- Range right nullable int Key

DROP TABLE SalesOrderHeader;
go
drop table t1;
go
drop table t2;
go
drop table t3;
go
CREATE TABLE [SalesOrderHeader](
	[SalesOrderID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[RevisionNumber] [tinyint] NOT NULL,
	[OrderDate] [datetime] NOT NULL,
	[DueDate] [datetime] NOT NULL,
	[ShipDate] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[OnlineOrderFlag] [dbo].[Flag] NOT NULL,
	[SalesOrderNumber]  AS (isnull(N'SO'+CONVERT([nvarchar](23),[SalesOrderID],0),N'*** ERROR ***')),
	[PurchaseOrderNumber] [dbo].[OrderNumber] NULL,
	[AccountNumber] [dbo].[AccountNumber] NULL,
	[CustomerID] [int] NOT NULL,
	[SalesPersonID] [int] NULL,
	[TerritoryID] [int] NULL,
	[BillToAddressID] [int] NOT NULL,
	[ShipToAddressID] [int] NOT NULL,
	[ShipMethodID] [int] NOT NULL,
	[CreditCardID] [int] NULL,
	[CreditCardApprovalCode] [varchar](15) NULL,
	[CurrencyRateID] [int] NULL,
	[SubTotal] [money] NOT NULL,
	[TaxAmt] [money] NOT NULL,
	[Freight] [money] NOT NULL,
	[TotalDue]  AS (isnull(([SubTotal]+[TaxAmt])+[Freight],(0))) PERSISTED,
	[Comment] [nvarchar](128)  NULL,
	[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL,
	[ModifiedDate] [datetime] NOT NULL
)
 ON ps_r_salespers(SalesPersonID)
go

Create Clustered Index CI_SOH_Spers on SalesOrderHeader (SalesPersonID, TerritoryID)
ON ps_r_salespers(SalesPersonID)


Create nonclustered index NC_SOH_CUST on SalesOrderHeader(CustomerID)
on ps_r_salespers(SalesPersonID)
go

Create nonclustered index NC_SOH_ACCT on SalesOrderHeader(AccountNumber)
on ps_r_salespers(SalesPersonID)
go

Create nonclustered index NC_SOH_SONBR on SalesOrderHeader(SalesOrderNumber)
on ps_r_salespers(SalesPersonID)
go

Create nonclustered index NC_SOH_TERR on SalesOrderHeader(TerritoryID, AccountNumber)
INCLUDE(ShipDate, DueDate)
on ps_r_salespers(SalesPersonID)
go

Create unique nonclustered index NC_SOH_SOID on SalesOrderHeader([SalesOrderID], [OrderDate], [SalesPersonID])
on ps_r_salespers(SalesPersonID)
go

set identity_insert SalesOrderHeader ON

insert into SalesOrderHeader with (TABLOCK) 
(	   [SalesOrderID]
      ,[RevisionNumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
      ,[Status]
      ,[OnlineOrderFlag]
--      ,[SalesOrderNumber]
      ,[PurchaseOrderNumber]
      ,[AccountNumber]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[TerritoryID]
      ,[BillToAddressID]
      ,[ShipToAddressID]
      ,[ShipMethodID]
      ,[CreditCardID]
      ,[CreditCardApprovalCode]
      ,[CurrencyRateID]
      ,[SubTotal]
      ,[TaxAmt]
      ,[Freight]
--      ,[TotalDue]
      ,[Comment]
      ,[rowguid]
      ,[ModifiedDate])
SELECT [SalesOrderID]
      ,[RevisionNumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
      ,[Status]
      ,[OnlineOrderFlag]
--      ,[SalesOrderNumber]
      ,[PurchaseOrderNumber]
      ,[AccountNumber]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[TerritoryID]
      ,[BillToAddressID]
      ,[ShipToAddressID]
      ,[ShipMethodID]
      ,[CreditCardID]
      ,[CreditCardApprovalCode]
      ,[CurrencyRateID]
      ,[SubTotal]
      ,[TaxAmt]
      ,[Freight]
--      ,[TotalDue]
      ,[Comment]
      ,[rowguid]
      ,[ModifiedDate]
 from AdventureWorks.Sales.SalesOrderHeader;

SET ANSI_PADDING ON
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_BillToAddressID] FOREIGN KEY([BillToAddressID])
REFERENCES [Address] ([AddressID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_BillToAddressID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_ShipToAddressID] FOREIGN KEY([ShipToAddressID])
REFERENCES [Address] ([AddressID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_ShipToAddressID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_CurrencyRate_CurrencyRateID] FOREIGN KEY([CurrencyRateID])
REFERENCES [CurrencyRate] ([CurrencyRateID])
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_CurrencyRate_CurrencyRateID]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_DueDate] CHECK  (([DueDate]>=[OrderDate]))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_DueDate]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_Freight] CHECK  (([Freight]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_Freight]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_ShipDate] CHECK  (([ShipDate]>=[OrderDate] OR [ShipDate] IS NULL))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_ShipDate]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_Status] CHECK  (([Status]>=(0) AND [Status]<=(8)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_Status]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_SubTotal] CHECK  (([SubTotal]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_SubTotal]
GO

ALTER TABLE [SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [CK_SalesOrderHeader_TaxAmt] CHECK  (([TaxAmt]>=(0.00)))
GO

ALTER TABLE [SalesOrderHeader] CHECK CONSTRAINT [CK_SalesOrderHeader_TaxAmt]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_RevisionNumber]  DEFAULT ((0)) FOR [RevisionNumber]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_OrderDate]  DEFAULT (getdate()) FOR [OrderDate]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_Status]  DEFAULT ((1)) FOR [Status]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_OnlineOrderFlag]  DEFAULT ((1)) FOR [OnlineOrderFlag]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_SubTotal]  DEFAULT ((0.00)) FOR [SubTotal]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_TaxAmt]  DEFAULT ((0.00)) FOR [TaxAmt]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_Freight]  DEFAULT ((0.00)) FOR [Freight]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_rowguid]  DEFAULT (newid()) FOR [rowguid]
GO

ALTER TABLE [SalesOrderHeader] ADD  CONSTRAINT [DF_SalesOrderHeader_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO

ALTER INDEX NC_SOH_SONBR on SalesOrderHeader DISABLE
go


----------------------------------------------------

/* ManagePartition tests:

ManagePartition  /C:CreateStagingFull /PartitionRangeValue:280 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t1

ManagePartition  /C:CreateStagingClusteredIndex /PartitionRangeValue:295 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2

ManagePartition  /C:CreateStagingNoIndex /PartitionRangeValue:270 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3

*/
-------------------------------------------------------
Alter table [SalesOrderHeader] switch partition 3 to t1;
go
Alter table t1 switch to [SalesOrderHeader] partition 3;
go

-- ManagePartition  /C:IndexStaging /PartitionRangeValue:290 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2

Alter table [SalesOrderHeader] switch partition 5 to t2;
go
Alter table t2 switch to [SalesOrderHeader] partition 5;
go
-- ManagePartition  /C:IndexStaging /PartitionRangeValue:270 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3

Alter table [SalesOrderHeader] switch partition 1 to t3;
go
Alter table t3 switch to [SalesOrderHeader] partition 1;
go

------------------
-- ClearPartition tests

drop table t1;
go
drop table t2;
go
drop table t3;
go

---------------------------------------------------------
/*

ManagePartition  /C:ClearPartition /PartitionRangeValue:285 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /K+

ManagePartition  /C:ClearPartition /PartitionRangeValue:280 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t2 /K+

ManagePartition  /C:ClearPartition /PartitionRangeValue:290 /S:localhost /E /d:PartitionTest /s:dbo /t:SalesOrderHeader /A:t3 /K-

*/
---------------------------------------------------------

-- Should be empty:

select * from SalesOrderHeader where SalesPersonID between 280 and 290;