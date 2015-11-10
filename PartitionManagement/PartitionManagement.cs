/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  PartitionManagement.cs
//
//  Version 3.1
//  Forked to Github: https://github.com/lucazav/sqlserverpartitionmanager
//
//  Version 3.0
//  Stuart Ozer
//  Microsoft Corporation
//  December 2012
//  
//  Class library to create a table that can be populated to load data for a partition SWITCH operation,
//  or used as a destination to empty rows from a partition SWITCH.   Table structure and filegroup
//  location are based on the definition of the Partition Table and the Partition Number being used.
//
//  Library supports creating the table fully, or deferring creation of the indexes until table has
//  been populated by a fast load.
//   
//  Version History
//  V 3.1 -- November 2015
//      Requires SQL2012 version of SMO
//      Added support for SQL Server 2014 in a different project
//      Fixed the deadlock issue when occurring when one instance of the exe was trying read from sys.tables
//          and the other one was trying to alter the schema with an Alter table command.
//      Fixed the missing throw issue in the catch block of the Run method
//
//  V 3.0 -- December 2012
//      Requires SQL2012 version of SMO
//      Added support for Columnstore Indexes
//      Added support to script generated TSQL commands 
//      Added global locale support for date formats
//      Fixed partitioning columns requiring quoted names
//      Added support for binary partitioning column types
//      Fixed problem with tables containing long text or long binary tyoes
//
//  V 2.0 -- February 2009
//      Requires SQL2008 version of SMO
//      Added support for SQL2008 data types 
//      Added support for nullable partition columns
//      Added support for filtered indexes and sparse columns
//      Added support for table and index compression
//      Added support for default constraints to assist when populating staging tables
//      Added support for partition-aligned indexed views
//      Added backward compatibility for SQL2005
//
//      NOTE:  Filestream columns are not supported because SWITCH has limited value in filestream environments
//
//  V 1.0 -- May 2006
//
//  Provided as-is, with no warranties expressed or implied
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;

namespace PartitionManagement
{
    class PartitionManager
    {
	   private Database db;
	   private Table partitionTable;
	   private PartitionFunction pf;
	   private Table stgTable;
	   private ServerConnection conn;
	   private Server srv;
	   private int partitionNumber;
	   private StreamWriter scriptWriter;
	   bool executeCommands;
	   private List<System.Collections.Specialized.StringCollection> scriptChunks;

	   // This version of the constructor relies on an explicit partition number to be provided
	   public PartitionManager(ServerConnection conn, String dbName, String schName,
		  String partitionTblName, String stgTblName, int partitionNumber, StreamWriter scriptWriter, 
		  bool executeCommands)
	   {
		  // Create all objects  
		  this.conn = conn;
		  this.partitionNumber = partitionNumber;
		  this.executeCommands = executeCommands;
		  this.scriptWriter = scriptWriter;

		  srv = new Server(conn);
            
		  db = srv.Databases[dbName];

		  scriptChunks = new List<System.Collections.Specialized.StringCollection>();

		  // validate table
		  if ((partitionTable = db.Tables[partitionTblName, schName]) == null)
		  {
			 throw new System.ArgumentException("Table [" + schName + "].[" + partitionTblName + "] not found in database [" + dbName + "]");
		  }
		  // validate it is partitioned
		  if (String.IsNullOrEmpty(partitionTable.PartitionScheme))
		  {
			 throw new System.ArgumentException("Table [" + schName + "].[" + partitionTblName + "] is not partitioned");
		  }
		  else
		  {
			 pf = db.PartitionFunctions[db.PartitionSchemes[partitionTable.PartitionScheme].PartitionFunction];
		  }
		  // validate the partition number
		  if ((pf.NumberOfPartitions < partitionNumber) || (partitionNumber <= 0))
		  {
			 throw new System.ArgumentException("Invalid Partition Number");
		  } 
		  // check for presence of staging table with the same name
		  if (db.Tables.Contains(stgTblName, schName))
		  {
			 stgTable = db.Tables[stgTblName,schName];
		  }
		  else
		  {
			 stgTable = new Table(db, stgTblName, schName);
		  }
	   }

	   // Alternate version of constructor computes the partition number given an
	   // input string representing the range value of the partitioning column
	   public PartitionManager(ServerConnection conn, String dbName, String schName,
		  String partitionTblName, String stgTblName, String partitionRangeValue, StreamWriter scriptWriter, 
		  bool executeCommands) 
		  : this(conn, dbName, schName, partitionTblName, stgTblName, 1, scriptWriter, executeCommands)
	   {
		  //Determine the correct partition number based on the range value
		  this.partitionNumber = getPartitionNumber(partitionRangeValue);
	   }

        public void SetTransactionIsolationLevelReaduncommitted()
        {
            System.Collections.Specialized.StringCollection sc = new System.Collections.Specialized.StringCollection();

            string readUncomm = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED";
            sc.Add(readUncomm);

            scriptChunks.Add(sc);
            if (executeCommands) conn.ExecuteScalar(readUncomm);
        }

	   public void CreateStgTable()
	   {
		  // Create the new table in the appropriate filegroup for the SWITCH
		  foreach (Column c in partitionTable.Columns)
		  {
			 // Populate the table with each column and associated properties from the partition table
			 Column stgCol = new Column(stgTable, c.Name, c.DataType);
			 stgCol.Collation = c.Collation;
			 stgCol.Nullable = c.Nullable;
			 stgCol.Computed = c.Computed;
			 stgCol.ComputedText = c.ComputedText;
			 stgCol.Default = c.Default;
			 // Apply default constraint value, if present, as a default value 
			 if (c.DefaultConstraint != null)
			 {
				stgCol.AddDefaultConstraint(stgTable.Name + "_" + c.DefaultConstraint.Name);
				stgCol.DefaultConstraint.Text = c.DefaultConstraint.Text;
			 }
			 stgCol.IsPersisted = c.IsPersisted;
			 stgCol.DefaultSchema = c.DefaultSchema;
			 stgCol.RowGuidCol = c.RowGuidCol;
			 if (srv.VersionMajor >= 10)                
			 {
				stgCol.IsFileStream = c.IsFileStream;
				stgCol.IsSparse = c.IsSparse;
				stgCol.IsColumnSet = c.IsColumnSet;
			 }
			 stgTable.Columns.Add(stgCol);
		  }
		  // Match other new table attributes to the partition table; required for SWITCH compatibility
		  stgTable.AnsiNullsStatus = partitionTable.AnsiNullsStatus;
		  stgTable.QuotedIdentifierStatus = partitionTable.QuotedIdentifierStatus;
		  // Calculate the filegroup associated with the partition nunber to switch; create temp table in that filegroup
		  stgTable.FileGroup = db.PartitionSchemes[partitionTable.PartitionScheme].FileGroups[partitionNumber - 1];
		  stgTable.TextFileGroup = db.PartitionSchemes[partitionTable.PartitionScheme].FileGroups[partitionNumber - 1];

		  if (srv.VersionMajor >= 10)
		  {
			 // Define compression property to match by creating a Physical Partition object 
			 PhysicalPartition stgPartition = new PhysicalPartition(stgTable, 1, partitionTable.PhysicalPartitions[partitionNumber - 1].DataCompression);
			 stgTable.PhysicalPartitions.Add(stgPartition);
		  }
		  scriptChunks.Add(stgTable.Script());
		  if (executeCommands) stgTable.Create();
	   }
/*
	public void CreateStgIndexes()
	{
		CreateStgIndexes(true);
	 }
*/  
	   public void CreateStgIndexes(bool createNonClusteredIndexes)
	   {
		  // start with Clustered
		  createStgClusteredIndex();
		  // then create non-clustered indexes, along with indexed views, if requested
		  if (createNonClusteredIndexes)
		  {
			 createStgNonclusteredIndexes();
			 createStgIndexedViews();
		  }
	   }
	   public void CreateStgFkeys()
	   {
		  // Apply any Foreign Key constraints on the new table that are present on the Partition Table
		  foreach (ForeignKey fKey in partitionTable.ForeignKeys)
		  {
			 ForeignKey newFKey = new ForeignKey(stgTable, stgTable.Name + "_" + fKey.Name);
			 newFKey.DeleteAction = fKey.DeleteAction;
			 newFKey.IsChecked = fKey.IsChecked;
			 newFKey.IsEnabled = fKey.IsEnabled;
			 newFKey.ReferencedTable = fKey.ReferencedTable;
			 newFKey.ReferencedTableSchema = fKey.ReferencedTableSchema;
			 newFKey.UpdateAction = fKey.UpdateAction;
			 foreach (ForeignKeyColumn col in fKey.Columns)
			 {
				ForeignKeyColumn newCol = new ForeignKeyColumn(newFKey, col.Name, col.ReferencedColumn);
				newFKey.Columns.Add(newCol);
			 }
			 scriptChunks.Add(newFKey.Script());
			 if (executeCommands) newFKey.Create();
		  }
	   }
	   public void CreateStgChecks()
	   {
	   // Apply any Check constraints to the new table that are present on the Partition Table
		  foreach (Check chkConstr in partitionTable.Checks)
		  {
			 Check newCheck = new Check(stgTable, stgTable.Name + "_" + chkConstr.Name);
			 newCheck.IsChecked = chkConstr.IsChecked;
			 newCheck.IsEnabled = chkConstr.IsEnabled;
			 newCheck.Text = chkConstr.Text;
			 scriptChunks.Add(newCheck.Script());
			 if (executeCommands) newCheck.Create();
		  }
	   }
	   public void CreateStgPartitionCheck()
	   {
		  // Now construct appropriate CHECK CONSTRAINT based on partition boundaries
		  // We need to distinguish between dates, unicode vs. nonunicode strings, and numeric
		  // values embedded in the partition boundarty collection.  We do this by evaluating the 
		  // data type of the partition column, and then construct the Check Constraint string
		  // appropriately quoted
		  Check partitionCheckConstraint = new Check(stgTable, "chk_" + stgTable.Name + "_partition_" + partitionNumber.ToString(System.Globalization.CultureInfo.InvariantCulture));
		  String leftBoundary = "";
		  String rightBoundary = "";
		  String partitionColumnName = partitionTable.PartitionSchemeParameters[0].Name;
		  String partitionColumnQuotedName = "[" + partitionColumnName + "]";
		  SqlDataType partitionColumnType = partitionTable.Columns[partitionColumnName].DataType.SqlDataType;

	 
		  // Construct the minimum value predicate string in the check constraint definition
		  if (partitionNumber > 1)
		  {
			 leftBoundary = partitionColumnQuotedName + ((pf.RangeType == RangeType.Right) ? ">=" : ">") + localeIndependentText(partitionColumnType, pf.RangeValues[partitionNumber - 2]);
		  }               
		  // Construct the maximum value predicate string in the check constraint definition
		  if (partitionNumber < pf.NumberOfPartitions)
		  {
			 rightBoundary = partitionColumnQuotedName + ((pf.RangeType == RangeType.Right) ? "<" : "<=") + localeIndependentText(partitionColumnType, pf.RangeValues[partitionNumber - 1]);
		  }
		  // Assemble the full Check Constraint string
		  // If the partitioning column is nullable
		  //      If the partition is the leftmost, allow NULLs in the check constraint, otherwise add NOT NULL check constraint
		  String constraintText =
				((partitionTable.Columns[partitionColumnName].Nullable) ? partitionColumnQuotedName +
				    ((partitionNumber == 1) ? " IS NULL OR " : " IS NOT NULL AND ")
				    : ""
				)
				+ "(" + leftBoundary +
				(((partitionNumber > 1) && (partitionNumber < pf.NumberOfPartitions)) ? " AND " : "") +
				rightBoundary + ")";
		  partitionCheckConstraint.IsEnabled = true;
		  partitionCheckConstraint.Text = constraintText;
		  // Create the Check Constraint
		  scriptChunks.Add(partitionCheckConstraint.Script());
		  if (executeCommands) partitionCheckConstraint.Create();
	   }
/*
	   public void ClearPartition()
	   {
		  String s = ClearPartition(false); 
	   }
*/
	   public string ClearPartition(bool keepTable)
	   {
		  string cmd = "ALTER TABLE [" + partitionTable.Schema + "].[" + partitionTable.Name + "] SWITCH PARTITION " +
			 partitionNumber.ToString() + " TO [" + stgTable.Schema + "].[" + stgTable.Name + "];";
		  System.Collections.Specialized.StringCollection sc = new System.Collections.Specialized.StringCollection();
		  sc.Add(cmd);
		  if (executeCommands) partitionTable.SwitchPartition(partitionNumber, stgTable);

		    if (!keepTable)
		    {
			 string dropCmd = "DROP TABLE [" + stgTable.Schema + "].[" + stgTable.Name + "];";
			 sc.Add(dropCmd);
			 if (executeCommands)  stgTable.Drop();
		    }
		  scriptChunks.Add(sc);
		  return keepTable ? stgTable.Name : null;
	   }
	   
	   private void createStgClusteredIndex()
	   {
		  // Ignore if clustered index already exists, or if we are scripting only
		  if ((!executeCommands) || (!stgTable.HasClusteredIndex))
		  {
			 foreach (Index i in partitionTable.Indexes)
			 {
				if (i.IsClustered && !i.IsDisabled)
				{
				    createStgIndex(i, stgTable);
				    break;
				}
			 }
		  }
	   }
	   private void createStgIndexedViews()
	   {
		  // Create a list of existing Indexed Views on the partition table
		 List<View> indexedViews = new List<View>();
		  // Examine each view in the database
		 foreach (View v in db.Views)
		  {
			 if (v.HasIndex)
			 {
				// If it has an index, check if parent table is the partitioned table of interest
				string fullName = (string)conn.ExecuteScalar("SELECT DISTINCT Referenced_Schema_Name+'.'+Referenced_Entity_Name FROM sys.dm_sql_referenced_entities ('" + v.Schema + "." + v.Name + "', 'OBJECT')");
				string sch = fullName.Substring(0, fullName.IndexOf('.'));
				string tab = fullName.Substring(fullName.IndexOf('.') + 1, fullName.Length - (fullName.IndexOf('.')+1));
				if (sch == partitionTable.Schema && tab == partitionTable.Name)
				{
				    indexedViews.Add(v);
				}
			 }
		  }
		 foreach (View v in indexedViews)
		 {
			createIndexedView(v);
		 }
	   }
	   private void createIndexedView(View sourceView)
	   {
		  View stgView = new View(db, stgTable.Name + "_" + sourceView.Name, stgTable.Schema);
		  stgView.TextHeader = "CREATE VIEW " + stgTable.Schema + "." + stgView.Name + " WITH SCHEMABINDING AS ";
		  // Replace name of Partitioned Table in the view definition with the Staging Table name, wherever it occurs
		  stgView.TextBody = Regex.Replace
				(sourceView.TextBody, @"([\W\s])" + partitionTable.Name + @"([\W\s])", @"$1" + stgTable.Name + @"$2",RegexOptions.IgnoreCase);
		  // Create the view
		  scriptChunks.Add(stgView.Script());
		  if (executeCommands) stgView.Create();

		  // Create the view's clustered index first
		  foreach (Index i in sourceView.Indexes)
		  {
			 if (i.IsClustered)
			 {
				createStgIndex(i, stgView);
				break;
			 }
		  }
		  // Create any nonclustered indexes
		  foreach (Index i in sourceView.Indexes)
		  {
			 if (!i.IsClustered)
			 {
				createStgIndex(i, stgView);
			 }
		  }

	   }
	   private void createStgNonclusteredIndexes()
	   {
		  foreach (Index i in partitionTable.Indexes)
		  {
			 if (!i.IsClustered && !i.IsXmlIndex  && !i.IsDisabled)
			 {
				createStgIndex(i, stgTable);
			 }
		  }
	   }
	   private void createStgIndex(Index i, TableViewBase parent)
	   {
		  if (i.PartitionScheme == "")
				throw (new System.NotSupportedException(
					String.Format("The index '{0}' is not aligned to a Partition Scheme", i.Name)));
			
		  // todo:  differentiate between Base Table as source, and View as source
		  
		  // LZAV:  Index stgIndex = new Index(parent, parent.Name + "_" + i.Name);
		  String indexName = parent.Name + "_" + i.Name;		// LZAV
		  if (indexName.Length > 128)						// LZAV
		  indexName = "IX_CL_" + parent.Name;				// LZAV

		  Index stgIndex = new Index(parent, indexName);		// LZAV

		  foreach (IndexedColumn iCol in i.IndexedColumns)
		  {
			 IndexedColumn stgICol = new IndexedColumn(stgIndex, iCol.Name, iCol.Descending);
			 stgICol.IsIncluded = iCol.IsIncluded;
			 stgIndex.IndexedColumns.Add(stgICol);
		  }
		  stgIndex.IndexType = i.IndexType;
		  stgIndex.IndexKeyType = i.IndexKeyType;
		  stgIndex.IsClustered = i.IsClustered;
		  stgIndex.IsUnique = i.IsUnique;
		  stgIndex.CompactLargeObjects = i.CompactLargeObjects;
		  stgIndex.IgnoreDuplicateKeys = i.IgnoreDuplicateKeys;
		  stgIndex.IsFullTextKey = i.IsFullTextKey;
		  stgIndex.PadIndex = i.PadIndex;
		  stgIndex.FileGroup = db.PartitionSchemes[i.PartitionScheme].FileGroups[partitionNumber - 1];

		  // add the partitioning column to the index if it is not already there
		  String partitionKeyName = i.PartitionSchemeParameters[0].Name;
		  if (stgIndex.IndexedColumns[partitionKeyName] == null)
		  {
			 IndexedColumn stgICol = new IndexedColumn(stgIndex, partitionKeyName);
			 // It is added as a Key to the Clustered index and as an Include column to a Nonclustered
			 stgICol.IsIncluded = !stgIndex.IsClustered;
			 stgIndex.IndexedColumns.Add(stgICol);
		  }

		  if (srv.VersionMajor >= 10) 
		  {
			 // Define compression property to match by creating a Physical Partition object (not applicable to Colstore) 
			 {
				PhysicalPartition stgPartition = new PhysicalPartition(stgIndex, 1);
				if (i.IndexType != IndexType.NonClusteredColumnStoreIndex)
				{
				    stgPartition.DataCompression = i.PhysicalPartitions[partitionNumber - 1].DataCompression;
				}
				stgIndex.PhysicalPartitions.Add(stgPartition);
			 }
			 // Handle Filtered Index
			 if (i.HasFilter)
			 {
				stgIndex.FilterDefinition = i.FilterDefinition;
			 }
		  }
		  scriptChunks.Add(stgIndex.Script());
		  if (executeCommands) stgIndex.Create();
	   }
	   // Compute the partition number corresponding to a range value of this instance's partition function
	   // Done by executing the query:
	   //       Select <db name>.$partition.<function name>(<range value>) 
	   // and relying on implicit conversion from string to the appropriate data type of the partitioning column
	   private int getPartitionNumber(String rangeValue)
	   {
		  SqlConnection c = conn.SqlConnectionObject;
		  String sqlText = "Select "+db.Name+".$partition."+pf.Name+"(@rangeValue)";
		  using (SqlCommand cmd = new SqlCommand(sqlText, c))
		  {
			 cmd.Parameters.Add(new SqlParameter("@rangeValue", rangeValue));
			 return (int)cmd.ExecuteScalar();
		  }
	   }

	   // Create a string representation of a partition boundary value appropriate for use in CHECK CONSTRAINT 
	   // expressions, taking a SQL Data Type and Range Value object as inputs.   
	   // In particular ensure that date strings are represented in locale-independent formats appropriate for global 
	   // use.  And unpack binary datatypes to deliver "0x..." string representation. 
	   [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1800:DoNotCastUnnecessarily")]
	   private static string localeIndependentText(SqlDataType sqlType, object rangeValue)
	   {
		  string hexString = "";
		  switch (sqlType)
		  {
			 case SqlDataType.Date:
				return "'" + ((DateTime)rangeValue).ToString("yyyy-MM-ddTHH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + "'";
			 case SqlDataType.SmallDateTime:
				return "'" + ((DateTime)rangeValue).ToString("yyyy-MM-ddTHH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + "'";
			 case SqlDataType.DateTime:
				return "'" + ((DateTime)rangeValue).ToString("yyyy-MM-ddTHH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture) + "'";
			 case SqlDataType.DateTime2:
				return "'" + ((DateTime)rangeValue).ToString("yyyy-MM-ddTHH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture) + "'";
			 case SqlDataType.DateTimeOffset:
				return "'" + ((DateTimeOffset)rangeValue).ToString("yyyy-MM-ddTHH:mm:ss.fffffffK", System.Globalization.CultureInfo.InvariantCulture) + "'";
			 case SqlDataType.Time:
				return "'" + ((TimeSpan)rangeValue).ToString() + "'";
			 case SqlDataType.Char:
				return "'" + (String)rangeValue + "'";
			 case SqlDataType.VarChar:
				return "'" + (String)rangeValue + "'";
			 case SqlDataType.NChar:
				return "N'" + (String)rangeValue + "'";
			 case SqlDataType.NVarChar:
				return "N'" + (String)rangeValue + "'";
			 case SqlDataType.Binary:
				foreach (Byte b in (Byte[])rangeValue)
				{
				    hexString += b.ToString("x2", System.Globalization.CultureInfo.InvariantCulture);
				}
				return "0x" + hexString;
			 case SqlDataType.VarBinary:
				foreach (Byte b in (Byte[])rangeValue)
				{
				    hexString += b.ToString("x2", System.Globalization.CultureInfo.InvariantCulture);
				}
				return "0x" + hexString;
			 case SqlDataType.Bit:
				return (((Boolean)rangeValue) ? "1" : "0");
			 case SqlDataType.Int:
				return rangeValue.ToString();
			 case SqlDataType.BigInt:
				return rangeValue.ToString();
			 case SqlDataType.SmallInt:
				return rangeValue.ToString();
			 case SqlDataType.TinyInt:
				return rangeValue.ToString();

			 // Note -- SQL Server partition boundary values for FLOATs are precise only to 14 digits, so ensure the 
			 // Check Constraint value matches that precision
			 case SqlDataType.Float:
				return ((Double)rangeValue).ToString("E14", System.Globalization.CultureInfo.InvariantCulture);

			 // Note -- SQL2012 RTM incorrectly handles REAL / (Float(24)) metadata in Check Constraints, 
			 // use Float (53) instead to support partition switching.  Bug has been filed.
			 case SqlDataType.Real:
				return ((Single)rangeValue).ToString("E6", System.Globalization.CultureInfo.InvariantCulture);

			 case SqlDataType.Numeric:
				return rangeValue.ToString();
			 case SqlDataType.Decimal:
				return rangeValue.ToString();
			 case SqlDataType.Money:
				return rangeValue.ToString();
			 case SqlDataType.SmallMoney:
				return rangeValue.ToString();
			 default:
				throw (new System.NotSupportedException("Unsupported Data Type found as Partition Key"));
		  }
	   }

	   public void outputScript()
	   {
		  foreach (System.Collections.Specialized.StringCollection sc in scriptChunks)
		  {
			 foreach (string s in sc)
			 {
				if (scriptWriter == null)
				{
				    Console.WriteLine(s);
				}
				else
				{
				    scriptWriter.WriteLine(s);
				}
			 }
		  }
	   }
    }
}
