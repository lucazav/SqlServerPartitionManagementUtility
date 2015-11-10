/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  ManagePartition.cs
//
//  Version 3.0
//  Stuart Ozer
//  Microsoft Corporation
//  December 2012
//  
//  Application to drive the PartitionManagement class library.  Presents 4 commands:
//
//      ClearPartition          -- use to empty a single partition; option to keep or discard emptied rows
//      CreateStagingFull       -- create a staging table and all indexes, with constraints, to support a partition SWITCH
//      CreateStagingClusteredIndex    
//                              -- create a staging table to support a SWITCH, without indexes present except for the clustered index. 
//      CreateStagingNoindex    -- create a staging table to support a SWITCH, without any indexes present. 
//      IndexStaging            -- create the remaining indexes for a staging table that was created using CreateStagingNoindexes or CreateStagingClusteredIndex
//
//      The later 2 options are designed to support high performance data loading into a partition:
//          CreateStagingNoindex, followed by bulk insert, followed by IndexStaging
//
//      Note that you have the following choices in parameters:
//              Integrated security OR Non-Integrated with user name / password
//              Provide a staging table name or have the system construct one
//              Provide either a partition number OR the value for the partitioning column 
//                  corresponding to the desired range
//
//
//       /Command:<string>                   short form /C  -- See the 5 commnds above
//       /Server:<string>                    short form /S
//       /User:<string>                      short form /U
//       /Password:<string>                  short form /P
//       /Integrated[+|-]                    short form /E
//       /Database:<string>                  short form /d
//       /Schema:<string>                    short form /s
//       /PartitionTable:<string>            short form /t
//       /PartitionNumber:<int>              short form /p
//       /PartitionRangeValue:<string>       short form /v
//       /StagingTable:<string>              short form /A
//       /ScriptOption:<char>                short form /O  -- i|I == Include Script, o|O == Script Only (no execute)
//       /ScriptFile:<string>                short form /f  -- if excluded, script output to stdout if ScriptOption indicates a script
//       /Keep[+|-]                          short form /K
//
//       Return Codes:   
//          0 – normal exit
//          1 – invalid or missing parameters
//          2 – exception encountered
// 
//  Version History
//  V 3.0 -- December 2012
//          Support for SQL Server 2012
//          Added Support for Columnstore Indexes
//          Added Support for all types of allowed Partitioning Columns including Binary
//          Support global formats for dates and numerics, ensuring that Check Constraints are valid in all locales
//          Optionally generate TSQL output in addition to (or in lieu of) executing commands
//          Fixed handling of tables containing large text or large binary datatypes
//
//  V 2.0 -- December 2008
//          Support for SQL Server 2008
//          Support for option to create Clustered Index only
//          Clustered index is always first index created
//          Eliminated connection timeout
//
//  V 1.0 -- April 2006
//
//  Provided as-is, with no warranties expressed or implied
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////


using System;
using System.Collections.Generic;
using Microsoft.SqlServer.Management.Common;
using System.Text;



namespace PartitionManagement
{
    class App
    {
	   [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1303:Do not pass literals as localized parameters", MessageId = "System.Console.WriteLine(System.String)"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "ClearPartition"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "CreateStagingClusteredIndex"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "CreateStagingFull"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "IndexStaging"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "CreateStagingNoindex")]
	   static void Main(string[] args)
	   {
		  try
		  {
			 AppArgs parsedArgs = new AppArgs();
			 if (Utilities.Utility.ParseCommandLineArguments(args, parsedArgs))
			 {
				App app = new App();
				app.Run(ref parsedArgs);
			 }
			 else
			 {
				Console.Write(Utilities.Utility.CommandLineArgumentsUsage(typeof(AppArgs)));
				Console.WriteLine("<Command> = ClearPartition | CreateStagingFull | CreateStagingNoindex | CreateStagingClusteredIndex | IndexStaging");
				Environment.Exit(1);
			 }
		  }
		  catch (Exception ex)
		  {
			 Exception thisEx = ex;
			 while (thisEx != null)
			 // Recursively print exception stack
			 {
				Console.WriteLine(thisEx.Message, thisEx.Source);
				thisEx = thisEx.InnerException;
			 }
			 Environment.Exit(2);
		  }
		  Environment.Exit(0);
	   }

	   [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "ClearPartition"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "CreateStagingClusteredIndex"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "CreateStagingNoindex"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "CreateStagingFull"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "IndexStaging"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1304:SpecifyCultureInfo", MessageId = "System.String.ToLower")]
	   void Run(ref AppArgs parsedArgs)
	   {
		  ServerConnection sc = new ServerConnection();


		  try
		  {
			 sc.ServerInstance = parsedArgs.Server;
			 sc.LoginSecure = parsedArgs.Integrated;
			 sc.StatementTimeout = 0;

			 if (!parsedArgs.Integrated)
			 {
				sc.Login = parsedArgs.User;
				sc.Password = parsedArgs.Password;
			 }
			 sc.Connect();

			 String command = parsedArgs.Command.ToLower();
			 String dbName = parsedArgs.Database;
			 String schName = parsedArgs.Schema;
			 String partitionTblName = parsedArgs.PartitionTable;
			 String scriptOption = (parsedArgs.ScriptOption == null) ? null : parsedArgs.ScriptOption.ToLower();

			 // Set flag indicating whether scripts will be generated
			 bool generateScript = (scriptOption == "i" || scriptOption == "o") ? true : false;
			 // set flag indicated whether commands will be executed on SQL Server (as opposed to just scripting)
			 bool executeCommands = (scriptOption == "o") ? false : true;


			 // Ensure that either a partition number or range value is provided, not both
			 int partitionNumber = parsedArgs.PartitionNumber;
			 string partitionRangeValue = parsedArgs.PartitionRangeValue;
			 if (((partitionRangeValue == null) && (partitionNumber == 0)) ||
				((partitionRangeValue != null) && (partitionNumber != 0)))
			 {
				throw new System.ArgumentException("Specify either a partition number OR a partition range value");
			 }

			 // Construct a staging table name likely to be unique, if no name is provided
			 String stagingTblName = parsedArgs.StagingTable;
			 if (stagingTblName == null)
			 {
				stagingTblName = partitionTblName + "_part" + partitionNumber.ToString("####", System.Globalization.CultureInfo.InvariantCulture)
				    + partitionRangeValue + "_" + System.DateTime.Now.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);
			 }

			 bool keepStaging = parsedArgs.Keep;
			 PartitionManager pm = null;

			 System.IO.StreamWriter scriptWriter = null;

			 if ((scriptOption == "i" || scriptOption == "o")
				&& parsedArgs.ScriptFile != null)
			 {
				try
				{
				    scriptWriter = new System.IO.StreamWriter(parsedArgs.ScriptFile, true, Encoding.Unicode);
				}
				catch (System.IO.IOException ex)
				{
				    Console.WriteLine(ex.Message, ex.Source);
				    Console.WriteLine("Output will be sent to console instead");
				}
			 }

			 using (scriptWriter)
			 {
				// Call appropriate Partition Manager constructor depending on whether a partition number or range value is provided
				if (partitionNumber != 0)
				{
				    pm = new PartitionManagement.PartitionManager(sc, dbName, schName, partitionTblName,
					   stagingTblName, partitionNumber, scriptWriter, executeCommands);
				}
				else
				{
				    pm = new PartitionManagement.PartitionManager(sc, dbName, schName, partitionTblName,
					   stagingTblName, partitionRangeValue, scriptWriter, executeCommands);
				}

				pm.SetTransactionIsolationLevelReaduncommitted();

				switch (command)
				{
				    case "clearpartition":
					   pm.CreateStgTable();
					   pm.CreateStgFkeys();
					   pm.CreateStgChecks();
					   pm.CreateStgPartitionCheck();
					   // If staging table is being deleted, no need to create non-clustered indexes & views
					   pm.CreateStgIndexes(keepStaging);
					   pm.ClearPartition(keepStaging);
					   if (generateScript) pm.outputScript();
					   break;
				    case "createstagingfull":
					   pm.CreateStgTable();
					   pm.CreateStgFkeys();
					   pm.CreateStgChecks();
					   pm.CreateStgPartitionCheck();
					   pm.CreateStgIndexes(true);
					   if (generateScript) pm.outputScript();
					   break;
				    case "createstagingclusteredindex":
					   pm.CreateStgTable();
					   pm.CreateStgFkeys();
					   pm.CreateStgChecks();
					   pm.CreateStgPartitionCheck();
					   pm.CreateStgIndexes(false);
					   if (generateScript) pm.outputScript();
					   break;
				    case "createstagingnoindex":
					   pm.CreateStgTable();
					   pm.CreateStgFkeys();
					   pm.CreateStgChecks();
					   pm.CreateStgPartitionCheck();
					   if (generateScript) pm.outputScript();
					   break;
				    case "indexstaging":
					   pm.CreateStgIndexes(true);
					   if (generateScript) pm.outputScript();
					   break;
				    default:
					   throw new System.InvalidOperationException("Invalid command choice\nCommand Choices: ClearPartition | CreateStagingFull | CreateStagingClusteredIndex | CreateStagingNoindex | IndexStaging");
				}
			 }
		  }
		  catch (Exception e)
		  {
			 sc.Disconnect();
			 sc = null;
			 Console.WriteLine(e);
			 throw e;
		  }
		  sc.Disconnect();
		  sc = null;
	   }
    }

    class AppArgs
	{
		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "C")]
		public string Command;

	   [Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "S")]
		public string Server;

		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "U")]
		public string User;

		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "P")]
		public string Password;

		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "E")]
		public bool Integrated;

		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.Required, ShortName = "d")]
		public string Database;

		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.Required, ShortName = "s")]
		public string Schema;

		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.Required, ShortName = "t")]
		public string PartitionTable;

		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "p")]
		public int PartitionNumber;

	   [Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "v")]
	   public string PartitionRangeValue;

	   [Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "A")]
		public string StagingTable;

		[Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "K")]
		public bool Keep;

	   [Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "f")]
	   public string ScriptFile;

	   [Utilities.CommandLineArgument(Utilities.CommandLineArgumentType.AtMostOnce, ShortName = "O")]
	   public string ScriptOption;

		public AppArgs()
		{
			Command = "CreateStagingFull";
		  Server = "(local)";
			User = null;
			Password = null;
			Integrated = true;
			Database = null;
			Schema = null;
			PartitionTable = null;
			StagingTable = null;
			PartitionNumber = 0;
		  PartitionRangeValue = null;
			Keep = true;
		  ScriptFile = null;
		  ScriptOption = null;
		}
	}
}