using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Inedo.BuildMaster;
using Inedo.BuildMaster.Extensibility.Actions;
using Inedo.BuildMaster.Extensibility.Providers;
using Inedo.BuildMaster.Extensibility.Providers.Database;
using Inedo.Diagnostics;

namespace Inedo.BuildMasterExtensions.SqlServer
{
    internal sealed class InternalSqlProvider : IDisposable
    {
        private SqlConnection sharedConnection;
        private SqlCommand sharedCommand;
        private List<LogMessage> messages = new List<LogMessage>();
        private DateTime idleSince = DateTime.UtcNow;
        private int usageCount;
        private object timeoutLock = new object();
        private Timer idleTimer;
        private const int IdleTimeoutPeriodMilliseconds = 30 * 1000;

        public InternalSqlProvider(string connectionString)
        {
            this.ConnectionString = connectionString;
            this.EndUsage();
        }

        public event EventHandler IdleTimeoutExpired; 

        public string ConnectionString { get; private set; }

        public void InitializeDatabase()
        {
            this.BeginUsage();
            try
            {
                if (this.IsDatabaseInitialized())
                    throw new InvalidOperationException("The database has already been initialized.");

                try
                {
                    this.ExecuteNonQuery(Properties.Resources.Initialize);
                }
                finally
                {
                    this.ClearMessages();
                }
            }
            finally
            {
                this.EndUsage();
            }
        }
        public bool IsDatabaseInitialized()
        {
            this.BeginUsage();
            try
            {
                this.ValidateConnection();
                return (bool)this.ExecuteDataTable("SELECT CAST(CASE WHEN EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '__BuildMaster_DbSchemaChanges') THEN 1 ELSE 0 END AS BIT)").Rows[0][0];
            }
            finally
            {
                this.EndUsage();
            }
        }
        public long GetSchemaVersion()
        {
            this.BeginUsage();
            try
            {
                this.ValidateInitialization();
                return (long)this.ExecuteDataTable("SELECT COALESCE(MAX(Numeric_Release_Number),0) FROM __BuildMaster_DbSchemaChanges").Rows[0][0];
            }
            finally
            {
                this.EndUsage();
            }
        }
        public ChangeScript[] GetChangeHistory()
        {
            this.BeginUsage();
            try
            {
                this.ValidateInitialization();

                var dt = ExecuteDataTable(
@"  SELECT [Numeric_Release_Number]
        ,[Script_Id]
        ,[Batch_Name]
        ,MIN([Executed_Date]) [Executed_Date]
        ,MIN([Success_Indicator]) [Success_Indicator]
    FROM [__BuildMaster_DbSchemaChanges]
GROUP BY [Script_Id], [Numeric_Release_Number], [Batch_Name]
ORDER BY [Numeric_Release_Number], MIN([Executed_Date]), [Batch_Name]");

                return dt.Rows
                    .Cast<DataRow>()
                    .Select(r => new SqlServerChangeScript(r))
                    .ToArray();
            }
            finally
            {
                this.EndUsage();
            }
        }
        public ExecutionResult ExecuteChangeScript(long numericReleaseNumber, int scriptId, string scriptName, string scriptText)
        {
            this.BeginUsage();
            try
            {
                this.ValidateInitialization();

                var tables = this.ExecuteDataTable("SELECT * FROM __BuildMaster_DbSchemaChanges");
                var rows = tables.Rows.Cast<DataRow>();

                if (rows.Any(r => (int)r["Script_Id"] == scriptId))
                    return new ExecutionResult(ExecutionResult.Results.Skipped, string.Format("The script \"{0}\" was already executed.", scriptName));

                var sqlMessageBuffer = new StringBuilder();
                bool errorOccured = false;
                EventHandler<LogReceivedEventArgs> logMessage = (s, e) =>
                {
                    if (e.LogLevel == MessageLevel.Error)
                        errorOccured = true;

                    sqlMessageBuffer.AppendLine(e.Message);
                };

                try
                {
                    var cmd = this.CreateCommand();
                    try
                    {
                        int scriptSequence = 0;
                        foreach (var sqlCommand in SqlSplitter.SplitSqlScript(scriptText))
                        {
                            if (string.IsNullOrWhiteSpace(sqlCommand))
                                continue;

                            scriptSequence++;
                            try
                            {
                                cmd.CommandText = sqlCommand;
                                cmd.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                this.InsertSchemaChange(numericReleaseNumber, scriptId, scriptName, scriptSequence, false);
                                return new ExecutionResult(ExecutionResult.Results.Failed, string.Format("The script \"{0}\" execution encountered a fatal error. Error details: {1}", scriptName, ex.Message) + Util.ConcatNE(" Additional SQL Output: ", sqlMessageBuffer.ToString()));
                            }

                            this.InsertSchemaChange(numericReleaseNumber, scriptId, scriptName, scriptSequence, true);

                            if (errorOccured)
                                return new ExecutionResult(ExecutionResult.Results.Failed, string.Format("The script \"{0}\" execution failed.", scriptName) + Util.ConcatNE(" SQL Error: ", sqlMessageBuffer.ToString()));
                        }
                    }
                    finally
                    {
                        if (this.sharedCommand == null)
                            cmd.Dispose();

                        if (this.sharedConnection == null)
                            cmd.Connection.Dispose();
                    }

                    return new ExecutionResult(ExecutionResult.Results.Success, string.Format("The script \"{0}\" executed successfully.", scriptName) + Util.ConcatNE(" SQL Output: ", sqlMessageBuffer.ToString()));
                }
                finally
                {
                }
            }
            finally
            {
                this.EndUsage();
            }
        }

        public LogMessage[] BackupDatabase(string databaseName, string destinationPath)
        {
            this.BeginUsage();
            try
            {
                if (!Directory.Exists(Path.GetDirectoryName(destinationPath)))
                    Directory.CreateDirectory(Path.GetDirectoryName(destinationPath));

                this.ExecuteNonQuery(string.Format(
                    "BACKUP DATABASE [{0}] TO DISK = N'{1}' WITH FORMAT",
                    databaseName.Replace("]", "]]"),
                    destinationPath.Replace("'", "''")));

                return this.ReadMessages();
            }
            finally
            {
                this.EndUsage();
            }
        }
        public LogMessage[] RestoreDatabase(string databaseName, string sourcePath)
        {
            this.BeginUsage();
            try
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException("databaseName");
                if (string.IsNullOrEmpty(sourcePath))
                    throw new ArgumentNullException("sourcePath");

                var quotedDatabaseName = databaseName.Replace("'", "''");
                var bracketedDatabaseName = databaseName.Replace("]", "]]");
                var quotedSourcePath = sourcePath.Replace("'", "''");

                this.ExecuteNonQuery(string.Format("USE master IF DB_ID('{0}') IS NULL CREATE DATABASE [{1}]", quotedDatabaseName, bracketedDatabaseName));
                this.ExecuteNonQuery(string.Format("ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", bracketedDatabaseName));
                this.ExecuteNonQuery(string.Format("USE master RESTORE DATABASE [{0}] FROM DISK = N'{1}' WITH REPLACE", bracketedDatabaseName, quotedSourcePath));
                this.ExecuteNonQuery(string.Format("ALTER DATABASE [{0}] SET MULTI_USER", bracketedDatabaseName));

                return this.ReadMessages();
            }
            finally
            {
                this.EndUsage();
            }
        }

        public LogMessage[] ExecuteQuery(string query)
        {
            this.BeginUsage();
            try
            {
                this.ExecuteNonQuery(query);
                return this.ReadMessages();
            }
            finally
            {
                this.EndUsage();
            }
        }
        public LogMessage[] ExecuteQueries(string[] queries)
        {
            this.BeginUsage();
            try
            {
                if (queries == null)
                    throw new ArgumentNullException("queries");
                if (queries.Length == 0)
                    return new LogMessage[0];

                var cmd = this.CreateCommand();
                try
                {
                    foreach (var query in queries)
                    {
                        foreach (string splitQuery in SqlSplitter.SplitSqlScript(query))
                        {
                            try
                            {
                                cmd.CommandText = splitQuery;
                                cmd.ExecuteNonQuery();
                            }
                            catch
                            {
                            }
                        }
                    }

                    return this.ReadMessages();
                }
                finally
                {
                    if (this.sharedCommand == null)
                        cmd.Dispose();

                    if (this.sharedConnection == null)
                        cmd.Connection.Close();
                }
            }
            finally
            {
                this.EndUsage();
            }
        }
        public void OpenConnection()
        {
            this.BeginUsage();
            try
            {
                if (this.sharedConnection != null)
                {
                    this.sharedConnection = this.CreateConnection();
                    this.sharedCommand = this.CreateCommand();
                }
            }
            finally
            {
                this.EndUsage();
            }
        }
        public void CloseConnection()
        {
            this.BeginUsage();
            try
            {
                if (this.sharedCommand != null)
                {
                    this.sharedCommand.Dispose();
                    this.sharedCommand = null;
                }

                if (this.sharedConnection != null)
                {
                    this.sharedConnection.Dispose();
                    this.sharedConnection = null;
                }
            }
            finally
            {
                this.EndUsage();
            }
        }
        public void ValidateConnection()
        {
            this.BeginUsage();
            try
            {
                var dr = this.ExecuteDataTable("SELECT CAST(IS_MEMBER('db_owner') AS BIT) isDbOwner").Rows[0];
                bool db_owner = !Convert.IsDBNull(dr[0]) && (bool)dr[0];
                if (!db_owner)
                    throw new NotAvailableException("The ConnectionString credentials must have 'db_owner' privileges.");
            }
            finally
            {
                this.EndUsage();
            }
        }

        private LogMessage[] ReadMessages()
        {
            lock (this.messages)
            {
                var messages = this.messages.ToArray();
                this.messages.Clear();
                return messages;
            }
        }
        private void ClearMessages()
        {
            lock (this.messages)
            {
                this.messages.Clear();
            }
        }

        public void Dispose()
        {
            lock (this.timeoutLock)
            {
                this.usageCount = 0;

                if (this.idleTimer != null)
                {
                    this.idleTimer.Dispose();
                    this.idleTimer = null;
                }

                if (this.sharedCommand != null)
                {
                    this.sharedCommand.Dispose();
                    this.sharedCommand = null;
                }

                if (this.sharedConnection != null)
                {
                    this.sharedConnection.Dispose();
                    this.sharedConnection = null;
                }
            }
        }

        private void ValidateInitialization()
        {
            if (!this.IsDatabaseInitialized())
                throw new InvalidOperationException("The database has not been initialized.");
        }

        private SqlConnection CreateConnection()
        {
            var conStr = new SqlConnectionStringBuilder(ConnectionString)
            {
                Pooling = false
            };

            var con = new SqlConnection(conStr.ToString())
            {
                FireInfoMessageEventOnUserErrors = true
            };

            con.InfoMessage +=
                (s, e) =>
                {
                    lock (this.messages)
                    {
                        foreach (SqlError errorMessage in e.Errors)
                        {
                            if (errorMessage.Class > 10)
                                this.messages.Add(new LogMessage(MessageLevel.Error, errorMessage.Message));
                            else
                                this.messages.Add(new LogMessage(MessageLevel.Information, errorMessage.Message));
                        }
                    }
                };

            return con;
        }
        private SqlCommand CreateCommand()
        {
            if (this.sharedCommand != null)
            {
                this.sharedCommand.Parameters.Clear();
                return this.sharedCommand;
            }

            var cmd = new SqlCommand
            {
                CommandTimeout = 0,
                CommandType = CommandType.Text,
                CommandText = string.Empty
            };

            if (this.sharedConnection != null)
            {
                cmd.Connection = this.sharedConnection;
            }
            else
            {
                var con = this.CreateConnection();
                con.Open();
                cmd.Connection = con;
            }

            return cmd;
        }

        private void ExecuteNonQuery(string cmdText)
        {
            if (string.IsNullOrEmpty(cmdText))
                return;

            var cmd = this.CreateCommand();
            try
            {
                foreach (var commandText in SqlSplitter.SplitSqlScript(cmdText))
                {
                    try
                    {
                        cmd.CommandText = commandText;
                        cmd.ExecuteNonQuery();
                    }
                    catch (SqlException)
                    {
                        throw;
                    }
                }
            }
            finally
            {
                if (this.sharedCommand == null)
                    cmd.Dispose();

                if (this.sharedConnection == null)
                    cmd.Connection.Close();
            }
        }
        private DataTable ExecuteDataTable(string cmdText)
        {
            var dt = new DataTable();
            var cmd = this.CreateCommand();
            cmd.CommandText = cmdText;
            try
            {
                dt.Load(cmd.ExecuteReader());
            }
            finally
            {
                if (this.sharedCommand == null)
                    cmd.Dispose();

                if (this.sharedConnection == null)
                    cmd.Connection.Close();
            }

            return dt;
        }

        private void InsertSchemaChange(long numericReleaseNumber, int scriptId, string scriptName, int scriptSequence, bool success)
        {
            this.ExecuteQuery(string.Format(
                "INSERT INTO __BuildMaster_DbSchemaChanges "
                + " (Numeric_Release_Number, Script_Id, Script_Sequence, Batch_Name, Executed_Date, Success_Indicator) "
                + "VALUES "
                + "({0}, {1}, {2}, '{3}', GETDATE(), '{4}')",
                numericReleaseNumber,
                scriptId,
                scriptSequence,
                scriptName.Replace("'", "''"),
                success ? "Y" : "N")
            );
        }

        private void BeginUsage()
        {
            lock (this.timeoutLock)
            {
                if (this.idleTimer != null)
                {
                    this.idleTimer.Dispose();
                    this.idleTimer = null;
                }

                this.usageCount++;
            }
        }
        private void EndUsage()
        {
            lock (this.timeoutLock)
            {
                if (this.usageCount <= 1)
                {
                    this.usageCount = 0;
                    this.idleTimer = new Timer(this.HandleTimeoutExpired, null, IdleTimeoutPeriodMilliseconds, Timeout.Infinite);
                }
                else
                {
                    this.usageCount--;
                }
            }
        }
        private void HandleTimeoutExpired(object state)
        {
            try
            {
                var handler = this.IdleTimeoutExpired;
                if (handler != null)
                    handler(this, EventArgs.Empty);
            }
            catch
            {
            }
        }
    }
}
