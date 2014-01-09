using System;
using System.Data.SqlClient;
using System.Text;
using Inedo.BuildMaster.Extensibility.Agents;
using Inedo.BuildMaster.Extensibility.Providers;
using Inedo.BuildMaster.Extensibility.Providers.Database;
using Inedo.BuildMaster.Web;

namespace Inedo.BuildMasterExtensions.SqlServer
{
    [ProviderProperties(
        "SQL Server",
        "Provides functionality for managing change scripts in Microsoft SQL Server databases.")]
    [CustomEditor(typeof(SqlServerDatabaseProviderEditor))]
    public sealed class SqlServerDatabaseProvider : DatabaseProviderBase, IRestoreProvider, IChangeScriptProvider
    {
        private Lazy<Guid> remoteId;
        private bool disposed;

        public SqlServerDatabaseProvider()
        {
            this.remoteId = new Lazy<Guid>(
                () => this.Agent.GetService<IRemoteMethodExecuter>().InvokeFunc(RemoteSqlProvider.CreateInstance, this.ConnectionString)
            );
        }

        private Guid RemoteId
        {
            get { return this.remoteId.Value; }
        }
        private IRemoteMethodExecuter Remote
        {
            get { return this.Agent.GetService<IRemoteMethodExecuter>(); }
        }

        public void InitializeDatabase()
        {
            this.Remote.InvokeAction(RemoteSqlProvider.InitializeDatabase, this.RemoteId);
        }
        public bool IsDatabaseInitialized()
        {
            return this.Remote.InvokeFunc(RemoteSqlProvider.IsDatabaseInitialized, this.RemoteId);
        }
        public long GetSchemaVersion()
        {
            return this.Remote.InvokeFunc(RemoteSqlProvider.GetSchemaVersion, this.RemoteId);
        }
        public ChangeScript[] GetChangeHistory()
        {
            return this.Remote.InvokeFunc(RemoteSqlProvider.GetChangeHistory, this.RemoteId);
        }
        public ExecutionResult ExecuteChangeScript(long numericReleaseNumber, int scriptId, string scriptName, string scriptText)
        {
            return (ExecutionResult)this.Remote.InvokeMethod(
                new Func<Guid, long, int, string, string, ExecutionResult>(RemoteSqlProvider.ExecuteChangeScript),
                this.RemoteId,
                numericReleaseNumber,
                scriptId,
                scriptName,
                scriptText
            );
        }

        public void BackupDatabase(string databaseName, string destinationPath)
        {
            var messages = this.Remote.InvokeFunc(RemoteSqlProvider.BackupDatabase, this.RemoteId, databaseName, destinationPath);
            this.RaiseLogMessages(messages);
        }
        public void RestoreDatabase(string databaseName, string sourcePath)
        {
            var messages = this.Remote.InvokeFunc(RemoteSqlProvider.RestoreDatabase, this.RemoteId, databaseName, sourcePath);
            this.RaiseLogMessages(messages);
        }

        public override bool IsAvailable()
        {
            return true;
        }
        public override string ToString()
        {
            try
            {
                var csb = new SqlConnectionStringBuilder(this.ConnectionString);
                var buffer = new StringBuilder("SQL Server database");
                if (!string.IsNullOrEmpty(csb.InitialCatalog))
                {
                    buffer.Append(" \"");
                    buffer.Append(csb.InitialCatalog);
                    buffer.Append('\"');
                }

                if (!string.IsNullOrEmpty(csb.DataSource))
                {
                    buffer.Append(" on server \"");
                    buffer.Append(csb.DataSource);
                    buffer.Append('\"');
                }

                return buffer.ToString();
            }
            catch
            {
                return "SQL Server database";
            }
        }

        public override void ExecuteQuery(string query)
        {
            var messages = this.Remote.InvokeFunc(RemoteSqlProvider.ExecuteQuery, this.RemoteId, query);
            this.RaiseLogMessages(messages);
        }
        public override void ExecuteQueries(string[] queries)
        {
            var messages = this.Remote.InvokeFunc(RemoteSqlProvider.ExecuteQueries, this.RemoteId, queries);
            this.RaiseLogMessages(messages);
        }
        public override void OpenConnection()
        {
            this.Remote.InvokeAction(RemoteSqlProvider.OpenConnection, this.RemoteId);
        }
        public override void CloseConnection()
        {
            this.Remote.InvokeAction(RemoteSqlProvider.CloseConnection, this.RemoteId);
        }
        public override void ValidateConnection()
        {
            this.Remote.InvokeAction(RemoteSqlProvider.ValidateConnection, this.RemoteId);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !this.disposed)
            {
                if (this.remoteId.IsValueCreated)
                    this.Agent.GetService<IRemoteMethodExecuter>().InvokeAction(RemoteSqlProvider.Dispose, this.remoteId.Value);

                this.disposed = true;
            }

            base.Dispose(disposing);
        }

        private void RaiseLogMessages(LogMessage[] messages)
        {
            if (messages == null || messages.Length == 0)
                return;

            foreach (var message in messages)
                this.Log(message.Level, message.Message);
        }
    }
}
