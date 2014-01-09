using System;
using System.Collections.Concurrent;
using Inedo.BuildMaster.Extensibility.Providers.Database;

namespace Inedo.BuildMasterExtensions.SqlServer
{
    [Serializable]
    internal static class RemoteSqlProvider
    {
        private static readonly ConcurrentDictionary<Guid, InternalSqlProvider> ActiveProviders = new ConcurrentDictionary<Guid, InternalSqlProvider>();

        public static Guid CreateInstance(string connectionString)
        {
            var provider = new InternalSqlProvider(connectionString);
            var id = Guid.NewGuid();
            ActiveProviders[id] = provider;
            provider.IdleTimeoutExpired +=
                (s, e) =>
                {
                    try
                    {
                        InternalSqlProvider value;
                        ActiveProviders.TryRemove(id, out value);
                        if (value != null)
                            value.Dispose();
                    }
                    catch
                    {
                    }
                };

            return id;
        }

        public static void InitializeDatabase(Guid id)
        {
            GetInstance(id).InitializeDatabase();
        }
        public static bool IsDatabaseInitialized(Guid id)
        {
            return GetInstance(id).IsDatabaseInitialized();
        }
        public static long GetSchemaVersion(Guid id)
        {
            return GetInstance(id).GetSchemaVersion();
        }
        public static ChangeScript[] GetChangeHistory(Guid id)
        {
            return GetInstance(id).GetChangeHistory();
        }
        public static ExecutionResult ExecuteChangeScript(Guid id, long numericReleaseNumber, int scriptId, string scriptName, string scriptText)
        {
            return GetInstance(id).ExecuteChangeScript(numericReleaseNumber, scriptId, scriptName, scriptText);
        }
        public static LogMessage[] BackupDatabase(Guid id, string databaseName, string destinationPath)
        {
            return GetInstance(id).BackupDatabase(databaseName, destinationPath);
        }
        public static LogMessage[] RestoreDatabase(Guid id, string databaseName, string sourcePath)
        {
            return GetInstance(id).RestoreDatabase(databaseName, sourcePath);
        }
        public static LogMessage[] ExecuteQuery(Guid id, string query)
        {
            return GetInstance(id).ExecuteQuery(query);
        }
        public static LogMessage[] ExecuteQueries(Guid id, string[] queries)
        {
            return GetInstance(id).ExecuteQueries(queries);
        }
        public static void OpenConnection(Guid id)
        {
            GetInstance(id).OpenConnection();
        }
        public static void CloseConnection(Guid id)
        {
            GetInstance(id).CloseConnection();
        }
        public static void ValidateConnection(Guid id)
        {
            GetInstance(id).ValidateConnection();
        }
        public static void Dispose(Guid id)
        {
            InternalSqlProvider provider;
            if (ActiveProviders.TryRemove(id, out provider))
                provider.Dispose();
        }

        private static InternalSqlProvider GetInstance(Guid id)
        {
            InternalSqlProvider provider;
            if (!ActiveProviders.TryGetValue(id, out provider))
                throw new ArgumentException();

            return provider;
        }
    }
}
