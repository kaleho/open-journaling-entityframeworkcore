using System;
using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Open.Journaling.EntityFrameworkCore.Model;
using Open.Monikers;

namespace Open.Journaling.EntityFrameworkCore.Tests
{
    public class SqlServerTestFixture
        : IDisposable
    {
        private readonly ConcurrentDictionary<string, string> _cache;

        public SqlServerTestFixture()
        {
            ConnectionString =
                "Data Source=localhost;" +
                "Initial Catalog=Journal_DEV;" +
                "Persist Security Info=True;" +
                "User ID=sa;" +
                "Password=4xp6euati4lrcume4xeb";

            try
            {
                using var connection = new SqlConnection(ConnectionString);

                connection.Open();

                IsServerAvailable = true;
            }
            catch (SqlException)
            {
                IsServerAvailable = false;
            }

            _cache = new ConcurrentDictionary<string, string>();
        }

        public string ConnectionString { get; }

        public bool IsServerAvailable { get; }

        public void Dispose()
        {
            if (!IsServerAvailable)
            {
                return;
            }

            using var connection = new SqlConnection(ConnectionString);
            connection.Open();

            foreach (var (key, value) in _cache)
            {
                var dropPropsCommand = new SqlCommand($"DROP TABLE [{key}-props]", connection);
                dropPropsCommand.ExecuteNonQuery();

                var dropEntityCommand = new SqlCommand($"DROP TABLE [{key}]", connection);
                dropEntityCommand.ExecuteNonQuery();
            }

            connection.Close();
        }

        public void CleanupDatabase(
            string journalId)
        {
            var separatorIndex = journalId.IndexOf(IRefId.NameSeparator, StringComparison.OrdinalIgnoreCase);

            var tableName =
                separatorIndex > -1
                    ? journalId.Substring(0, separatorIndex)
                    : journalId;

            _cache.TryAdd(tableName, journalId);
        }

        public ModelBuilder GetModelBuilder()
        {
            return new ModelBuilder(SqlServerConventionSetBuilder.Build());
        }

        public DbContextOptions<JournalContext> GetOptions()
        {
            return
                new DbContextOptionsBuilder<JournalContext>()
                    .UseSqlServer(ConnectionString)
                    .UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking)
                    .Options;
        }
    }
}