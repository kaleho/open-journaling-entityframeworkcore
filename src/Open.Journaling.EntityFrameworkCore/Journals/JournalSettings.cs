using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Open.Journaling.EntityFrameworkCore.Model;

namespace Open.Journaling.EntityFrameworkCore.Journals
{
    public class JournalSettings
        : IJournalSettings
    {
        public const int DefaultWriterRetryLimit = 5;

        public JournalSettings(
            string providerName,
            string connectionString,
            int connectionTimeout,
            int initializationTimeout,
            JournalId journalId,
            ModelBuilder modelBuilder,
            DbContextOptions<JournalContext> dbContextOptions,
            int writerRetryLimit = DefaultWriterRetryLimit)
        {
            ProviderName = providerName;
            ConnectionString = connectionString;
            ConnectionTimeout = connectionTimeout;
            InitializationTimeout = initializationTimeout;
            JournalId = journalId;
            WriterRetryLimit = writerRetryLimit;

            DbContextOptions =
                new DbContextOptionsBuilder<JournalContext>(dbContextOptions)
                    .UseModel(BuildModel(modelBuilder))
                    .Options;
        }

        public string ConnectionString { get; }

        public int ConnectionTimeout { get; }

        public DbContextOptions<JournalContext> DbContextOptions { get; }

        public int InitializationTimeout { get; }

        public string ProviderName { get; }

        public int WriterRetryLimit { get; }

        public JournalId JournalId { get; }

        private IMutableModel BuildModel(
            ModelBuilder modelBuilder)
        {
            IMutableModel returnValue = null;

            var tableName = JournalId.Name;

            modelBuilder
                .Entity<JournalPropsEntity>(
                    builder =>
                    {
                        builder
                            .Property(x => x.JournalId)
                            .HasMaxLength(384)
                            .IsRequired();

                        builder
                            .Property(x => x.HighestSequenceNumber)
                            .IsRequired();

                        builder
                            .Property(x => x.InitialUtcTicks)
                            .IsRequired();

                        builder.HasKey(x => x.JournalId);

                        builder.ToTable($"{tableName}-props");
                    });

            modelBuilder
                .Entity<TableEntity>(
                    builder =>
                    {
                        builder
                            .Property(x => x.EntryId)
                            .HasMaxLength(512)
                            .IsRequired();

                        builder
                            .Property(x => x.JournalId)
                            .HasMaxLength(384)
                            .IsRequired();

                        builder
                            .Property(x => x.Meta)
                            .IsRequired();

                        builder
                            .Property(x => x.Payload)
                            .IsRequired();

                        builder
                            .Property(x => x.RowKey)
                            .HasMaxLength(384)
                            .IsRequired();

                        builder
                            .Property(x => x.Tags)
                            .HasMaxLength(768)
                            .IsRequired();

                        builder
                            .Property(x => x.Sequence)
                            .IsRequired();

                        builder
                            .Property(x => x.UtcTicks)
                            .IsRequired();

                        builder.HasKey(x => new { x.JournalId, x.RowKey });

                        builder.HasIndex(x => x.JournalId);
                        builder.HasIndex(x => x.EntryId);
                        builder.HasIndex(x => x.Sequence);
                        builder.HasIndex(x => x.UtcTicks);
                        builder.HasIndex(x => x.Tags);

                        builder.ToTable(tableName);
                    });

            modelBuilder
                .Entity<ConsistentEntity>();

            modelBuilder
                .HasDbFunction(
                    typeof(JournalContext).GetMethod(nameof(JournalContext.CharIndex)))
                .HasTranslation(
                    args =>
                        SqlFunctionExpression.Create("CHARINDEX", args, typeof(int?), null));

            modelBuilder.FinalizeModel();

            returnValue = modelBuilder.Model;

            return returnValue;
        }
    }
}