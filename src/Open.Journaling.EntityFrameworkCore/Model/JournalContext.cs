using System;
using Microsoft.EntityFrameworkCore;

namespace Open.Journaling.EntityFrameworkCore.Model
{
    public class JournalContext
        : DbContext
    {
        public const string DefaultTableName = "Entity";

        public JournalContext(
            DbContextOptions<JournalContext> options,
            string tableName = DefaultTableName)
            : base(options)
        {
            TableName = tableName;
        }

        public DbSet<JournalPropsEntity> JournalPropsEntities { get; set; }

        public DbSet<TableEntity> TableEntities { get; set; }

        public string TableName { get; }

        [DbFunction("CHARINDEX")]
        public static int? CharIndex(
            string expressionToFind, 
            string expressionToSearch,
            long startLocation) => throw new Exception();
    }
}