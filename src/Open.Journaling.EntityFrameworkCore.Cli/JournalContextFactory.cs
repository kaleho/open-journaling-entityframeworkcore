using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Open.Journaling.EntityFrameworkCore.Model;

namespace Open.Journaling.EntityFrameworkCore.Cli
{
    public class JournalContextFactory 
        : IDesignTimeDbContextFactory<JournalContext>
    {
        public JournalContext CreateDbContext(
            string[] args)
        {
            var optionsBuilder = new DbContextOptionsBuilder<JournalContext>();
            
            optionsBuilder.UseSqlServer(
                "Data Source=localhost;" +
                "Initial Catalog=Journal_DEV;" +
                "Persist Security Info=True;" +
                "User ID=sa;" +
                "Password=4xp6euati4lrcume4xeb");

            return new JournalContext(optionsBuilder.Options);
        }
    }
}