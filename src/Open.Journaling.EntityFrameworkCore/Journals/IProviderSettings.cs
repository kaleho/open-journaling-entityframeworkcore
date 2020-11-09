using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Open.Journaling.EntityFrameworkCore.Model;
using Open.Journaling.Traits;
using System;
using System.Threading.Tasks;

namespace Open.Journaling.EntityFrameworkCore.Journals
{
    public interface IProviderSettings
    {
        string ConnectionString { get; }

        int ConnectionTimeout { get; }

        Func<ConventionSet> ConventionSetBuilder { get; }

        Func<string, Task<bool>> HasJournalFunc { get; }

        int InitializationTimeout { get; }

        JournalTraits JournalTraits { get; }

        string Name { get; }

        Func<DbContextOptions<JournalContext>> OptionsBuilder { get; }

        int WriterRetryLimit { get; }
    }
}