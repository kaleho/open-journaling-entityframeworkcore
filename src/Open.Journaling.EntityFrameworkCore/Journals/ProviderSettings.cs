using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Open.Journaling.EntityFrameworkCore.Model;
using Open.Journaling.Traits;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Open.Journaling.EntityFrameworkCore.Journals
{
    public sealed class ProviderSettings
        : IProviderSettings
    {
        public const int DefaultWriterRetryLimit = 5;

        public static ImmutableList<IJournalTrait> DefaultProviderTraits =
            ImmutableList.CreateRange(
                new IJournalTrait[]
                {
                    new AtomicTrait(TriState.True),
                    new DurableTrait(TriState.True),
                    new EntityFrameworkCoreTrait(TriState.True)
                });

        public ProviderSettings(
            string name,
            string connectionString,
            int connectionTimeout,
            int initializationTimeout,
            Func<ConventionSet> conventionSetBuilder,
            Func<DbContextOptions<JournalContext>> optionsBuilder,
            Func<string, Task<bool>> hasJournalFunc,
            IEnumerable<IJournalTrait> additionalTraits = null,
            int writerRetryLimit = DefaultWriterRetryLimit)
        {
            Name = name;
            ConnectionString = connectionString;
            ConnectionTimeout = connectionTimeout;
            InitializationTimeout = initializationTimeout;
            ConventionSetBuilder = conventionSetBuilder;
            OptionsBuilder = optionsBuilder;
            HasJournalFunc = hasJournalFunc;
            WriterRetryLimit = writerRetryLimit;

            var traits = new List<IJournalTrait>(DefaultProviderTraits);

            if (additionalTraits != null)
            {
                traits.AddRange(
                    additionalTraits.Where(
                        trait => DefaultProviderTraits.All(x => trait.GetType() != x.GetType())));
            }

            JournalTraits = new JournalTraits(traits);
        }

        public string ConnectionString { get; }

        public int ConnectionTimeout { get; }

        public Func<ConventionSet> ConventionSetBuilder { get; }

        public Func<string, Task<bool>> HasJournalFunc { get; }

        public int InitializationTimeout { get; }

        public JournalTraits JournalTraits { get; }

        public string Name { get; }

        public Func<DbContextOptions<JournalContext>> OptionsBuilder { get; }

        public int WriterRetryLimit { get; }
    }
}