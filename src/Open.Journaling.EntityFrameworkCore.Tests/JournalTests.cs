using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging.Abstractions;
using Open.Journaling.EntityFrameworkCore.Journals;
using Open.Journaling.Model;
using Open.Monikers;
using Xunit;
using Xunit.Abstractions;

namespace Open.Journaling.EntityFrameworkCore.Tests
{
    public class JournalTests
        : IClassFixture<SqlServerTestFixture>
    {
        public JournalTests(
            ITestOutputHelper testOutputHelper,
            SqlServerTestFixture sqlServerTestFixture)
        {
            _testOutputHelper = testOutputHelper;

            _sqlServerTestFixture = sqlServerTestFixture;
        }

        private readonly SqlServerTestFixture _sqlServerTestFixture;
        private readonly ITestOutputHelper _testOutputHelper;

        [Fact]
        public void Can_Insert_Consistent_Item()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var entry =
                new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{Guid.NewGuid():N}");

            var savedEntry =
                journal.Write(
                        new CancellationToken(),
                        new KnownMutableEntry(entry.EntryId, new byte[0], new byte[0], 0, "on\te"))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.True(savedEntry.Length > 0);
        }

        [Fact]
        public void Can_Insert_Consistent_With_Sequential_Writes()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions(),
                        0));

            var journalEntries = new List<IJournalEntry>();

            var entry =
                new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}-0");

            var consistentEntry1 = new KnownMutableEntry(entry.EntryId, new byte[0], new byte[0], 0, "first-tag");

            journal.Write(
                    new CancellationToken(),
                    consistentEntry1)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();

            var consistentEntry2 = new KnownMutableEntry(entry.EntryId, new byte[0], new byte[0], 1, "second-tag");

            journalEntries.AddRange(
                journal.Write(
                        new CancellationToken(),
                        consistentEntry2)
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult());

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Single(journalEntries);
            Assert.Equal(consistentEntry2.Tags.First(), journalEntries.First().Tags.First());
        }

        [Fact]
        public void Can_Insert_ConsistentUnique_Item()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var entry =
                new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{Guid.NewGuid():N}");

            var savedEntry =
                journal.Write(
                        new CancellationToken(),
                        new KnownImmutableEntry($"{entry.EntryId}-0", new byte[0], new byte[0], 0, "on\te"))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.True(savedEntry.Length > 0);
        }

        [Fact]
        public void Can_Insert_Single_Item()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var entry =
                new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{Guid.NewGuid():N}");

            var savedEntry =
                journal.Write(
                        new CancellationToken(),
                        new SerializedEntry(entry.EntryId, new byte[0], new byte[0], "on\te"))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.True(savedEntry.Length > 0);
        }

        [Fact]
        public void Can_Insert_Ten_Items()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(entry.EntryId, new byte[0], new byte[0]))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());
            }

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Equal(journalEntries.Count, journal.Props.HighestSequenceNumber);
        }

        [Fact]
        public void Can_Not_Insert_Consistent_With_Duplicate_EntryIds()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions(),
                        0));

            var journalEntries = new List<IJournalEntry>();

            var entries =
                2.Items(
                    i =>
                    {
                        var entry =
                            new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}-0");

                        return new KnownMutableEntry(entry.EntryId, new byte[0], new byte[0], 0);
                    });

            Assert.Throws<InvalidOperationException>(
                () =>
                {
                    journalEntries.AddRange(
                        journal.Write(
                                new CancellationToken(),
                                entries.ToArray())
                            .ConfigureAwait(false)
                            .GetAwaiter()
                            .GetResult());
                });

            // HighestSequenceNumber will reflect the addition of the
            // entries but the table will not have been updated
            journal.ResetProps();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Empty(journalEntries);
            Assert.Equal(0, journal.Props.HighestSequenceNumber);
        }

        [Fact]
        public void Can_Not_Insert_ConsistentUnique_With_Duplicate_EntryIds()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions(),
                        0));

            var journalEntries = new List<IJournalEntry>();

            var entries =
                2.Items(
                    i =>
                    {
                        var entry =
                            new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}-0");

                        return new KnownImmutableEntry(entry.EntryId, new byte[0], new byte[0], 0);
                    });

            Assert.Throws<InvalidOperationException>(
                () =>
                {
                    journalEntries.AddRange(
                        journal.Write(
                                new CancellationToken(),
                                entries.ToArray())
                            .ConfigureAwait(false)
                            .GetAwaiter()
                            .GetResult());
                });

            // HighestSequenceNumber will reflect the addition of the
            // entries but the table will not have been updated
            journal.ResetProps();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Empty(journalEntries);
            Assert.Equal(0, journal.Props.HighestSequenceNumber);
        }

        [Fact]
        public void Can_ReadByEntryId_For_ConsistentEntry()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new KnownMutableEntry(entry.EntryId, new byte[0], new byte[0], 0))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var readEntry =
                journal.ReadByEntryId(
                        fourthEntry.EntryId,
                        new CancellationToken())
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.NotNull(readEntry);
            Assert.Equal(fourthEntry.EntryId, readEntry.EntryId);
        }

        [Fact]
        public void Can_ReadByEntryId_For_ConsistentUniqueEntry()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new KnownImmutableEntry(entry.EntryId, new byte[0], new byte[0], 0))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var readEntry =
                journal.ReadByEntryId(
                        fourthEntry.EntryId,
                        new CancellationToken())
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.NotNull(readEntry);
            Assert.Equal(fourthEntry.EntryId, readEntry.EntryId);
        }

        [Fact]
        public void Can_ReadBySequence_For_Range()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                journal.Write(
                        new CancellationToken(),
                        new SerializedEntry(entry.EntryId, new byte[0], new byte[0]))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();
            }

            var readEntries =
                journal.Read(
                        LocationKind.Sequence,
                        4L,
                        new CancellationToken(),
                        6L)
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.Sequence)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Equal(2, readEntries.Count);
            Assert.Equal(5, readEntries.First().Sequence);
            Assert.Equal(6, readEntries.Last().Sequence);
        }

        [Fact]
        public void Can_ReadBySequence_With_Unbound_ToSequence()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                journal.Write(
                        new CancellationToken(),
                        new SerializedEntry(entry.EntryId, new byte[0], new byte[0]))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();
            }

            var readEntries =
                journal.Read(
                        LocationKind.Sequence,
                        4L,
                        new CancellationToken())
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.Sequence)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Equal(6, readEntries.Count);
            Assert.Equal(5, readEntries.First().Sequence);
            Assert.Equal(10, readEntries.Last().Sequence);
        }

        [Fact]
        public void Can_ReadBySequenceWithTags_For_Range()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                var isEven = (i + 1) % 2 == 0;

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(
                                entry.EntryId, new byte[0], new byte[0],
                                isEven ? "even\t" : "o%dd\t"))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var sixthEntry = journalEntries[5];

            var readEntries =
                journal.ReadWithTags(
                        LocationKind.Sequence,
                        fourthEntry.Sequence,
                        new CancellationToken(),
                        sixthEntry.Sequence,
                        "o%dd\t")
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.UtcTicks)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.True(readEntries.Count == 1);
            Assert.Collection(readEntries, entry => entry.Tags.Contains("o%dd\t"));
        }

        [Fact]
        public void Can_ReadBySequenceWithTags_For_Range_With_Multiple_Tags()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                var isEven = (i + 1) % 2 == 0;

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(
                                entry.EntryId, new byte[0], new byte[0],
                                isEven ? "even" : "odd"))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var sixthEntry = journalEntries[5];

            var readEntries =
                journal.ReadWithTags(
                        LocationKind.Sequence,
                        fourthEntry.Sequence,
                        new CancellationToken(),
                        sixthEntry.Sequence,
                        "odd",
                        "even")
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.UtcTicks)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.True(readEntries.Count == 2);
            Assert.True(readEntries.All(x => x.Tags.Contains("even") || x.Tags.Contains("odd")));
        }

        [Fact]
        public void Can_ReadBySequenceWithTags_With_Unbound_ToSequence()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                var isEven = (i + 1) % 2 == 0;

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(
                                entry.EntryId, new byte[0], new byte[0],
                                isEven ? "even" : "odd"))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var readEntries =
                journal.ReadWithTags(
                        LocationKind.Sequence,
                        fourthEntry.Sequence,
                        new CancellationToken(),
                        tags: "odd")
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.UtcTicks)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.True(readEntries.Count == 3);
            Assert.All(readEntries, entry => entry.Tags.Contains("odd"));
        }

        [Fact]
        public void Can_ReadByUtcTicks_For_Range()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(entry.EntryId, new byte[0], new byte[0]))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var fifthEntry = journalEntries[4];

            var sixthEntry = journalEntries[5];

            var readEntries =
                journal.Read(
                        LocationKind.UtcTicks,
                        fourthEntry.UtcTicks,
                        new CancellationToken(),
                        sixthEntry.UtcTicks)
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.UtcTicks)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Equal(2, readEntries.Count);
            Assert.Equal(fifthEntry.UtcTicks, readEntries.First().UtcTicks);
            Assert.Equal(sixthEntry.UtcTicks, readEntries.Last().UtcTicks);
        }

        [Fact]
        public void Can_ReadByUtcTicks_With_Unbound_ToUtcTick()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(entry.EntryId, new byte[0], new byte[0]))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var fifthEntry = journalEntries[4];

            var lastEntry = journalEntries[9];

            var readEntries =
                journal.Read(
                        LocationKind.UtcTicks,
                        fourthEntry.UtcTicks,
                        new CancellationToken())
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.UtcTicks)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Equal(6, readEntries.Count);
            Assert.Equal(fifthEntry.UtcTicks, readEntries.First().UtcTicks);
            Assert.Equal(lastEntry.UtcTicks, readEntries.Last().UtcTicks);
        }

        [Fact]
        public void Can_ReadByUtcTicksWithTags_For_Range()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                var isEven = (i + 1) % 2 == 0;

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(
                                entry.EntryId, new byte[0], new byte[0],
                                isEven ? "even" : "odd"))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var sixthEntry = journalEntries[5];

            var readEntries =
                journal.ReadWithTags(
                        LocationKind.UtcTicks,
                        fourthEntry.UtcTicks,
                        new CancellationToken(),
                        sixthEntry.UtcTicks,
                        "odd")
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.UtcTicks)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.Single(readEntries);
            Assert.All(readEntries, entry => entry.Tags.Contains("odd"));
        }

        [Fact]
        public void Can_ReadByUtcTicksWithTags_For_Range_With_Multiple_Tags()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                var isEven = (i + 1) % 2 == 0;

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(
                                entry.EntryId, new byte[0], new byte[0],
                                isEven ? "even" : "odd"))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var sixthEntry = journalEntries[5];

            var readEntries =
                journal.ReadWithTags(
                        LocationKind.UtcTicks,
                        fourthEntry.UtcTicks,
                        new CancellationToken(),
                        sixthEntry.UtcTicks,
                        "even",
                        "odd")
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.UtcTicks)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.True(readEntries.Count == 2);
            Assert.True(readEntries.All(x => x.Tags.Contains("even") || x.Tags.Contains("odd")));
        }

        [Fact]
        public void Can_ReadByUtcTicksWithTags_With_Unbound_ToUtcTicks()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId = new JournalId("j" + Guid.NewGuid().ToString("N"));

            _testOutputHelper.WriteLine($"journalId: {journalId}");

            var journal =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journalEntries = new List<IJournalEntry>();

            for (var i = 0; i < 10; i++)
            {
                var entry =
                    new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}{IRefId.NameSeparator}{i + 1}");

                var isEven = (i + 1) % 2 == 0;

                journalEntries.AddRange(
                    journal.Write(
                            new CancellationToken(),
                            new SerializedEntry(
                                entry.EntryId, new byte[0], new byte[0],
                                isEven ? "even" : "odd"))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                Thread.Sleep(10);
            }

            var fourthEntry = journalEntries[3];

            var readEntries =
                journal.ReadWithTags(
                        LocationKind.UtcTicks,
                        fourthEntry.UtcTicks,
                        new CancellationToken(),
                        tags: "odd")
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult()
                    .OrderBy(x => x.UtcTicks)
                    .ToList();

            _sqlServerTestFixture.CleanupDatabase(journalId.ToString());

            Assert.True(readEntries.Count == 3);
            Assert.All(readEntries, entry => entry.Tags.Contains("odd"));
        }

        [Fact]
        public void Writing_From_Multiple_Writers_Should_Cause_Props_To_Reset_And_Retry()
        {
            if (!_sqlServerTestFixture.IsServerAvailable)
            {
                _testOutputHelper.WriteLine("Server unavailable.");

                return;
            }

            var journalId1 = new JournalId("test1");

            _testOutputHelper.WriteLine($"journalId1: {journalId1}");

            var journalId2 = new JournalId("test2");

            _testOutputHelper.WriteLine($"journalId2: {journalId2}");

            var journal1 =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId1,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var journal2 =
                new Journal(
                    new NullLogger<Journal>(),
                    new JournalSettings(
                        "testprovider",
                        _sqlServerTestFixture.ConnectionString,
                        3000,
                        30000,
                        journalId2,
                        _sqlServerTestFixture.GetModelBuilder(),
                        _sqlServerTestFixture.GetOptions()));

            var entry1 =
                new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}-entry1");

            var savedEntry1 =
                journal1.Write(
                        new CancellationToken(),
                        new SerializedEntry(entry1.EntryId, new byte[0], new byte[0], "on\te"))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

            var entry2 =
                new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}-entry2");

            var savedEntry2 =
                journal2.Write(
                        new CancellationToken(),
                        new SerializedEntry(entry2.EntryId, new byte[0], new byte[0], "on\te"))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

            var entry3 =
                new TestEntry($"{nameof(TestEntry).ToLowerInvariant()}-entry3");

            var savedEntry3 =
                journal1.Write(
                        new CancellationToken(),
                        new SerializedEntry(entry3.EntryId, new byte[0], new byte[0], "on\te"))
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

            _sqlServerTestFixture.CleanupDatabase(journalId1.ToString());
            _sqlServerTestFixture.CleanupDatabase(journalId2.ToString());

            Assert.True(savedEntry1.Length > 0);
        }
    }

    public sealed class TestEntry
    {
        public TestEntry(
            string entryId)
        {
            EntryId = entryId;
        }

        public string EntryId { get; }
    }
}