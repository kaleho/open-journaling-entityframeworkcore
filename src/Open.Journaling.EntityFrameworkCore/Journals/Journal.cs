using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Open.Journaling.EntityFrameworkCore.Model;
using Open.Journaling.Model;

namespace Open.Journaling.EntityFrameworkCore.Journals
{
    public sealed class Journal
        : IJournal,
          IJournalReader,
          IJournalWriter
    {
        private readonly DateTime _initializationTimeLimit;
        private readonly ILogger<Journal> _log;
        private readonly JournalSettings _settings;
        private JournalProps _props;

        public Journal(
            in ILogger<Journal> log,
            in JournalSettings settings)
        {
            _initializationTimeLimit = DateTime.UtcNow.AddMilliseconds(settings.InitializationTimeout);

            _log = log;

            _settings = settings;

            SyncRunner.Run(() => Initialize(250));
        }

        public JournalId JournalId => _settings.JournalId;

        public IJournalProps Props => _props;

        public Task<IJournalEntry[]> Read(
            LocationKind kind,
            long from,
            CancellationToken cancellationToken,
            long? to = null)
        {
            return
                kind == LocationKind.Sequence
                    ? ReadBySequence(from, cancellationToken, to)
                    : ReadByUtcTicks(from, cancellationToken, to);
        }

        public async Task<IJournalEntry> ReadByEntryId(
            string entryId,
            CancellationToken cancellationToken)
        {
            JournalEntry returnValue = null;

            await using var context = new JournalContext(_settings.DbContextOptions, _settings.JournalId.ToString());

            var journalId = _settings.JournalId.ToString();

            var result =
                await context.TableEntities.FirstOrDefaultAsync(
                        x =>
                            x.JournalId == journalId &&
                            x.EntryId == entryId,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (result != null)
            {
                returnValue = new JournalEntry(
                    result.JournalId,
                    result.EntryId,
                    result.Sequence,
                    result.UtcTicks,
                    result.Meta,
                    result.Payload,
                    result.Tags?.Split(Delimiters.Delimiter, StringSplitOptions.RemoveEmptyEntries));
            }

            return returnValue;
        }

        public async Task<IJournalEntry[]> ReadWithTags(
            LocationKind kind,
            long from,
            CancellationToken cancellationToken,
            long? to = null,
            params string[] tags)
        {
            return
                kind == LocationKind.Sequence
                    ? await ReadWithTagsBySequence(from, cancellationToken, to, tags).ConfigureAwait(false)
                    : await ReadWithTagsByUtcTicks(from, cancellationToken, to, tags).ConfigureAwait(false);
        }

        public async Task<IJournalEntry[]> Write(
            CancellationToken cancellationToken,
            params ISerializedEntry[] entries)
        {
            return await WriteWithRetry(cancellationToken, entries, 0).ConfigureAwait(false);
        }

        public void ResetProps()
        {
            using var context = new JournalContext(_settings.DbContextOptions, _settings.JournalId.ToString());

            var props = context.JournalPropsEntities.FirstOrDefault(x => x.JournalId == _settings.JournalId.ToString());

            if (props != null)
            {
                var slimLock = new SlimLock();

                using (slimLock.WriteLock())
                {
                    _props =
                        new JournalProps(
                            props.HighestSequenceNumber,
                            props.InitialUtcTicks);
                }
            }
            else
            {
                throw new Exception(
                    "Cannot reset props when the database is not in a consistent state. The journal was not initialized correctly.");
            }
        }

        private IJournalEntry[] GetResults(
            IEnumerable<TableEntity> results)
        {
            var returnValue = new List<IJournalEntry>();

            returnValue.AddRange(
                results
                    .Select(
                        x =>
                            new JournalEntry(
                                x.JournalId,
                                x.EntryId,
                                x.Sequence,
                                x.UtcTicks,
                                x.Meta,
                                x.Payload,
                                x.Tags?.Split(Delimiters.Delimiter, StringSplitOptions.RemoveEmptyEntries))));

            return returnValue.ToArray();
        }

        private async Task<bool> Initialize(
            int delayMilliseconds)
        {
            try
            {
                var journalId = _settings.JournalId.ToString();

                await using var context = new JournalContext(_settings.DbContextOptions, _settings.JournalId.Name);

                var createScript = context.Database.GenerateCreateScript();

                var scripts =
                    createScript
                        .Trim()
                        .Split(";", StringSplitOptions.RemoveEmptyEntries)
                        .Select(x => x.Replace("GO", "").Trim())
                        .ToList();

                foreach (var script in scripts.Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    try
                    {
                        await context.Database.ExecuteSqlRawAsync(script);
                    }
                    catch (Exception ex)
                    {
                        if (ex.Message.IndexOf("already an object", StringComparison.Ordinal) == -1 &&
                            ex.Message.IndexOf("already exists", StringComparison.Ordinal) == -1)
                        {
                            throw;
                        }
                    }
                }

                var props = context.JournalPropsEntities.FirstOrDefault(x => x.JournalId == journalId);

                if (props == null)
                {
                    var initialTicks = DateTime.UtcNow.Ticks;

                    await context.JournalPropsEntities
                        .AddAsync(
                            new JournalPropsEntity(
                                journalId,
                                0,
                                initialTicks));

                    await context.SaveChangesAsync().ConfigureAwait(false);

                    _props = new JournalProps(0, initialTicks);
                }
                else
                {
                    _props = new JournalProps(props.HighestSequenceNumber, props.InitialUtcTicks);
                }

                return true;
            }
            // TODO: Revisit an error occurring here
            catch (Exception ex)
                //catch
            {
                if (DateTime.UtcNow >= _initializationTimeLimit)
                {
                    throw;
                }

                await Task.Delay(delayMilliseconds).ConfigureAwait(false);

                return await Initialize(delayMilliseconds * 2).ConfigureAwait(false);
            }
        }

        private async Task<IJournalEntry[]> ReadBySequence(
            long from,
            CancellationToken cancellationToken,
            long? to)
        {
            await using var context = new JournalContext(_settings.DbContextOptions, _settings.JournalId.ToString());

            var journalId = _settings.JournalId.ToString();

            var results =
                await context.TableEntities
                    .Where(
                        x =>
                            x.JournalId == journalId &&
                            x.Sequence > from &&
                            x.Sequence <= (to ?? _props.HighestSequenceNumber))
                    .ToListAsync(cancellationToken)
                    .ConfigureAwait(false);

            return GetResults(results);
        }

        private async Task<IJournalEntry[]> ReadByUtcTicks(
            long from,
            CancellationToken cancellationToken,
            long? to)
        {
            await using var context = new JournalContext(_settings.DbContextOptions, _settings.JournalId.ToString());

            var toUtcTicks = to ?? DateTime.UtcNow.Ticks;

            var journalId = _settings.JournalId.ToString();

            var results =
                await context.TableEntities
                    .Where(
                        x =>
                            x.JournalId == journalId &&
                            x.UtcTicks > from &&
                            x.UtcTicks <= toUtcTicks)
                    .ToListAsync(cancellationToken)
                    .ConfigureAwait(false);

            return GetResults(results);
        }

        private async Task<IJournalEntry[]> ReadWithTagsBySequence(
            long from,
            CancellationToken cancellationToken,
            long? to = null,
            params string[] tags)
        {
            await using var context = new JournalContext(_settings.DbContextOptions, _settings.JournalId.ToString());

            var journalId = _settings.JournalId.ToString();

            var formattedTags =
                tags
                    .Select(x => $"{Delimiters.Delimiter}{x.ToLowerInvariant()}{Delimiters.Delimiter}")
                    .ToArray();

            IQueryable<TableEntity> queryable = null;

            foreach (var tag in formattedTags)
            {
                var currentQuery =
                    context.TableEntities
                        .Where(
                            x =>
                                x.JournalId == journalId &&
                                x.Sequence > from &&
                                x.Sequence <= (to ?? _props.HighestSequenceNumber) &&
                                JournalContext.CharIndex(tag, x.Tags, 0) > 0);

                queryable =
                    queryable == null
                        ? currentQuery
                        : queryable.Concat(currentQuery);
            }

            var results = await queryable.ToListAsync(cancellationToken).ConfigureAwait(false);

            return GetResults(results);
        }

        private async Task<IJournalEntry[]> ReadWithTagsByUtcTicks(
            long from,
            CancellationToken cancellationToken,
            long? to = null,
            params string[] tags)
        {
            await using var context = new JournalContext(_settings.DbContextOptions, _settings.JournalId.ToString());

            var journalId = _settings.JournalId.ToString();

            var formattedTags =
                tags
                    .Select(x => $"{Delimiters.Delimiter}{x.ToLowerInvariant()}{Delimiters.Delimiter}")
                    .ToArray();

            var toUtcTicks = to ?? DateTime.UtcNow.Ticks;

            IQueryable<TableEntity> queryable = null;

            foreach (var tag in formattedTags)
            {
                var currentQuery =
                    context.TableEntities
                        .Where(
                            x =>
                                x.JournalId == journalId &&
                                x.UtcTicks > from &&
                                x.UtcTicks <= toUtcTicks &&
                                JournalContext.CharIndex(tag, x.Tags, 0) > 0);

                queryable =
                    queryable == null
                        ? currentQuery
                        : queryable.Concat(currentQuery);
            }

            var results = await queryable.ToListAsync(cancellationToken).ConfigureAwait(false);

            return GetResults(results);
        }

        private async Task<IJournalEntry[]> WriteWithRetry(
            CancellationToken cancellationToken,
            ISerializedEntry[] entries,
            int retryCount)
        {
            var journalId = _settings.JournalId.ToString();

            var inserted = new List<TableEntity>();

            var updated = new List<TableEntity>();

            try
            {
                foreach (var entry in entries)
                {
                    TableEntity item;

                    switch (entry)
                    {
                        case IKnownImmutableEntry uniqueEntry:
                            item = new ConsistentEntity(
                                journalId,
                                uniqueEntry.EntryId,
                                -1,
                                (byte[]) uniqueEntry.Payload,
                                (byte[]) uniqueEntry.Meta,
                                entry.Tags);

                            inserted.Add(item);

                            break;

                        case IKnownMutableEntry consistentEntry:
                            item =
                                new ConsistentEntity(
                                    journalId,
                                    entry.EntryId,
                                    -1,
                                    (byte[]) entry.Payload,
                                    (byte[]) entry.Meta,
                                    entry.Tags);

                            if (consistentEntry.Version == 0)
                            {
                                inserted.Add(item);
                            }
                            else
                            {
                                updated.Add(item);
                            }

                            break;

                        default:
                            item = new TableEntity(
                                journalId,
                                entry.EntryId,
                                _props.IncrementAndReturnHighestSequenceNumber(),
                                (byte[]) entry.Payload,
                                (byte[]) entry.Meta,
                                entry.Tags);

                            inserted.Add(item);

                            break;
                    }
                }

                await using var context = new JournalContext(_settings.DbContextOptions, journalId);

                await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

                await context.TableEntities.AddRangeAsync(inserted, cancellationToken);

                context.TableEntities.UpdateRange(updated);

                context.JournalPropsEntities.Update(
                    new JournalPropsEntity(
                        journalId,
                        _props.HighestSequenceNumber,
                        _props.InitialUtcTicks));

                await context.SaveChangesAsync(cancellationToken).ConfigureAwait(false);

                await transaction.CommitAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, $"Unable to persist entries, retry count is {retryCount}.");

                //if (retryCount < _settings.WriterRetryLimit)
                //{
                //    ResetProps();

                //    await Task.Delay((retryCount + 1) * 100, cancellationToken);

                //    return await WriteWithRetry(cancellationToken, entries, retryCount + 1).ConfigureAwait(false);
                //}

                throw;
            }

            return GetResults(inserted.Concat(updated));
        }
    }
}