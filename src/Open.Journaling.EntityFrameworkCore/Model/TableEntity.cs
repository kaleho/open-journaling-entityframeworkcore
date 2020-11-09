using System;
using System.Linq;

namespace Open.Journaling.EntityFrameworkCore.Model
{
    public class TableEntity
    {
        public TableEntity(
            string journalId,
            string entryId,
            long sequence,
            byte[] payload,
            byte[] meta,
            params string[] tags)
        {
            JournalId = journalId.ToLowerInvariant();
            EntryId = entryId.ToLowerInvariant();
            Sequence = sequence;
            Payload = payload;
            Meta = meta;
            Tags = ToTagsString(tags);
            UtcTicks = DateTime.UtcNow.Ticks;

            RowKey = $"{EntityType.Entry}{Delimiters.Delimiter}{sequence:D19}";
        }

        protected TableEntity()
        {
        }

        public string EntryId { get; set; }

        public string JournalId { get; set; }

        public byte[] Meta { get; set; }

        public byte[] Payload { get; set; }

        public string RowKey { get; set; }

        public long Sequence { get; set; }

        public string Tags { get; set; }

        public long UtcTicks { get; set; }

        public static string ToTagsString(
            string[] tags)
        {
            return
                tags.Any()
                    ? Delimiters.Delimiter +
                      string.Join(Delimiters.Delimiter, tags.Select(x => x.ToLowerInvariant())) +
                      Delimiters.Delimiter
                    : "";
        }
    }
}