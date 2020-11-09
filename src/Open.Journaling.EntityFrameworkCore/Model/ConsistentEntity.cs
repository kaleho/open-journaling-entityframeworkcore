namespace Open.Journaling.EntityFrameworkCore.Model
{
    public class ConsistentEntity
        : TableEntity
    {
        public ConsistentEntity(
            string journalId,
            string entryId,
            long sequence,
            byte[] payload,
            byte[] meta,
            params string[] tags)
            : base(
                journalId,
                entryId,
                sequence,
                payload,
                meta,
                tags)
        {
            RowKey = $"{EntityType.Consistent}{Delimiters.Delimiter}{entryId}";
        }

        private ConsistentEntity()
        {
        }
    }
}