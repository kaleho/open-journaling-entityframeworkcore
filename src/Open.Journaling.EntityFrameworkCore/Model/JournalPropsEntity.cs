namespace Open.Journaling.EntityFrameworkCore.Model
{
    public class JournalPropsEntity
    {
        public JournalPropsEntity(
            string journalId,
            long highestSequenceNumber,
            long initialUtcTicks)
        {
            JournalId = journalId;
            HighestSequenceNumber = highestSequenceNumber;
            InitialUtcTicks = initialUtcTicks;
        }

        private JournalPropsEntity()
        {
        }

        public long HighestSequenceNumber { get; set; }

        public long InitialUtcTicks { get; set; }

        public string JournalId { get; set; }
    }
}