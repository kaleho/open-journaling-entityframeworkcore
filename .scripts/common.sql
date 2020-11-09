
SELECT * FROM [Journal_DEV].[dbo].[journal] order by journalid, rowkey
SELECT * FROM [Journal_DEV].[dbo].[journal-props] ORDER BY JournalId


/*
SELECT * FROM [Journal_DEV].[dbo].[journal] where journalid = 'app~runners~http_listener~journal'
SELECT * FROM [Journal_DEV].[dbo].[journal-props] where journalid = 'app~runners~http_listener~journal'

declare @journalId varchar(max) = 'app~procs~tenantssupervisor~journal'
SELECT * FROM [Journal_DEV].[dbo].[journal] where journalid = @journalId
SELECT * FROM [Journal_DEV].[dbo].[journal-props] where journalid = @journalId
*/



/*
TRUNCATE TABLE [Journal_DEV].[dbo].[journal]
TRUNCATE TABLE [Journal_DEV].[dbo].[journal-props]
*/

/*
DROP TABLE [dbo].[journal]
DROP TABLE [dbo].[journal-props]
GO
*/

/*
SELECT COUNT(*) as EntryCount FROM journal WHERE Sequence > -1
SELECT COUNT(*) as SnapshotCount FROM journal WHERE Sequence = -1
*/

/*
SELECT * FROM journal WHERE Sequence > -1
SELECT * FROM journal WHERE Sequence = -1
*/

/*
SELECT 
    EntryId,
    DATALENGTH(CONVERT(VARBINARY(max), JournalId)) as JournalIdByteCount,
    DATALENGTH(CONVERT(VARBINARY(max), RowKey)) as RowKeyByteCount,
    DATALENGTH(Meta) as MetaByteCount,
    DATALENGTH(Payload) as PayloadByteCount,
    DATALENGTH(CONVERT(VARBINARY(max), Tags)) as TagByteCount
FROM [Journal_DEV].[dbo].[journal]
--WHERE Sequence = -1
ORDER BY DATALENGTH(Payload) DESC
*/

/*
SELECT * FROM [Journal_DEV].[dbo].[journal] where entryid like 'tenantnotcreated-e7b572ca0ea44e%'

SELECT * FROM [Journal_DEV].[dbo].[journal] where journalid = 'idealhomes-user-dev_at_xtch_dot_in-journal'
SELECT * FROM [Journal_DEV].[dbo].[journal-props] where journalid = 'idealhomes-user-dev_at_xtch_dot_in-journal'
*/

