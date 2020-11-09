using Open.Journaling.Traits;
using System;

namespace Open.Journaling.EntityFrameworkCore.Journals
{
    public class EntityFrameworkCoreTrait
        : IJournalTrait
    {
        public EntityFrameworkCoreTrait(
            TriState value)
        {
            Value = value;
        }

        public TriState Value { get; }
    }
}