﻿namespace Open.Journaling.EntityFrameworkCore.Model
{
    public sealed class Delimiters
    {
        public const char Delimiter = '(';
        public const char AsciiIncrementedDelimiter = ')'; //(char)((byte)Delimiter + 1)
    }
}