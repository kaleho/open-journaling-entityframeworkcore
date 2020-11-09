using System;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Open.Journaling.EntityFrameworkCore.Cli
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
        }

        private IMutableModel BuildModel()
        {
            IMutableModel returnValue = null;

            //var modelBuilder = new SqlServerConventionSetBuilder();



            return returnValue;
        }
    }
}
