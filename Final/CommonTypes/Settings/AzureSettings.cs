using System;
using System.Collections.Generic;
using System.Text;

namespace CommonTypes.Settings
{
    public class AzureSettings
    {
        public NoSQLSettings NoSQL { get; set; }

        public class NoSQLSettings
        {
            public string ConnectionString { get; set; }
            public string DatabaseName { get; set; }
            public string Region { get; set; }
        }
    }
}
