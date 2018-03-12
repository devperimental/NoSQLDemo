using System;
using System.Collections.Generic;
using System.Text;

namespace CommonTypes.Settings
{
    public class GCPSettings
    {
        public NoSQLSettings NoSQL { get; set; }

        public class NoSQLSettings
        {
            public string ProjectId { get; set; }
            public string JsonAuthPath { get; set; }
        }
    }

}
