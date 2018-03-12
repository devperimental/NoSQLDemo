using System;
using System.Collections.Generic;
using System.Text;

namespace CommonTypes.Settings
{
    public class AWSSettings
    {
        public NoSQLSettings NoSQL { get; set; }

        public class NoSQLSettings
        {
            public string AccessKey { get; set; }
            public string SecretKey { get; set; }
            public string Region { get; set; }
        }
    }
}