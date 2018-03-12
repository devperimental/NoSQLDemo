using NoSQL.Behaviours;
using System;
using System.Collections.Generic;
using System.Text;

namespace CommonTypes
{
    public class SearchCriteria : ICriteria
    {
        public Dictionary<string, string> SearchFields { get; set ; }
        public int PageSize { get; set; }
        public string NextPageState { get; set; }
    }
}
