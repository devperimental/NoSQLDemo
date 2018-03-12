using System;
using System.Collections.Generic;
using System.Text;

namespace NoSQL.Behaviours
{
    public interface ICriteria
    {
        Dictionary<string, string> SearchFields { get; set; }
        int PageSize { get; set; }
        string NextPageState { get; set; }
    }
}
