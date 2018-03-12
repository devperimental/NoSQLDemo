using System;
using System.Collections.Generic;

namespace CommonTypes.Messages
{
    public class GameState
    {
        public string RecordId { get; set; }
        public string PlayerId { get; set; }
        public int Health { get; set; }
        public int CurrentLevel { get; set; }
        public Dictionary<string, string> Inventory { get; set; }
        public string GameId { get; set; }
        public DateTime RecordCreatedAt { get; set; }
        public string PlatformKey { get; set; }
        public string PlatformType { get; set; }
    }
}
