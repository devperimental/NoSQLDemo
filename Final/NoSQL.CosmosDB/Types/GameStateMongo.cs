using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace NoSQL.CosmosDB.Types
{
    public class GameStateMongo
    {
        [BsonId]
        public ObjectId Id { get; set; }
        [BsonElement]
        public string RecordId { get; set; }
        [BsonElement]
        public string PlayerId { get; set; }
        [BsonElement]
        public int Health { get; set; }
        [BsonElement]
        public int CurrentLevel { get; set; }
        [BsonElement]
        public Dictionary<string, string> Inventory { get; set; }
        [BsonElement]
        public string GameId { get; set; }
        [BsonElement]
        public DateTime RecordCreatedAt { get; set; }

    }
}
