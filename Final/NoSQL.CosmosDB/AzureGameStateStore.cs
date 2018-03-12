using CommonTypes.Behaviours;
using CommonTypes.Messages;
using CommonTypes.Settings;
using MongoDB.Bson;
using MongoDB.Driver;
using NoSQL.Behaviours;
using NoSQL.CosmosDB.Types;
using Polly;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NoSQL.CosmosDB
{
    public class AzureGameStateStore : IDataStore<GameState>
    {
        private AzureSettings _azureSettings;
        private IAppLogger _appLogger;

        private Policy _retryPolicy;
        private string _tableName = "GameState";

        private IMongoClient _client;
        private IMongoDatabase _database;
        private IMongoCollection<GameStateMongo> _gameStateCollection;

        public AzureGameStateStore(IAppLogger appLogger, AzureSettings azureSettings)
        {
            _appLogger = appLogger;
            _azureSettings = azureSettings;

            _retryPolicy = Policy
                        .Handle<Exception>()
                        .WaitAndRetryAsync(3,
                           retryAttempt => TimeSpan.FromMilliseconds(200),
                       (exception, timeSpan, retryCount, context) =>
                       {
                           var msg = $"AzureGameStateStore - Count:{retryCount}, Exception:{exception.Message}";
                           _appLogger.LogWarning(msg);
                       });

            _client = new MongoClient(_azureSettings.NoSQL.ConnectionString);
            _database = _client.GetDatabase(_azureSettings.NoSQL.DatabaseName);
            _gameStateCollection = _database.GetCollection<GameStateMongo>(_tableName);
        }

        #region Object Mapping

        private GameStateMongo ConvertToMongoType(GameState item)
        {
            return new GameStateMongo()
            {
                RecordId = item.RecordId,
                PlayerId = item.PlayerId,
                Health = item.Health,
                CurrentLevel = item.CurrentLevel,
                Inventory = item.Inventory,
                GameId = item.GameId,
                RecordCreatedAt = item.RecordCreatedAt
            };
        }

        private GameState MongoToPOCO(GameStateMongo item)
        {
            return new GameState()
            {
                RecordId = item.RecordId,
                PlayerId = item.PlayerId,
                Health = item.Health,
                CurrentLevel = item.CurrentLevel,
                Inventory = item.Inventory,
                GameId = item.GameId,
                RecordCreatedAt = item.RecordCreatedAt,

                // Add the platform row identifier to mapped entity
                PlatformKey = item.Id.ToString(),
                PlatformType = "COSMOS-MONGO"
            };

        }

        #endregion

        #region Interface Implementation
        public async Task<GameState> AddEntity(GameState item)
        {
            try
            {
                await _retryPolicy.ExecuteAsync(async () => {
                    await AddGameState(item);
                });
            }
            catch (Exception ex)
            {
                _appLogger?.LogError(ex);
                throw;
            }

            return item;
        }

        public async Task<bool> UpdateEntity(GameState item)
        {
            var outcome = false;
            try
            {
                await _retryPolicy.ExecuteAsync(async () => {
                    outcome = await UpdateGameState(item);
                });
            }
            catch (Exception ex)
            {
                _appLogger?.LogError(ex);
                throw;
            }

            return outcome;
        }

        public async Task<bool> DeleteEntity(GameState item)
        {
            var outcome = false;
            try
            {
                await _retryPolicy.ExecuteAsync(async () => {
                    outcome = await DeleteGameState(item);
                });
            }
            catch (Exception ex)
            {
                _appLogger?.LogError(ex);
                throw;
            }

            return outcome;
        }

        public async Task<GameState> GetEntity(GameState item)
        {
            var list = await GetGameState(item);

            if (list != null)
            {
                item = MongoToPOCO(list[0]);
            }

            return item;
        }

        public async Task<(List<GameState> list, string nextPageState)> QueryEntity(ICriteria searchCriteria)
        {
            var items = new List<GameState>();
            var (list, nextPageState) = await QueryGameState(searchCriteria);

            if (list != null)
            {
                items = list.ConvertAll(MongoToPOCO);
            }

            return (items, nextPageState);
        }

        #endregion

        private async Task AddGameState(GameState item)
        {
            var mongoItem = ConvertToMongoType(item);

            await _gameStateCollection.InsertOneAsync(mongoItem);

            item.PlatformKey = mongoItem.Id.ToString();
        }
        
        private async Task<bool> UpdateGameState(GameState item)
        {
            var filter = Builders<GameStateMongo>.Filter.Eq("PlayerId", item.PlayerId);
            filter = filter & (Builders<GameStateMongo>.Filter.Eq("RecordCreatedAt", item.RecordCreatedAt));

            var update = Builders<GameStateMongo>.Update.Set("Health", item.Health)
                .Set("CurrentLevel", item.CurrentLevel)
                .Set("Inventory", item.Inventory);

            var result = await _gameStateCollection.UpdateOneAsync(filter, update);

            return result.ModifiedCount != 0;
        }

        private async Task<bool> DeleteGameState(GameState item)
        {
            ObjectId id = new ObjectId(item.PlatformKey);

            var filter = Builders<GameStateMongo>.Filter.Eq("PlayerId", item.PlayerId);
            filter = filter & (Builders<GameStateMongo>.Filter.Eq("RecordCreatedAt", item.RecordCreatedAt));

            var result = await _gameStateCollection.DeleteOneAsync(filter);
            return result.DeletedCount != 0;
        }

        private async Task<List<GameStateMongo>> GetGameState(GameState item)
        {
            var filter = Builders<GameStateMongo>.Filter.Eq("PlayerId", item.PlayerId);
            filter = filter & (Builders<GameStateMongo>.Filter.Eq("RecordCreatedAt", item.RecordCreatedAt));

            return await _gameStateCollection.Find(filter).ToListAsync();
        }

        private async Task<(List<GameStateMongo> Items, string NextPageState)> QueryGameState(ICriteria searchCriteria)
        {
            var filter = Builders<GameStateMongo>.Filter.Eq("PlayerId", searchCriteria.SearchFields["PlayerId"]);
            filter = filter & (Builders<GameStateMongo>.Filter.Gte("CurrentLevel", searchCriteria.SearchFields["CurrentLevel"]));

            var skipCount = !string.IsNullOrEmpty(searchCriteria.NextPageState) ? Convert.ToInt32(searchCriteria.NextPageState) : 0;

            var items = await _gameStateCollection.Find(filter)
                                               .Skip(skipCount)
                                               .Limit(searchCriteria.PageSize)
                                               .ToListAsync();

            var nextPageState = items.Count == searchCriteria.PageSize ? (skipCount + searchCriteria.PageSize).ToString() : string.Empty;

            return (items, nextPageState); 
        }
    }
}
