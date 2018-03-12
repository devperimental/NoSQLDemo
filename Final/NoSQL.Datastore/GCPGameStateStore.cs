using CommonTypes.Behaviours;
using CommonTypes.Messages;
using CommonTypes.Settings;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Datastore.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Auth;
using Grpc.Core;
using Newtonsoft.Json;
using NoSQL.Behaviours;
using Polly;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace NoSQL.Datastore
{
    public class GCPGameStateStore : IDataStore<GameState>
    {
        private GCPSettings _gcpSettings;
        private IAppLogger _appLogger;

        private Policy _retryPolicy;
        private readonly DatastoreDb _db;
        private readonly KeyFactory _keyFactory;
        private string _tableName = "GameState";

        private readonly DateTime baseDate = new DateTime(1970, 01, 01);
        
        public GCPGameStateStore(IAppLogger appLogger, GCPSettings gcpSettings)
        {
            _gcpSettings = gcpSettings;
            _appLogger = appLogger;

            _retryPolicy = Policy
                        .Handle<Exception>()
                        .WaitAndRetryAsync(3,
                   retryAttempt => TimeSpan.FromMilliseconds(200),
                       (exception, timeSpan, retryCount, context) =>
                       {
                           var msg = $"GCPGameStateStore - Count:{retryCount}, Exception:{exception.Message}";
                           _appLogger.LogWarning(msg);
                       });

            var datastoreClient = InitDatastoreClient();

            _db = DatastoreDb.Create(_gcpSettings.NoSQL.ProjectId, client: datastoreClient);

            _keyFactory = _db.CreateKeyFactory(_tableName);
        }

        private DatastoreClient InitDatastoreClient()
        {
            GoogleCredential googleCredential;
            var scopes = new string[] { "https://www.googleapis.com/auth/datastore" };

            using (var stream = new FileStream(_gcpSettings.NoSQL.JsonAuthPath, FileMode.Open, FileAccess.Read))
            {
                googleCredential = GoogleCredential.FromStream(stream).CreateScoped(scopes);
            }

            var channel = new Channel(DatastoreClient.DefaultEndpoint.ToString(), googleCredential.ToChannelCredentials());
            return DatastoreClient.Create(channel);
        }

        #region Object Mapping

        private Entity ConvertToEntity(GameState item)
        {
            return new Entity()
            {
                Key = _keyFactory.CreateIncompleteKey(),
                ["RecordId"] = item.RecordId,
                ["PlayerId"] = item.PlayerId,
                ["CurrentLevel"] = item.CurrentLevel,
                ["Health"] = item.Health,
                ["Inventory"] = JsonConvert.SerializeObject(item.Inventory, Formatting.None),
                ["GameId"] = item.GameId,
                ["RecordCreatedAt"] = new Timestamp
                {
                    Seconds = Convert.ToInt64(item.RecordCreatedAt.Subtract(baseDate).TotalSeconds)
                }
            };
        }

        private GameState EntityToPOCO(Entity entity)
        {
            var t = new Timestamp()
            {
                // Return as microseconds so need to convert
                Seconds = entity["RecordCreatedAt"].IntegerValue / 1000000
            };

            return new GameState()
            {
                PlatformKey = entity.Key.Path[0].Id.ToString(),
                RecordId = entity["RecordId"] != null ? entity["RecordId"].StringValue : string.Empty,
                PlayerId = entity["PlayerId"] != null ? entity["PlayerId"].StringValue : string.Empty,
                CurrentLevel = Convert.ToInt32(entity["CurrentLevel"].IntegerValue),
                Health = Convert.ToInt32(entity["Health"].IntegerValue),
                Inventory = JsonConvert.DeserializeObject<Dictionary<string, string>>(entity["Inventory"].StringValue),
                GameId = entity["GameId"].StringValue,
                RecordCreatedAt = t.ToDateTime()
            };
        }

        #endregion

        #region Interface Methods

        public async Task<GameState> AddEntity(GameState item)
        {
            try
            {
                await _retryPolicy.ExecuteAsync(async () => {
                    var key = await AddGameState(item);
                    item.PlatformKey = key.Path[0].Id.ToString();
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
            Entity entity = null;
            await _retryPolicy.ExecuteAsync(async () => {
                entity = await GetGameState(item);
            });

            item = EntityToPOCO(entity);

            return item;
        }

        public async Task<(List<GameState> list, string nextPageState)> QueryEntity(ICriteria criteria)
        {
            IEnumerable<Entity> entityList = null;
            string nextPageToken = null;
            var list = new List<GameState>();

            await _retryPolicy.ExecuteAsync(async () => {

                (entityList, nextPageToken) = await QueryGameState(criteria);

                foreach (var entity in entityList)
                {
                    list.Add(EntityToPOCO(entity));
                }
            });

            return (list, nextPageToken);
        }

        #endregion

        private async Task<Key> AddGameState(GameState item)
        {
            var entity = ConvertToEntity(item);

            return await _db.InsertAsync(entity);
        }

        private async Task<bool> UpdateGameState(GameState item)
        {
            using (var transaction = _db.BeginTransaction())
            {
                Entity entity = _db.Lookup(_keyFactory.CreateKey(Convert.ToInt64(item.PlatformKey)));
                if (entity != null)
                {
                    entity["CurrentLevel"] = item.CurrentLevel;
                    entity["Health"] = item.Health;
                    entity["Inventory"] = JsonConvert.SerializeObject(item.Inventory, Formatting.None);

                    transaction.Update(entity);
                }
                await transaction.CommitAsync();
                return entity != null;
            }
        }

        private async Task<Entity> GetGameState(GameState item)
        {
            var key = _keyFactory.CreateKey(Convert.ToInt64(item.PlatformKey));
            var result = await _db.LookupAsync(key);

            return result;
        }

        private async Task<(IEnumerable<Entity> Entities, string NextPageToken)> QueryGameState(ICriteria searchCriteria)
        {
            // Create our query
            var query = new Query(_tableName)
            {
                Limit = searchCriteria.PageSize,
                Filter = Filter.And(Filter.Equal("PlayerId", searchCriteria.SearchFields["PlayerId"]),
                                        Filter.GreaterThanOrEqual("CurrentLevel", Convert.ToInt32(searchCriteria.SearchFields["CurrentLevel"]))),
                Projection = { "RecordId", "CurrentLevel", "Health", "Inventory", "GameId", "RecordCreatedAt" },
                Order = { { "CurrentLevel", PropertyOrder.Types.Direction.Descending },
                    { "RecordCreatedAt", PropertyOrder.Types.Direction.Descending } }
            };

            // Use NextPageState if specified
            if (!string.IsNullOrWhiteSpace(searchCriteria.NextPageState))
            {
                query.StartCursor = ByteString.FromBase64(searchCriteria.NextPageState);
            }

            // Run the query
            var results = await _db.RunQueryAsync(query);

            // Check for next page token
            var nextPageToken = results.Entities.Count == searchCriteria.PageSize ? results.EndCursor.ToBase64() : null;

            // Return the tuple
            return (results.Entities, nextPageToken);
        }

        private async Task<bool> DeleteGameState(GameState item)
        {
            await _db.DeleteAsync(_keyFactory.CreateKey(item.PlatformKey));

            return true;
        }
    }
}
