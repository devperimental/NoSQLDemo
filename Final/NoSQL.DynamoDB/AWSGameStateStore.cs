using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using CommonTypes.Behaviours;
using CommonTypes.Messages;
using CommonTypes.Settings;
using Newtonsoft.Json;
using NoSQL.Behaviours;
using Polly;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NoSQL.DynamoDB
{
    public class AWSGameStateStore : IDataStore<GameState>
    {
        private AWSSettings _awsSettings;
        private IAppLogger _appLogger;

        private Policy _retryPolicy;
        private const string _tableName = "GameState";

        public AWSGameStateStore(IAppLogger appLogger, AWSSettings awsSettings)
        {
            _awsSettings = awsSettings;
            _appLogger = appLogger;

            _retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timeSpan, retryCount, context) =>
                    {
                        var msg = $"AWSGameStateStore Retry - Count:{retryCount}, Exception:{exception.Message}";
                        _appLogger?.LogWarning(msg);
                    }
                );
        }

        private AmazonDynamoDBClient GetDynamoClient()
        {
            var credentials = new BasicAWSCredentials(
                accessKey: _awsSettings.NoSQL.AccessKey,
                secretKey: _awsSettings.NoSQL.SecretKey);

            var config = new AmazonDynamoDBConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(_awsSettings.NoSQL.Region)
            };

            return new AmazonDynamoDBClient(credentials, config);
        }

        #region Object Mapping

        private Dictionary<string, AttributeValue> ConvertToDynamoDictionary(GameState item)
        {
            return new Dictionary<string, AttributeValue>
            {
                {"RecordId", new AttributeValue {S = item.RecordId}},
                {"PlayerId", new AttributeValue {S = item.PlayerId}},
                {"CurrentLevel", new AttributeValue {N = item.CurrentLevel.ToString()}},
                {"Health", new AttributeValue {N = item.Health.ToString()}},
                {"Inventory", new AttributeValue {M = item.Inventory.ToDictionary(x => x.Key, x => new AttributeValue { S = x.Value })}},
                {"GameId", new AttributeValue {S = item.GameId}},
                {"RecordCreatedAt", new AttributeValue {S = item.RecordCreatedAt.ToString()}}
            };
        }

        private GameState DynamoToPOCO(Dictionary<string, AttributeValue> item)
        {

            return new GameState
            {
                RecordId = item["RecordId"].S,
                PlayerId = item["PlayerId"].S,
                CurrentLevel = Convert.ToInt32(item["CurrentLevel"].N),
                Health = Convert.ToInt32(item["Health"].N),
                Inventory = item["Inventory"].M.ToDictionary(x => x.Key, x => x.Value.S),
                GameId = item["GameId"].S,
                RecordCreatedAt = DateTime.Parse(item["RecordCreatedAt"].S)
            };
        }

        #endregion

        #region Interface Implementation
        public async Task<GameState> AddEntity(GameState item)
        {
            try
            {
                await _retryPolicy.ExecuteAsync(async () => {
                    item = await AddGameState(item);
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
            bool outcome = false;
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
            bool outcome = false;
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
            var itemAttributes = await GetGameState(item);

            if (itemAttributes != null)
            {
                item = DynamoToPOCO(itemAttributes);
            }

            return item;
        }

        public async Task<(List<GameState> list, string nextPageState)> QueryEntity(ICriteria searchCriteria)
        {
            var list = new List<GameState>();

            var (collection, nextPageState) = await QueryGameState(searchCriteria);

            foreach (var dict in collection)
            {
                list.Add(DynamoToPOCO(dict));
            }

            return (list, nextPageState);
        }
        #endregion

        private async Task<GameState> AddGameState(GameState item)
        {
            var client = GetDynamoClient();

            var response = await client.PutItemAsync(
                tableName: _tableName,
                item: ConvertToDynamoDictionary(item)
            );

            return item;
        }

        private async Task<bool> UpdateGameState(GameState item)
        {
            var client = GetDynamoClient();

            var response = await client.UpdateItemAsync(
                tableName: _tableName,
                key: new Dictionary<string, AttributeValue>
                {
                    {"PlayerId", new AttributeValue { S = item.PlayerId.ToString()}},
                    {"RecordCreatedAt", new AttributeValue { S = item.RecordCreatedAt.ToString()}},
                },
                attributeUpdates: new Dictionary<string, AttributeValueUpdate>
                {
                    {"CurrentLevel", new AttributeValueUpdate {Value = new AttributeValue {N = item.CurrentLevel.ToString()} } },
                    {"Health", new AttributeValueUpdate {Value = new AttributeValue {N = item.Health.ToString()} } },
                    {"Inventory", new AttributeValueUpdate {Value = new AttributeValue {M = item.Inventory.ToDictionary(x => x.Key, x => new AttributeValue { S = x.Value })} } }
                }
            );

            return true;
        }

        private async Task<bool> DeleteGameState(GameState item)
        {
            var client = GetDynamoClient();

            var response = await client.DeleteItemAsync(
                tableName: _tableName,
                key: new Dictionary<string, AttributeValue>
                {
                    {"PlayerId", new AttributeValue { S = item.PlayerId.ToString()}},
                    {"RecordCreatedAt", new AttributeValue { S = item.RecordCreatedAt.ToString()}},
                }
            );

            return true;
        }

        private async Task<Dictionary<string, AttributeValue>> GetGameState(GameState item)
        {
            var client = GetDynamoClient();

            var response = await client.GetItemAsync(
                tableName: _tableName,
                key: new Dictionary<string, AttributeValue>
                {
                    {"PlayerId", new AttributeValue { S = item.PlayerId}},
                    {"RecordCreatedAt", new AttributeValue { S = item.RecordCreatedAt.ToString() }}
                }
            );

            return response.Item;
        }

        private async Task<(List<Dictionary<string, AttributeValue>> Items, string NextPageState)> QueryGameState(ICriteria searchCriteria)
        {
            // Create our query
            var queryRequest = new QueryRequest
            {
                Limit = searchCriteria.PageSize,
                TableName = _tableName,
                FilterExpression = "CurrentLevel >= :v_CurrentLevel",
                KeyConditionExpression = "PlayerId = :v_PlayerId",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":v_PlayerId", new AttributeValue { S =  searchCriteria.SearchFields["PlayerId"] }},
                    {":v_CurrentLevel", new AttributeValue { N = searchCriteria.SearchFields["CurrentLevel"] }}
                },
                ProjectionExpression = "RecordId, PlayerId, CurrentLevel, Health, Inventory, GameId, RecordCreatedAt",
                ConsistentRead = true
            };

            // Use NextPageState if specified
            if (!string.IsNullOrEmpty(searchCriteria.NextPageState))
            {
                queryRequest.ExclusiveStartKey = JsonConvert.DeserializeObject<Dictionary<string, AttributeValue>>(searchCriteria.NextPageState);
            }

            // Run the query
            var client = GetDynamoClient();
            var response = await client.QueryAsync(queryRequest);

            // Check for next page token
            var nextPageState = response.Items.Count == searchCriteria.PageSize ? JsonConvert.SerializeObject(response.LastEvaluatedKey) : null;

            // Return the tuple
            return (response.Items, nextPageState);
        }
    }
}
