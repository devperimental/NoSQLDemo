using CommonTypes;
using CommonTypes.Behaviours;
using CommonTypes.Messages;
using CommonTypes.Settings;
using Microsoft.Extensions.Configuration;
using NoSQL.Behaviours;
using NoSQL.CosmosDB;
using NoSQL.Datastore;
using NoSQL.DynamoDB;
using System;
using System.Collections.Generic;
using System.IO;

namespace TestHarness
{
    class Program
    {
        private static AWSSettings _awsSettings;
        private static AzureSettings _azureSettings;
        private static GCPSettings _gcpSettings;
        private static IAppLogger _consoleLogger;

        static void Main(string[] args)
        {
            _consoleLogger = new ConsoleLogger();

            _consoleLogger.LogMessage("Starting Test Harness!");

            try
            {
                InitConfiguration();
                TestPersistenceFunctionality();
            }
            catch (Exception ex)
            {
                _consoleLogger.LogError(ex);
            }
            finally
            {
                _consoleLogger.LogMessage("End Test Harness!");
            }
            Console.ReadLine();
        }

        static void InitConfiguration()
        {
            _consoleLogger.LogMessage("Start Init Config");

            // Used to build key/value based configuration settings for use in an application
            // Note: AddJsonFile is an extension methods for adding JsonConfigurationProvider.
            var builder = new ConfigurationBuilder()
           .SetBasePath(Directory.GetCurrentDirectory())
           .AddJsonFile("appSettings.json");

            // Builds an IConfiguration with keys and values from the set of sources
            var configuration = builder.Build();

            // Bind the respective section to the respective settings class 
            _awsSettings = configuration.GetSection("aws").Get<AWSSettings>();
            _azureSettings = configuration.GetSection("azure").Get<AzureSettings>();
            _gcpSettings = configuration.GetSection("gcp").Get<GCPSettings>();

            _consoleLogger.LogMessage("End Init Config");
        }

        static void TestPersistenceFunctionality()
        {
            _consoleLogger.LogMessage("Start TestPersistenceFunctionality");

            var dataStores = new List<IDataStore<GameState>>
            {
                new AWSGameStateStore(_consoleLogger, _awsSettings)
            };

            try
            {
                dataStores.ForEach(c => { TestPersistenceOperations(c); });
            }
            catch (Exception ex)
            {
                _consoleLogger.LogError(ex);
            }

            _consoleLogger.LogMessage("End TestPersistenceFunctionality");

        }

        static void TestPersistenceOperations(IDataStore<GameState> dataStore)
        {
            _consoleLogger.LogMessage($"Calling operations for {dataStore.GetType()}");

            var playerIds = new List<string>();
            var gameIds = new List<string>();
            
            for (var i=0; i<5; i++)
            {
                playerIds.Add(Guid.NewGuid().ToString());
                gameIds.Add(Guid.NewGuid().ToString());
            }

            var rnd = new Random();

            // Lets create 50 records for one of the game ids
            for (var j = 20; j > 0; j--)
            {
                var gameState = new GameState()
                {
                    RecordId = Guid.NewGuid().ToString(),
                    PlayerId = playerIds[j%5],
                    Health = rnd.Next(1, 100),
                    CurrentLevel = rnd.Next(1, 10),
                    Inventory = new Dictionary<string, string>() { { "Item", "Amulet" }, { "Weapon", "Ion Blaster" } },
                    GameId = gameIds[0],
                    RecordCreatedAt = DateTime.Now.AddDays(-j).ToUniversalTime()
                };

                gameState = dataStore.AddEntity(gameState).Result;
            }

            _consoleLogger.LogMessage($"Test Data Created");

            // Lets retrieve the records for the first player id above level 3 and above
            var searchCriteria = new SearchCriteria
            {
                PageSize = 20,
                NextPageState = string.Empty,
                SearchFields = new Dictionary<string, string>() {
                    { "PlayerId", playerIds[0] },
                    { "CurrentLevel", "3" }
                }
            };

            var (items, nextPageState) = dataStore.QueryEntity(searchCriteria).Result;

            _consoleLogger.LogMessage($"Called QueryEntity with RecordCount: {items.Count}");
            _consoleLogger.LogMessage($"NextPageState: {nextPageState}");

            if (items.Count > 0)
            {
                var retrievedItem = dataStore.GetEntity(items[0]).Result;
                _consoleLogger.LogMessage($"Single Record retrieved");

                retrievedItem.CurrentLevel += 1;
                retrievedItem.Health += 10;

                // Lets update the record
                var updateOutcome = dataStore.UpdateEntity(retrievedItem).Result;
                _consoleLogger.LogMessage($"Entry updated {updateOutcome}");

                // Lets delete a row
                var deleteOutcome = dataStore.DeleteEntity(items[0]).Result;
                _consoleLogger.LogMessage($"Entry deleted {deleteOutcome} with PlatformKey:{items[0].PlatformKey}");
            }
            else
            {
                _consoleLogger.LogMessage($"No records returned for query");
            }
        }
    }
}
