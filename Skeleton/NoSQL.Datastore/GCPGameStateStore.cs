using CommonTypes.Behaviours;
using CommonTypes.Messages;
using CommonTypes.Settings;
using NoSQL.Behaviours;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NoSQL.Datastore
{
    public class GCPGameStateStore : IDataStore<GameState>
    {
        private GCPSettings _gcpSettings;
        private IAppLogger _appLogger;

        public GCPGameStateStore(IAppLogger appLogger, GCPSettings gcpSettings)
        {
            _gcpSettings = gcpSettings;
            _appLogger = appLogger;
        }

        public Task<GameState> AddEntity(GameState item)
        {
            throw new NotImplementedException();
        }

        public Task<bool> DeleteEntity(GameState item)
        {
            throw new NotImplementedException();
        }

        public Task<GameState> GetEntity(GameState item)
        {
            throw new NotImplementedException();
        }

        public Task<(List<GameState> list, string nextPageState)> QueryEntity(ICriteria criteria)
        {
            throw new NotImplementedException();
        }

        public Task<bool> UpdateEntity(GameState item)
        {
            throw new NotImplementedException();
        }
    }
}