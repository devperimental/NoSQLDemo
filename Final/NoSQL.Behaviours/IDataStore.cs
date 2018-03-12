using System.Collections.Generic;
using System.Threading.Tasks;

namespace NoSQL.Behaviours
{
    public interface IDataStore<T>
    {
        Task<T> AddEntity(T item);

        Task<bool> UpdateEntity(T item);

        Task<bool> DeleteEntity(T item);

        Task<T> GetEntity(T item);

        Task<(List<T> list, string nextPageState)> QueryEntity(ICriteria criteria);
    }
}
