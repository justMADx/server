using server.Services;

namespace server.Storage;

public interface IStorage
{
    Task<User?> Get(string key);
    Task<User> Set(string key, string value);
}