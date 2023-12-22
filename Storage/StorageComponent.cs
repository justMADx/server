using Grpc.Core;
using server.Services;

namespace server.Storage;

public class StorageComponent : IStorage
{
    static List<User> _listOfUsers = new() { new User("1", "Tomas"), new User("2", "Bad") };

    public Task<User?> Get(string id)
    {
        return Task.FromResult(_listOfUsers.FirstOrDefault(user => user.Id==id));
    }
    
    public Task<List<User>> GetAllUsers()
    {
        return Task.FromResult(_listOfUsers);
    }
    
    public Task<User> Set(string key, string name)
    {
        return CheckForUserWithId(key) ? UpdateUser(key, name) : CreateUser(name);
    }

    private Task<User> CreateUser(string value)
    {
        var id = GenerateKey();
        while (CheckForUserWithId(id))
        {
            id = GenerateKey();
        }
        var user = new User(id, value);
        _listOfUsers.Add(user);
        return Task.FromResult(user);
    }
    
    private Task<User> UpdateUser(string key, string name)
    {
        if (!CheckForUserWithId(key))
        {
            throw new RpcException(new Status(StatusCode.NotFound, "User not found"));
        }
        var user = _listOfUsers.Find(user => user.Id == key);
        user.Name = name;
        return Task.FromResult(user);
    }
    
    private static string GenerateKey()
    {
        return Guid.NewGuid().ToString()[..8];
    }

    private bool CheckForUserWithId(string key)
    {
        return _listOfUsers.Any(user => user.Id == key);
    }

}