namespace server.Storage;


public class User
{
    public string Id { get; }
    public string Name { get; set; }
    public User(string id, string name)
    {
        Id = id;
        Name = name; 
    }
}
