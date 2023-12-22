using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using LSMTreeNamespace;
using server.Storage;

namespace server.Services;

public class ServerComponentService : ServerClient.ServerClientBase
{
   private IStorage _storageComponent = new StorageComponent();
   /*private static string segmentsDirectory = "D:\\Segmets\\Segments\\";
   private static string segmentBasename = "LSMTree-1";
   private static string walBasename = "memtablebackup.txt";
   private static string filePath = Path.Combine(segmentsDirectory, "metadata.json");
   private IStorage _storageComponent = new LSMTree(segmentBasename, segmentsDirectory, walBasename, filePath);*/
   
    public override Task<UserReply> Get(GetUserRequest request, ServerCallContext context)
    {
        try
        {
            var user = _storageComponent.Get(request.Id).Result;
            if (user is null)
            {
                throw new Exception("There is no such user in data..");
            }
            UserReply userReply = new UserReply() { Id = user.Id, Name = user.Name};
            return Task.FromResult(userReply);
        }
        catch (RpcException e)
        {
            Console.WriteLine(e.Status.Detail);
            throw;
        }
    }

    public override Task<UserReply> Set(SetUserRequest request, ServerCallContext context)
    {
        var user = _storageComponent.Set(request.Id, request.Name).Result;
        var reply = new UserReply() { Id = user.Id, Name = user.Name };
        return Task.FromResult(reply);
    }
}

