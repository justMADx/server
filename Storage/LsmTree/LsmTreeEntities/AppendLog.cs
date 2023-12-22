namespace server.Storage.LsmTree.LsmTreeEntities;

public class AppendLog
{
    private string _filename;
    private StreamWriter stream;
    private static AppendLog instance;

    private AppendLog(string filename)
    {
        _filename = filename;
    }

    public static AppendLog GetInstance(string filename)
    {
        if (instance == null)
        {
            instance = new AppendLog(filename);
        }
        return instance;
    }

    public void Write(string val)
    {
        try
        {
            using (stream = new StreamWriter(_filename, true))
            {
                stream.Write(val);
                stream.Flush();
                stream.Close();
            }
        }
        catch (IOException e)
        {
            Console.WriteLine(e);
            Console.WriteLine("The file stream isn't currently open");
        }
    }

    public void Clear()
    {
        stream.Close();
        File.WriteAllText(_filename,string.Empty);
    }
}