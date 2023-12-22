namespace server.Storage.LsmTree.LsmTreeEntities;

public class Node : IComparable<Node> {
    public Node(string key, string value)
    {
        Key = key;
        Value = value;
    }
    public string Key { get; private set; }
    public string Value { get; set; }
    
    public string Segment { get; set; }

    public int Offset { get; set; }

    public int CompareTo(Node? other)
    {
        if (ReferenceEquals(this, other)) return 0;
        if (ReferenceEquals(null, other)) return 1;
        var keyComparison = String.Compare(Key, other.Key, StringComparison.Ordinal);
        if (keyComparison != 0) return keyComparison;
        return string.Compare(Value, other.Value, StringComparison.Ordinal);
    }
}