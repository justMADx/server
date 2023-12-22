namespace server.Storage.LsmTree;
using System.Collections;
using System.Text;
using System.Security.Cryptography;

public class BloomFilter
{
    private double _falsePositiveProb;
    private int _bitArraySize;
    private int _numHashFns;
    private BitArray _bitArray;

    public BloomFilter(double false_positive_prob, int num_items, List<int> bitArrayValues)
    {
        _falsePositiveProb = false_positive_prob;
        _bitArraySize = BitArraySize(num_items, false_positive_prob);
        _numHashFns = GetHashCount(_bitArraySize, num_items);
        _bitArray = new BitArray(_bitArraySize);
        for (int i = 0; i < _bitArraySize; i++)
        {
            _bitArray[i] = false;
        }

        foreach (var value in bitArrayValues)
        {
            _bitArray[value] = true;
        }
    }

    public List<int> Add(string item)
    {
        List<int> digests = new List<int>();
        for (int seed = 0; seed < _numHashFns; seed++)
        {
            int digest = Hash(item, seed) % _bitArraySize;
            digests.Add(digest);
            _bitArray[digest] = true;
        }

        return digests;
    }

    public bool Check(string item)
    {
        for (int seed = 0; seed < _numHashFns; seed++)
        {
            int digest = Hash(item, seed) % _bitArraySize;
            if (_bitArray[digest] == false)
            {
                return false;
            }
        }

        return true;
    }

    public int BitArraySize(int num_items, double probability)
    {
        double m = -(num_items * Math.Log(probability)) / (Math.Log(2) * Math.Log(2));
        return (int)m;
    }

    public int GetHashCount(int bit_arr_size, int num_items)
    {
        return (int)((bit_arr_size / num_items) * Math.Log(2));
    }

    public int Hash(string item, int seed)
    {
        using (MD5 md5 = MD5.Create())
        {
            byte[] inputBytes = Encoding.ASCII.GetBytes(item + seed);
            byte[] hashBytes = md5.ComputeHash(inputBytes);
            int hash = BitConverter.ToInt32(hashBytes, 0);
            return Math.Abs(hash);
        }
    }
}