using server.Storage.LsmTree.LsmTreeEntities;
using System.IO.Compression;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SelfBalancedTree;
using server.Storage;
using server.Storage.LsmTree;

namespace LSMTreeNamespace
{
    public class LSMTree: IStorage
    {
        private string _segmentsDirectory;
        private string _walBasename;
        private string _currentSegment;
        private List<string> _segments;
        private readonly int _threshold;
        private AVLTree<Node?> _memtable;
        private AVLTree<Node> _index;
        private readonly int _sparsityFactor;
        private int _bfNumItems;
        private double _bfFalsePosProb;
        private BloomFilter _bloomFilter;
        private List<int> _bloomFilterValues = new();
        private string _metaInfoPath;
        private int _totalbytes = 0;
        /*string sourceFile = "sourceFile.txt";
        string compressedFile = "compressedFile.deflate";
        int blockSize = 1024;*/


        public LSMTree(string segmentBasename, string segmentsDirectory, string walBasename, string metaInfoPath)
        {
            _metaInfoPath = metaInfoPath;
            _segmentsDirectory = segmentsDirectory;
            _walBasename = walBasename;
            _currentSegment = segmentBasename;
            _segments = new List<string>();
            _threshold = 100;
            _memtable = new AVLTree<Node?>();
            _index = new AVLTree<Node>();
            _sparsityFactor = 100;
            _bfNumItems = 1000000;
            _bfFalsePosProb = 0.2;
            if (!Directory.Exists(segmentsDirectory))
            {
                Directory.CreateDirectory(segmentsDirectory);
            }
            RestoreMemtable();
            LoadMetadata();
            _bloomFilter = new BloomFilter(_bfFalsePosProb, _bfNumItems, _bloomFilterValues);
        }

        public Task<User> Set(string key, string value)
        {
            string log = ToLogEntry(key, value);
            Node node = Find(key);
            int additionalSize = key.Length + value.Length;
            if (node is not null)
            {
                MemtableWal().Write(log);
                node.Value = value;
                _totalbytes += additionalSize;
                return Task.FromResult(new User(key,value));
            }
            if (_totalbytes + additionalSize > _threshold)
            {
                Compact();
                FlushMemtableToDisk(CurrentSegmentPath());

                _memtable = new AVLTree<Node>();
                MemtableWal().Clear();

                _segments.Add(_currentSegment);
                string newSegmentName = IncrementedSegmentName();
                _currentSegment = newSegmentName;
                _totalbytes = 0;
            }
            MemtableWal().Write(log);

            _memtable.Add(new Node(key, value));
            _totalbytes += additionalSize;
            Console.WriteLine(_totalbytes);
            return Task.FromResult(new User(key,value));
        }
        
        public Task<User?> Get(string key)
        {
            var memtable_result =
                _memtable.ValuesCollection.LastOrDefault(node => node.Key == key);
            if (memtable_result is not null)
            {
                return Task.FromResult(new User(key,memtable_result.Value));
            }

            if (!_bloomFilter.Check(key))
            {
                return null;
            }

            var indexResult = _index.ValuesCollection.LastOrDefault(node => node.Key == key);
            
            if (indexResult != null)
            {
                var path = SegmentPath(indexResult.Segment);
                using (var s = new StreamReader(path))
                {
                    s.BaseStream.Seek(indexResult.Offset, SeekOrigin.Begin);
                    string line;
                    while ((line = s.ReadLine()) != null)
                    {
                        var parts = line.Split(',');
                        var k = parts[0];
                        var v = parts[1];
                        if (k == key)
                        {
                            return Task.FromResult(new User(key, v.Trim()));
                        }
                    }
                }
            }

            var valueFromSegments = SearchAllSegments(key);
            if (valueFromSegments is not null && valueFromSegments!="")
            {
                return Task.FromResult(new User(key,  SearchAllSegments(key)));
            }

            return null;
        }
        
        private int GetSizeOf(List<Node> keyvalues)
        {
            int byteCount = 0;
            foreach (var node in keyvalues)
            {
                byteCount += Encoding.UTF8.GetBytes(node.Key + node.Value).Length;
            }

            return byteCount;
        }

        public void SaveMetadata()
        {
            var metadata = new
            {
                segmentsDirectory = _segmentsDirectory,
                walBasename = _walBasename,
                currentSegment = _currentSegment,
                segments = _segments,
                threshold = _threshold,
                sparsityFactor = _sparsityFactor,
                bfNumItems = _bfNumItems,
                bfFalsePosProb = _bfFalsePosProb,
                bloomFilterValues = _bloomFilterValues
            };

            string json = JsonConvert.SerializeObject(metadata);
            File.WriteAllText(_metaInfoPath, json);
        }


        public void LoadMetadata()
        {
            if (File.Exists(_metaInfoPath))
            {
                string json = File.ReadAllText(_metaInfoPath);
                Dictionary<string, object> metaData = JsonConvert.DeserializeObject<Dictionary<string, object>>(json);

                _segmentsDirectory = metaData["segmentsDirectory"].ToString();
                _walBasename = metaData["walBasename"].ToString();
                _currentSegment = metaData["currentSegment"].ToString();
                _segments = ((JArray)metaData["segments"]).ToObject<List<string>>();
                _bfNumItems = Convert.ToInt32(metaData["bfNumItems"]);
                _bfFalsePosProb = Convert.ToDouble(metaData["bfFalsePosProb"]);
                _bloomFilterValues = ((JArray)metaData["bloomFilterValues"]).ToObject<List<int>>();
            }
        }

        private Node Find(string key)
        {
            return _memtable.ValuesCollection.LastOrDefault(node => node.Key == key);
        }


        private void RestoreMemtable()
        {
            if (File.Exists(MemtableWalPath()))
            {
                using (StreamReader reader = new StreamReader(MemtableWalPath()))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] parts = line.Split(',');
                        string key = parts[0];
                        string value = parts[1];
                        _memtable.Add(new Node(key, value));
                        _totalbytes += key.Length + value.Length;
                    }
                }
            }
        }
        
        
        private void FlushMemtableToDisk(string currentSegmentPath)
        {
            int sparsityCounter = Sparsity();
            int keyOffset = 0;


            using (FileStream fstream = new FileStream(currentSegmentPath, FileMode.Append))
            {
                foreach (var node in _memtable.ValuesCollection)
                {
                    string log = ToLogEntry(node.Key, node.Value);

                    if (sparsityCounter == 1)
                    {
                        Node tempNode = new Node(node.Key, node.Value);
                        tempNode.Segment = _currentSegment;
                        tempNode.Offset = keyOffset;
                        _index.Add(tempNode);
                        sparsityCounter = Sparsity() + 1;
                    }
                    _bloomFilterValues.AddRange(_bloomFilter.Add(node.Key));
                    fstream.Write(Encoding.Default.GetBytes(log));
                    keyOffset += log.Length;
                    sparsityCounter -= 1;
                }
            }
            SaveMetadata();
        }

        private int Sparsity()
        {
            return _threshold / _sparsityFactor;
        }
        private void Compact()
        {
            List<Node> memtableNodes = _memtable.ValuesCollection.ToList();
            var keysOnDisk = new HashSet<string>();
            foreach (var node in memtableNodes)
            {
                if (_bloomFilter.Check(node.Key))
                {
                    keysOnDisk.Add(node.Key);
                }

                DeleteKeysFromSegments(keysOnDisk.ToList(), _segments);
            }
        }

        public void Merge()
        {
            var segments = _segments;
            for (int i = segments.Count-1; i > 0; i--)
            {
                MergeProcess(segments[i], segments[i - 1]);
            }
            _currentSegment = segments[^1];
            SaveMetadata();
        }

        private void MergeProcess(string segment1, string segment2)
        {
            string path1 = _segmentsDirectory + segment1;
            string path2 = _segmentsDirectory + segment2;
            string newPath = _segmentsDirectory + "temp";
            using (StreamWriter s0 = new StreamWriter(newPath))
            {
                using (StreamReader s1 = new StreamReader(path1))
                {
                    using (StreamReader s2 = new StreamReader(path2))
                    {
                        string line1 = s1.ReadLine();
                        string line2 = s2.ReadLine();
                        while (!(line1 == "" && line2 == ""))
                        {
                            line1 = line1 is null ? "" : line1;
                            line2 = line2 is null ? "" : line2;
                            if (line1 == "" && line2 == "")
                            {
                                break;
                            }
                            string key1 = line1 is null ? "" : line1.Split(',')[0];
                            string key2 = line2 is null ? "" : line2.Split(',')[0];
                            if (key1 == "" || key1 == key2)
                            {
                                s0.WriteLine(line2);
                                line1 = s1.ReadLine();
                                line2 = s2.ReadLine();
                            }
                            else if (key2 == "" || String.CompareOrdinal(key1, key2) < 0)
                            {
                                s0.WriteLine(line1);
                                line1 = s1.ReadLine();
                            }
                            else if (String.CompareOrdinal(key1, key2) > 0)
                            {
                                s0.WriteLine(line2);
                                line2 = s2.ReadLine();
                            }
                        }
                    }
                }
            }
    
            File.Delete(path1);
            File.Delete(path2);
            File.Move(newPath,path2);
            _segments.Remove(path1);
        }
        
        public void CompressFile(string sourceFile, string compressedFile, int blockSize)
        {
            using (FileStream sourceStream = new FileStream(sourceFile, FileMode.Open))
            {
                using (FileStream targetStream = File.Create(compressedFile))
                {
                    using (DeflateStream compressionStream = new DeflateStream(targetStream, CompressionMode.Compress))
                    {
                        byte[] buffer = new byte[blockSize];
                        int bytesRead;
                        while ((bytesRead = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            compressionStream.Write(buffer, 0, bytesRead);
                        }
                    }
                }
            }
        }

        static void DecompressFile(string compressedFile, string decompressedFile)
        {
            using (FileStream sourceStream = new FileStream(compressedFile, FileMode.Open))
            {
                using (FileStream targetStream = File.Create(decompressedFile))
                {
                    using (DeflateStream decompressionStream =
                           new DeflateStream(sourceStream, CompressionMode.Decompress))
                    {
                        byte[] buffer = new byte[1024];
                        int bytesRead;
                        while ((bytesRead = decompressionStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            targetStream.Write(buffer, 0, bytesRead);
                        }
                    }
                }
            }
        }



        public long GetFileSize(string path)
        {
            return new FileInfo(path).Length;
        }
        
        public string SearchAllSegments(string key)
        {
            var segments = new List<string>(_segments);

            foreach (var segment in segments)
            {
                var value = SearchSegment(key, segment);
                if (value is not null)
                {
                    return value;
                }
            }

            return null;
        }

        public string SearchSegment(string key, string segmentName)
        {
            using (StreamReader s = new StreamReader(SegmentPath(segmentName)))
            {
                List<string> pairs = new List<string>();
                string line;
                while ((line = s.ReadLine()) != null)
                {
                    pairs.Add(line.Trim());
                }
                while (pairs.Count > 0)
                {
                    int ptr = (pairs.Count - 1) / 2;
                    string[] pair = pairs[ptr].Split(',');
                    string k = pair[0];
                    string v = pair[1];
                    if (k == key)
                    {
                        return v;
                    }
                    if (key.CompareTo(k) < 0)
                    {
                        pairs = pairs.GetRange(0, ptr);
                    }
                    else
                    {
                        pairs = pairs.GetRange(ptr + 1, pairs.Count - ptr - 1);
                    }
                }
            }
            return null;
        }
        

        private void DeleteKeysFromSegments(List<string> keysToDelete, List<string> segmentNames)
        {
            foreach (var segment in segmentNames)
            {
                string segmentPath = SegmentPath(segment);
                DeleteKeysFromSingleSegment(keysToDelete, segmentPath);
            }
        }


        private void DeleteKeysFromSingleSegment(List<string> keysToDelete, string segmentPath)
        {
            string tempPath = segmentPath + "_temp";

            using (StreamReader input = new StreamReader(segmentPath))
            {
                using (StreamWriter output = new StreamWriter(tempPath))
                {
                    string line;
                    while ((line = input.ReadLine()) != null)
                    {
                        string[] parts = line.Split(',');
                        string key = parts[0];
                        string value = parts[1];

                        if (!keysToDelete.Contains(key))
                        {
                            output.WriteLine(line);
                        }
                    }
                }
            }
            File.Delete(segmentPath);
            File.Move(tempPath, segmentPath);
        }

        private string SegmentPath(string segmentName)
        {
            return _segmentsDirectory + segmentName;
        }

        private string IncrementedSegmentName()
        {
            string name = _currentSegment.Split('-')[0];
            int number = int.Parse(_currentSegment.Split('-')[1]);
            string newNum = (number + 1).ToString();

            return name + '-' + newNum;
        }

        private AppendLog MemtableWal()
        {
            return AppendLog.GetInstance(MemtableWalPath());
        }

        private string MemtableWalPath()
        {
            return _segmentsDirectory + _walBasename;
        }

        private string CurrentSegmentPath()
        {
            return _segmentsDirectory + _currentSegment;
        }
        
        static string ToLogEntry(string key, string value)
        {
            return key + ',' + value + '\n';
        }
    }
}
