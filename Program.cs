using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Text;
using System.Linq;
using Ionic.Zlib;

namespace minetestnodestats
{
    class Program
    {
        static IEnumerable<Block> LoadBlocks()
        {
            using (var connection = new SQLiteConnection(@"Data Source=C:\Dump\minetest-backup\home\filhit\.minetest\worlds\our-world\map.sqlite"))
            {
                connection.Open();
                var countCommand = new SQLiteCommand("select count(1) from blocks", connection);
                long totalBlocks = (long)countCommand.ExecuteScalar();
                long i = 0;
                var tablesCommand = new SQLiteCommand("SELECT pos,data,length(data) FROM blocks", connection);
                var reader = tablesCommand.ExecuteReader();
                var consoleThrottleStopwatch = Stopwatch.StartNew();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        if (consoleThrottleStopwatch.Elapsed > TimeSpan.FromSeconds(1))
                        {
                            consoleThrottleStopwatch.Restart();
                            Console.WriteLine($"{((double)i / totalBlocks * 100):0.00}% ({i} of {totalBlocks})");
                        }
                        i++;
                        var pos = reader.GetInt64(0);
                        var length = reader.GetInt32(2);
                        var data = new byte[length];
                        reader.GetBytes(1, 0, data, 0, length);
                        var block = new Block(pos, data);
                        yield return block;
                    }
                }
            }
        }

        static Dictionary<string, long> GetNodesMap(Block block)
        {
            Dictionary<string, long> nodesMap = new Dictionary<string, long>();
            block.Decode();
            for (byte x = 0; x < 16; x++)
                for (byte y = 0; y < 16; y++)
                    for (byte z = 0; z < 16; z++)
                    {
                        string node = block.getNode(x, y, z);
                        if (nodesMap.ContainsKey(node))
                        {
                            nodesMap[node]++;
                        }
                        else
                        {
                            nodesMap[node] = 1;
                        }
                    }
            return nodesMap;
        }

        static void Main(string[] args)
        {
            var stopwatch = Stopwatch.StartNew();
            Dictionary<string, long> result = new Dictionary<string, long>();

            var nodesMaps = LoadBlocks().AsParallel()
                .Select(GetNodesMap)
                .ToList();

            foreach (var nodesMap in nodesMaps)
            {
                foreach (var type in nodesMap)
                    if (result.ContainsKey(type.Key))
                    {
                        result[type.Key] += type.Value;
                    }
                    else
                    {
                        result[type.Key] = type.Value;
                    }
            }

            foreach (var type in result.OrderBy(x => x.Key))
            {
                Console.WriteLine($"{type.Key}: {type.Value}");
            }

            Console.WriteLine($"Processed world in {stopwatch.Elapsed}");
        }
    }

    class Block
    {
        public Block(Int64 position, byte[] data)
        {
            _position = position;
            this.data = data;
        }

        public Int64 _position;
        private byte[] data;

        byte[] m_mapData;

        UInt16 m_blockAirId;
        UInt16 m_blockIgnoreId;

        private byte m_version;

        public Dictionary<UInt16, string> m_nameMap = new Dictionary<ushort, string>();

        public byte Version
        {
            get
            {
                return data[0];
            }
        }

        public string getNode(byte x, byte y, byte z)
        {
            uint position = (uint)(x + ((long)y << 4) + ((long)z << 8));
            UInt16 content = readBlockContent(m_mapData, m_version, position);
            if (content == m_blockAirId || content == m_blockIgnoreId)
                return "";
            if (m_nameMap.TryGetValue(content, out string type))
            {
                return type;
            }
            else
            {
                Console.Error.WriteLine("Skipping node with invalid ID.");
                return "";
            }
        }

        static UInt16 readBlockContent(byte[] mapData, byte version, UInt32 datapos)
        {
            if (version >= 24)
            {
                uint index = datapos << 1;
                return (UInt16)((mapData[index] << 8) | mapData[index + 1]);
            }
            else if (version >= 20)
            {
                if (mapData[datapos] <= 0x80)
                    return mapData[datapos];
                else
                    return (UInt16)(((UInt16)(mapData[datapos]) << 4) | ((UInt16)(mapData[datapos + 0x2000]) >> 4));
            }
            throw new Exception($"Unsupported map version {version}");
        }

        private UInt16 readU16(int offset)
        {
            return (UInt16)(data[offset] << 8 | data[offset + 1]);
        }

        public void Decode()
        {
            byte version = data[0];
            m_version = version;
            //uint8_t flags = data[1];


            int dataOffset = 0;
            if (version >= 27)
                dataOffset = 6;
            else if (version >= 22)
                dataOffset = 4;
            else
                dataOffset = 2;


            ZlibDecompressor decompressor = new ZlibDecompressor(data);
            decompressor.setSeekPos(dataOffset);
            m_mapData = decompressor.decompress();
            decompressor.decompress(); // unused metadata
            dataOffset = decompressor.seekPos();

            // Skip unused data
            if (version <= 21)
                dataOffset += 2;
            if (version == 23)
                dataOffset += 1;
            if (version == 24)
            {
                byte ver = data[dataOffset++];
                if (ver == 1)
                {
                    UInt16 num = readU16(dataOffset);
                    dataOffset += 2;
                    dataOffset += 10 * num;
                }
            }

            // Skip unused static objects
            dataOffset++; // Skip static object version
            int staticObjectCount = readU16(dataOffset);
            dataOffset += 2;
            for (int i = 0; i < staticObjectCount; ++i)
            {
                dataOffset += 13;
                UInt16 dataSize = readU16(dataOffset);
                dataOffset += dataSize + 2;
            }
            dataOffset += 4; // Skip timestamp

            // Read mapping
            if (version >= 22)
            {
                dataOffset++; // mapping version
                UInt16 numMappings = readU16(dataOffset);
                dataOffset += 2;
                for (int i = 0; i < numMappings; ++i)
                {
                    UInt16 nodeId = readU16(dataOffset);
                    dataOffset += 2;
                    UInt16 nameLen = readU16(dataOffset);
                    dataOffset += 2;
                    string name = Encoding.UTF8.GetString(new Span<byte>(data, dataOffset, nameLen));

                    if (name == "air")
                        m_blockAirId = nodeId;
                    else if (name == "ignore")
                        m_blockIgnoreId = nodeId;
                    else
                        m_nameMap[nodeId] = name;
                    dataOffset += nameLen;
                }
            }

            // Node timers
            if (version >= 25)
            {
                dataOffset++;
                UInt16 numTimers = readU16(dataOffset);
                dataOffset += 2;
                dataOffset += numTimers * 10;
            }
        }
    }

    class ZlibDecompressor
    {
        public ZlibDecompressor(byte[] data)
        {
            m_data = data;
        }
        private byte[] m_data;
        private int m_seekPos;

        public void setSeekPos(int seekPos)
        {
            m_seekPos = seekPos;
        }

        public int seekPos()
        {
            return m_seekPos;
        }

        public byte[] decompress()
        {
            const int BUFSIZE = 128 * 1024;
            byte[] temp_buffer = new byte[BUFSIZE];
            List<byte> buffer = new List<byte>();
            ZlibCodec strm = new ZlibCodec(CompressionMode.Decompress);
            strm.AvailableBytesIn = m_data.Length;

            if (strm.InitializeInflate() != ZlibConstants.Z_OK)
            {
                throw new Exception("decompress error");
            }

            strm.InputBuffer = m_data;
            strm.AvailableBytesOut = BUFSIZE;
            strm.OutputBuffer = temp_buffer;
            strm.NextIn = m_seekPos;

            int ret = 0;
            do
            {
                strm.NextOut = 0;
                ret = strm.Inflate(FlushType.None); // inflate(&strm, Z_NO_FLUSH);
                buffer.AddRange(new Span<byte>(temp_buffer, 0, BUFSIZE - strm.AvailableBytesOut).ToArray());
            } while (ret == ZlibConstants.Z_OK);
            if (ret != ZlibConstants.Z_STREAM_END)
            {
                throw new Exception("decompress error");
            }
            m_seekPos += strm.NextIn - m_seekPos;
            strm.EndInflate();
            return buffer.ToArray();
        }
    }
}
