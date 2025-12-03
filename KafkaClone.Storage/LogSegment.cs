using System;
using System.IO;
using System.Text;


namespace KafkaClone.Storage;

public class LogSegment : IDisposable
{

    private readonly FileStream _fileStream;
    private readonly FileStream _indexStream;

    private readonly bool _autoFlush;

    public long Length => _fileStream.Length;
    public long IndexLength => _indexStream.Length;
    
    public LogSegment(string path, bool autoFlush = false)
{
       _fileStream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite);

       _fileStream.Position = _fileStream.Length;

        // 2. Open the Index File (.index)
        // Change "path/00000.log" -> "path/00000.index"
        string indexPath = Path.ChangeExtension(path, ".index");
        _indexStream = new FileStream(indexPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _indexStream.Position = _indexStream.Length;

       _autoFlush = autoFlush;
    
}

        // [Size (4 bytes)] [Message Data]
    public async Task<long> AppendAsync(byte[] data)
    {
    // 1. CAPTURE START POSITION
    long startingPosition = _fileStream.Position;

    // 2. Write to the Data Log
    byte[] lengthBytes = BitConverter.GetBytes(data.Length);
    await _fileStream.WriteAsync(lengthBytes);
    await _fileStream.WriteAsync(data);

    // 3. Write to the Index
    await _indexStream.WriteAsync(BitConverter.GetBytes(startingPosition));

    if (_autoFlush)
    {
        await _fileStream.FlushAsync();
        await _indexStream.FlushAsync();
    }

    // 4. Return Logical ID (0, 1, 2...) instead of Byte Position
    // Since each index entry is 8 bytes, the count is Length / 8
    return _indexStream.Length / 8;
    }


    public async Task<byte[]> ReadAsync(long offset)
    {

        long indexPosition = offset * 8;

        if (indexPosition >= _indexStream.Length)
        {
            throw new IndexOutOfRangeException("Message not found");
        }

        _indexStream.Position = indexPosition;

        byte[] indexBuffer = new byte[8];
        
        await _indexStream.ReadExactlyAsync(indexBuffer,0,8);

        _fileStream.Position = BitConverter.ToInt64(indexBuffer,0);

        byte[] lengthBuffer = new byte[4];

        await _fileStream.ReadExactlyAsync(lengthBuffer);

        int messageLength = BitConverter.ToInt32(lengthBuffer, 0);

        byte[] payload = new byte[messageLength];
        
        await _fileStream.ReadExactlyAsync(payload);

        return payload;

    }

    public void Dispose()
    {
        _fileStream.Close();
        _indexStream.Close();
    }

}
