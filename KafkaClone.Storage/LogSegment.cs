using System;
using System.IO;
using System.Text;


namespace KafkaClone.Storage;

public class LogSegment : IDisposable
{

    private readonly FileStream _fileStream;
    
    public LogSegment(string path)
{
        _fileStream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite);

       _fileStream.Position = _fileStream.Length;
    
}

        // [Size (4 bytes)] [Message Data]
        public async Task<long> AppendAsync(byte[] data)
    {

       byte[] lengthBytes = BitConverter.GetBytes(data.Length);

       await _fileStream.WriteAsync(lengthBytes);
       await _fileStream.WriteAsync(data);

       long position = _fileStream.Position;

        return position;
    }


    public async Task<byte[]> ReadAsync(long offset)
    {

        _fileStream.Position = offset;

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
    }

}
