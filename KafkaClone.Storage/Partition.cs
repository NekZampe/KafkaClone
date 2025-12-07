using Microsoft.Extensions.Logging;


namespace KafkaClone.Storage;

public class Partition : IDisposable
{
    private readonly string _directoryPath;
    private FileStream _fileStream;
    private FileStream _indexStream;
    private readonly int _maxFileSize = 1024; 

    private readonly System.TimeSpan _retentionMaxAge = TimeSpan.FromMinutes(5) ;
    private readonly bool _autoFlush = false; //debug mode

    public long FileLength => _fileStream.Length;
    public long IndexLength => _indexStream.Length;

    private SortedDictionary<long,string> _offsets;

    private long _activeBaseOffset;

     private readonly ILogger<Partition> _logger;


    public Partition(string directoryPath, bool autoFlush, ILogger<Partition> logger,TimeSpan timeSpan )
    {
        _logger = logger;

        _retentionMaxAge = timeSpan;

        _autoFlush = autoFlush;

        _directoryPath = directoryPath;

        _offsets = new SortedDictionary<long, string>();
        
        // Ensure folder exists
        if (!Directory.Exists(_directoryPath))
        {
            Directory.CreateDirectory(_directoryPath);
        }

        // STEP 1: Find the latest log file
        string latestFileName = "00000"; // Default if empty

        
        string[] files = Directory.GetFiles(_directoryPath, "*.log");
        if (files.Length > 0)
        {
            long max = long.MinValue;

            foreach (var file in files)
            {
                string currentPath = Path.GetFileNameWithoutExtension(file);
                long currentLong = long.Parse(currentPath);
                _offsets.Add(currentLong,currentPath);
                max = Math.Max(max,currentLong);
            }

            latestFileName = max.ToString("D5");
        }

        // The Safety Net: If nothing was found, create the default
        if (_offsets.Count == 0)
        {
            _offsets.Add(0, "00000");
        }

        // STEP 2: Open that file (Reusing your old logic)
        OpenSegment(latestFileName);

        _logger = logger;
    }

    // Stream opening logic
    private void OpenSegment(string nameWithoutExtension)
    {
        _activeBaseOffset = long.Parse(nameWithoutExtension);

        _logger.LogInformation("Switching active segment to: {SegmentName}", nameWithoutExtension);

        if (!_offsets.ContainsKey( _activeBaseOffset))
    {
        _offsets.Add(_activeBaseOffset, nameWithoutExtension);
    }

        string fullLogPath = Path.Combine(_directoryPath, nameWithoutExtension + ".log");
        string fullIndexPath = Path.Combine(_directoryPath, nameWithoutExtension + ".index");

        
        _fileStream = new FileStream(fullLogPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _fileStream.Position = _fileStream.Length;

        _indexStream = new FileStream(fullIndexPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _indexStream.Position = _indexStream.Length;

    }
    
public async Task<long> AppendAsync(byte[] data)
{
    // 1. CAPTURE STATE (Crucial for Return Value)
    // We calculate the ID this message will have (e.g., 0)
    long currentLogicalId = _activeBaseOffset + (_indexStream.Length / 8);
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

    // 4. Check for Roll Over
    if (_fileStream.Position >= _maxFileSize)
    {
        // The NEXT message will be at ID = current + 1
        long nextId = currentLogicalId + 1;
        
        string newFileName = nextId.ToString("D5");

        // Close the old file
        Dispose();

        // Open the new one
        OpenSegment(newFileName);

        //Run Prune to remove old files
        PruneOldSegments(_retentionMaxAge);
    }

    // 5. Return the ID of the message we just wrote
    return currentLogicalId;
}


public async Task<byte[]> ReadAsync(long offset)
{

    var validKeys = _offsets.Keys.Where(k => k <= offset);

    if (!validKeys.Any())
        {
            _logger.LogWarning("Client requested offset {Offset}, but it was already deleted by retention.", offset);
            throw new IndexOutOfRangeException();
        }

    long baseOffset = validKeys.Last();
    
    string fileName = _offsets[baseOffset];

    // 2. Construct the paths
    string indexName = Path.Combine(_directoryPath, fileName + ".index");
    string logName = Path.Combine(_directoryPath, fileName + ".log");

    // 3. Calculate Relative Position
    // If we want Offset 75, and the file starts at 74:
    // (75 - 74) * 8 = Position 8
    long relativeOffset = offset - baseOffset;
    long indexPosition = relativeOffset * 8;

    // 4. Open a TEMPORARY stream just for this read
    // We use "using" so it closes automatically
    using (FileStream tempIndex = File.OpenRead(indexName))
    {
        // Safety Check
        if (indexPosition >= tempIndex.Length)
            throw new IndexOutOfRangeException("Message not found");

        tempIndex.Position = indexPosition;

        byte[] indexBuffer = new byte[8];
        await tempIndex.ReadExactlyAsync(indexBuffer, 0, 8);
        long logPosition = BitConverter.ToInt64(indexBuffer, 0);

        // 5. Open the Log File to get the data
        using (FileStream tempLog = File.OpenRead(logName))
        {
            tempLog.Position = logPosition;
            
            // Read Length
            byte[] lengthBuffer = new byte[4];
            await tempLog.ReadExactlyAsync(lengthBuffer);
            int messageLength = BitConverter.ToInt32(lengthBuffer, 0);

            // Read Payload
            byte[] payload = new byte[messageLength];
            await tempLog.ReadExactlyAsync(payload);

            return payload;
        }
    }
}

// Remove old logs
public void PruneOldSegments(TimeSpan maxAge)
{
    DateTime cutoff = DateTime.Now - maxAge;
    
    // Create a copy of the keys so we can remove items safely
    var segmentOffsets = _offsets.Keys.ToList();

    foreach (var baseOffset in segmentOffsets)
    {
        // RULE 1: Never delete the active file
        if (baseOffset == _activeBaseOffset) continue;

        string fileName = _offsets[baseOffset];
        string fullLogPath = Path.Combine(_directoryPath, fileName + ".log");
        string fullIndexPath = Path.Combine(_directoryPath, fileName + ".index");

        // RULE 2: Check the age
        FileInfo info = new FileInfo(fullLogPath);
        if (info.LastWriteTime < cutoff)
        {
            _logger.LogInformation("[Retention] Deleting expired segment: {SegmentName}", fileName);

            // Delete files
            if (File.Exists(fullLogPath)) File.Delete(fullLogPath);
            if (File.Exists(fullIndexPath)) File.Delete(fullIndexPath);

            // Remove from Registry
            _offsets.Remove(baseOffset);
        }
    }
}

    public void Dispose()
    {
        _fileStream.Close();
        _indexStream.Close();
    }

}
