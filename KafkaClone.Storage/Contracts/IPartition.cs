using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaClone.Storage.Contracts
{
    public interface IPartition : IDisposable
    {

    int Id { get; }
    long CurrentOffset { get; }

    Task<long> AppendAsync(byte[] data);

    Task<long> AppendBatchAsync(List<byte[]> data);

    Task<byte[]> ReadAsync(long offset);

    Task<(List<byte[]> Messages, long NextOffset)> ReadBatchAsync(long offset,int maxCount);

    Task TruncateFromIndexAsync(long index);

    }

}

