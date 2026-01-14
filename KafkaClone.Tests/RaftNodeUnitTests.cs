using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Moq;
using KafkaClone.Server;
using KafkaClone.Server.DTOs;
using KafkaClone.Shared;
using KafkaClone.Storage.Contracts;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using KafkaClone.Storage;

namespace KafkaClone.Tests.Server
{
    /// <summary>
    /// Unit tests for RaftNode helper methods (non-consensus logic)
    /// Tests cover: serialization, log helpers, state queries, and utility methods
    /// </summary>
    public class RaftNodeHelperMethodsTests
    {
        private readonly Mock<ILogger<Partition>> _mockLogger;
        private readonly Mock<IRaftTransport> _mockTransport;
        private readonly Mock<IPartition> _mockRaftLog;
        private readonly Mock<ClusterState> _mockClusterState;
        private readonly Broker _testBroker;
        private readonly List<Broker> _clusterMembers;

        public RaftNodeHelperMethodsTests()
        {
            _mockLogger = new Mock<ILogger<Partition>>();
            _mockTransport = new Mock<IRaftTransport>();
            _mockRaftLog = new Mock<IPartition>();
            _mockClusterState = new Mock<ClusterState>();
            
            _testBroker = new Broker(1,5001, 6001,"localhost");
            _clusterMembers = new List<Broker>
            {
                _testBroker,
                new Broker(2,5002, 6002,"localhost"),
                new Broker(3,5003, 6003,"localhost")
            };
            
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(0);
        }

        #region Serialization Tests

        [Fact]
        public async Task SerializeLogEntry_ShouldProduceValidJsonBytes()
        {
            // Arrange
            var node = await CreateTestNode();
            var mockCommand = new Mock<IClusterCommand>();
            var entry = new LogEntry
            {
                Term = 5,
                Index = 10,
                Command = mockCommand.Object
            };

            // Act
            var bytes = node.SerializeLogEntry(entry);

            // Assert
            Assert.NotNull(bytes);
            Assert.NotEmpty(bytes);
            
            // Verify it's valid JSON by deserializing
            var json = System.Text.Encoding.UTF8.GetString(bytes);
            Assert.Contains("\"Term\":5", json);
            Assert.Contains("\"Index\":10", json);
        }

        [Fact]
        public async Task SerializeLogEntry_WithNullCommand_ShouldSucceed()
        {
            // Arrange
            var node = await CreateTestNode();
            var entry = new LogEntry
            {
                Term = 1,
                Index = 0,
                Command = null
            };

            // Act
            var bytes = node.SerializeLogEntry(entry);

            // Assert
            Assert.NotNull(bytes);
            Assert.NotEmpty(bytes);
        }

        [Fact]
        public async Task DeserializeLogEntry_ShouldRestoreOriginalData()
        {
            // Arrange
            var node = await CreateTestNode();
            var originalEntry = new LogEntry
            {
                Term = 3,
                Index = 7,
                Command = null
            };
            var bytes = node.SerializeLogEntry(originalEntry);

            // Act
            var deserialized = node.DeserializeLogEntry(bytes);

            // Assert
            Assert.NotNull(deserialized);
            Assert.Equal(3, deserialized.Term);
            Assert.Equal(7, deserialized.Index);
        }

        [Fact]
        public async Task DeserializeLogEntry_WithEmptyBytes_ShouldReturnNull()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            var result = node.DeserializeLogEntry(new byte[0]);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task DeserializeLogEntry_WithNullBytes_ShouldReturnNull()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            var result = node.DeserializeLogEntry(null);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task DeserializeLogEntry_WithSentinelByte_ShouldReturnNull()
        {
            // Arrange
            var node = await CreateTestNode();
            var sentinelBytes = new byte[] { 0xFF };

            // Act
            var result = node.DeserializeLogEntry(sentinelBytes);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task SerializeDeserialize_RoundTrip_ShouldPreserveData()
        {
            // Arrange
            var node = await CreateTestNode();
            var entries = new List<LogEntry>
            {
                new LogEntry { Term = 1, Index = 0, Command = null },
                new LogEntry { Term = 1, Index = 1, Command = null },
                new LogEntry { Term = 2, Index = 2, Command = null }
            };

            // Act & Assert
            foreach (var original in entries)
            {
                var bytes = node.SerializeLogEntry(original);
                var restored = node.DeserializeLogEntry(bytes);
                
                Assert.Equal(original.Term, restored.Term);
                Assert.Equal(original.Index, restored.Index);
            }
        }

        #endregion

        #region GetLogEntries Tests

        [Fact]
        public async Task GetLogEntries_ShouldReturnCorrectRange()
        {
            // Arrange
            var node = await CreateTestNode();
            var mockEntries = new List<byte[]>
            {
                CreateSerializedLogEntry(1, 5),
                CreateSerializedLogEntry(1, 6),
                CreateSerializedLogEntry(1, 7)
            };

            _mockRaftLog.Setup(x => x.ReadBatchAsync(5, 3))
                .ReturnsAsync((mockEntries, 8L));

            // Act
            var result = await node.GetLogEntries(5L, 7L);

            // Assert
            Assert.Equal(3, result.Count);
            Assert.Equal(5, result[0].Index);
            Assert.Equal(6, result[1].Index);
            Assert.Equal(7, result[2].Index);
        }

        [Fact]
        public async Task GetLogEntries_WithSingleEntry_ShouldReturnOne()
        {
            // Arrange
            var node = await CreateTestNode();
            var mockEntries = new List<byte[]>
            {
                CreateSerializedLogEntry(2, 10)
            };

            _mockRaftLog.Setup(x => x.ReadBatchAsync(10, 1))
                .ReturnsAsync((mockEntries, 11L));

            // Act
            var result = await node.GetLogEntries(10L, 10L);

            // Assert
            Assert.Single(result);
            Assert.Equal(10, result[0].Index);
            Assert.Equal(2, result[0].Term);
        }

        [Fact]
        public async Task GetLogEntries_EmptyRange_ShouldReturnEmptyList()
        {
            // Arrange
            var node = await CreateTestNode();
            _mockRaftLog.Setup(x => x.ReadBatchAsync(It.IsAny<long>(), It.IsAny<int>()))
                .ReturnsAsync((new List<byte[]>(), 0L));

            // Act
            var result = await node.GetLogEntries(5, 5);

            // Assert
            Assert.Empty(result);
        }

        [Fact]
        public async Task GetLogEntries_ShouldCalculateCountCorrectly()
        {
            // Arrange
            var node = await CreateTestNode();
            long startIndex = 10L;
            long endIndex = 20L;
            int expectedCount = 11;

            _mockRaftLog.Setup(x => x.ReadBatchAsync(startIndex, expectedCount))
                .ReturnsAsync((new List<byte[]>(), 21L));

            // Act
            await node.GetLogEntries(startIndex, endIndex);

            // Assert
            _mockRaftLog.Verify(x => x.ReadBatchAsync(startIndex, expectedCount), Times.Once);
        }

        #endregion

        #region GetTermByIndex Tests

        [Fact]
        public async Task GetTermByIndex_WithValidIndex_ShouldReturnCorrectTerm()
        {
            // Arrange
            var node = await CreateTestNode();
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(10);
            _mockRaftLog.Setup(x => x.ReadAsync(5))
                .ReturnsAsync(CreateSerializedLogEntry(3, 5));

            // Act
            var term = await GetTermByIndexViaReflection(node, 5);

            // Assert
            Assert.Equal(3, term);
        }

        [Fact]
        public async Task GetTermByIndex_WithNegativeIndex_ShouldReturnZero()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            var term = await GetTermByIndexViaReflection(node, -1);

            // Assert
            Assert.Equal(0, term);
        }

        [Fact]
        public async Task GetTermByIndex_BeyondLogSize_ShouldReturnZero()
        {
            // Arrange
            var node = await CreateTestNode();
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(5);

            // Act
            var term = await GetTermByIndexViaReflection(node, 10);

            // Assert
            Assert.Equal(0, term);
        }

        [Fact]
        public async Task GetTermByIndex_WithException_ShouldReturnZero()
        {
            // Arrange
            var node = await CreateTestNode();
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(10);
            _mockRaftLog.Setup(x => x.ReadAsync(It.IsAny<long>()))
                .ThrowsAsync(new Exception("Read error"));

            // Act
            var term = await GetTermByIndexViaReflection(node, 5);

            // Assert
            Assert.Equal(0, term);
        }

        [Fact]
        public async Task GetTermByIndex_AtBoundary_ShouldHandleCorrectly()
        {
            // Arrange
            var node = await CreateTestNode();
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(10);
            _mockRaftLog.Setup(x => x.ReadAsync(9))
                .ReturnsAsync(CreateSerializedLogEntry(4, 9));

            // Act
            var term = await GetTermByIndexViaReflection(node, 9);

            // Assert
            Assert.Equal(4, term);
        }

        #endregion

        #region GetLastTermAsync Tests

        [Fact]
        public async Task GetLastTermAsync_WithEmptyLog_ShouldReturnZero()
        {
            // Arrange
            var node = await CreateTestNode();
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(0);

            // Act
            var term = await node.GetLastTermAsync();

            // Assert
            Assert.Equal(0, term);
        }

        [Fact]
        public async Task GetLastTermAsync_WithEntries_ShouldReturnLastTerm()
        {
            // Arrange
            var node = await CreateTestNode();
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(5);
            _mockRaftLog.Setup(x => x.ReadAsync(4)) // LastLogIndex - 1
                .ReturnsAsync(CreateSerializedLogEntry(7, 4));

            // Act
            var term = await node.GetLastTermAsync();

            // Assert
            Assert.Equal(7, term);
        }

        [Fact]
        public async Task GetLastTermAsync_WithNullEntry_ShouldReturnZero()
        {
            // Arrange
            var node = await CreateTestNode();
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(1);
            _mockRaftLog.Setup(x => x.ReadAsync(0))
                .ReturnsAsync(new byte[] { 0xFF }); // Sentinel byte returns null

            // Act
            var term = await node.GetLastTermAsync();

            // Assert
            Assert.Equal(0, term);
        }

        #endregion

        #region Cluster State Query Tests

        [Fact]
        public async Task GetBrokerList_ShouldReturnAllClusterMembers()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            var brokers = node.GetBrokerList();

            // Assert
            Assert.Equal(3, brokers.Count);
            Assert.Contains(brokers, b => b.Id == 2);
            Assert.Contains(brokers, b => b.Id == 3);
        }

        [Fact]
        public async Task GetBrokerList_ShouldIncludeSelf()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            var brokers = node.GetBrokerList();

            // Assert
            Assert.Contains(brokers, b => b.Id == _testBroker.Id);
        }

        [Fact]
        public async Task GetBrokerList_ShouldReturnSameListEachTime()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            var brokers1 = node.GetBrokerList();
            var brokers2 = node.GetBrokerList();

            // Assert
            Assert.Same(brokers1, brokers2);
        }

        [Fact]
        public async Task GetSerializedTopicMetadataAsync_ShouldCallClusterState()
        {
            // Arrange
            var node = await CreateTestNode();
            var expectedData = new byte[] { 1, 2, 3 };
            _mockClusterState.Setup(x => x.GetSerializedTopicMetadataAsync("test-topic"))
                .ReturnsAsync((expectedData, 5));

            // Act
            var (data, count) = await node.GetSerializedTopicMetadataAsync("test-topic");

            // Assert
            Assert.Equal(expectedData, data);
            Assert.Equal(5, count);
            _mockClusterState.Verify(x => x.GetSerializedTopicMetadataAsync("test-topic"), Times.Once);
        }

        [Fact]
        public async Task GetSerializedBrokersAsync_ShouldCallClusterState()
        {
            // Arrange
            var node = await CreateTestNode();
            var expectedData = new byte[] { 4, 5, 6 };
            _mockClusterState.Setup(x => x.GetSerializedBrokersAsync())
                .ReturnsAsync((expectedData, 3));

            // Act
            var (data, count) = await node.GetSerializedBrokersAsync();

            // Assert
            Assert.Equal(expectedData, data);
            Assert.Equal(3, count);
            _mockClusterState.Verify(x => x.GetSerializedBrokersAsync(), Times.Once);
        }

        [Fact]
        public async Task GetListOfPartitionIdsByTopicAndBroker_ShouldCallClusterState()
        {
            // Arrange
            var node = await CreateTestNode();
            var expectedPartitions = new int[] { 0, 1, 2 };
            _mockClusterState.Setup(x => x.GetListofTopicPartitionsByBrokerId("test-topic", 2))
                .ReturnsAsync(expectedPartitions);

            // Act
            var partitions = await node.GetListOfPartitionIdsByTopicAndBroker("test-topic", 2);

            // Assert
            Assert.Equal(expectedPartitions, partitions);
            _mockClusterState.Verify(x => x.GetListofTopicPartitionsByBrokerId("test-topic", 2), Times.Once);
        }

        #endregion

        #region UpdateLastApplied Tests

        [Fact]
        public async Task UpdateLastApplied_ShouldUpdateInternalState()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            await node.UpdateLastApplied(10);

            // Assert
            Assert.Equal(10, node.LastApplied);
        }

        [Fact]
        public async Task UpdateLastApplied_MultipleUpdates_ShouldUseLatestValue()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            await node.UpdateLastApplied(5);
            await node.UpdateLastApplied(10);
            await node.UpdateLastApplied(15);

            // Assert
            Assert.Equal(15, node.LastApplied);
        }

        [Fact]
        public async Task UpdateLastApplied_WithZero_ShouldWork()
        {
            // Arrange
            var node = await CreateTestNode();
            await node.UpdateLastApplied(5);

            // Act
            await node.UpdateLastApplied(0);

            // Assert
            Assert.Equal(0, node.LastApplied);
        }

        #endregion

        #region Property Tests

        [Fact]
        public async Task Id_ShouldReturnBrokerId()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act
            var id = node.Id;

            // Assert
            Assert.Equal(_testBroker.Id, id);
        }

        [Fact]
        public async Task LastLogIndex_ShouldReturnCurrentOffset()
        {
            // Arrange
            _mockRaftLog.Setup(x => x.CurrentOffset).Returns(42);
            var node = await CreateTestNode();

            // Act
            var index = node.LastLogIndex;

            // Assert
            Assert.Equal(42, index);
        }

        [Fact]
        public async Task LastApplied_ShouldInitializeToZero()
        {
            // Arrange & Act
            var node = await CreateTestNode();

            // Assert
            Assert.Equal(0, node.LastApplied);
        }

        [Fact]
        public async Task State_ShouldInitializeToFollower()
        {
            // Arrange & Act
            var node = await CreateTestNode();

            // Assert
            Assert.Equal(NodeState.Follower, node.State);
        }

        [Fact]
        public async Task CurrentTerm_ShouldInitializeToZero()
        {
            // Arrange & Act
            var node = await CreateTestNode();

            // Assert
            Assert.Equal(0, node.CurrentTerm);
        }

        [Fact]
        public async Task VotedFor_ShouldInitializeToNull()
        {
            // Arrange & Act
            var node = await CreateTestNode();

            // Assert
            Assert.Null(node.VotedFor);
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public async Task Dispose_ShouldNotThrow()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act & Assert
            node.Dispose();
        }

        [Fact]
        public async Task Dispose_CalledMultipleTimes_ShouldNotThrow()
        {
            // Arrange
            var node = await CreateTestNode();

            // Act & Assert
            node.Dispose();
            node.Dispose();
            node.Dispose();
        }

        [Fact]
        public async Task Dispose_ShouldDisposeRaftLog()
        {
            // Arrange
            var mockDisposableLog = new Mock<IPartition>();
            var disposeCalled = false;
            mockDisposableLog.Setup(x => x.CurrentOffset).Returns(0);
            mockDisposableLog.As<IDisposable>()
                .Setup(x => x.Dispose())
                .Callback(() => disposeCalled = true);

            var node = await CreateTestNode(mockDisposableLog.Object);

            // Act
            node.Dispose();

            // Assert
            Assert.True(disposeCalled);
        }

        #endregion

        #region Helper Methods

private async Task<RaftNode> CreateTestNode(IPartition partition = null, int term = 0)
{
    var tempPath = Path.Combine(Path.GetTempPath(), $"raft_test_{Guid.NewGuid()}");
    Directory.CreateDirectory(tempPath);

    return await RaftNode.InitializeNode(
        tempPath,
        _testBroker,
        _mockLogger.Object,
        _mockTransport.Object,
        _mockClusterState.Object,
        _clusterMembers,
        partition ?? _mockRaftLog.Object
    );
}

        private byte[] CreateSerializedLogEntry(int term, long index)
        {
            var entry = new LogEntry
            {
                Term = term,
                Index = index,
                Command = null
            };
            return JsonSerializer.SerializeToUtf8Bytes(entry);
        }

        private async Task<int> GetTermByIndexViaReflection(RaftNode node, long index)
        {
            var method = typeof(RaftNode).GetMethod("GetTermByIndex",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            
            return await (Task<int>)method.Invoke(node, new object[] { index });
        }

        #endregion
    }
}