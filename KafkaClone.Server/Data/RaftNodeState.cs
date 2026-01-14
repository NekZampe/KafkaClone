using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaClone.Server.DTOs;
using System.Text.Json;

namespace KafkaClone.Server
{
    public class RaftNodeState
    {
        private readonly string _raftNodeJsonPath;

       public RaftNodeData RaftNodeData { get; private set; }

    // Private constructor
    private RaftNodeState(string raftNodeDirectory)
    {
        _raftNodeJsonPath = Path.Combine(raftNodeDirectory, "nodestate.json");
    }

    // ---------- FACTORIES ----------

    // Create new state and save it
    public static async Task<RaftNodeState> CreateAsync(string raftNodeDirectory,RaftNodeData nodeData)
    {
        var state = new RaftNodeState(raftNodeDirectory)
        {
            RaftNodeData = nodeData
        };

        await state.SaveAsync();
        return state;
    }


        public static async Task SaveStateAsync(string raftNodeDirectory,RaftNodeData nodeData)
    {
        var state = new RaftNodeState(raftNodeDirectory)
        {
            RaftNodeData = nodeData
        };

        await state.SaveAsync();
    }

    // Load existing state
    public static async Task<RaftNodeState?> LoadAsync(string raftNodeDirectory)
    {
        var state = new RaftNodeState(raftNodeDirectory);

        if (!File.Exists(state._raftNodeJsonPath))
            return null;

        await state.LoadInternalAsync();
        return state;
    }

    // ---------- PERSISTENCE ----------

    private async Task LoadInternalAsync()
    {
        using FileStream fs = File.OpenRead(_raftNodeJsonPath);

        RaftNodeData? raftNodeData =
            await JsonSerializer.DeserializeAsync<RaftNodeData>(fs);

        if (RaftNodeData is null)
            throw new InvalidDataException("state.json is empty or invalid");

        RaftNodeData = raftNodeData;
    }

    public async Task SaveAsync()
    {
        using FileStream fs = File.Create(_raftNodeJsonPath);

        await JsonSerializer.SerializeAsync(fs, RaftNodeData);
    }
    }
}
