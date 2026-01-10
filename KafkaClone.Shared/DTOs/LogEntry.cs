using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaClone.Server.DTOs
{

//-------------------------------- LOG TYPES ------------------------------------------------

    // 1. The Interface
    public interface IClusterCommand
    {
        string CommandType { get; }
    }

    // 2. Create Topic
    public class CreateTopic : IClusterCommand
    {
        public string CommandType => "CreateTopic";
        public string Name { get; set; }
        public int Partitions { get; set; }

        public Dictionary<int, int> Assignments { get; set; } = new(); 
        public override string ToString()
        {
            return $"[CreateTopic] Name={Name}, Partitions={Partitions}, AssgnCount={Assignments?.Count ?? 0}";
        }

    }

    // 4. Register Broker
    public class RegisterBroker : IClusterCommand
    {
        public string CommandType => "RegisterBroker";
        public int Id { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public int GrpcPort { get; set; }

        public override string ToString()
        {
            return $"[RegisterBroker] Id={Id}, Addr={Host}:{Port}/{GrpcPort}";
        }
    }

    //-------------------------------- FORWARD COMMAND RESPONSE ----------------------------
        public class ForwardCommandResponse
            {
            public bool Success { get; set; }
            public string? ErrorMessage { get; set; }

            public override string ToString()
        {
            return Success ? "[ForwardResponse] OK" : $"[ForwardResponse] FAIL: {ErrorMessage}";
        }

            }


    //-------------------------------- LOG ENTRY ----------------------------
    public class LogEntry
    {
        public int Term { get; set; }
        public long Index {get;set;}
        public IClusterCommand Command { get; set; } 

        public override string ToString()
        {
            // Helper to get a short description of the command
            string cmdDesc = Command?.ToString() ?? "null";
            return $"[LogEntry] Idx={Index}, Term={Term}, Cmd={cmdDesc}";
        }
    }


}