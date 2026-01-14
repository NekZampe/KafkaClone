using KafkaClone.Server;
using KafkaClone.Service;
using KafkaClone.Storage;
using KafkaClone.Shared;
using KafkaClone.Server.DTOs;
using KafkaClone.Shared.Grpc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Grpc.Core;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Builder;

namespace KafkaClone;

class Program
{
    static async Task Main(string[] args)
    {
        // ===================================================================
        // CONFIGURATION
        // ===================================================================
        
        int brokerId = int.Parse(args.Length > 0 ? args[0] : "0");
        string hostName = args.Length > 1 ? args[1] : "localhost";
        int tcpPort = int.Parse(args.Length > 2 ? args[2] : "9092");
        int grpcPort = int.Parse(args.Length > 3 ? args[3] : "6000");
        string basePath = args.Length > 4 ? args[4] : 
            Path.Combine(Directory.GetCurrentDirectory(), $"kafka-data/broker_{brokerId}");

        // Bootstrap cluster configuration (all brokers in the cluster)
        var bootstrapCluster = new List<Broker>
        {
            new Broker(0, 9092, 6000, "localhost"), // TCP: 9092, gRPC: 6000
            new Broker(1, 9093, 6001, "localhost"), // TCP: 9093, gRPC: 6001
            new Broker(2, 9094, 6002, "localhost")  // TCP: 9094, gRPC: 6002
        };

        // Create data directory
        Directory.CreateDirectory(basePath);
        
        Console.WriteLine("===========================================");
        Console.WriteLine($"       Starting Broker {brokerId}");
        Console.WriteLine("===========================================");
        Console.WriteLine($"Data Plane (TCP):      {hostName}:{tcpPort}");
        Console.WriteLine($"Control Plane (gRPC):  {hostName}:{grpcPort}");
        Console.WriteLine($"Data Path:             {basePath}");
        Console.WriteLine("===========================================\n");

        // ===================================================================
        // BUILD HOST WITH DEPENDENCY INJECTION
        // ===================================================================
        
        var hostBuilder = Host.CreateDefaultBuilder(args)
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Information);
            })
            .ConfigureServices((context, services) =>
            {
                // === IDENTITY ===
                var myIdentity = new Broker(brokerId, tcpPort, grpcPort, hostName);
                services.AddSingleton(myIdentity);

                // === CLUSTER STATE (Metadata Brain) ===
                services.AddSingleton(sp => 
                {
                    var logger = sp.GetRequiredService<ILogger<ClusterState>>();
                    Console.WriteLine("[Init] Loading ClusterState...");
                    var task = ClusterState.InitializeAsync(basePath);
                    var state = task.GetAwaiter().GetResult();
                    Console.WriteLine($"[Init] ✓ ClusterState loaded");
                    return state;
                });

                // === RAFT TRANSPORT (gRPC Clients to Other Brokers) ===
                services.AddSingleton<IRaftTransport>(sp =>
                {
                    var otherBrokers = bootstrapCluster
                        .Where(b => b.Id != brokerId)
                        .ToList();
                    
                    Console.WriteLine($"[Init] Connecting to {otherBrokers.Count} peer brokers...");
                    var transport = new GrpcRaftTransport(otherBrokers);
                    Console.WriteLine($"[Init] ✓ RaftTransport initialized");
                    return transport;
                });

                string logFolder = Path.Combine(basePath, "__nodeLogEntries__");

                // === RAFT NODE (Consensus Engine) ===
                services.AddSingleton(sp =>
                {
                    var partitionLogger = sp.GetRequiredService<ILogger<Partition>>();
                    var transport = sp.GetRequiredService<IRaftTransport>();
                    var clusterState = sp.GetRequiredService<ClusterState>();
                    var identity = sp.GetRequiredService<Broker>();
                    Partition partition = new Partition( 0, logFolder, false, partitionLogger, TimeSpan.FromHours(5), 1024);
                    
                    Console.WriteLine("[Init] Initializing RaftNode...");

                    var otherBrokers = bootstrapCluster
                    .Where(b => b.Id != brokerId)
                    .ToList();

                    var task = RaftNode.InitializeNode(
                        basePath,
                        identity,
                        partitionLogger,
                        transport,
                        clusterState,
                        otherBrokers,
                        partition
                    );
                    
                    var raftNode = task.GetAwaiter().GetResult();
                    Console.WriteLine($"[Init] ✓ RaftNode initialized");
                    return raftNode;
                });

                // === STORAGE LAYER ===
                services.AddSingleton<TopicManager>(sp => 
                {
                    var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                    Console.WriteLine("[Init] Initializing TopicManager...");
                    var task = TopicManager.InitializeAsync(basePath, loggerFactory, defaultPartitions: 1);
                    var manager = task.GetAwaiter().GetResult();
                    Console.WriteLine($"[Init] ✓ TopicManager initialized");
                    return manager;
                });
                
                services.AddSingleton<OffsetManager>(sp =>
                {
                    Console.WriteLine("[Init] Initializing OffsetManager...");
                    var manager = new OffsetManager(basePath);
                    Console.WriteLine($"[Init] ✓ OffsetManager initialized");
                    return manager;
                });

                // === BROKER SERVICE (Orchestrator) ===
                services.AddSingleton<BrokerService>();

                // === BACKGROUND SERVICES ===
                services.AddHostedService<BrokerServiceLifecycle>();
                services.AddHostedService<TcpServerService>();
                
                // === GRPC SERVER ===
                services.AddGrpc();
            })
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseKestrel(options =>
                {
                    // Configure gRPC port (HTTP/2)
                    options.ListenAnyIP(grpcPort, listenOptions =>
                    {
                        listenOptions.Protocols = 
                            Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2;
                    });
                });
                
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        endpoints.MapGrpcService<RaftGrpcService>();
                    });
                });
            })
            .Build();

        // ===================================================================
        // POST-INITIALIZATION: Bootstrap Registration
        // ===================================================================
        
        Console.WriteLine("\n[Bootstrap] Waiting for leader election...");
        await Task.Delay(3000); // Allow time for Raft elections
        
        var brokerService = hostBuilder.Services.GetRequiredService<BrokerService>();
        var identity = hostBuilder.Services.GetRequiredService<Broker>();
        
        var registerCmd = new RegisterBroker
        {
            Id = identity.Id,
            Host = identity.Host,
            Port = identity.Port,
            GrpcPort = identity.GrpcPort 
        };
        
        Console.WriteLine($"[Bootstrap] Registering broker {identity.Id} with cluster...");

        var raftNode = hostBuilder.Services.GetRequiredService<RaftNode>();
        var result = await raftNode.Propose(registerCmd);
        
        if (result.Success)
        {
            Console.WriteLine($"[Bootstrap] ✓ Successfully registered with cluster");
        }
        else
        {
            Console.WriteLine($"[Bootstrap] ✗ Registration failed: {result.ErrorMessage}");
            Console.WriteLine($"[Bootstrap] (This is normal if cluster is still electing a leader)");
        }

        // ===================================================================
        // START THE BROKER
        // ===================================================================
        
        Console.WriteLine("\n===========================================");
        Console.WriteLine($"   Broker {brokerId} is READY!");
        Console.WriteLine("===========================================\n");
        
        await hostBuilder.RunAsync();
    }
}

// ===================================================================
// BACKGROUND SERVICE: Manages BrokerService Apply Loop Lifecycle
// ===================================================================
public class BrokerServiceLifecycle : IHostedService
{
    private readonly BrokerService _brokerService;
    private readonly ILogger<BrokerServiceLifecycle> _logger;

    public BrokerServiceLifecycle(
        BrokerService brokerService,
        ILogger<BrokerServiceLifecycle> logger)
    {
        _brokerService = brokerService;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("▶  Starting BrokerService apply loop");
        _brokerService.Start();
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("⏹  Stopping BrokerService apply loop");
        await _brokerService.Stop();
    }
}