using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;


namespace KafkaClone.Shared;

public class ClusterController
{

    private Dictionary<int,Broker> _brokers;

    public int brokersCount =>_brokers.Count;



    public ClusterController()
    {
        _brokers = new Dictionary<int, Broker>();
    }

    public void RegisterBroker(Broker broker)
    {
        if (_brokers.ContainsKey(broker.Id))
        {
            Console.WriteLine("Already contains this broker");
            return;
        }

        _brokers[broker.Id] = broker;
    }

    public Broker? GetBroker(int id)
    {
        if (_brokers.TryGetValue(id, out Broker? broker))
        {
            return broker;
        }
        return null;
    }


    public async Task<(byte[],int)> GetAllBrokers()
    {
        List<Broker> snapshot = _brokers.Values.ToList();
        int count = snapshot.Count;

        var ms = new MemoryStream();

        foreach(var broker in snapshot)
        {
            byte[] currentBroker = broker.Serialize();
            await ms.WriteAsync(currentBroker);
        }

        return (ms.ToArray(),count);
    }
    
}