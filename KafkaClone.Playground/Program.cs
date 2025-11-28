using System.Text;
using System.Threading.Tasks;
using KafkaClone.Storage;


string directory = Directory.GetCurrentDirectory();
string fullPath = Path.Combine(directory, "test.log");

Console.WriteLine($"Saving logs to: {fullPath}");

using LogSegment logSegment = new LogSegment(fullPath);

string test1 = "Hello World";
string test2 = "Goodbye World";

byte[] data1 = Encoding.UTF8.GetBytes(test1);
byte[] data2 = Encoding.UTF8.GetBytes(test2);

long position1 = await logSegment.AppendAsync(data1);
long position2 = await logSegment.AppendAsync(data2);

byte[] readBytes1 = await logSegment.ReadAsync(0);
byte[] readBytes2 = await logSegment.ReadAsync(position1);

string result1 = Encoding.UTF8.GetString(readBytes1);
string result2 = Encoding.UTF8.GetString(readBytes2);

Console.WriteLine(result1);
Console.WriteLine(result2);