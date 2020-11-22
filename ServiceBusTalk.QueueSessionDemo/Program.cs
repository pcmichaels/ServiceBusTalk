using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusTalk.QueueSessionDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", true, true)
               .AddUserSecrets<Program>()
               .Build();

            string connectionString = configuration.GetValue<string>("ServiceBusConnectionString");

            while (true)
            {
                Console.WriteLine("Choose Action:");
                Console.WriteLine("1: Send Messages");                
                Console.WriteLine("2: Receive Messages session1");
                Console.WriteLine("3: Send and await reply");
                Console.WriteLine("4: Receive Messages session1Send");
                Console.WriteLine("5: Reply");
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await SendMessage(connectionString, "Message", "session1");
                        await SendMessage(connectionString, "NoiseMessage", "session2");
                        break;

                    case ConsoleKey.D2:
                        await ReadMessage(connectionString, "session1");
                        break;

                    case ConsoleKey.D3:
                        await SendMessage(connectionString, "High noon oh I'd sell my soul for water, Nine years worth of breaking my back", "session1Send");
                        await ReadMessage(connectionString, "session2Reply");
                        break;

                    case ConsoleKey.D4:
                        await ReadMessage(connectionString, "session1Send");
                        break;

                    case ConsoleKey.D5:
                        await SendMessage(connectionString, "Stargazer, Rainbow", "session2Reply");
                        break;

                }
            }
        }

        private static async Task SendMessage(string connectionString, string messageText, string sessionId)
        {
            var queueClient = new QueueClient(connectionString, "session-queue-test");

            string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
            var message = new Message(Encoding.UTF8.GetBytes(messageBody));
            message.SessionId = sessionId;

            await queueClient.SendAsync(message);
            await queueClient.CloseAsync();
        }

        private static async Task ReadMessage(string connectionString, string sessionId)
        {            
            var sessionClient = new SessionClient(connectionString, "session-queue-test");
            var session = await sessionClient.AcceptMessageSessionAsync(sessionId);

            var message = await session.ReceiveAsync();
            await session.CompleteAsync(message.SystemProperties.LockToken);

            string messageBody = Encoding.UTF8.GetString(message.Body);

            Console.WriteLine("Message received: {0}", messageBody);
        }

    }
}
