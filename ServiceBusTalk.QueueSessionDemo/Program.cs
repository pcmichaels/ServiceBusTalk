using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusTalk.QueueSessionDemo
{
    class Program
    {
        private static string QUEUE_NAME = "queue-session-demo";
        private static ServiceBusClient _serviceBusClient;

        static async Task Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", true, true)
               .AddUserSecrets<Program>()
               .Build();

            string connectionString = configuration.GetValue<string>("ServiceBusConnectionString");
            _serviceBusClient = new ServiceBusClient(connectionString);

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
                        await SendMessage("Message", "session1");
                        await SendMessage("NoiseMessage", "session2");
                        break;

                    case ConsoleKey.D2:
                        await ReadMessage("session1");
                        break;

                    case ConsoleKey.D3:
                        await SendMessage("High noon oh I'd sell my soul for water, Nine years worth of breaking my back", "session1Send");
                        await ReadMessage("session2Reply");
                        break;

                    case ConsoleKey.D4:
                        await ReadMessage("session1Send");
                        break;

                    case ConsoleKey.D5:
                        await SendMessage("Stargazer, Rainbow", "session2Reply");
                        break;

                }
            }
        }

        private static async Task SendMessage(string messageText, string sessionId)
        {
            var messageSender = _serviceBusClient.CreateSender(QUEUE_NAME);            

            string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
            var message = new ServiceBusMessage(messageBody);
            message.SessionId = sessionId;

            await messageSender.SendMessageAsync(message);
            await messageSender.CloseAsync();
        }

        private static async Task ReadMessage(string sessionId)
        {            
            var sessionReceiver = await _serviceBusClient.AcceptNextSessionAsync(QUEUE_NAME, sessionId);
            var message = await sessionReceiver.ReceiveMessageAsync();

            string messageBody = message.Body.ToString();

            await sessionReceiver.CompleteMessageAsync(message);

            Console.WriteLine("Message received: {0}", messageBody);
        }

    }
}
