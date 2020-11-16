using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusTalk.TopicsClientDemo
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
                Console.WriteLine("2: Receive Messages");
                Console.WriteLine("0: Exit");
                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await SendMessage(connectionString, 100);
                        break;

                    case ConsoleKey.D2:
                        await ReadMessage(connectionString);
                        break;
                }
            }
        }

        private static Task ReadMessage(string connectionString)
        {
            string subscriptionName = "TestSubscription";
            var subscriptionClient = new SubscriptionClient(connectionString, "topictest", subscriptionName);

            var messageHandlerOptions = new MessageHandlerOptions(ExceptionHandler);
            subscriptionClient.RegisterMessageHandler(handleMessage, messageHandlerOptions);

            return Task.CompletedTask;
        }

        private static Task ExceptionHandler(ExceptionReceivedEventArgs arg)
        {
            Console.WriteLine("Something bad happened!");
            return Task.CompletedTask;
        }

        private static Task handleMessage(Message message, CancellationToken cancellation)
        {
            string messageBody = Encoding.UTF8.GetString(message.Body);
            Console.WriteLine("Message received: {0}", messageBody);

            return Task.CompletedTask;
        }

        private static async Task SendMessage(string connectionString, int count)
        {            
            var topicClient = new TopicClient(connectionString, "topictest");

            for (int i = 1; i <= count; i++)
            {
                string messageBody = $"{DateTime.Now}: Hello Everybody! ({Guid.NewGuid()})";
                var message = new Message(Encoding.UTF8.GetBytes(messageBody));

                await topicClient.SendAsync(message);
            }
            await topicClient.CloseAsync();
        }

    }
}
