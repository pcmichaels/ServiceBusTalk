using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;

namespace ServiceBusTalk.TopicsClientDemo
{
    class Program
    {
        private static string TOPIC_NAME = "topic-client-demo";
        private static string SUBSCRIPTION_NAME = "TestSubscription";
        private static ServiceBusClient _serviceBusClient;
        private static ServiceBusProcessor _serviceBusProcessor;

        static async Task Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", true, true)
               .AddUserSecrets<Program>()
               .Build();

            string connectionString = configuration.GetValue<string>("ServiceBusConnectionString");
            _serviceBusClient = new ServiceBusClient(connectionString);
            _serviceBusProcessor = _serviceBusClient.CreateProcessor(TOPIC_NAME, SUBSCRIPTION_NAME);
            _serviceBusProcessor.ProcessMessageAsync += handleMessage;
            _serviceBusProcessor.ProcessErrorAsync += ExceptionHandler;

            while (true)
            {
                Console.WriteLine("Choose Action:");
                Console.WriteLine("1: Send Messages");
                Console.WriteLine("2: Receive Messages");
                Console.WriteLine("3: Stop Receive Messages");
                Console.WriteLine("0: Exit");
                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await SendMessage(100);
                        break;

                    case ConsoleKey.D2:
                        await StartReadMessage();
                        break;

                    case ConsoleKey.D3:
                        await StopReadMessage();
                        break;

                }
            }
        }

        private static async Task StopReadMessage()
        {
            await _serviceBusProcessor.StopProcessingAsync();
        }

        private static async Task StartReadMessage()
        {
            await _serviceBusProcessor.StartProcessingAsync();            
        }

        private static Task ExceptionHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine("Something bad happened!");
            return Task.CompletedTask;
        }

        private static Task handleMessage(ProcessMessageEventArgs args)
        {
            string messageBody = args.Message.Body.ToString();
            Console.WriteLine("Message received: {0}", messageBody);

            return Task.CompletedTask;
        }

        private static async Task SendMessage(int count)
        {
            var sender = _serviceBusClient.CreateSender(TOPIC_NAME);            

            for (int i = 1; i <= count; i++)
            {
                string messageBody = $"{DateTime.Now}: Hello Everybody! ({Guid.NewGuid()})";
                var message = new ServiceBusMessage(messageBody);

                await sender.SendMessageAsync(message);
            }
            await sender.CloseAsync();
        }

    }
}
