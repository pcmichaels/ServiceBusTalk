using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;

namespace ServiceBusTalk.QueueClientDemo
{
    class Program
    {
        private static string QUEUE_NAME = "queue-client-demo";
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
                Console.WriteLine("2: Receive Messages (Events)");
                Console.WriteLine("3: Receive Messages (Direct)");
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
                        await ReadMessageEvent();
                        break;

                    case ConsoleKey.D3:
                        await ReadMessage();
                        break;

                }

            }
        }

        private static async Task ReadMessage()
        {
            var options = new ServiceBusReceiverOptions()
            {
                //ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
            };
            var messageReceiver = _serviceBusClient.CreateReceiver(QUEUE_NAME, options);            
            var message = await messageReceiver.ReceiveMessageAsync();

            //string messageBody = Encoding.UTF8.GetString(message.Body);
            string messageBody = message.Body.ToString();

            Console.WriteLine("Message received: {0}", messageBody);
        }

        private static async Task ReadMessageEvent()
        {
            var options = new ServiceBusProcessorOptions()
            {
                //ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
                AutoCompleteMessages = false
            };
            var processor = _serviceBusClient.CreateProcessor(QUEUE_NAME, options);
            processor.ProcessMessageAsync += handleMessage;
            processor.ProcessErrorAsync += ExceptionHandler;

            await processor.StartProcessingAsync();                        

            await Task.Delay(2000);
            await processor.StopProcessingAsync();

        }

        private static Task ExceptionHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine("Something bad happened!");
            return Task.CompletedTask;
        }

        private static Task handleMessage(ProcessMessageEventArgs args)
        {
            string messageBody = args.Message.ToString();
            Console.WriteLine("Message received: {0}", messageBody);

            //throw new Exception("Something was wrong with the message!");

            //await args.CompleteMessageAsync(args.Message);

            return Task.CompletedTask;
        }

        private static async Task SendMessage(int count)
        {
            var queueClient = _serviceBusClient.CreateSender(QUEUE_NAME);

            for (int i = 1; i <= count; i++)
            {
                string messageBody = $"{DateTime.Now}: Hello Everybody! ({Guid.NewGuid()})";
                var message = new ServiceBusMessage(messageBody);

                await queueClient.SendMessageAsync(message);
            }
            await queueClient.CloseAsync();
        }
    }
}
