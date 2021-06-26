using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusTalk.QueueDeferralDemo
{
    class Program
    {
        private static string QUEUE_NAME = "deferral-queue";
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
                Console.WriteLine("1: Receive Messages (Events)");
                Console.WriteLine("2: Send Messages");                
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:                        
                        await ReadMessageEvent();
                        break;

                    case ConsoleKey.D2:
                        await SendScheduledMessage(connectionString, DateTime.UtcNow.AddSeconds(10));
                        break;

                }

            }
        }

        private static Task ReadMessageEvent()
        {
            var messageProcessor = _serviceBusClient.CreateProcessor(QUEUE_NAME);

            messageProcessor.ProcessMessageAsync += handleMessage;
            messageProcessor.ProcessErrorAsync += ExceptionHandler;

            return Task.CompletedTask;
        }

        private static Task ExceptionHandler(ProcessErrorEventArgs arg)
        {
            Console.WriteLine("Something bad happened!");
            return Task.CompletedTask;
        }

        private static Task handleMessage(ProcessMessageEventArgs arg)
        {
            string messageBody = arg.Message.Body.ToString();
            Console.WriteLine("Message received: {0}", messageBody);

            return Task.CompletedTask;
        }

        private static async Task SendScheduledMessage(string connectionString, DateTime dateTime)
        {
            var messageSender = _serviceBusClient.CreateSender(QUEUE_NAME);            

            string messageBody = $"{DateTime.Now}: Hello Everybody! ({Guid.NewGuid()}) You won't get this until {dateTime}";
            var message = new ServiceBusMessage(messageBody);

            long sequenceNumber = await messageSender.ScheduleMessageAsync(message, dateTime);
            //await queueClient.CancelScheduledMessageAsync(sequenceNumber);

            await messageSender.CloseAsync();
        }
    }
}
