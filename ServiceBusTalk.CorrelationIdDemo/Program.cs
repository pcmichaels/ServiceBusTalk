using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusTalk.CorrlationIdDemo
{
    class Program
    {
        private static string QUEUE_NAME = "correlation-id-demo-queue";
        private static string REPLY_QUEUE_NAME = "correlation-id-demo-replyqueue";

        private static string _connectionString;
        private static TaskCompletionSource<bool> _taskCompletionSource;

        private static ServiceBusClient _serviceBusClient;        

        static async Task Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", true, true)
               .AddUserSecrets<Program>()
               .Build();

            _connectionString = configuration.GetValue<string>("ServiceBusConnectionString");
            _serviceBusClient = new ServiceBusClient(_connectionString);

            while (true)
            {
                Console.WriteLine("Choose Action:");
                Console.WriteLine("1: Send Message and await reply");
                Console.WriteLine("2: Reply to Message");
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await SendMessageAndAwaitReply();
                        Console.WriteLine("Press any key to end");
                        Console.ReadKey();
                        break;

                    case ConsoleKey.D2:
                        await ReplyToMessage();
                        break;

                }

            }
        }

        private static async Task ReplyToMessage()
        {
            Console.WriteLine("Correlation Id: ");
            string correlationId = Console.ReadLine();

            await SendMessage(REPLY_QUEUE_NAME, "Motorhead", correlationId);
        }

        private static async Task SendMessageAndAwaitReply()
        {
            _taskCompletionSource = new TaskCompletionSource<bool>();

            string correlationId = Guid.NewGuid().ToString();
            Console.WriteLine($"Correlation Id {correlationId}");
            await SendMessage(QUEUE_NAME, "What is the greatest band of all time?", correlationId);
            await ReadMessage(REPLY_QUEUE_NAME, correlationId);
        }

        private static async Task ReadMessage(string queue, string correlationId)
        {
            var options = new ServiceBusProcessorOptions()
            {
                AutoCompleteMessages = false
            };
            var messageProcessor = _serviceBusClient.CreateProcessor(queue, options);

            messageProcessor.ProcessMessageAsync += (arg) => handleMessage(arg, correlationId);
            messageProcessor.ProcessErrorAsync += ExceptionHandler;

            await messageProcessor.StartProcessingAsync();
            await _taskCompletionSource.Task;
            await messageProcessor.StopProcessingAsync();
        }

        private static Task ExceptionHandler(ProcessErrorEventArgs arg)
        {
            Console.WriteLine("Something bad happened!");
            Console.WriteLine($"Error: {arg.Exception.Message}");
            return Task.CompletedTask;
        }

        private static async Task handleMessage(ProcessMessageEventArgs arg, string correlationId)
        {            
            string messageBody = arg.Message.Body.ToString();
            Console.WriteLine("Message received: {0}", messageBody);

            
            if (arg.Message.CorrelationId == correlationId)
            {
                Console.WriteLine("Received MATCHING reply");
                await arg.CompleteMessageAsync(arg.Message);                

                _taskCompletionSource.SetResult(true);
            }
            else
            {
                Console.WriteLine("Received NOT MATCHING reply");                
            }            
        }

        private static async Task SendMessage(string queueName, string text, string correlationId)
        {
            Console.WriteLine($"Sending Message: {text} to {queueName}");

            var sender = _serviceBusClient.CreateSender(queueName);                        
            var message = new ServiceBusMessage(text);
            message.CorrelationId = correlationId;

            await sender.SendMessageAsync(message);

            await sender.CloseAsync();
        }
    }
}
