using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusTalk.CorrlationIdDemo
{
    class Program
    {
        private static string _correlationId;
        private static string _connectionString;
        private static TaskCompletionSource<bool> _taskCompletionSource;

        static async Task Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", true, true)
               .AddUserSecrets<Program>()
               .Build();

            _connectionString = configuration.GetValue<string>("ServiceBusConnectionString");

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
            _correlationId = Console.ReadLine();

            await SendMessage("reply-queue", "Motorhead");
        }

        private static async Task SendMessageAndAwaitReply()
        {
            _taskCompletionSource = new TaskCompletionSource<bool>();

            _correlationId = Guid.NewGuid().ToString();
            Console.WriteLine($"Correlation Id {_correlationId}");
            await SendMessage("test-queue", "What is the greatest band of all time?");
            await ReadMessage("reply-queue");
        }

        private static async Task ReadMessage(string queue)
        {
            var queueClient = new QueueClient(_connectionString, queue);

            var messageHandlerOptions = new MessageHandlerOptions(ExceptionHandler)
            {
                AutoComplete = false
            };
            queueClient.RegisterMessageHandler(
                (message, cancellationToken) => handleMessage(queueClient, message, cancellationToken), 
                messageHandlerOptions);

            await _taskCompletionSource.Task;
        }

        private static Task ExceptionHandler(ExceptionReceivedEventArgs arg)
        {
            Console.WriteLine("Something bad happened!");
            return Task.CompletedTask;
        }

        private static async Task handleMessage(QueueClient client, Message message, CancellationToken cancellation)
        {
            string messageBody = Encoding.UTF8.GetString(message.Body);
            Console.WriteLine("Message received: {0}", messageBody);

            if (message.CorrelationId == _correlationId)
            {
                Console.WriteLine("Received MATCHING reply");
                await client.CompleteAsync(message.SystemProperties.LockToken);

                _taskCompletionSource.SetResult(true);
            }
            else
            {
                Console.WriteLine("Received NOT MATCHING reply");                
            }            
        }

        private static async Task SendMessage(string queueName, string text)
        {
            Console.WriteLine($"Sending Message: {text} to {queueName}");

            var queueClient = new QueueClient(_connectionString, queueName);
            
            var message = new Message(Encoding.UTF8.GetBytes(text));
            message.CorrelationId = _correlationId.ToString();

            await queueClient.SendAsync(message);

            await queueClient.CloseAsync();
        }
    }
}
