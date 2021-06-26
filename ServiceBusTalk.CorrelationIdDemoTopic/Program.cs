using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Configuration;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusTalk.CorrelationIdDemoTopic
{
    class Program
    {
        private static string TOPIC_NAME = "topic-correlationid-demo";

        private static string _connectionString;
        private static TaskCompletionSource<bool> _taskCompletionSource;

        private static ServiceBusClient _serviceBusClient;
        private static ServiceBusAdministrationClient _adminClient;

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

            await SendMessage(TOPIC_NAME, "MyUniqueClientName", "Motorhead", correlationId);
        }

        private static async Task SendMessageAndAwaitReply()
        {
            _taskCompletionSource = new TaskCompletionSource<bool>();

            string correlationId = Guid.NewGuid().ToString();
            Console.WriteLine($"Correlation Id {correlationId}");
            await SendMessage(TOPIC_NAME, "", "What is the greatest band of all time?", correlationId);
            await ReadMessage(TOPIC_NAME, correlationId);            
        }

        private static async Task ReadMessage(string topicName, string correlationId)
        {
            string subscriptionName = "ReplySubscription";

            var subscription = await _adminClient.GetSubscriptionAsync(topicName, subscriptionName);

            await _adminClient.DeleteRuleAsync(topicName, subscriptionName, "OnlyToMe");
            await _adminClient.DeleteRuleAsync(topicName, subscriptionName, RuleProperties.DefaultRuleName);

            var filter = new CorrelationRuleFilter(correlationId);
            filter.To = "MyUniqueClientName";
            var createRuleOptions = new CreateRuleOptions(
                "OnlyToMe", filter);

            await _adminClient.CreateRuleAsync(topicName, subscriptionName, createRuleOptions);

            var options = new ServiceBusProcessorOptions()
            {
                AutoCompleteMessages = false
            };
            var messageProcessor = _serviceBusClient.CreateProcessor(topicName, options);

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

            Console.WriteLine("Received MATCHING reply");
            await arg.CompleteMessageAsync(arg.Message);

            _taskCompletionSource.SetResult(true);
        }

        private static async Task SendMessage(string topicName, string to, string text, string correlationId)
        {
            Console.WriteLine($"Sending Message: {text} to {topicName}");

            var messageSender = _serviceBusClient.CreateSender(topicName);            

            var message = new ServiceBusMessage(text);
            message.CorrelationId = correlationId;
            message.To = to;

            await messageSender.SendMessageAsync(message);
            await messageSender.CloseAsync();
        }
    }
}
