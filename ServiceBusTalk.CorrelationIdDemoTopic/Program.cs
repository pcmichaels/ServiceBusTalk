using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
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
            string correlationId = Console.ReadLine();

            await SendMessage("topictest", "MyUniqueClientName", "Motorhead", correlationId);
        }

        private static async Task SendMessageAndAwaitReply()
        {
            _taskCompletionSource = new TaskCompletionSource<bool>();

            string correlationId = Guid.NewGuid().ToString();
            Console.WriteLine($"Correlation Id {correlationId}");
            await SendMessage("topictest", "", "What is the greatest band of all time?", correlationId);
            await ReadMessage("topictest", correlationId);            
        }

        private static async Task ReadMessage(string topicName, string correlationId)
        {
            string subscriptionName = "ReplySubscription";            
            var subscriptionClient = new SubscriptionClient(_connectionString, topicName, subscriptionName);

            var rules = await subscriptionClient.GetRulesAsync();
            if (rules.Any(a => a.Name == "OnlyToMe"))
                await subscriptionClient.RemoveRuleAsync("OnlyToMe");
            if (rules.Any(a => a.Name == RuleDescription.DefaultRuleName))
                await subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName);

            var filter = new CorrelationFilter();
            filter.To = "MyUniqueClientName";
            var ruleDescription = new RuleDescription("OnlyToMe", filter);
            await subscriptionClient.AddRuleAsync(ruleDescription);
            
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionHandler)
            {
                AutoComplete = false
            };
            subscriptionClient.RegisterMessageHandler(
                (message, cancellationToken) => handleMessage(correlationId, subscriptionClient, message, cancellationToken),
                messageHandlerOptions);

            await _taskCompletionSource.Task;

            await subscriptionClient.UnregisterMessageHandlerAsync(
                new TimeSpan(0, 0, 20));
        }

        private static Task ExceptionHandler(ExceptionReceivedEventArgs arg)
        {
            Console.WriteLine("Something bad happened!");
            Console.WriteLine($"Error: {arg.Exception.Message}");
            return Task.CompletedTask;
        }

        private static async Task handleMessage(string correlationId, SubscriptionClient client, Message message, CancellationToken cancellation)
        {
            string messageBody = Encoding.UTF8.GetString(message.Body);
            Console.WriteLine("Message received: {0}", messageBody);

            if (message.CorrelationId == correlationId)
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

        private static async Task SendMessage(string topicName, string to, string text, string correlationId)
        {
            Console.WriteLine($"Sending Message: {text} to {topicName}");

            var topicClient = new TopicClient(_connectionString, topicName);

            var message = new Message(Encoding.UTF8.GetBytes(text));
            message.CorrelationId = correlationId;
            message.To = to;

            await topicClient.SendAsync(message);

            await topicClient.CloseAsync();
        }
    }
}
