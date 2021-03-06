﻿using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusTalk.CorrlationIdDemo
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

            await SendMessage("reply-queue", "Motorhead", correlationId);
        }

        private static async Task SendMessageAndAwaitReply()
        {
            _taskCompletionSource = new TaskCompletionSource<bool>();

            string correlationId = Guid.NewGuid().ToString();
            Console.WriteLine($"Correlation Id {correlationId}");
            await SendMessage("test-queue", "What is the greatest band of all time?", correlationId);
            await ReadMessage("reply-queue", correlationId);
        }

        private static async Task ReadMessage(string queue, string correlationId)
        {
            var queueClient = new QueueClient(_connectionString, queue);

            var messageHandlerOptions = new MessageHandlerOptions(ExceptionHandler)
            {
                AutoComplete = false
            };
            queueClient.RegisterMessageHandler(
                (message, cancellationToken) => handleMessage(correlationId, queueClient, message, cancellationToken), 
                messageHandlerOptions);

            await _taskCompletionSource.Task;

            await queueClient.UnregisterMessageHandlerAsync(
                new TimeSpan(0, 0, 20));
        }

        private static Task ExceptionHandler(ExceptionReceivedEventArgs arg)
        {
            Console.WriteLine("Something bad happened!");
            Console.WriteLine($"Error: {arg.Exception.Message}");
            return Task.CompletedTask;
        }

        private static async Task handleMessage(string correlationId, QueueClient client, Message message, CancellationToken cancellation)
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

        private static async Task SendMessage(string queueName, string text, string correlationId)
        {
            Console.WriteLine($"Sending Message: {text} to {queueName}");

            var queueClient = new QueueClient(_connectionString, queueName);
            
            var message = new Message(Encoding.UTF8.GetBytes(text));
            message.CorrelationId = correlationId;

            await queueClient.SendAsync(message);

            await queueClient.CloseAsync();
        }
    }
}
