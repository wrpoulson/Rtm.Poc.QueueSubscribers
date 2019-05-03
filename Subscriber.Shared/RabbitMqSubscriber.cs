using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;

namespace Subscriber.Shared
{
  public class RabbitMqSubscriber 
  {
    public string ExchangeName { get; set; }

    public string QueueName { get; set; }

    public string RoutingKey { get; set; }

    public bool EnableConsoleOutput { get; set; }

    private int MessagesReceivedCount = 0;

    public RabbitMqSubscriber(Settings settings)
    {
      QueueName = settings.QueueName;
      ExchangeName = settings.ExchangeName;
      RoutingKey = settings.RoutingKey;
      EnableConsoleOutput = settings.EnableConsoleOutput;
    }

    public void Start()
    {
      Log.Information($"Started listening for work on queue: {QueueName}");
      Console.WriteLine($" Started listening for work on queue: {QueueName}");

      var factory = new ConnectionFactory() { HostName = "localhost" };
      using (var connection = factory.CreateConnection())
      using (var channel = connection.CreateModel())
      {
        channel.QueueDeclare(
          queue: QueueName,
          durable: true,
          exclusive: false,
          autoDelete: false,
          arguments: null
        );

        var consumer = new EventingBasicConsumer(channel);

        consumer.Received += (model, ea) =>
        {
          var body = ea.Body;
          var message = Encoding.UTF8.GetString(body);
          if(EnableConsoleOutput) Console.WriteLine($" [x] Received {message}");
          Log.Information($"Received message: {message}");
          MessagesReceivedCount++;
        };

        channel.BasicConsume(
          queue: QueueName,
          autoAck: true,
          consumer: consumer
        );

        Console.WriteLine("\n Press 'q' to close application.\n");
        while (Console.Read() != 'q') ;
        Log.Information($"Total messages received: {MessagesReceivedCount}");
      }
    }
  }
}
