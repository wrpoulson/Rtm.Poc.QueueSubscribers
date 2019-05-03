using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Serilog;
using Subscriber.Shared;

namespace Subscriber.Charlie
{
  class Program
  {
    static void Main(string[] args)
    {
      Console.Title = typeof(Program).Assembly.GetName().Name;
      Log.Logger = new LoggerConfiguration()
        .WriteTo.File($"C:\\RealTimeHimFileDropTest\\Logs\\{DateTime.Now.ToString("MM_dd_yyyy")}_{Environment.UserName}.{typeof(Program).Assembly.GetName().Name}.log")
        .CreateLogger();
      var subscriber = new RabbitMqSubscriber(GetSettings());
      subscriber.Start();
    }

    private static Settings GetSettings()
    {
      var builder = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

      IConfigurationRoot configuration = builder.Build();
      var settings = configuration.GetSection(nameof(Settings)).Get<Settings>();
      Log.Information($"Settings on launch: {Newtonsoft.Json.JsonConvert.SerializeObject(settings)}");
      return settings;
    }
  }
}
