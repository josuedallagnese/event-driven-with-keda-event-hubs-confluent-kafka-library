using System;
using Microsoft.Extensions.Configuration;

namespace Shared
{
    public class EventHubsConfiguration
    {
        public string BootstrapServers { get; set; }
        public string Topic { get; set; }
        public string ReplyTopic { get; set; }
        public string ConnectionString { get; }
        public string ConsumerGroup { get; }

        public EventHubsConfiguration(IConfiguration configuration)
        {
            BootstrapServers = configuration.GetValue<string>("EventHubs:BootstrapServers");

            if (string.IsNullOrWhiteSpace(BootstrapServers))
                throw new ArgumentNullException($"Invalid EventHubs:BootstrapServers configuration value");

            Topic = configuration.GetValue<string>("EventHubs:Topic");

            if (string.IsNullOrWhiteSpace(Topic))
                throw new ArgumentNullException($"Invalid EventHubs:Topic configuration value");

            ReplyTopic = configuration.GetValue<string>("EventHubs:ReplyTopic");

            if (string.IsNullOrWhiteSpace(ReplyTopic))
                throw new ArgumentNullException($"Invalid EventHubs:ReplyTopic configuration value");

            ConnectionString = configuration.GetValue<string>("EventHubs:ConnectionString");

            if (string.IsNullOrWhiteSpace(ConnectionString))
                throw new ArgumentNullException($"Invalid EventHubs:ConnectionString configuration value");

            ConsumerGroup = configuration.GetValue<string>("EventHubs:ConsumerGroup");

            if (string.IsNullOrWhiteSpace(ConsumerGroup))
                throw new ArgumentNullException($"Invalid EventHubs:ConsumerGroup configuration value");
        }
    }
}
