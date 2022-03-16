using System.Net;
using System.Text.Json;
using Confluent.Kafka;
using Shared;

namespace Producer
{
    public class ReplyConsumer
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly EventHubsConfiguration _hubConfiguration;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public ReplyConsumer(EventHubsConfiguration hubConfiguration)
        {
            _consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = hubConfiguration.BootstrapServers,
                ClientId = Dns.GetHostName(),
                SecurityProtocol = SecurityProtocol.SaslSsl,

                //request.timeout.ms
                SocketTimeoutMs = 60000,
                SessionTimeoutMs = 30000,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = hubConfiguration.ConnectionString,
                GroupId = hubConfiguration.ConsumerGroup,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,

                //Event Hubs for Kafka Ecosystems supports Kafka v1.0+, a fallback to an older API will fail
                BrokerVersionFallback = "1.0.0",
            };

            _hubConfiguration = hubConfiguration;

            _cancellationTokenSource = new CancellationTokenSource();
        }

        public void Start()
        {
            Task.Run(() =>
            {
                using var consumer = new ConsumerBuilder<string, string>(_consumerConfig)
                    .Build();

                consumer.Subscribe(_hubConfiguration.ReplyTopic);

                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(_cancellationTokenSource.Token);

                        var replyMessage = JsonSerializer.Deserialize<Shared.Messages.ReplyMessage>(result.Message.Value);

                        MemoryStore.Save(replyMessage);

                        // https://docs.confluent.io/clients-confluent-kafka-dotnet/current/overview.html#store-offsets
                        consumer.StoreOffset(result);
                    }
                    catch (Exception)
                    {
                    }
                }

                consumer.Close();
            });
        }

        public void Stop()
        {
            _cancellationTokenSource.Cancel();
        }
    }
}
