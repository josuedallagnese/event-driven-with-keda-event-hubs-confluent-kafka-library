using System.Net;
using Confluent.Kafka;
using KedaWorker.Requests;
using MediatR;
using Shared;

namespace KedaWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly EventHubsConfiguration _hubConfiguration;
        private readonly ConsumerConfig _consumerConfig;

        public Worker(
            ILogger<Worker> logger,
            IServiceScopeFactory scopeFactory,
            EventHubsConfiguration hubConfiguration)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
            _hubConfiguration = hubConfiguration;

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
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var consumer = new ConsumerBuilder<string, string>(_consumerConfig)
                .Build();

            _logger.LogInformation($"Subscribing topic {_hubConfiguration.Topic}");

            consumer.Subscribe(_hubConfiguration.Topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Run(async () =>
                {
                    var result = consumer.Consume(stoppingToken);

                    await ProcessEventAsync(result);

                    // https://docs.confluent.io/clients-confluent-kafka-dotnet/current/overview.html#store-offsets
                    consumer.StoreOffset(result);
                });
            }

            consumer.Close();
        }

        private async Task ProcessEventAsync(ConsumeResult<string, string> result)
        {
            using var scope = _scopeFactory.CreateScope();

            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

            await mediator.Send(new ProcessMessageRequest(result));
        }
    }
}
