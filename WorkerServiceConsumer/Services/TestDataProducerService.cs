using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WorkerServiceConsumer.Models;

namespace WorkerServiceConsumer.Services
{
    public class TestDataProducerService : IHostedService
    {
        private readonly ILogger<TestDataProducerService> _logger;
        private readonly IOptions<KafkaSettings> _kafkaSettings;
        private Timer? _timer;

        public TestDataProducerService(ILogger<TestDataProducerService> logger, IOptions<KafkaSettings> kafkaSettings)
        {
            _logger = logger;
            _kafkaSettings = kafkaSettings;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("TestDataProducerService запущен");

            // Запускаем отправку тестовых сообщений через 5 секунд после старта
            _timer = new Timer(SendTestMessages, null, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(5));

            return Task.CompletedTask;
        }

        private async void SendTestMessages(object? state)
        {
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = _kafkaSettings.Value.BootstrapServers,
                    Acks = Acks.All
                };

                using var producer = new ProducerBuilder<Null, string>(config).Build();

                var messages = new[]
                {
                "Тестовое сообщение 1",
                "Тестовое сообщение 2",
                "Тестовое сообщение 3",
                "Важное сообщение для обработки",
                "Сообщение с данными: { \"id\": 123, \"name\": \"test\" }"
            };

                foreach (var message in messages)
                {
                    var kafkaMessage = new Message<Null, string>
                    {
                        Value = $"{message} - {DateTime.Now:HH:mm:ss}"
                    };

                    await producer.ProduceAsync(_kafkaSettings.Value.Topic, kafkaMessage);
                    _logger.LogInformation("Отправлено тестовое сообщение: {Message}", message);

                    await Task.Delay(500); // Пауза между сообщениями
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при отправке тестовых сообщений");
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _timer?.Dispose();
            _logger.LogInformation("TestDataProducerService остановлен");
            return Task.CompletedTask;
        }
    }
}
