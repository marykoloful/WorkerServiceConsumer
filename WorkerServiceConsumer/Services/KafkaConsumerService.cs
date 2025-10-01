using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using WorkerServiceConsumer.Models;

namespace WorkerServiceConsumer.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly KafkaSettings _kafkaSettings;
        private readonly IMessageProcessor _messageProcessor;
        private IConsumer<Ignore, string>? _consumer;

        //Конструктор сервиса
        public KafkaConsumerService(
            ILogger<KafkaConsumerService> logger,
            IOptions<KafkaSettings> kafkaSettings,
            IMessageProcessor messageProcessor)
        {
            _logger = logger;
            _kafkaSettings = kafkaSettings.Value;
            _messageProcessor = messageProcessor;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Запуск Kafka Consumer Service...");
            InitializeConsumer();
            await base.StartAsync(cancellationToken);
        }

        // Инициализация консьюмера
        private void InitializeConsumer()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _kafkaSettings.BootstrapServers,
                GroupId = _kafkaSettings.GroupId,
                AutoOffsetReset = (AutoOffsetReset)Enum.Parse(typeof(AutoOffsetReset), _kafkaSettings.AutoOffsetReset, true),
                EnableAutoCommit = _kafkaSettings.EnableAutoCommit,
                EnableAutoOffsetStore = _kafkaSettings.EnableAutoOffsetStore,

                //Таймауты для надежности, можно потом убрать
                SessionTimeoutMs = 10000,
                MaxPollIntervalMs = 300000
            };

            _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            _consumer.Subscribe(_kafkaSettings.Topic);

            _logger.LogInformation("Консьюмер Kafka запущен и подписан на топик: {Topic}", _kafkaSettings.Topic);

        }

        // Запуск работы консьюмера
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Сервис Kafka запущен");

            await Task.Delay(2000, cancellationToken);

            //await Task.Yield(); //В другом источнике удостовериваются, что метод запущен асинхронно

            while (!cancellationToken.IsCancellationRequested)
            {
                {
                    try
                    {
                        var result = _consumer?.Consume(TimeSpan.FromMilliseconds(1000));
                        if (result == null || string.IsNullOrWhiteSpace(result.Message?.Value))
                            continue;

                        await ProcessKafkaMessage(result, cancellationToken);
                        //TODO: Обработка. Десериализуем сообщение в класс
                    }
                    catch(ConsumeException ex)
                    {
                        _logger.LogError(ex, "Kafka Consume error: {Reason}", ex.Error.Reason);
                    }
                    catch(OperationCanceledException)
                    {
                        _logger.LogInformation("Операция потребления сообщений отменена");
                    }
                    catch(JsonException ex)
                    {
                        _logger.LogError(ex, "Не удалось десериализовать сообщение Kafka");
                    }
                    catch(Exception ex)
                    {
                        _logger.LogError(ex, "Неожиданная ошибка обработки сообщений Kafka");
                    }
                }
            }
            _logger.LogInformation("Kafka сервис остановлен");
        }

        // Обработка сообщений от Kafka
        private async Task ProcessKafkaMessage(ConsumeResult<Ignore, string> consumerResult, CancellationToken cancellationToken)
        {
            try
            {
                _logger.LogDebug("Получено сообщение: {Message} из топика {Topic}, partition {Partition}, offset {Offset}",
                    consumerResult.Message.Value,
                    consumerResult.Topic,
                    consumerResult.Partition.Value,
                    consumerResult.Offset.Value);

                //Обрабатываем сообщение
                await _messageProcessor.ProcessMessageAsync(consumerResult.Message.Value, cancellationToken);

                //Если обработка прошла успешно, сохраняем Offset
                if(!_kafkaSettings.EnableAutoCommit)
                {
                    _consumer?.StoreOffset(consumerResult);
                    _logger.LogDebug("Offset сохранен: {Offset}", consumerResult.Offset.Value);

                }
            }
            catch (Exception ex) 
            {
                _logger.LogError(ex, "Ошибка обработки сообщения. Offset: {Offset}", consumerResult.Offset.Value);
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Завершение работы Kafka Consumer Service...");
            await base.StopAsync(cancellationToken);
        }

        // Завершение работы консьюмера
        public override void Dispose()
        {
            _logger.LogInformation("Завершение работы консьюмера Kafka");

            try
            {
                _consumer?.Close();
                _consumer?.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при закрытии консьюмера Kafka");
            }

            base.Dispose();
            _logger.LogInformation("Консьюмер Kafka завершил работу");
        }
    }
}
