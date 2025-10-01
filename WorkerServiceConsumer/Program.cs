using WorkerServiceConsumer;
using WorkerServiceConsumer.Models;
using WorkerServiceConsumer.Services;

var builder = Host.CreateApplicationBuilder(args);

// Подключение конфигурации
builder.Configuration.AddJsonFile("appsettings.json", false, true);

// Настройка Kafka
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));

builder.Services.AddHostedService<TestDataProducerService>();

// Регистрация сервисов
builder.Services.AddSingleton<IMessageProcessor, MessageProcessor>();
builder.Services.AddHostedService<KafkaConsumerService>();

// Настройка логирования
builder.Services.AddLogging(logging =>
{
    logging.AddConsole();
    logging.AddDebug();
});

var host = builder.Build();
host.Run();
