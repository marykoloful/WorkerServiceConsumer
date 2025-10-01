using WorkerServiceConsumer;
using WorkerServiceConsumer.Models;
using WorkerServiceConsumer.Services;

var builder = Host.CreateApplicationBuilder(args);

// ����������� ������������
builder.Configuration.AddJsonFile("appsettings.json", false, true);

// ��������� Kafka
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));

builder.Services.AddHostedService<TestDataProducerService>();

// ����������� ��������
builder.Services.AddSingleton<IMessageProcessor, MessageProcessor>();
builder.Services.AddHostedService<KafkaConsumerService>();

// ��������� �����������
builder.Services.AddLogging(logging =>
{
    logging.AddConsole();
    logging.AddDebug();
});

var host = builder.Build();
host.Run();
