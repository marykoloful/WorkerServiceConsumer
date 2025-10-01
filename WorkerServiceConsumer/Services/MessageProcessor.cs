using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkerServiceConsumer.Services
{
    public class MessageProcessor : IMessageProcessor
    {
        private readonly ILogger<MessageProcessor> _logger;
        public MessageProcessor(ILogger<MessageProcessor> logger)
        {
            _logger = logger;
        }
        public Task ProcessMessageAsync(string message, CancellationToken cancellationToken = default)
        {
            try
            {
                _logger.LogInformation("Начало обработки сообщения: {Message}", message);

                //TODO: Логика сохранения BatchId в RAM

                _logger.LogInformation("Сообщение успешно обработано: {Message}", message);
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при обработке сообщения {Message}", message);
                throw;
            }
        }
    }
}
