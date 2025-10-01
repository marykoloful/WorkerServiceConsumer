using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkerServiceConsumer.Services
{
    public interface IMessageProcessor
    {
        Task ProcessMessageAsync(string message, CancellationToken cancellationToken = default);
    }
}
