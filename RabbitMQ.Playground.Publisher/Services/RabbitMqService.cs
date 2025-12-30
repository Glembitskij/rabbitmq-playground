using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbitMQ.Playground.Publisher.Services
{
    public class RabbitMqService
    {
        private readonly ConnectionFactory _factory;
        private readonly string _queueName;

        public RabbitMqService(IConfiguration config)
        {
            var rabbitConfig = config.GetSection("RabbitMQ");
            _queueName = rabbitConfig["QueueName"];

            _factory = new ConnectionFactory()
            {
                HostName = rabbitConfig["Host"],
                Port = int.Parse(rabbitConfig["Port"]),
                UserName = rabbitConfig["Username"],
                Password = rabbitConfig["Password"]
            };
        }

        // Асинхронне публікування
        public async Task PublishAsync(string message)
        {
            await using var connection = await _factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            await channel.QueueDeclareAsync(queue: _queueName,
                                            durable: false,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null);

            var body = Encoding.UTF8.GetBytes(message);
            await channel.BasicPublishAsync(
                                    exchange: "",
                                    routingKey: _queueName,
                                    body: body);

            Console.WriteLine($"[x] Sent {message}");
        }

        /// <summary>
        /// Вичитує всі повідомлення з черги, ACK-ає їх і повертає список
        /// </summary>
        public async Task<List<string>> ReadAndDeleteAllAsync()
        {
            var result = new List<string>();

            await using var connection = await _factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            await channel.QueueDeclareAsync(
                queue: _queueName,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            while (true)
            {
                var response = await channel.BasicGetAsync(queue: _queueName, autoAck: false);

                if (response == null)
                    break; // черга порожня

                var message = Encoding.UTF8.GetString(response.Body.ToArray());
                result.Add(message);

                // підтверджуємо → RabbitMQ видаляє повідомлення
                await channel.BasicAckAsync(response.DeliveryTag, multiple: false);
            }

            return result;
        }

        // Асинхронне споживання
        public async Task ConsumeAsync()
        {
            var connection = await _factory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();

            await channel.QueueDeclareAsync(queue: _queueName,
                                            durable: true,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"[x] Received {message}");

                // Тут можна робити асинхронні дії, наприклад, з базою
                await Task.Yield();
            };

            await channel.BasicConsumeAsync(queue: _queueName,
                                 autoAck: false, // Повідомлення не прочитане
                                 consumer: consumer);
        }
    }
}
