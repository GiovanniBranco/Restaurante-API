using ItemService.EventProcessing;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace ItemService.RabbitMqClient
{
    public class RabbitMqSubscriber : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly string _fila;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private IProcessaEvento _processaEvento;

        public RabbitMqSubscriber(IConfiguration configuration, IProcessaEvento processaEvento)
        {
            _configuration = configuration;
            _connection = new ConnectionFactory() 
            { 
                HostName = _configuration["RabbitMQHost"], 
                Port = Int32.Parse(_configuration["RabbitMQPort"]) 
            }.CreateConnection();
            _channel = _connection.CreateModel();
            _channel.ExchangeDeclare(exchange: "trigger", type: ExchangeType.Fanout);
            _fila = _channel.QueueDeclare().QueueName;
            _channel.QueueBind(queue: _fila, exchange: "trigger", routingKey: "");
            _processaEvento = processaEvento;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new EventingBasicConsumer(_channel);

            consumer.Received += (ModuleHandle, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body.ToArray());

                _processaEvento.Processa(message);

                _channel.BasicConsume(queue: _fila, autoAck: true, consumer: consumer);

            };

            return Task.CompletedTask;
        }
    }
}
