﻿using RabbitMQ.Client;
using RestauranteService.Dtos;
using System.Text;
using System.Text.Json;

namespace RestauranteService.RabbitMqClient
{
    public class RabbitMqClient : IRabbitMqClient
    {
        private readonly IConfiguration _configuration;
        private readonly IConnection _connection;
        private readonly IModel _channel;

        public RabbitMqClient(IConfiguration configuration)
        {
            _configuration = configuration;
            _connection = new ConnectionFactory()
            {
                HostName = _configuration["RabbitMQHost"],
                Port = Int32.Parse(_configuration["RabbitMQPort"])
            }.CreateConnection();
            _channel = _connection.CreateModel();
            _channel.ExchangeDeclare(exchange: "trigger", type: ExchangeType.Fanout);
        }

        public void PublicaRestaurante(RestauranteReadDto restauranteReadDto)
        {
            Console.WriteLine(restauranteReadDto);
            var mensagem = JsonSerializer.Serialize(restauranteReadDto);
            var body = Encoding.UTF8.GetBytes(mensagem);

            Console.WriteLine(body.ToString());
            Console.WriteLine(mensagem);
            _channel.BasicPublish(
                exchange: "trigger", 
                routingKey: "", 
                basicProperties: null, 
                body: body
            );
        }
    }
}
