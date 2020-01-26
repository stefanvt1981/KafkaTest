using Confluent.Kafka;
using System;

namespace KafkaTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var config = new ProducerConfig { BootstrapServers = "localhost:9092"};

            Action<DeliveryReport<Null, string>> handler = r => Console.WriteLine(!r.Error.IsError ? 
                $"Delivered message to {r.TopicPartitionOffset}" : $"Delivery error: {r.Error.Reason}");

            using(var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var stringValue = "";
                for (int i = 0; i < 100; i++)
                {
                    stringValue += "@";
                    producer.ProduceAsync(topic: "monkeytail-topic", new Message<Null, string> { Value = stringValue });
                }

                producer.Flush(timeout: TimeSpan.FromSeconds(10));
            }
        }
    }
}
