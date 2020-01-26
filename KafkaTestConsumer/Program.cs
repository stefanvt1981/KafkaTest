using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaTestConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            //var config = new Dictionary<string, object>
            //{
            //    { "group.id", "monkeytail.consumers" },
            //    { "bootstrap.servers", "localhost:9092" },
            //    { "enable.auto.commit", "false" }
            //};

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "monkeytail.consumers",
                EnableAutoCommit = false
            };

            using(var consumer = new ConsumerBuilder<Null,string>(config).Build())
            {
                consumer.Subscribe("monkeytail-topic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }
    }
}
