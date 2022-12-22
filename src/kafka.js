const { Kafka } = require('kafkajs');
const kafka = new Kafka({
  clientId: 'response-producer',
  brokers: ['localhost:9092'],
});
const producer = kafka.producer();
const connection = producer.connect();

async function produce(key, value) {
  await connection;

  await producer.send({
    topic: 'signing-response',
    messages: [{ key, value }]
  });

  await producer.disconnect();
}

module.exports = { produce, producer };
