import { Kafka, Partitioners } from "kafkajs";

const kafka = new Kafka({
  clientId: 'producer',
  brokers: ['localhost:29092', 'localhost:39092', 'localhost:49092']
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

const eventData = { id: crypto.randomUUID(), firstName: 'Naseem', email: 'nas@nas.com' }

async function eventPublisher() {
  await producer.connect();
  await producer.send({
    topic: 'user-created',
    messages: [{
      key: 'user-created-p2',
      value: JSON.stringify(eventData)
    }],
    acks: 1
  });
  console.log("Event published successfully!")
}

eventPublisher();
