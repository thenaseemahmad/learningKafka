import { Kafka, PartitionAssigners } from "kafkajs";

const kafka = new Kafka({
  clientId: 'consumer',
  brokers: ['localhost:29092', 'localhost:39092', 'localhost:49092']
});

const consumer = kafka.consumer({
  groupId: 'new-user-registered'
});

async function messageConsumer() {
  await consumer.connect();
  console.log('Consumer connected with KAFKA');
  await consumer.subscribe({ topics: ['user-created'], fromBeginning: true });
  await consumer.run({
    partitionsConsumedConcurrently: 2,
    eachBatchAutoResolve: true,
    autoCommit: true,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary,
      uncommittedOffsets,
      isRunning,
      isStale,
      pause,
    }) => {
      for (let message of batch.messages) {
        console.log({
          topic: batch.topic,
          partition: batch.partition,
          highWatermark: batch.highWatermark,
          message: {
            offset: message.offset,
            key: message.key !== null ? message.key.toString() : '',
            value: message.value !== null ? message.value.toString() : '',
            headers: message.headers,
          }
        })

        resolveOffset(message.offset)
        await heartbeat()
      }
    },
  })
  // await consumer.run({
  //   eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
  //     console.log({
  //       partition: partition,
  //       temp: message.attributes,
  //       key: message.key !== null ? message.key.toString() : '',
  //       value: message.value === null ? '' : message.value.toString("utf8"),
  //       headers: message.headers,
  //     })
  //   },
  // })
}

messageConsumer();