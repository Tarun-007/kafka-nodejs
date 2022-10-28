const { Kafka, logLevel } = require("kafkajs");
const config = require("../config");
const {
  saveMessageToTables,
  fetchLatestOffset,
  getConnection,
  getDb,
} = require("./mysql");

const CONSUMER_SPEED = 5;

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  clientId: config.kafka.CLIENTID,
  brokers: config.kafka.BROKERS,
  retry: {
    initialRetryTime: 10000,
    retries: 3,
  },
});

const topic1 = config.kafka.TOPIC1;
const topic2 = config.kafka.TOPIC2;
const consumer = kafka.consumer({
  groupId: config.kafka.GROUPID,
});

let count = 0;

const producer = kafka.producer({
  transactionalId: 'my-transactional-producer',
  maxInFlightRequests: 1,
  idempotent: true
});

const run = async () => {
  const { CONNECT } = consumer.events;
  consumer.on(CONNECT, async (e) => {
    console.log(`CONNECT at ${new Date()}`);
    let offset = await fetchLatestOffset();
    consumer.seek({ topic: topic1, partition: 0, offset: offset + 1 });
  });

  //cosumer
  await consumer.connect();
  await consumer.subscribe({ topic: topic1, fromBeginning: true });

  //producer
  await producer.connect();
  
  //db connection
  let dbConnection = getConnection();
  let db = getDb();

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, message, pause }) => {

      db.beginTransaction(dbConnection);
      try {
        let { offset, key, value } = message;
        console.log(`recieved message  at ${new Date().getSeconds()}`, {
          offset,
          key: key.toString(),
          value: value.toString(),
        });

        //error simulation
        if (offset % 5 == 0) {
          if (count < 3) {
            count++;
            throw Error(offset);
          } else {
            count = 0;
          }
        }

        const jsonObj = JSON.parse(value.toString());
        await saveMessageToTables(jsonObj, offset);

        await sendMessage(producer, topic2, jsonObj);

        //consumer commit offset
        await commitOffset(offset);

        //db commit insert
        await db.commit(dbConnection);

      } catch (error) {
        console.log(
          `error occured, rolling back transaction ${error.message} \n`
        );
        await db.rollback(dbConnection);
        consumer.seek({ topic: topic1, partition: 0, offset: error.message });
      }

      const resumeThisPartition = pause();
      setTimeout(resumeThisPartition, CONSUMER_SPEED * 1000);
    },
  });
};

const sendMessage = async (transaction, topic, message) => {
  payload = {
    topic: topic2,
    messages: [{ key: "test-message-topic2", value: JSON.stringify(message) }],
  };
  await transaction.send(payload);
};

const commitOffset = async (offset) => {
  console.log(`commiting offset ${offset}`);
  await consumer.commitOffsets([{ topic: topic1, offset, partition: 0 }]);
};

run().catch((e) => console.error(`[example/consumer] ${e.message}`, e));

module.exports = {};
