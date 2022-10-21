const { Kafka } = require('kafkajs')
const config = require('../config')
const { saveMessageToTables, fetchLatestOffset } = require('./mysql')

const kafka = new Kafka({
  clientId: config.kafka.CLIENTID,
  brokers: config.kafka.BROKERS
})

const topic1 = config.kafka.TOPIC1
const topic2 = config.kafka.TOPIC2
const consumer = kafka.consumer({
  groupId: config.kafka.GROUPID
})

const producer = kafka.producer()


const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic:topic1, fromBeginning: true })
  let latestOffset = fetchLatestOffset();
  consumer.seek({ topic: topic1, partition: 0, offset: latestOffset })
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message }) => {
      try {
        let{offset, key, value} = message;
        console.log('recieved message ', {offset, key:key.toString(), value:value.toString()})
        const jsonObj = JSON.parse(value.toString());
        await saveMessageToTables(jsonObj, offset);
        //throw new Error("hey this is an error");
        await sendMessage(producer, topic2, jsonObj);
        setTimeout(async ()=>{
          console.log(`commiting offset ${offset}`)
          await commitOffset(offset);
        },5000)
      } catch (error) {
        console.log('err=', error)
      }
    }
  })
}


const sendMessage = async (producer, topic, message) => {
  await producer.connect();
  payload = {
    topic: topic,
    messages: [{ key: "test-message-topic2", value: JSON.stringify(message) }],
  };
  producer.send(payload);
};

const commitOffset = async (offset)=>{
  consumer.commitOffsets([
    {topic: topic1, offset, partition:0}
  ])
}


run().catch(e => console.error(`[example/consumer] ${e.message}`, e))




module.exports = {
  
}
