const { Kafka } = require('kafkajs')
const config = require('../config')
const messages = require('../../input.json')

const client = new Kafka({
  brokers: config.kafka.BROKERS,
  clientId: config.kafka.CLIENTID
})

const producer = client.producer()



const topic1 = config.kafka.TOPIC1
let i = 0
const sendMessage = async (producer, topic) => {
  await producer.connect();
  var times = 0;
  const intervalId = setInterval(function () {
    i = i >= messages.length - 1 ? 0 : i + 1;
    payload = {
      topic: topic,
      messages: [
        { key: "test-message", value: JSON.stringify(messages[i]) },
      ],
    };
    console.log("payloads=", payload);
    producer.send(payload);
    if (++times == 10) {
      clearInterval(intervalId);
    }
  }, 2000);
};

sendMessage(producer, topic1)



