const { Kafka } = require("kafkajs");
require("dotenv").config();

const kafka = new Kafka({
  clientId: "Digest-app",
  brokers: [process.env.KAFKA_BROKER],
});

module.exports = { kafka };
