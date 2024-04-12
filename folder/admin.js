const { kafka } = require("./kafkaClient");

async function init() {
  const admin = kafka.admin();
  console.log("Admin connecting..........");
  await admin.connect();
  console.log("Admin connected..........");

  console.log("creating topic.......");
  await admin.createTopics({
    topics: [
      {
        topic: process.env.TOPIC,
        numPartitions: 2,
      },
    ],
  });
  console.log("topic created.......");

  await admin.disconnect();
  console.log("admin disconnected.........");
}

init();
