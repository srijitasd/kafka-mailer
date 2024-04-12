const { kafka } = require("./kafkaClient");

const event = "follower-update";

async function init() {
  const producer = kafka.producer();
  console.log("producer connecting..........");
  await producer.connect();
  console.log("producer connected..........");

  console.log("sending message..........");
  await producer.send({
    topic: "social-media-events",
    messages: [
      {
        // partition: event == "follower-update" ? 0 : 1,
        key: "follower-update",
        value: JSON.stringify({
          id: 2,
          email: "srijit29032001@gmail.com",
          type: "FOLLOWER_UPDATE",
          content: { name: "jhon doe" },
        }),
      },
    ],
  });
  console.log("message sent..........");

  await producer.disconnect();
  console.log("producer disconnected.........");
}

init();
