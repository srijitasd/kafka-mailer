const { kafka } = require("./kafkaClient");
const { MongoClient } = require("mongodb");
var cron = require("node-cron");
const nodemailer = require("nodemailer");
const getISOWeek = require("date-fns").getISOWeek;

require("dotenv").config();

const client = new MongoClient(process.env.MONGO_URI);
const db = client.db(process.env.DB_NAME);
const collection = db.collection(process.env.COLLECTION_NAME);

const consumer = kafka.consumer({ groupId: "digest-email-group" });

const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: process.env.TRANSPORTER_EMAIL,
    pass: process.env.TRANSPORTER_PASSWORD,
  },
});

const aggregateAndStore = async (data) => {
  console.log(data);
  await collection.bulkWrite(data);
};

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: process.env.TOPIC,
    fromBeginning: true,
  });

  await consumer.run({
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      let updates = [];
      const date = new Date();
      for (let message of batch.rawMessages) {
        if (!isRunning() || isStale()) break;

        const notification = JSON.parse(message.value.toString());
        // Process the notification here (e.g., accumulate for batch send)
        console.log(`Received notification: ${JSON.stringify(notification)}`);

        updates.push({
          updateOne: {
            filter: {
              id: notification.id,
              email: notification.email,
              type: notification.type,
              week: getISOWeek(date),
            },
            update: { $push: { messages: notification.content } },
            upsert: true,
          },
        });

        resolveOffset(message.offset);
        await heartbeat();
      }

      // After processing the whole batch, you can send out the batched notifications here
      await aggregateAndStore(updates);
    },
  });
};

runConsumer();

cron.schedule("* * * * * 6", async () => {
  const date = new Date();
  const currentWeek = getISOWeek(date);
  const usersDataForTheWeek = await collection
    .find({ week: currentWeek })
    .toArray();

  console.log(usersDataForTheWeek);

  for (const userData of usersDataForTheWeek) {
    transporter.sendMail(
      {
        from: "weekly digest",
        to: userData.email,
        subject: "The kafka digest project",
        text: JSON.stringify(userData.messages),
      },
      (err) => console.log(err)
    );
  }
});
