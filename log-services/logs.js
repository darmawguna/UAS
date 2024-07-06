const { Kafka } = require("kafkajs");
const { createClient } = require("@supabase/supabase-js");
const dotenv = require("dotenv");

dotenv.config();

const kafkaBroker = process.env.KAFKA_BROKER || "redpanda:9092";
const kafkaClientId = process.env.KAFKA_CLIENT_ID || "log-service";
const kafkaLogTopic = process.env.KAFKA_LOG_TOPIC || "logs";

// Supabase connection
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

// Initialize Kafka consumer
const kafka = new Kafka({
  clientId: kafkaClientId,
  brokers: [kafkaBroker],
});

const consumer = kafka.consumer({ groupId: kafkaClientId });

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaLogTopic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const logMessage = JSON.parse(message.value.toString());
      await supabase.from("logs").insert([
        {
          topic: logMessage.topic,
          message: logMessage.message,
          sender: logMessage.sender,
          status: logMessage.status,
          created_at: logMessage.created_at,
        },
      ]);
    },
  });
}

run().catch(console.error);
