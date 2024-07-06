require("dotenv").config();
const express = require("express");
const mysql = require("mysql2");
const app = express();
const { Kafka } = require("kafkajs");

app.use(express.json());

// Buat pool koneksi
const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectionLimit: 10, // Sesuaikan dengan kebutuhan Anda
});

// Kafka connection
const kafkaBroker = process.env.KAFKA_BROKER || "redpanda:9092";
const kafkaClientId = process.env.KAFKA_CLIENT_ID || "api-service";
const kafkaTopic = process.env.KAFKA_TOPIC || "KAFKAUAS";
const kafkaLogTopic = process.env.KAFKA_LOG_TOPIC || "logs";

// Initialize Kafka producer
const kafka = new Kafka({
  clientId: kafkaClientId,
  brokers: [kafkaBroker],
});

const producer = kafka.producer();

// Connect to Kafka producer
async function connectProducer() {
  await producer.connect();
  console.log("Connected to Kafka producer");
}

connectProducer().catch(console.error);

// Gunakan pool untuk query
app.get("/items", (req, res) => {
  pool.query("SELECT * FROM items", (err, results) => {
    if (err) {
      console.error("Error fetching items:", err);
      res.status(500).send("Error fetching items");
      return;
    }
    console.log("Items fetched:", results);
    res.json(results);
  });
});

// Endpoint yang akan dijadikan message di Kafka
app.post("/add-item", async (req, res) => {
  const { name, description } = req.body;
  try {
    const [result] = await pool.query("INSERT INTO items (name, description) VALUES (?, ?)", [name, description]);
    const newItem = { id: result.insertId, name, description };

    console.log("Item added to database:", newItem);

    await producer.send({
      topic: KAFKAUAS,
      messages: [
        {
          value: JSON.stringify({
            topic: KAFKAUAS,
            message: JSON.stringify(newItem),
            sender: kafkaClientId,
            status: true,
            created_at: new Date().toISOString(),
          }),
        },
      ],
    });

    console.log("Message sent to Kafka topic:", kafkaTopic, newItem);

    res.status(201).json(newItem);
  } catch (error) {
    console.error("Error adding item:", error);

    const errorMessage = JSON.stringify({
      topic: KAFKAUAS,
      message: JSON.stringify({ name, description }),
      sender: kafkaClientId,
      status: false,
      created_at: new Date().toISOString(),
      error: error.message,
    });

    // Send error log message to Kafka
    await producer.send({
      topic: kafkaLogTopic,
      messages: [
        {
          value: errorMessage,
        },
      ],
    });

    console.log("Error log sent to Kafka topic:", kafkaLogTopic, errorMessage);

    res.status(500).json({ error: "Error adding item" });
  }
});

app.listen(3000, () => {
  console.log("Server running on port 3000");
});
