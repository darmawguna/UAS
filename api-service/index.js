const express = require("express");
const bodyParser = require("body-parser");
const mysql = require("mysql2");

// Ambil variabel lingkungan dari Docker Compose
const db = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
});

db.connect((err) => {
  if (err) {
    console.error("Error connecting to MySQL:", err);
    return;
  }
  console.log("Connected to MySQL");
});

// Inisialisasi Express
const app = express();
app.use(bodyParser.json());

// CRUD endpoints

// Create
app.post("/items", (req, res) => {
  const { name, description } = req.body;
  const query = "INSERT INTO items (name, description) VALUES (?, ?)";
  db.query(query, [name, description], (err, results) => {
    if (err) {
      console.error("Error creating item:", err);
      res.status(500).send("Error creating item");
      return;
    }
    res.status(201).send({ id: results.insertId });
  });
});

// Read all
app.get("/items", (req, res) => {
  const query = "SELECT * FROM items";
  db.query(query, (err, results) => {
    if (err) {
      console.error("Error fetching items:", err);
      res.status(500).send("Error fetching items");
      return;
    }
    res.status(200).send(results);
  });
});

// Read one
app.get("/items/:id", (req, res) => {
  const { id } = req.params;
  const query = "SELECT * FROM items WHERE id = ?";
  db.query(query, [id], (err, results) => {
    if (err) {
      console.error("Error fetching item:", err);
      res.status(500).send("Error fetching item");
      return;
    }
    if (results.length === 0) {
      res.status(404).send("Item not found");
      return;
    }
    res.status(200).send(results[0]);
  });
});

// Update
app.put("/items/:id", (req, res) => {
  const { id } = req.params;
  const { name, description } = req.body;
  const query = "UPDATE items SET name = ?, description = ? WHERE id = ?";
  db.query(query, [name, description, id], (err) => {
    if (err) {
      console.error("Error updating item:", err);
      res.status(500).send("Error updating item");
      return;
    }
    res.status(200).send("Item updated");
  });
});

// Delete
app.delete("/items/:id", (req, res) => {
  const { id } = req.params;
  const query = "DELETE FROM items WHERE id = ?";
  db.query(query, [id], (err) => {
    if (err) {
      console.error("Error deleting item:", err);
      res.status(500).send("Error deleting item");
      return;
    }
    res.status(200).send("Item deleted");
  });
});

// Start server
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
