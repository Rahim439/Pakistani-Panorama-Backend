import { app } from "./app.js";
import mongoose from "mongoose";
import { closeRedisConnection } from "./config/redis.config.js";

const PORT = process.env.PORT || 5000;

const options = {
  redis: process.env.REDIS_CLOUD_URL,
};
const renderQueue = new Queue("render", options);

app.get("/healthz", async (req, res) => {
  try {
    const redisStatus = await renderQueue.checkHealth();
    if (redisStatus) {
      return res.sendStatus(200);
    } else {
      return res.status(500).json({ message: "Redis connection failed" });
    }
  } catch (err) {
    console.error("Health check failed:", err);
    return res
      .status(500)
      .json({ message: "Redis health check failed", error: err.message });
  }
});

// Connect to MongoDB
mongoose
  .connect(process.env.MONGODB_URI)
  .then(() => {
    console.log("Connected to MongoDB");
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  })
  .catch((error) => {
    console.error("MongoDB connection failed:", error.message);
    process.exit(1);
  });

app.get("/", (req, res) => {
  res.send("Welcome to the backend !");
}); // Handle graceful shutdown
const gracefulShutdown = async () => {
  try {
    await mongoose.connection.close();
    console.log("MongoDB connection closed");

    await closeRedisConnection();

    process.exit(0);
  } catch (err) {
    console.error("Error during graceful shutdown:", err);
    process.exit(1);
  }
};

// Listen for termination signals
process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);
