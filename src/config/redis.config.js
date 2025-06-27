import { createClient } from 'redis';

// Create Redis client with optimized configuration
const redisClient = createClient({
  url: process.env.REDIS_CLOUD_URL,
  socket: {
    reconnectStrategy: (retries) => Math.min(retries * 50, 1000),
    connectTimeout: 5000, // Even faster timeout
  },
  // Add connection pool settings
  pingInterval: 10000, // Keep connections alive
  commandsQueueMaxLength: 1000, // Add command queue settings
});

// Handle connection errors
redisClient.on('error', (err) => {
  console.error('Redis Cloud connection error:', err);
});

redisClient.on('connect', () => {
  console.log('Connected to Redis Cloud');
});

redisClient.on('reconnecting', () => {
  console.log('Reconnecting to Redis Cloud');
});

// Make sure connection is established early and maintained
let isConnecting = false;
const connectRedis = async () => {
  try {
    if (!redisClient.isOpen && !isConnecting) {
      isConnecting = true;
      await redisClient.connect();
      isConnecting = false;
      console.log('Redis Cloud client connected');
    }
    return redisClient;
  } catch (err) {
    isConnecting = false;
    console.error('Redis Cloud connection failed:', err);
    throw err;
  }
};

// Ensure client is connected before operations
const getRedisClient = async () => {
  if (!redisClient.isOpen) {
    await connectRedis();
  }
  return redisClient;
};

// Graceful shutdown
const closeRedisConnection = async () => {
  try {
    if (redisClient.isOpen) {
      await redisClient.quit();
      console.log('Redis Cloud connection closed');
    }
  } catch (err) {
    console.error('Error closing Redis Cloud connection:', err);
  }
};

export { getRedisClient, connectRedis, redisClient, closeRedisConnection };