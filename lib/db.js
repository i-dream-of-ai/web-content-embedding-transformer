import { MongoClient, MongoNetworkError } from "mongodb";

if (!process.env.MONGODB_URI) {
  throw new Error('Invalid/Missing environment variable: "MONGODB_URI"');
}

if (!process.env.MONGODB_DB) {
  throw new Error('Invalid/Missing environment variable: "MONGODB_DB"');
}

const uri = process.env.MONGODB_URI;
const dbName = process.env.MONGODB_DB;

let cachedClient = null;

const client = new MongoClient(uri, {
  serverSelectionTimeoutMS: 5000,
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

const maxRetries = 3;
const retryInterval = 1000; // Time in milliseconds between retries

async function connectWithRetry(attempt = 1) {
  try {
    await client.connect();
    console.log("Connected to MongoDB successfully!");

    // Cache the connected client
    cachedClient = client;

    return client;
  } catch (error) {
    if (attempt < maxRetries) {
      console.log(`Failed to connect, retrying (${attempt}/${maxRetries})...`);
      await new Promise(resolve => setTimeout(resolve, retryInterval));
      return connectWithRetry(attempt + 1);
    } else {
      console.error("Max retries reached, exiting...");
      process.exit(1);
    }
  }
}

// Start the connection process right away and return a promise
const clientPromise = connectWithRetry();

async function connectToDatabase() {
  // Wait for the connection to be established
  await clientPromise;

  if (cachedClient) {
    try {
      // Try to perform a database operation
      await cachedClient.db(dbName).command({ ping: 1 });
    } catch (error) {
      if (error instanceof MongoNetworkError) {
        // If a network error occurs, attempt to reconnect
        console.log("Connection dropped, reconnecting...");
        await connectWithRetry();
      } else {
        // If any other error occurs, throw it
        throw error;
      }
    }
  } else {
    throw new Error('Failed to connect to MongoDB.');
  }

  const db = cachedClient.db(dbName);
  return { client: cachedClient, db };
}

export {
  dbName,
  connectToDatabase,
};