
// src/lib/mongodb.ts
import { MongoClient, Db } from 'mongodb';

const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB_NAME = process.env.MONGODB_DB_NAME;

if (!MONGODB_URI) {
  throw new Error('Please define the MONGODB_URI environment variable inside .env');
}

if (!MONGODB_DB_NAME) {
    throw new Error('Please define the MONGODB_DB_NAME environment variable inside .env');
}


// This approach is not recommended for serverless environments like Vercel.
// A new connection should be established for each request.
// However, for a long-running server process, this can be more efficient.
// We will use a mixed approach: cache the connection but be ready to reconnect.

let client: MongoClient | null = null;
let db: Db | null = null;

export async function getDb(): Promise<{ db: Db; client: MongoClient }> {
  // Check if we have a cached connection that's still open
  if (client && db) {
    try {
      // Ping the database to check if the connection is still alive
      await client.db('admin').command({ ping: 1 });
      return { client, db };
    } catch (e) {
      // Connection might have been closed, so we'll reconnect
      client = null;
      db = null;
    }
  }

  // If no valid cached connection, create a new one
  const newClient = new MongoClient(MONGODB_URI!);
  await newClient.connect();
  const newDb = newClient.db(MONGODB_DB_NAME);

  client = newClient;
  db = newDb;

  return { client, db };
}

// Optional: A function to close the connection, e.g., on app shutdown
export async function closeDbConnection() {
    if (client) {
        await client.close();
        client = null;
        db = null;
    }
}
