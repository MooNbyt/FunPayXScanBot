
import { MongoClient } from 'mongodb';
import { createClient } from 'redis';
import { NextResponse } from 'next/server';
import { getIronSession } from 'iron-session';
import { cookies } from 'next/headers';
import { SessionData, sessionOptions } from '@/lib/session';

const SETTINGS_COLLECTION = 'settings';
const GLOBAL_SETTINGS_ID = 'global'; // Use a fixed ID for the single settings document

// Default configuration, used if nothing is in the DB
const defaultConfig = {
  MONGODB_URI: process.env.MONGODB_URI || "",
  MONGODB_DB_NAME: process.env.MONGODB_DB_NAME || "funpayxscanbot",
  REDIS_URI: process.env.REDIS_URI || "",
  TELEGRAM_TOKEN: "",
  TELEGRAM_PROVIDER_TOKEN: "",
  TELEGRAM_PAYMENT_CURRENCY: "RUB",
  TELEGRAM_PAYMENT_ENABLED: false,
  TELEGRAM_SEARCH_COST_STARS: 1,
  TELEGRAM_SEARCH_COST_REAL: 10,
  TELEGRAM_CONNECTION_PAYMENT_ENABLED: false,
  TELEGRAM_CONNECTION_COST_STARS: 5,
  TELEGRAM_CONNECTION_COST_REAL: 50,
  TELEGRAM_SHOP_BUTTON_NAME: "Магазин",
  TELEGRAM_BOT_LINK: "",
  TELEGRAM_WELCOME_MESSAGE: "🤖 Привет! Я твой помощник @FunPayXScanBot по базе данных.\nВ системе уже зарегистрировано {user_count} пользователей!\nВведи `user_id` или `username`, и я найду всё, что смогу 😉",
  TELEGRAM_WELCOME_IMAGE_URL: "",
  NEXT_PUBLIC_APP_URL: process.env.NEXT_PUBLIC_APP_URL || "", // Will be overridden by env
  WORKER_ID: "worker-1", // Default fallback if not in DB or env
  SCRAPER_PAUSE_DURATION_MS: 6 * 60 * 60 * 1000, // 6 hours
  SCRAPER_CONSECUTIVE_ERROR_LIMIT: 100,
  SCRAPER_RECENT_PROFILES_LIMIT: 100,
  SCRAPER_BAN_PAUSE_MINUTES: 30,
  SCRAPER_BATCH_SIZE: 50,
  SCRAPER_WRITE_BATCH_SIZE: 50,
  SETTINGS_PASSWORD: process.env.SETTINGS_PASSWORD || "",
  PROJECT_LOGS_TTL_HOURS: 1,
};

async function getDbConnection() {
    const mongoUri = process.env.MONGODB_URI;
    if (!mongoUri) {
        throw new Error("MongoDB URI is not configured in environment variables.");
    }
     const client = new MongoClient(mongoUri);
    await client.connect();
    // The database used for settings is always 'funpay', regardless of the user's choice
    // to ensure settings are loaded from a consistent location.
    const dbName = 'funpay';
    return { client, db: client.db(dbName) };
}


export async function getConfig(workerIdOverride?: string): Promise<typeof defaultConfig & { isSettingsUnlocked?: boolean }> {
  let client: MongoClient | undefined;
  
  const config: any = { ...defaultConfig };

  // This still identifies the current instance, but isn't used for settings lookup
  const currentWorkerId = workerIdOverride || process.env.WORKER_ID || "worker-1";
  config.WORKER_ID = currentWorkerId;
  
  try {
    const { client: connectedClient, db } = await getDbConnection();
    client = connectedClient;
    const settingsCollection = db.collection(SETTINGS_COLLECTION);
    
    // Fetch the single global settings document
    const globalConfig = await settingsCollection.findOne({ _id: GLOBAL_SETTINGS_ID });
    
    if (globalConfig) {
      const { _id, ...dbSettings } = globalConfig;
      Object.assign(config, dbSettings);
    }

  } catch (error) {
    console.warn(`Could not get global config from DB, using default/env values:`, error instanceof Error ? error.message : String(error));
  } finally {
    if (client) {
      await client.close();
    }
  }

  // Environment variables always override DB settings
  if (process.env.MONGODB_URI) config.MONGODB_URI = process.env.MONGODB_URI;
  if (process.env.MONGODB_DB_NAME) config.MONGODB_DB_NAME = process.env.MONGODB_DB_NAME;
  if (process.env.REDIS_URI) config.REDIS_URI = process.env.REDIS_URI;
  if (process.env.NEXT_PUBLIC_APP_URL) config.NEXT_PUBLIC_APP_URL = process.env.NEXT_PUBLIC_APP_URL;
  if (process.env.TELEGRAM_PROVIDER_TOKEN) config.TELEGRAM_PROVIDER_TOKEN = process.env.TELEGRAM_PROVIDER_TOKEN;
  if (process.env.SETTINGS_PASSWORD) config.SETTINGS_PASSWORD = process.env.SETTINGS_PASSWORD;
  
  try {
    const session = await getIronSession<SessionData>(cookies(), sessionOptions);
    config.isSettingsUnlocked = !!session.isSettingsUnlocked;
  } catch (e) {
    // Ignore error if session is not available (e.g. during build)
  }

  return config;
}

export async function updateConfig(newConfig: Partial<typeof defaultConfig>, requireUnlock: boolean = false) {
    if (requireUnlock) {
      const session = await getIronSession<SessionData>(cookies(), sessionOptions);
      if (!session.isSettingsUnlocked) {
          throw new Error("Access denied. You must unlock settings.");
      }
    }

    let client: MongoClient | undefined;
    try {
        const { client: connectedClient, db } = await getDbConnection();
        client = connectedClient;
        const settingsCollection = db.collection(SETTINGS_COLLECTION);
        
        // Remove keys that should not be saved to the shared settings document
        const { WORKER_ID, NEXT_PUBLIC_APP_URL, MONGODB_URI, MONGODB_DB_NAME, REDIS_URI, SETTINGS_PASSWORD, isSettingsUnlocked, ...configToSave } = newConfig as any;

        const fieldsToProcess = {
            SCRAPER_PAUSE_DURATION_MS: Number,
            SCRAPER_CONSECUTIVE_ERROR_LIMIT: Number,
            TELEGRAM_PAYMENT_ENABLED: Boolean,
            TELEGRAM_SEARCH_COST_STARS: Number,
            TELEGRAM_SEARCH_COST_REAL: Number,
            TELEGRAM_CONNECTION_PAYMENT_ENABLED: Boolean,
            TELEGRAM_CONNECTION_COST_STARS: Number,
            TELEGRAM_CONNECTION_COST_REAL: Number,
            SCRAPER_RECENT_PROFILES_LIMIT: Number,
            SCRAPER_BAN_PAUSE_MINUTES: Number,
            SCRAPER_BATCH_SIZE: Number,
            SCRAPER_WRITE_BATCH_SIZE: Number,
            PROJECT_LOGS_TTL_HOURS: Number,
        };

        for (const [key, type] of Object.entries(fieldsToProcess)) {
            if (configToSave[key] !== undefined) {
                 (configToSave as any)[key] = type((configToSave as any)[key]);
            }
        }
        
        if (Object.keys(configToSave).length > 0) {
            await settingsCollection.updateOne(
                { _id: GLOBAL_SETTINGS_ID }, 
                { $set: configToSave }, 
                { upsert: true }
            );
        }
    } catch (error) {
        console.error("Failed to update global config in DB:", error);
        throw error;
    } finally {
        if (client) {
            await client.close();
        }
    }
}


export async function GET() {
  const config = await getConfig();
  const mongoStatus = await checkMongoConnection(config.MONGODB_URI, config.MONGODB_DB_NAME);
  const redisStatus = await checkRedisConnection(config.REDIS_URI);

  return NextResponse.json({
    mongodb: mongoStatus,
    redis: redisStatus,
  });
}

function formatBytes(bytes: number, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}


async function checkMongoConnection(mongoUri: string, dbName: string): Promise<{ status: 'connected' | 'error', memory: string | null }> {
  if (!mongoUri) return { status: 'error', memory: null };
  let client: MongoClient | undefined;
  
  try {
    client = new MongoClient(mongoUri);
    await client.connect();
    const targetDbName = new URL(mongoUri).pathname.substring(1) || dbName;
    const db = client.db(targetDbName);
    await db.command({ ping: 1 });
    const stats = await db.stats();

    return { status: 'connected', memory: formatBytes(stats.storageSize) };
  } catch (error) {
    console.error('MongoDB connection error:', error);
    return { status: 'error', memory: null };
  } finally {
    if (client) {
      await client.close();
    }
  }
}

async function checkRedisConnection(redisUri: string): Promise<{ status: 'connected' | 'error', memory: string | null }> {
  if (!redisUri) return { status: 'error', memory: null };
  let client: ReturnType<typeof createClient> | undefined;
  
  try {
    client = createClient({ url: redisUri });
    await client.connect();
    await client.ping();
    const info = await client.info('memory');
    const memoryMatch = info.match(/used_memory_human:([\d.]+.)/);
    const memory = memoryMatch ? `${memoryMatch[1]}B` : null;

    return { status: 'connected', memory: memory };
  } catch (error) {
    console.error('Redis connection error:', error);
    return { status: 'error', memory: null };
  } finally {
    if (client) {
      await client.quit();
    }
  }
}
