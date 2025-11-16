
import { MongoClient } from 'mongodb';
import { createClient } from 'redis';
import { NextResponse } from 'next/server';
import { getIronSession } from 'iron-session';
import { cookies } from 'next/headers';
import { SessionData, sessionOptions } from '@/lib/session';
import { headers } from 'next/headers';

const SETTINGS_COLLECTION = 'settings';
const GLOBAL_SETTINGS_ID = 'global_settings'; // Fixed ID for global scraper settings

// Default GLOBAL configuration
const defaultScraperConfig = {
  SCRAPER_PAUSE_DURATION_MS: 6 * 60 * 60 * 1000, // 6 hours
  SCRAPER_CONSECUTIVE_ERROR_LIMIT: 100,
  SCRAPER_RECENT_PROFILES_LIMIT: 100,
  SCRAPER_BATCH_SIZE: 20,
  SCRAPER_WRITE_BATCH_SIZE: 50,
  PROJECT_LOGS_TTL_MINUTES: 60,
  SCRAPER_PARALLEL_REQUEST_LIMIT_MIN: 1,
  SCRAPER_PARALLEL_REQUEST_LIMIT_MAX: 10,
  SCRAPER_FILE_LOGGING_ENABLED: true,
  SCRAPER_ADAPTIVE_DELAY_MIN_MS: 500,
  SCRAPER_ADAPTIVE_DELAY_MAX_MS: 10000,
  SCRAPER_ADAPTIVE_DELAY_STEP_MS: 100,
  SCRAPER_DELAY_COMPENSATION_MS: 10,
  SCRAPER_SUCCESS_STREAK_TO_INCREASE_LIMIT: 3,
  SCRAPER_ANALYSIS_WINDOW: 200,
  SCRAPER_SUCCESS_THRESHOLD: 99,
  TELEGRAM_LOGS_LIMIT: 200,
  SCRAPER_INTEGRITY_CHECK_BATCH_SIZE: 50000,
};

// Default WORKER-SPECIFIC configuration
const defaultTelegramConfig = {
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
  TELEGRAM_CONNECTION_INFO_MESSAGE: "🤝 **Как работает установка связи?**\n\nНа FunPay запрещено обмениваться контактами в чатах, что мешает простому общению и дружбе вне площадки. Эта функция решает эту проблему, позволяя безопасно обменяться контактами в Telegram.\n\n**Принцип работы:**\n1.  Вы вводите свой ID профиля FunPay.\n2.  Затем вводите ID пользователя, с которым хотите связаться.\n3.  Система создает анонимный запрос.\n\nЕсли тот пользователь сделает то же самое (введет свой ID и ваш ID), система обнаружит совпадение и предложит вам обоим подтвердить обмен контактами. Только после взаимного согласия бот пришлет вам контакты друг друга.\n\n❗️**Важно:** Эта функция создана для общения и дружбы. Она не предназначена для проведения сделок вне FunPay, так как это нарушает правила площадки и может быть небезопасно. Вся ответственность за дальнейшее использование контактов лежит на вас.",
  productCategories: [] as string[],
  TELEGRAM_CUSTOM_LINKS: [] as { text: string; url: string }[],
};

// Environment-level settings that are not saved but used
const environmentConfig = {
  MONGODB_URI: process.env.MONGODB_URI || "",
  MONGODB_DB_NAME: process.env.MONGODB_DB_NAME || "funpayxscanbot",
  REDIS_URI: process.env.REDIS_URI || "",
  NEXT_PUBLIC_APP_URL: process.env.NEXT_PUBLIC_APP_URL || "",
  SETTINGS_PASSWORD: process.env.SETTINGS_PASSWORD || "",
  WORKER_ID: process.env.WORKER_ID || "worker-1",
};

async function getDbConnection() {
    const mongoUri = process.env.MONGODB_URI;
    if (!mongoUri) {
        throw new Error("MongoDB URI is not configured in environment variables.");
    }
    const client = new MongoClient(mongoUri);
    await client.connect();
    // Use a dedicated DB for settings to keep them separate.
    const dbName = 'funpay_settings';
    return { client, db: client.db(dbName) };
}

export async function getConfig(workerIdOverride?: string): Promise<any> {
  let client: MongoClient | undefined;
  
  // Start with default configs
  const finalConfig: any = { 
      ...defaultScraperConfig, 
      ...defaultTelegramConfig,
      ...environmentConfig // Apply env vars
  };

  // Determine App URL dynamically if not set in env
  if (!finalConfig.NEXT_PUBLIC_APP_URL) {
    try {
        const requestHeaders = headers();
        const host = requestHeaders.get('x-forwarded-host') || requestHeaders.get('host');
        if (host) {
            finalConfig.NEXT_PUBLIC_APP_URL = `https://${host}`;
        }
    } catch (e) {
        // This can fail during build time, it's okay.
    }
  }
  
  // Determine which worker's settings to fetch.
  // If an override is passed (e.g., from a specific Telegram webhook), use it.
  // Otherwise, use the one from the environment for the UI.
  const targetWorkerId = workerIdOverride || environmentConfig.WORKER_ID;
  finalConfig.WORKER_ID = targetWorkerId;
  
  try {
    const { client: connectedClient, db } = await getDbConnection();
    client = connectedClient;
    const settingsCollection = db.collection(SETTINGS_COLLECTION);
    
    // 1. Fetch the global scraper settings using the fixed ID.
    const globalSettings = await settingsCollection.findOne({ _id: GLOBAL_SETTINGS_ID });
    if (globalSettings) {
      const { _id, ...dbSettings } = globalSettings;
      Object.assign(finalConfig, dbSettings);
    }
    
    // 2. Fetch the worker-specific Telegram settings.
    const workerSettings = await settingsCollection.findOne({ _id: targetWorkerId });
    if (workerSettings) {
       const { _id, ...dbSettings } = workerSettings;
       // We only care about Telegram settings from the worker-specific document
       for (const key of Object.keys(defaultTelegramConfig)) {
         if (key in dbSettings) {
           finalConfig[key] = dbSettings[key];
         }
       }
    }

  } catch (error) {
    console.warn(`Could not get config from DB, using default/env values:`, error instanceof Error ? error.message : String(error));
  } finally {
    if (client) {
      await client.close();
    }
  }

  // Session unlock status for the UI
  try {
    const session = await getIronSession<SessionData>(cookies(), sessionOptions);
    finalConfig.isSettingsUnlocked = !!session.isSettingsUnlocked;
  } catch (e) {
    // Ignore error if session is not available (e.g. during build)
  }

  return finalConfig;
}

export async function updateConfig(newConfig: Partial<any>) {
    const session = await getIronSession<SessionData>(cookies(), sessionOptions);
    if (!session.isSettingsUnlocked) {
        throw new Error("Access denied. You must unlock settings.");
    }

    // This function will always update the main worker's Telegram settings from the UI.
    const currentWorkerId = process.env.WORKER_ID || "worker-1";

    // Separate keys into global and worker-specific
    const globalKeys = Object.keys(defaultScraperConfig);
    const workerKeys = Object.keys(defaultTelegramConfig);

    const configToSaveGlobal: any = {};
    const configToSaveWorker: any = {};

    // Distribute incoming keys into the two config objects
    for (const key in newConfig) {
        if (globalKeys.includes(key)) {
            configToSaveGlobal[key] = (newConfig as any)[key];
        } else if (workerKeys.includes(key)) {
            configToSaveWorker[key] = (newConfig as any)[key];
        }
    }
    
    // Type conversion for numeric/boolean fields
    const fieldsToProcess: { [key: string]: any } = {
        // Global
        SCRAPER_PAUSE_DURATION_MS: Number,
        SCRAPER_CONSECUTIVE_ERROR_LIMIT: Number,
        SCRAPER_RECENT_PROFILES_LIMIT: Number,
        SCRAPER_BATCH_SIZE: Number,
        SCRAPER_WRITE_BATCH_SIZE: Number,
        PROJECT_LOGS_TTL_MINUTES: Number,
        SCRAPER_PARALLEL_REQUEST_LIMIT_MIN: Number,
        SCRAPER_PARALLEL_REQUEST_LIMIT_MAX: Number,
        SCRAPER_FILE_LOGGING_ENABLED: Boolean,
        SCRAPER_ADAPTIVE_DELAY_MIN_MS: Number,
        SCRAPER_ADAPTIVE_DELAY_MAX_MS: Number,
        SCRAPER_ADAPTIVE_DELAY_STEP_MS: Number,
        SCRAPER_SUCCESS_STREAK_TO_INCREASE_LIMIT: Number,
        SCRAPER_DELAY_COMPENSATION_MS: Number,
        SCRAPER_ANALYSIS_WINDOW: Number,
        SCRAPER_SUCCESS_THRESHOLD: Number,
        TELEGRAM_LOGS_LIMIT: Number,
        SCRAPER_INTEGRITY_CHECK_BATCH_SIZE: Number,
        // Worker-specific
        TELEGRAM_PAYMENT_ENABLED: Boolean,
        TELEGRAM_SEARCH_COST_STARS: Number,
        TELEGRAM_SEARCH_COST_REAL: Number,
        TELEGRAM_CONNECTION_PAYMENT_ENABLED: Boolean,
        TELEGRAM_CONNECTION_COST_STARS: Number,
        TELEGRAM_CONNECTION_COST_REAL: Number,
    };

     for (const [key, type] of Object.entries(fieldsToProcess)) {
        if (key in configToSaveGlobal && configToSaveGlobal[key] !== undefined) {
            configToSaveGlobal[key] = type(configToSaveGlobal[key]);
        }
        if (key in configToSaveWorker && configToSaveWorker[key] !== undefined) {
            configToSaveWorker[key] = type(configToSaveWorker[key]);
        }
    }

    let client: MongoClient | undefined;
    try {
        const { client: connectedClient, db } = await getDbConnection();
        client = connectedClient;
        const settingsCollection = db.collection(SETTINGS_COLLECTION);
        
        // Save global settings if there are any to save
        if (Object.keys(configToSaveGlobal).length > 0) {
            await settingsCollection.updateOne(
                { _id: GLOBAL_SETTINGS_ID }, 
                { $set: configToSaveGlobal }, 
                { upsert: true }
            );
        }
        
        // Save worker-specific settings if there are any to save
        if (Object.keys(configToSaveWorker).length > 0) {
            await settingsCollection.updateOne(
                { _id: currentWorkerId },
                { $set: configToSaveWorker },
                { upsert: true }
            );
        }

    } catch (error) {
        console.error(`Failed to update config in DB:`, error);
        throw error;
    } finally {
        if (client) {
            await client.close();
        }
    }
}


export async function GET() {
  // Always fetch the "main" worker's config for the general status check
  const config = await getConfig(process.env.WORKER_ID || "worker-1");
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
    

    
