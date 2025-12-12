
"use server";
import { NextResponse } from 'next/server';
import { createClient } from 'redis';
import { getConfig } from '../../status/route';
import fs from 'fs/promises';
import path from 'path';
import { MongoClient } from 'mongodb';

const COLLECTION_NAME = "bot_users";
const STATUS_KEY = 'background_export_tg_status';
const PROGRESS_KEY = 'background_export_tg_progress';
const TOTAL_KEY = 'background_export_tg_total';
const FILE_PATH_KEY = 'background_export_tg_file_path';
const ERROR_KEY = 'background_export_tg_error';
const PROCESS_LOCK_KEY = 'background_export_tg_lock';

let redisClient: any;

async function getRedisClient() {
    if (redisClient && redisClient.isOpen) {
        return redisClient;
    }
    const { REDIS_URI } = await getConfig();
    if (!REDIS_URI) throw new Error("Redis не сконфигурирован.");
    
    redisClient = createClient({ url: REDIS_URI });
    await redisClient.connect();
    return redisClient;
}

async function getDb() {
    const mongoUri = process.env.MONGODB_URI;
    const dbName = process.env.MONGODB_DB_NAME;
    if (!mongoUri || !dbName) {
        throw new Error("MongoDB URI или имя БД не сконфигурированы.");
    }
    const client = new MongoClient(mongoUri);
    await client.connect();
    return { client, db: client.db(dbName) };
}


async function cleanupRedisState(client: any) {
    await client.del([STATUS_KEY, PROGRESS_KEY, TOTAL_KEY, FILE_PATH_KEY, ERROR_KEY, PROCESS_LOCK_KEY]);
}

async function runExportProcess() {
    let localRedis: any;
    const { WORKER_ID } = await getConfig();
    let mongoClient: MongoClient | undefined;

    try {
        localRedis = await getRedisClient();
        
        const lockAcquired = await localRedis.set(PROCESS_LOCK_KEY, 'locked', { NX: true, EX: 3600 });
        if (!lockAcquired) {
            console.log("Background Telegram export process is already running.");
            return;
        }

        const { client, db } = await getDb();
        mongoClient = client;
        const collection = db.collection(COLLECTION_NAME);
        
        const query = { $or: [{ ownerId: WORKER_ID }, { ownerId: { $exists: false } }] };
        const total = await collection.countDocuments(query);
        
        const backupDir = path.join(process.cwd(), 'backups');
        await fs.mkdir(backupDir, { recursive: true });
        const tempFilePath = path.join(backupDir, `telegram-users-backup-${Date.now()}.json.tmp`);
        const finalFilePath = tempFilePath.replace('.tmp', '');

        await localRedis.set(STATUS_KEY, 'running');
        await localRedis.set(TOTAL_KEY, total);
        await localRedis.set(PROGRESS_KEY, 0);
        await localRedis.set(FILE_PATH_KEY, finalFilePath);
        
        const fileHandle = await fs.open(tempFilePath, 'w');
        await fileHandle.write('[\n');

        const CHUNK_SIZE = 5000;
        let isFirstChunk = true;

        for (let i = 0; i < total; i += CHUNK_SIZE) {
            const currentStatus = await localRedis.get(STATUS_KEY);
            if (currentStatus !== 'running') {
                 throw new Error("Процесс экспорта был остановлен пользователем.");
            }

            const users = await collection.find(query).project({_id: 0}).sort({ joinedAt: 1 }).skip(i).limit(CHUNK_SIZE).toArray();
            
            if (users.length > 0) {
                 const chunkString = users.map(u => JSON.stringify(u)).join(',\n');
                 if (!isFirstChunk) {
                    await fileHandle.write(',\n');
                 }
                 await fileHandle.write(chunkString);
                 isFirstChunk = false;
            }
            
            await localRedis.set(PROGRESS_KEY, Math.min(i + CHUNK_SIZE, total));
        }

        await fileHandle.write('\n]');
        await fileHandle.close();

        await fs.rename(tempFilePath, finalFilePath);

        await localRedis.set(STATUS_KEY, 'completed');
        await localRedis.del(PROCESS_LOCK_KEY);

    } catch (error: any) {
        if(localRedis) {
            await localRedis.set(STATUS_KEY, 'error');
            await localRedis.set(ERROR_KEY, error.message);
            await localRedis.del(PROCESS_LOCK_KEY);
        }
        console.error("Ошибка фонового экспорта TG пользователей:", error);
    } finally {
        if (localRedis && localRedis.isOpen) await localRedis.quit();
        if (mongoClient) await mongoClient.close();
    }
}


export async function GET() {
    try {
        const redis = await getRedisClient();
        const [status, progress, total, filePath, error] = await redis.mGet([
            STATUS_KEY,
            PROGRESS_KEY,
            TOTAL_KEY,
            FILE_PATH_KEY,
            ERROR_KEY
        ]);

        return NextResponse.json({
            status: status || 'idle',
            progress: parseInt(progress || '0', 10),
            total: parseInt(total || '0', 10),
            filePath: filePath,
            error: error
        });
    } catch (error: any) {
        return NextResponse.json({ 
            status: 'error', 
            progress: 0, 
            total: 0, 
            filePath: null, 
            error: `Не удалось получить статус: ${error.message}` 
        }, { status: 500 });
    }
}

export async function POST(request: Request) {
    const { searchParams } = new URL(request.url);
    const actionParam = searchParams.get('action');

    // Handle import action
    if(actionParam === 'import') {
        const { WORKER_ID } = await getConfig();
        let client: MongoClient | undefined;
        try {
            const data = await request.json();

            if (!Array.isArray(data)) {
                return NextResponse.json({ error: 'Invalid data format. Expected an array of user profiles.' }, { status: 400 });
            }

            const { client: connectedClient, db } = await getDb();
            client = connectedClient;
            const collection = db.collection(COLLECTION_NAME);

            // Clear only the users belonging to the current worker
            await collection.deleteMany({ ownerId: WORKER_ID });

            if(data.length > 0){
                // Ensure all imported users are assigned to the current worker
                const dataWithOwner = data.map(user => ({ ...user, ownerId: WORKER_ID }));
                await collection.insertMany(dataWithOwner);
            }

            return NextResponse.json({ message: `База данных Telegram для воркера ${WORKER_ID} успешно импортирована.` });

        } catch (error: any) {
            console.error("Error importing TG data:", error);
            return NextResponse.json({ error: 'Failed to import TG data' }, { status: 500 });
        } finally {
            if(client) await client.close();
        }
    }


    // Handle background export actions
    try {
        const { action } = await request.json();
        const redis = await getRedisClient();

        if (action === 'start') {
            const currentStatus = await redis.get(STATUS_KEY);
            if (currentStatus === 'running') {
                return NextResponse.json({ message: 'Экспорт уже запущен.' }, { status: 409 });
            }
            await cleanupRedisState(redis);
            runExportProcess().catch(console.error);
            return NextResponse.json({ message: 'Фоновый экспорт пользователей Telegram запущен.' });

        } else if (action === 'stop') {
             await redis.set(STATUS_KEY, 'stopped');
             await redis.del(PROCESS_LOCK_KEY);
             return NextResponse.json({ message: 'Команда остановки отправлена.' });
        
        } else if (action === 'clear') {
            const filePath = await redis.get(FILE_PATH_KEY);
            await cleanupRedisState(redis);
            if(filePath){
                try { await fs.unlink(filePath); } catch(e) { /* ignore */ }
            }
            return NextResponse.json({ message: 'Статус экспорта очищен.' });
        }

        return NextResponse.json({ error: 'Неверное действие.' }, { status: 400 });

    } catch (error: any) {
        return NextResponse.json({ error: `Ошибка сервера: ${error.message}` }, { status: 500 });
    }
}
