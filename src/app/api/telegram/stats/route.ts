
"use server";
import { NextResponse } from 'next/server';
import { createClient } from 'redis';
import { getConfig } from '../../status/route';
import { getIronSession } from 'iron-session';
import { cookies } from 'next/headers';
import { SessionData, sessionOptions } from '@/lib/session';
import { MongoClient } from 'mongodb';

const BOT_USERS_COLLECTION = "bot_users";
const PROJECT_LOGS_KEY = 'project_logs';


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


async function getRpsData(config: any) {
    const { REDIS_URI } = config;
    if (!REDIS_URI) {
        return NextResponse.json([]);
    }
    const redis = createClient({ url: REDIS_URI });
    try {
        await redis.connect();
        const logStrings = await redis.lRange(PROJECT_LOGS_KEY, 0, -1);
        
        const now = Date.now();
        const twentyFourHoursAgo = now - 24 * 60 * 60 * 1000;
        
        const rpsByInterval: { [key: number]: { [workerId: string]: number } } = {};

        logStrings.forEach(logString => {
            try {
                const log = JSON.parse(logString);
                const logTimestamp = new Date(log.timestamp).getTime();

                if (logTimestamp < twentyFourHoursAgo) return;

                const match = log.message.match(/\[Скрейпер (.*?)\].*?Задержка: (\d+)ms. Лимит: (\d+)/);
                if (match) {
                    const [, workerId, delay, limit] = match;
                    const rps = (Number(limit) / (Number(delay) / 1000));
                    
                    // Group by 5-minute intervals
                    const interval = Math.floor(logTimestamp / (5 * 60 * 1000));
                    
                    if (!rpsByInterval[interval]) {
                        rpsByInterval[interval] = {};
                    }
                    // Store the latest RPS for each worker in that interval
                    rpsByInterval[interval][workerId] = rps;
                }
            } catch (e) {
                // Ignore parsing errors for malformed logs
            }
        });
        
        const chartData = Object.entries(rpsByInterval).map(([interval, workerData]) => {
            const time = Number(interval) * 5 * 60 * 1000;
            const totalRps = Object.values(workerData).reduce((sum, rps) => sum + rps, 0);

            return { time: new Date(time).toISOString(), rps: totalRps };
        }).sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime());

        return NextResponse.json(chartData);

    } catch (error: any) {
        console.error("Error fetching RPS data:", error);
        return NextResponse.json({ error: 'Failed to fetch RPS data' }, { status: 500 });
    } finally {
        if (redis.isOpen) await redis.quit();
    }
}


export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const fetchType = searchParams.get('type');
  
  const config = await getConfig();

  if (fetchType === 'rps') {
      return getRpsData(config);
  }
  
  let client: MongoClient | undefined;  
  try {
    const { WORKER_ID } = config; // Get current worker for the UI
    const { client: connectedClient, db } = await getDb();
    client = connectedClient;
    const collection = db.collection(BOT_USERS_COLLECTION);
    
    // Handle request for user list
    if (fetchType === 'users') {
        const page = parseInt(searchParams.get('page') || '1', 10);
        const limit = parseInt(searchParams.get('limit') || '15', 10);
        const searchQuery = searchParams.get('search') || '';

        const query: any = { ownerId: WORKER_ID };
        if (searchQuery) {
            const isNumeric = /^\d+$/.test(searchQuery);
            if(isNumeric) {
                 query.chatId = parseInt(searchQuery, 10);
            } else {
                query.$or = [
                    { username: { $regex: searchQuery, $options: 'i' } },
                    { firstName: { $regex: searchQuery, $options: 'i' } }
                ];
            }
        }

        const total = await collection.countDocuments(query);
        const users = await collection.find(query)
            .sort({ lastSeen: -1 })
            .skip((page - 1) * limit)
            .limit(limit)
            .toArray();
        
        return NextResponse.json({
            users: users.map(u => ({...u, _id: u._id.toString()})),
            totalPages: Math.ceil(total / limit),
            currentPage: page,
        });
    }

    // Default: handle request for stats
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());

    const [
      totalUsers,
      activeUsers,
      blockedUsers,
      newToday,
    ] = await Promise.all([
      collection.countDocuments({ ownerId: WORKER_ID }),
      collection.countDocuments({ ownerId: WORKER_ID, status: 'active' }),
      collection.countDocuments({ ownerId: WORKER_ID, status: 'blocked' }),
      collection.countDocuments({ ownerId: WORKER_ID, joinedAt: { $gte: today } }),
    ]);

    return NextResponse.json({
      totalUsers,
      activeUsers,
      blockedUsers,
      newToday,
    });

  } catch (error: any) {
    console.error("Error fetching Telegram stats:", error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  } finally {
      if(client) await client.close();
  }
}


export async function PUT(request: Request) {
    const session = await getIronSession<SessionData>(cookies(), sessionOptions);
    if (!session.isLoggedIn) {
      return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
    }

    let client: MongoClient | undefined;
    try {
        const { WORKER_ID } = await getConfig();
        const { chatId, status } = await request.json();
        if (!chatId || !status) {
            return NextResponse.json({ error: 'Необходимы chatId и status' }, { status: 400 });
        }
        
        const { client: connectedClient, db } = await getDb();
        client = connectedClient;
        const collection = db.collection(BOT_USERS_COLLECTION);
        
        const result = await collection.updateOne(
            { chatId: Number(chatId), ownerId: WORKER_ID },
            { $set: { status: status } }
        );

        if (result.matchedCount === 0) {
            return NextResponse.json({ error: 'Пользователь не найден' }, { status: 404 });
        }

        return NextResponse.json({ success: true, message: 'Статус пользователя обновлен.' });
        
    } catch (error: any) {
        console.error("Error updating user status:", error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    } finally {
        if(client) await client.close();
    }
}
