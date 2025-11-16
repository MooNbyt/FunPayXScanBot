
"use server";
import { NextResponse } from 'next/server';
import { MongoClient, ObjectId } from 'mongodb';
import { getConfig } from '../../status/route';
import { getIronSession } from 'iron-session';
import { cookies } from 'next/headers';
import { SessionData, sessionOptions } from '@/lib/session';

const BOT_USERS_COLLECTION = "bot_users";

async function getDb(workerId: string) {
  const { MONGODB_URI, MONGODB_DB_NAME } = await getConfig(workerId);
  if (!MONGODB_URI) {
    throw new Error('DB not configured');
  }
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
  return { db: client.db(dbName), client };
}

export async function GET(request: Request) {
  let client: MongoClient | undefined;
  try {
    const { WORKER_ID } = await getConfig(); // Get current worker for the UI
    const { searchParams } = new URL(request.url);
    const fetchType = searchParams.get('type');
    const { db, client: connectedClient } = await getDb(WORKER_ID);
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
    if (client) await client.close();
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
        
        const { db, client: connectedClient } = await getDb(WORKER_ID);
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
        if (client) await client.close();
    }
}
