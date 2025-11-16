
"use server";
import { NextResponse } from 'next/server';
import { MongoClient, ObjectId } from 'mongodb';
import { getConfig } from '../status/route';
import { getIronSession } from 'iron-session';
import { cookies } from 'next/headers';
import { SessionData, sessionOptions } from '@/lib/session';
import { apiCall } from '@/lib/telegram/api';
import { createClient } from 'redis';

const CAMPAIGNS_COLLECTION = "campaigns";
const BOT_USERS_COLLECTION = "bot_users";
const CAMPAIGN_STOP_PREFIX = "campaign_stop:";

async function checkAuth(): Promise<boolean> {
    const session = await getIronSession<SessionData>(cookies(), sessionOptions);
    return !!session.isLoggedIn;
}

async function getDb() {
  const { MONGODB_URI, MONGODB_DB_NAME } = await getConfig();
  if (!MONGODB_URI) {
    throw new Error('DB not configured');
  }
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
  return { db: client.db(dbName), client };
}


// GET all campaigns for the current worker
export async function GET(request: Request) {
  const isAuthorized = await checkAuth();
  if (!isAuthorized) {
      return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
  }

  let client: MongoClient | undefined;
  try {
    const { WORKER_ID } = await getConfig();
    const { db, client: connectedClient } = await getDb();
    client = connectedClient;
    const campaigns = await db.collection(CAMPAIGNS_COLLECTION).find({ ownerId: WORKER_ID }).sort({ createdAt: -1 }).toArray();
    return NextResponse.json({ campaigns });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  } finally {
    if (client) await client.close();
  }
}

async function sendCampaign(campaignId: ObjectId, workerId: string) {
    let mongoClient: MongoClient | undefined;
    let redisClient: any;
    const { TELEGRAM_TOKEN, MONGODB_URI, MONGODB_DB_NAME, REDIS_URI } = await getConfig(workerId);
    
    try {
        if (!TELEGRAM_TOKEN || !MONGODB_URI) {
            throw new Error(`Конфигурация для воркера ${workerId} не найдена.`);
        }
        
        mongoClient = new MongoClient(MONGODB_URI);
        await mongoClient.connect();
        const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
        const db = mongoClient.db(dbName);
        const campaignsCollection = db.collection(CAMPAIGNS_COLLECTION);
        const botUsersCollection = db.collection(BOT_USERS_COLLECTION);

        if(REDIS_URI) {
            redisClient = createClient({ url: REDIS_URI });
            await redisClient.connect();
            await redisClient.del(`${CAMPAIGN_STOP_PREFIX}${campaignId.toString()}`);
        }
        
        const campaign = await campaignsCollection.findOne({ _id: campaignId });
        if (!campaign) {
            console.error(`[Campaign] Кампания ${campaignId} не найдена.`);
            return;
        }

        const userCursor = botUsersCollection.find({ status: 'active' }, { projection: { chatId: 1 } });
        const allUsers = await userCursor.toArray();
        const totalUsers = allUsers.length;
        
        await campaignsCollection.updateOne(
            { _id: campaignId },
            { $set: { 'stats.total': totalUsers, 'stats.sent': 0, 'stats.errors': 0, status: 'sending' } }
        );

        let sentCount = 0;
        let errorCount = 0;

        for (const user of allUsers) {
            if(redisClient) {
                const stopSignal = await redisClient.get(`${CAMPAIGN_STOP_PREFIX}${campaignId.toString()}`);
                if (stopSignal) {
                    await campaignsCollection.updateOne({ _id: campaignId }, { $set: { status: 'stopped' } });
                    break; 
                }
            }
            
            if (!user.chatId) continue;
            
            try {
                let response;
                const payload: any = {
                    chat_id: user.chatId,
                    parse_mode: 'Markdown', // Assuming markdown is used
                };
                
                 if (campaign.text) payload.text = campaign.text;
                 if (campaign.text && campaign.imageUrl) payload.caption = campaign.text;


                if (campaign.imageUrl) {
                    response = await apiCall(TELEGRAM_TOKEN, 'sendPhoto', {
                        ...payload,
                        photo: campaign.imageUrl,
                    });
                } else {
                    response = await apiCall(TELEGRAM_TOKEN, 'sendMessage', payload);
                }
                
                if (response.ok) {
                    sentCount++;
                } else {
                    errorCount++;
                    if (response.description && response.description.includes("bot was blocked by the user")) {
                        await botUsersCollection.updateOne({ chatId: user.chatId }, { $set: { status: 'blocked' }});
                    }
                }
            } catch (e: any) {
                errorCount++;
                console.error(`[Campaign] Не удалось отправить сообщение для chatId ${user.chatId}:`, e);
            }
            
            // Update stats periodically
            if ((sentCount + errorCount) % 25 === 0) {
                 await campaignsCollection.updateOne(
                    { _id: campaignId },
                    { $set: { 'stats.sent': sentCount, 'stats.errors': errorCount } }
                );
            }
             await new Promise(resolve => setTimeout(resolve, 50)); // ~20 messages per second
        }
        
         const finalStatus = await redisClient?.get(`${CAMPAIGN_STOP_PREFIX}${campaignId.toString()}`) ? 'stopped' : 'completed';
         await campaignsCollection.updateOne(
            { _id: campaignId },
            { $set: { 'stats.sent': sentCount, 'stats.errors': errorCount, status: finalStatus } }
        );


    } catch (e: any) {
         console.error(`[Campaign] КРИТИЧЕСКАЯ ОШИБКА в процессе рассылки ${campaignId}:`, e);
         if (mongoClient) {
             const db = mongoClient.db(new URL(MONGODB_URI!).pathname.substring(1) || MONGODB_DB_NAME);
             await db.collection(CAMPAIGNS_COLLECTION).updateOne(
                { _id: campaignId },
                { $set: { status: 'error' } }
             );
         }
    } finally {
        if (mongoClient) await mongoClient.close();
        if (redisClient) await redisClient.quit();
    }
}

// POST a new campaign for the current worker
export async function POST(request: Request) {
  const isAuthorized = await checkAuth();
  if (!isAuthorized) {
    return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
  }

  let client: MongoClient | undefined;
  try {
    const { WORKER_ID } = await getConfig();
    const body = await request.json();

    if (body.action === 'restart' && body.campaignId) {
        sendCampaign(new ObjectId(body.campaignId), WORKER_ID);
        return NextResponse.json({ success: true, message: "Кампания перезапущена." });
    }

    if (!body.name || !body.text) {
        return NextResponse.json({ error: 'Название и текст кампании обязательны.' }, { status: 400 });
    }

    const newCampaign = {
        ...body,
        ownerId: WORKER_ID,
        createdAt: new Date().toISOString(),
        status: 'draft', // Start as draft, will be updated by the sending process
        stats: {
            sent: 0,
            total: 0, // Will be updated
            errors: 0,
        }
    };
    
    const { db, client: connectedClient } = await getDb();
    client = connectedClient;
    
    const result = await db.collection(CAMPAIGNS_COLLECTION).insertOne(newCampaign);
    
    // Fire-and-forget the sending process. Do not await it.
    sendCampaign(result.insertedId, WORKER_ID);

    return NextResponse.json({ success: true, insertedId: result.insertedId, campaign: { ...newCampaign, _id: result.insertedId } });

  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  } finally {
    if (client) await client.close();
  }
}

export async function DELETE(request: Request) {
    const isAuthorized = await checkAuth();
    if (!isAuthorized) {
        return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
    }

    let client: MongoClient | undefined;
    try {
        const { WORKER_ID } = await getConfig();
        const { campaignId } = await request.json();

        if (!campaignId) {
            return NextResponse.json({ error: 'ID кампании не указан' }, { status: 400 });
        }

        const { db, client: connectedClient } = await getDb();
        client = connectedClient;

        const result = await db.collection(CAMPAIGNS_COLLECTION).deleteOne({ _id: new ObjectId(campaignId), ownerId: WORKER_ID });
        
        if (result.deletedCount === 0) {
            return NextResponse.json({ error: 'Кампания не найдена или у вас нет прав на её удаление' }, { status: 404 });
        }

        return NextResponse.json({ success: true });

    } catch (error: any) {
        return NextResponse.json({ error: error.message }, { status: 500 });
    } finally {
        if (client) await client.close();
    }
}

export async function PUT(request: Request) {
    const isAuthorized = await checkAuth();
    if (!isAuthorized) {
        return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
    }

    try {
        const config = await getConfig();
        const { action, campaignId } = await request.json();
        
        if (action === 'stop' && campaignId) {
             if (!config.REDIS_URI) {
                return NextResponse.json({ error: 'Redis не сконфигурирован для остановки кампаний.' }, { status: 500 });
             }
             const redis = createClient({ url: config.REDIS_URI });
             await redis.connect();
             await redis.set(`${CAMPAIGN_STOP_PREFIX}${campaignId}`, "1", { EX: 60 * 60 }); // Stop signal for 1 hour
             await redis.quit();
             return NextResponse.json({ success: true, message: 'Команда остановки отправлена.' });
        }

        return NextResponse.json({ error: 'Неверное действие' }, { status: 400 });

    } catch (error: any) {
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}

    