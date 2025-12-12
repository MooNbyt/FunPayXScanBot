
"use server";
import { NextResponse } from 'next/server';
import { MongoClient, ObjectId } from 'mongodb';
import { createClient } from 'redis';
import { getConfig } from '../status/route';
import { getIronSession } from 'iron-session';
import { cookies } from 'next/headers';
import { SessionData, sessionOptions } from '@/lib/session';

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


async function checkAuth(): Promise<boolean> {
    const session = await getIronSession<SessionData>(cookies(), sessionOptions);
    return !!session.isSettingsUnlocked;
}

export async function GET(request: Request) {
    const isAuthorized = await checkAuth();
    if (!isAuthorized) {
        return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
    }

    const { searchParams } = new URL(request.url);
    const dbType = searchParams.get('db');
    const collectionName = searchParams.get('collection');
    const page = parseInt(searchParams.get('page') || '1', 10);
    const fetchAll = searchParams.get('all') === 'true';


    if (dbType === 'mongo') {
        return getMongoData({ collectionName, page, fetchAll });
    } else if (dbType === 'redis') {
        return getRedisData();
    }

    return NextResponse.json({ error: 'Неверный тип базы данных' }, { status: 400 });
}

const PAGE_SIZE = 10;

async function getMongoData({ collectionName: singleCollection, page, fetchAll }: { collectionName: string | null, page: number, fetchAll: boolean }) {
    let client: MongoClient | undefined;
    try {
        const { client: connectedClient, db } = await getDb();
        client = connectedClient;
        
        const adminDb = client.db().admin();
        const serverInfo = await adminDb.serverInfo();
        const dbStats = await db.stats();

        const connectionInfo = {
            host: client.options.hosts.join(', '),
            dbName: db.databaseName,
            version: serverInfo.version,
            storageSize: dbStats.storageSize,
        };

        if (singleCollection) {
            const collection = db.collection(singleCollection);
            
            if (fetchAll) {
                const allDocs = await collection.find({}).sort({ $natural: -1 }).toArray();
                return NextResponse.json(allDocs.map(doc => ({ ...doc, _id: doc._id.toString() })));
            }

            const count = await collection.countDocuments();
            const pages = Math.ceil(count / PAGE_SIZE);
            const sample = await collection.find()
                .sort({ $natural: -1 })
                .skip((page - 1) * PAGE_SIZE)
                .limit(PAGE_SIZE)
                .toArray();
            
            return NextResponse.json({
                count,
                pages,
                sample: sample.map(doc => ({ ...doc, _id: doc._id.toString() })),
            });
        }

        const collections = await db.listCollections().toArray();
        const collectionsData: any = {};

        for (const collectionInfo of collections) {
            const collectionName = collectionInfo.name;
            const collection = db.collection(collectionName);
            const count = await collection.countDocuments();
            const pages = Math.ceil(count / PAGE_SIZE);
            const sample = await collection.find().sort({ $natural: -1 }).limit(PAGE_SIZE).toArray();

            collectionsData[collectionName] = {
                count,
                pages,
                sample: sample.map(doc => ({ ...doc, _id: doc._id.toString() })),
            };
        }
        
        return NextResponse.json({ collections: collectionsData, connectionInfo });

    } catch (error: any) {
        console.error("Ошибка при получении данных из MongoDB:", error);
        return NextResponse.json({ error: 'Не удалось получить данные из MongoDB' }, { status: 500 });
    } finally {
        if (client) await client.close();
    }
}


async function getRedisData() {
    const { REDIS_URI } = await getConfig();
    if (!REDIS_URI) {
        return NextResponse.json({ error: 'Redis не сконфигурирован' }, { status: 500 });
    }

    let redisClient: any;
    try {
        redisClient = createClient({ url: REDIS_URI });
        await redisClient.connect();

        const keysToFetch = [
            'scraping_stats',
            'last_successful_write_id',
            'dedicated_writer_worker_id',
            'scraper_last_error_id',
            'scraper_404_error_count',
        ];

        const keyValues: any = {};
        for (const key of keysToFetch) {
            const type = await redisClient.type(key);
            if (type === 'string') {
                 keyValues[key] = await redisClient.get(key);
            } else if (type === 'hash') {
                keyValues[key] = await redisClient.hGetAll(key);
            } else {
                keyValues[key] = `(Тип: ${type}, не отображается)`;
            }
        }
        
        const queueKeys = ['task_queue', 'pending_writes', 'failed_tasks', 'recent_profiles'];
        const queueSizes: any = {};
        for (const key of queueKeys) {
            const type = await redisClient.type(key);
            if(type === 'list') {
                queueSizes[key] = await redisClient.lLen(key);
            } else if (type === 'zset') {
                queueSizes[key] = await redisClient.zCard(key);
            } else {
                 queueSizes[key] = 0;
            }
        }

        return NextResponse.json({ keyValues, queueSizes });

    } catch (error: any) {
        console.error("Ошибка при получении данных из Redis:", error);
        return NextResponse.json({ error: 'Не удалось получить данные из Redis' }, { status: 500 });
    } finally {
        if (redisClient?.isOpen) {
            await redisClient.quit();
        }
    }
}

export async function PUT(request: Request) {
    const isAuthorized = await checkAuth();
    if (!isAuthorized) {
        return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
    }
    
    let client: MongoClient | undefined;
    try {
        const { searchParams } = new URL(request.url);
        const collectionName = searchParams.get('collection');
        const docId = searchParams.get('id');

        if (!collectionName || !docId) {
            return NextResponse.json({ error: 'Не указана коллекция или ID документа' }, { status: 400 });
        }
        
        const body = await request.json();
        // The _id from the body is a string, remove it before updating to avoid conflicts
        delete body._id; 
        
        const { client: connectedClient, db } = await getDb();
        client = connectedClient;
        const collection = db.collection(collectionName);
        
        const result = await collection.updateOne(
            { _id: new ObjectId(docId) },
            { $set: body }
        );

        if (result.matchedCount === 0) {
             return NextResponse.json({ error: 'Документ не найден' }, { status: 404 });
        }

        return NextResponse.json({ message: 'Документ успешно обновлен' });
    } catch (error: any) {
        console.error("Ошибка обновления документа:", error);
        return NextResponse.json({ error: 'Не удалось обновить документ: ' + error.message }, { status: 500 });
    } finally {
        if(client) await client.close();
    }
}

export async function DELETE(request: Request) {
    const isAuthorized = await checkAuth();
    if (!isAuthorized) {
        return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
    }

    const { searchParams } = new URL(request.url);
    const dbType = searchParams.get('db');

    if (dbType === 'redis') {
        const { REDIS_URI } = await getConfig();
        if (!REDIS_URI) return NextResponse.json({ error: 'Redis не сконфигурирован' }, { status: 500 });
        
        let redisClient: any;
        try {
            const keyToDelete = searchParams.get('key');
            if (!keyToDelete) {
                return NextResponse.json({ error: 'Не указан ключ для удаления' }, { status: 400 });
            }

            redisClient = createClient({ url: REDIS_URI });
            await redisClient.connect();
            await redisClient.del(keyToDelete);
            
            return NextResponse.json({ message: `Ключ "${keyToDelete}" успешно удален.` });

        } catch (error: any) {
            console.error("Ошибка удаления ключа Redis:", error);
            return NextResponse.json({ error: 'Не удалось удалить ключ из Redis: ' + error.message }, { status: 500 });
        } finally {
            if (redisClient?.isOpen) {
                await redisClient.quit();
            }
        }
    }

    if (dbType === 'mongo') {
        let client: MongoClient | undefined;
        try {
            const collectionName = searchParams.get('collection');
            const docId = searchParams.get('id');

            if (!collectionName) {
                return NextResponse.json({ error: 'Не указана коллекция' }, { status: 400 });
            }
            
            const { client: connectedClient, db } = await getDb();
            client = connectedClient;

            // If docId is present, delete document. Otherwise, delete collection.
            if (docId) {
                const collection = db.collection(collectionName);
                const result = await collection.deleteOne({ _id: new ObjectId(docId) });
                 if (result.deletedCount === 0) {
                    return NextResponse.json({ error: 'Документ не найден' }, { status: 404 });
                }
                return NextResponse.json({ message: 'Документ успешно удален' });
            } else {
                 await db.collection(collectionName).drop();
                 return NextResponse.json({ message: `Коллекция "${collectionName}" успешно удалена.` });
            }
        } catch (error: any) {
            console.error("Ошибка удаления:", error);
            return NextResponse.json({ error: 'Не удалось выполнить удаление: ' + error.message }, { status: 500 });
        } finally {
            if(client) await client.close();
        }
    }

    return NextResponse.json({ error: 'Неверный тип базы данных' }, { status: 400 });
}


export async function POST(request: Request) {
    const isAuthorized = await checkAuth();
    if (!isAuthorized) {
        return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
    }
    
    let client: MongoClient | undefined;
    try {
        const { searchParams } = new URL(request.url);
        const collectionName = searchParams.get('collection');
        if (!collectionName) {
            return NextResponse.json({ error: 'Не указана коллекция' }, { status: 400 });
        }
        
        const newData = await request.json();
        if (!Array.isArray(newData)) {
            return NextResponse.json({ error: 'Данные должны быть массивом объектов' }, { status: 400 });
        }
        
        const { client: connectedClient, db } = await getDb();
        client = connectedClient;
        const collection = db.collection(collectionName);
        
        // Convert string _id back to ObjectId for valid insertion
        const docsToInsert = newData.map(doc => {
           if (doc._id && typeof doc._id === 'string' && ObjectId.isValid(doc._id)) {
               doc._id = new ObjectId(doc._id);
           }
           return doc;
        });

        await collection.deleteMany({});
        if (docsToInsert.length > 0) {
            await collection.insertMany(docsToInsert);
        }

        return NextResponse.json({ message: `Коллекция "${collectionName}" успешно перезаписана.` });
    } catch (error: any) {
        console.error(`Ошибка перезаписи коллекции:`, error);
        return NextResponse.json({ error: `Не удалось перезаписать коллекцию: ${error.message}` }, { status: 500 });
    } finally {
        if(client) await client.close();
    }
}
