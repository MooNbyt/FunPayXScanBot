
import { NextResponse } from 'next/server';
import { createClient } from 'redis';
import { MongoClient, ObjectId } from 'mongodb';
import { getConfig } from '../status/route';

const STATS_KEY = 'scraping_stats';
const RECENT_PROFILES_KEY = 'recent_profiles';
const COLLECTION_NAME = "users";
const PRODUCTS_COLLECTION = "products";
const SETTINGS_COLLECTION = 'settings';
const CONNECTION_REQUEST_PREFIX = 'connect:';
const CONNECTION_CONFIRM_PREFIX = 'confirm:';
const SCRAPER_STATUS_KEY_PREFIX = 'scraper_status:';


// Helper to add the URL back for display purposes
const addUrlToProfile = (profile: any) => ({
    ...profile,
    url: `https://funpay.com/users/${profile.id}/`
});

export async function GET(request: Request) {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get('query');
    const type = searchParams.get('type');
    const letter = searchParams.get('letter');

    const config = await getConfig();
    if (!config.REDIS_URI || !config.MONGODB_URI) {
        return NextResponse.json({ error: 'DB not configured' }, { status: 500 });
    }
    
    if(letter) {
        return searchProfilesByLetter(letter, config);
    }

    if (query !== null && type) {
         if (type === 'status') {
            return searchProfilesByStatusInRedis(query, config);
        }
        return searchProfiles(query, type, config);
    } else {
        return getDashboardData(config);
    }
}

async function getDashboardData(config: any) {
    const redisClient = createClient({ url: config.REDIS_URI });
    const mongoClient = new MongoClient(config.MONGODB_URI);
    
    try {
        await redisClient.connect();
        await mongoClient.connect();

        const statsData = await redisClient.hGetAll(STATS_KEY);
        const recentProfilesData = await redisClient.lRange(RECENT_PROFILES_KEY, 0, 99);
        const connectionRequestKeys = await redisClient.keys(`${CONNECTION_REQUEST_PREFIX}*`);
        const activeConnectionKeys = await redisClient.keys(`${CONNECTION_CONFIRM_PREFIX}*`);
        const errorCount = await redisClient.get('scraper_global_consecutive_404') || 0;
        
        const workerStatusKeys = await redisClient.keys(`${SCRAPER_STATUS_KEY_PREFIX}*`);
        const workerStatuses: { id: string; status: string; }[] = [];

        if (workerStatusKeys.length > 0) {
            const statuses = await redisClient.mGet(workerStatusKeys);
            workerStatusKeys.forEach((key, index) => {
                const workerId = key.replace(SCRAPER_STATUS_KEY_PREFIX, '');
                workerStatuses.push({ id: workerId, status: statuses[index] || 'unknown' });
            });
        }


        const dbName = new URL(config.MONGODB_URI).pathname.substring(1) || config.MONGODB_DB_NAME;
        const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);
        const totalUsersInDb = await collection.countDocuments();
        const foundByWorker = await collection.countDocuments({ scrapedBy: config.WORKER_ID });

        const stats = {
            processed: Number(statsData.processed) || 0,
            successful: Number(statsData.successful) || 0,
            errors: Number(errorCount),
            support: Number(statsData.support) || 0,
            banned: Number(statsData.banned) || 0,
            connectionRequests: connectionRequestKeys.length,
            activeConnections: activeConnectionKeys.length / 2, // Each connection has two keys
            totalUsersInDb: totalUsersInDb,
            foundByWorker: foundByWorker,
            workerStatuses: workerStatuses
        };
        
        const recentProfiles = recentProfilesData.map(p => p ? JSON.parse(p) : null).filter(Boolean);

        return NextResponse.json({ stats, recentProfiles });

    } catch (error: any) {
        console.error("Error fetching dashboard data:", error);
        return NextResponse.json({ error: 'Failed to fetch dashboard data' }, { status: 500 });
    } finally {
        if (redisClient.isOpen) {
            await redisClient.quit();
        }
        if (mongoClient) {
           await mongoClient.close();
        }
    }
}

async function searchProfilesByStatusInRedis(query: string, config: any) {
    const mongoClient = new MongoClient(config.MONGODB_URI);
    try {
        await mongoClient.connect();
        const dbName = new URL(config.MONGODB_URI).pathname.substring(1) || config.MONGODB_DB_NAME;
        const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);
        
        let findQuery: any = {};
        if (query.toLowerCase() === 'support') {
            findQuery = { isSupport: true };
        } else if (query.toLowerCase() === 'banned') {
            findQuery = { isBanned: true };
        } else {
             return NextResponse.json([]);
        }
        
        const results = await collection.find(findQuery).limit(200).toArray();
        const serializableResults = results.map(doc => ({
            ...addUrlToProfile(doc),
            _id: doc._id.toString()
        }))
        return NextResponse.json(serializableResults);

    } catch (error: any) {
        console.error("Error searching profiles by status:", error);
        return NextResponse.json({ error: 'Failed to search profiles' }, { status: 500 });
    } finally {
        await mongoClient.close();
    }
}

async function searchProfilesByLetter(letter: string, config: any) {
    const mongoClient = new MongoClient(config.MONGODB_URI);
    try {
        await mongoClient.connect();
        const dbName = new URL(config.MONGODB_URI).pathname.substring(1) || config.MONGODB_DB_NAME;
        const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);
        
        const regex = new RegExp(`^${letter}`, 'i');
        // This query can use a standard index on `nickname` if one exists.
        const findQuery = { nickname: { $regex: regex }, status: { $ne: 'not_found' } };

        const results = await collection.find(findQuery).limit(200).sort({ nickname: 1 }).toArray();
        const serializableResults = results.map(doc => ({
            ...addUrlToProfile(doc),
            _id: doc._id.toString()
        }));
        return NextResponse.json(serializableResults);

    } catch (error: any) {
        console.error("Error searching profiles by letter:", error);
        return NextResponse.json({ error: 'Failed to search profiles by letter' }, { status: 500 });
    } finally {
        await mongoClient.close();
    }
}


export async function searchProfiles(query: string, type: string, config: any, statusQuery?: string) {
    const mongoClient = new MongoClient(config.MONGODB_URI);
    try {
        await mongoClient.connect();
        const dbName = new URL(config.MONGODB_URI).pathname.substring(1) || config.MONGODB_DB_NAME;
        const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);
        
        let findQuery: any = {};
        if (type === 'id') {
            if(query === 'latest') {
                 const latestUser = await collection.find({ status: { $ne: 'not_found' } }).sort({id: -1}).limit(1).toArray();
                 const serializableResults = latestUser.map(doc => ({ ...doc, _id: doc._id.toString() }));
                 return NextResponse.json(serializableResults);
            }
            const numericQuery = parseInt(query, 10);
            if (!isNaN(numericQuery)) {
              findQuery = { id: numericQuery };
            } else {
              return NextResponse.json([]);
            }
        } else if (type === 'nickname') {
            // Check if a text index exists on the collection.
            const indexes = await collection.listIndexes().toArray();
            const hasTextIndex = indexes.some(index => Object.values(index.key).includes('text'));
            
            if (hasTextIndex) {
                 // Use the more efficient text search if an index is available.
                 findQuery = { $text: { $search: query }, status: { $ne: 'not_found' } };
            } else {
                 // Fallback to regex for case-insensitive search if no text index.
                 // This will be slower on large collections.
                 findQuery = { nickname: { $regex: query, $options: 'i' }, status: { $ne: 'not_found' } };
            }

        } else if (type === 'status' && statusQuery) {
             if (statusQuery.toLowerCase() === 'support') {
                findQuery = { isSupport: true };
            } else if (statusQuery.toLowerCase() === 'banned') {
                findQuery = { isBanned: true };
            } else {
                return NextResponse.json([]);
            }
        }
        else {
            return NextResponse.json({ error: 'Invalid search type' }, { status: 400 });
        }

        const results = await collection.find(findQuery).limit(200).sort({ id: 1 }).toArray();
        // MongoDB _id is not serializable, so we convert it to a string.
        const serializableResults = results.map(doc => ({
            ...addUrlToProfile(doc),
            _id: doc._id.toString()
        }))
        return NextResponse.json(serializableResults);

    } catch (error: any) {
        console.error("Error searching profiles:", error);
        return NextResponse.json({ error: 'Failed to search profiles' }, { status: 500 });
    } finally {
        await mongoClient.close();
    }
}


export async function DELETE() {
    const { REDIS_URI, MONGODB_URI, MONGODB_DB_NAME } = await getConfig();
    if (!REDIS_URI || !MONGODB_URI) {
        return NextResponse.json({ error: 'DB not configured' }, { status: 500 });
    }
    
    const redisClient = createClient({ url: REDIS_URI });
    const mongoClient = new MongoClient(MONGODB_URI);

    try {
        // --- Targeted Redis Cleanup ---
        await redisClient.connect();
        const keyPatterns = [
            'scraper_status:*',
            'telegram_logs:*',
            'telegram_user_state:*',
            'connect:*',
            'confirm:*',
            'user_active_request:*',
            'telegram_user_connections:*',
            'telegram_cart:*',
            'project_logs',
            'critical_project_logs',
            'scraping_stats',
            'recent_profiles',
            'failed_tasks',
            'last_successful_write_id',
            'dedicated_writer_worker_id',
            'scraper_process_lock',
            'writer_lock',
            'next_funpay_id_to_parse',
            'scraper_last_error_id',
            'scraper_global_consecutive_404',
            'scraper_global_404_start_id',
            'scraper_global_pause_until',
            'integrity_check_lock',
        ];
        
        let allKeys: string[] = [];
        for (const pattern of keyPatterns) {
            const keys = await redisClient.keys(pattern);
            allKeys.push(...keys);
        }

        // Remove duplicates and delete if any keys found
        const uniqueKeys = [...new Set(allKeys)];
        if (uniqueKeys.length > 0) {
            await redisClient.del(uniqueKeys);
        }


        // --- Targeted MongoDB Cleanup ---
        await mongoClient.connect();
        const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
        const db = mongoClient.db(dbName);
        
        const collectionsToDrop = [COLLECTION_NAME, PRODUCTS_COLLECTION, SETTINGS_COLLECTION];
        
        for (const collectionName of collectionsToDrop) {
            try {
                await db.collection(collectionName).drop();
            } catch (error: any) {
                // Ignore error if collection doesn't exist
                if (error.codeName !== 'NamespaceNotFound') {
                    throw error;
                }
            }
        }

        return NextResponse.json({ message: 'All project-specific data has been cleared from Redis and MongoDB.' });

    } catch (error: any) {
        console.error("Error clearing data:", error);
        return NextResponse.json({ error: 'Failed to clear data' }, { status: 500 });
    } finally {
         if (redisClient.isOpen) {
            await redisClient.quit();
        }
        await mongoClient.close();
    }
}

export async function POST(request: Request) {
    const { MONGODB_URI, REDIS_URI, MONGODB_DB_NAME } = await getConfig();
    if (!MONGODB_URI || !REDIS_URI) {
        return NextResponse.json({ error: 'DB not configured' }, { status: 500 });
    }

    const body = await request.json();
    const { action, status, missingIds } = body;

    if (action === 'recount' && (status === 'support' || status === 'banned')) {
        const mongoClient = new MongoClient(MONGODB_URI);
        const redisClient = createClient({ url: REDIS_URI });
        
        try {
            await mongoClient.connect();
            await redisClient.connect();
            
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);
            const query = status === 'support' ? { isSupport: true } : { isBanned: true };
            const count = await collection.countDocuments(query);
            
            await redisClient.hSet(STATS_KEY, status, count);
            
            return NextResponse.json({ message: `Recounted ${status}. New count: ${count}.`, count });
        } catch (error: any) {
            console.error(`Error recounting ${status}:`, error);
            return NextResponse.json({ error: `Failed to recount ${status}` }, { status: 500 });
        } finally {
            await mongoClient.close();
            if (redisClient.isOpen) {
                await redisClient.quit();
            }
        }
    }
    
    if (action === 'check_integrity') {
        const mongoClient = new MongoClient(MONGODB_URI);
        try {
            await mongoClient.connect();
            
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);

            const maxIdDoc = await collection.find({ status: { $ne: 'not_found' } }).sort({ id: -1 }).limit(1).project({ id: 1 }).toArray();
            const maxId = maxIdDoc.length > 0 ? maxIdDoc[0].id : 0;

            if (maxId === 0) {
                return NextResponse.json({ message: 'База данных пуста. Проверка не требуется.', missingCount: 0, missingIds: [] });
            }
            
            const pipeline = [
                 { $match: { id: { $lte: maxId } } },
                 { $group: { _id: null, ids: { $push: "$id" } } },
                 {
                    $project: {
                        _id: 0,
                        missingIds: {
                            $let: {
                                vars: { allIds: { $range: [1, maxId + 1] } },
                                in: { $setDifference: ["$$allIds", "$ids"] }
                            }
                        }
                    }
                }
            ];

            const result = await collection.aggregate(pipeline).toArray();
            const missingIds = result.length > 0 ? result[0].missingIds : [];

            if (missingIds.length > 0) {
                 return NextResponse.json({
                    message: `Обнаружено ${missingIds.length} пропущенных ID.`,
                    missingCount: missingIds.length,
                    missingIds: missingIds,
                });
            }

            return NextResponse.json({ message: 'Проверка завершена. Все ID на месте!', missingCount: 0, missingIds: [] });

        } catch (error: any) {
            console.error("Error checking DB integrity:", error);
            return NextResponse.json({ error: 'Не удалось проверить целостность базы данных' }, { status: 500 });
        } finally {
            await mongoClient.close();
        }
    }
    
    if (action === 'queue_missing' && Array.isArray(missingIds)) {
        const redisClient = createClient({ url: REDIS_URI });
        try {
            await redisClient.connect();
            if (missingIds.length > 0) {
                const multi = redisClient.multi();
                for (const id of missingIds) {
                    multi.lPush('failed_tasks', id.toString());
                }
                await multi.exec();
            }
            return NextResponse.json({ message: `${missingIds.length} ID были добавлены в приоритетную очередь для обработки.` });
        } catch (error: any) {
             console.error("Error queueing missing IDs:", error);
             return NextResponse.json({ error: 'Не удалось добавить ID в очередь' }, { status: 500 });
        } finally {
            if (redisClient.isOpen) {
                await redisClient.quit();
            }
        }
    }

    if (action === 'deduplicate') {
        const mongoClient = new MongoClient(MONGODB_URI);
        try {
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);

            const pipeline = [
                // Group by 'id' and keep the '_id' of all duplicates
                {
                    $group: {
                        _id: "$id",
                        dups: { $addToSet: "$_id" },
                        count: { $sum: 1 },
                        lastScraped: { $max: "$scrapedAt" } // Find the latest document
                    }
                },
                // Filter to get only those with duplicates
                { $match: { count: { $gt: 1 } } }
            ];

            const duplicates = await collection.aggregate(pipeline).toArray();
            let deletedCount = 0;

            if (duplicates.length === 0) {
                return NextResponse.json({ message: 'Дубликаты не найдены.' });
            }

            for (const group of duplicates) {
                // Find the document with the latest scrapedAt timestamp to keep it
                const latestDoc = await collection.findOne({ id: group._id, scrapedAt: group.lastScraped });
                
                if (latestDoc) {
                    const idsToDelete = group.dups.filter((dupId: ObjectId) => !dupId.equals(latestDoc._id));
                    if (idsToDelete.length > 0) {
                         const deleteResult = await collection.deleteMany({ _id: { $in: idsToDelete } });
                         deletedCount += deleteResult.deletedCount;
                    }
                }
            }

            return NextResponse.json({ message: `Удалено ${deletedCount} дубликатов.` });

        } catch (error: any) {
            console.error("Error removing duplicates:", error);
            return NextResponse.json({ error: 'Не удалось удалить дубликаты' }, { status: 500 });
        } finally {
            await mongoClient.close();
        }
    }


    return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
}

    