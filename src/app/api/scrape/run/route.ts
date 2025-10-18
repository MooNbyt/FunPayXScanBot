
"use server";
import { NextResponse, NextRequest } from 'next/server';
import { createClient } from 'redis';
import { MongoClient } from 'mongodb';
import { getConfig } from '../../status/route';
import { log } from '../../project-logs/route';
import * as cheerio from 'cheerio';


const RUN_STATUS_KEY_PREFIX = 'scraper_status:';
const NEXT_ID_KEY = 'next_funpay_id_to_parse';
const ERROR_404_COUNTER_KEY = 'scraper_404_error_count';
const LAST_ERROR_ID_KEY = 'scraper_last_error_id';
const RECENT_PROFILES_KEY = 'recent_profiles';
const STATS_KEY = 'scraping_stats';
const USERS_COLLECTION = 'users';
const FAILED_TASKS_KEY = 'failed_tasks';
const SCRAPER_LOCK_KEY = 'scraper_process_lock';
const LOCK_TTL_SECONDS = 10; // 10-секундный замок на случай, если воркер умрет, не освободив его

const getRunStatusKey = (workerId: string) => `${RUN_STATUS_KEY_PREFIX}${workerId}`;

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function scrapeUser(id: number, logPrefix: string): Promise<any> {
    const url = `https://funpay.com/users/${id}/`;
    try {
        const response = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            signal: AbortSignal.timeout(15000)
        });

        if (response.status === 404) {
            return { error: true, status: 404, message: "User not found" };
        }
        
        if (!response.ok) {
            return { error: true, status: response.status, message: `Server responded with ${response.status}` };
        }

        const html = await response.text();
        const $ = cheerio.load(html);

        const titleText = $('title').text();
        if (titleText.includes("Ошибка 404") || titleText.includes("Пользователь не найден") || !titleText) {
             return { error: true, status: 404, message: "User not found (title check)" };
        }
        
        const nicknameMatch = titleText.match(/Пользователь (.*?) \//);
        const nickname = nicknameMatch ? nicknameMatch[1] : null;

        if (!nickname) {
             return { error: true, status: 404, message: "User not found (empty nickname)" };
        }
        
        let regDate = "Не указана";
        $('.param-item').each((i, el) => {
            if ($(el).find('h5.text-bold').text().trim() === 'Дата регистрации') {
                const dateElement = $(el).find('.text-nowrap');
                if (dateElement.length > 0) {
                    const dateParts = dateElement.html()?.split('<br>').map(s => s.trim()) || [];
                    if (dateParts.length > 0) {
                        regDate = dateParts[0];
                    }
                }
            }
        });
        
        const reviewCountText = $('.rating-full-count a').text() || "0";
        const reviewCount = parseInt(reviewCountText.replace(/\D/g, '') || '0', 10);
        
        const lotCount = $('a[data-href*="/lots/offer?id="]').length;

        const isBanned = $('.label.label-danger').text().includes('заблокирован');
        const isSupport = $('.label.label-success').text().includes('поддержка');

        return {
            id,
            nickname,
            regDate,
            reviewCount,
            lotCount,
            isBanned,
            isSupport,
            scrapedAt: new Date().toISOString(),
        };

    } catch (error: any) {
        await log(`${logPrefix} CRITICAL: Error in scrapeUser for ID ${id}: ${error.message}`);
        return { error: true, status: 500, message: error.message };
    }
}


export async function GET(request: NextRequest) {
    const { searchParams } = new URL(request.url);
    const workerId = searchParams.get('workerId');

    if (!workerId) {
        return new NextResponse("Worker ID is required", { status: 400 });
    }
    
    // Initial config load
    let config = await getConfig();
    const { REDIS_URI, MONGODB_URI, MONGODB_DB_NAME } = config;
    
    if (!REDIS_URI || !MONGODB_URI) {
        await log(`[Scraper ${workerId}] CRITICAL: DB not configured. Exiting.`);
        return NextResponse.json({ message: "Database not configured. Exiting." });
    }

    const redis = createClient({ url: REDIS_URI });
    const mongo = new MongoClient(MONGODB_URI);
    const runStatusKey = getRunStatusKey(workerId);
    let writeBuffer: any[] = [];


    // This function will be called to save data and MUST be awaited.
    const saveBuffer = async () => {
        if (writeBuffer.length === 0) return;
        try {
            await mongo.db(new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME)
                       .collection(USERS_COLLECTION)
                       .insertMany(writeBuffer, { ordered: false });

            await log(`[Scraper ${workerId}] Successfully saved batch of ${writeBuffer.length} profiles.`);
            
            const redisMulti = redis.multi();
            writeBuffer.forEach(profile => {
                const profileString = JSON.stringify(profile);
                redisMulti.hIncrBy(STATS_KEY, 'successful', 1);
                redisMulti.lPush(RECENT_PROFILES_KEY, profileString);
                if (profile.isSupport) redisMulti.hIncrBy(STATS_KEY, 'support', 1);
                if (profile.isBanned) redisMulti.hIncrBy(STATS_KEY, 'banned', 1);
            });
            // Use the most up-to-date config for this
            redisMulti.lTrim(RECENT_PROFILES_KEY, 0, config.SCRAPER_RECENT_PROFILES_LIMIT - 1);
            await redisMulti.exec();
        
        } catch (dbError: any) {
            // Ignore duplicate key errors, which are expected with insertMany and multiple workers
            if (dbError.code !== 11000) {
                 await log(`[Scraper ${workerId}] CRITICAL: DB bulk write error: ${dbError.message}`);
            }
        } finally {
            writeBuffer = []; // Clear buffer after attempt
        }
    };

    try {
        await redis.connect();
        await mongo.connect();
        
        await log(`[Scraper ${workerId}] Started.`);
        await redis.set(runStatusKey, 'running');

        const counterExists = await redis.exists(NEXT_ID_KEY);
        if (!counterExists) {
             const lockAcquired = await redis.set(SCRAPER_LOCK_KEY, workerId, { NX: true, EX: LOCK_TTL_SECONDS });
             if (lockAcquired) {
                 try {
                    const lastUser = await mongo.db(new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME).collection(USERS_COLLECTION).find().sort({ id: -1 }).limit(1).project({ id: 1 }).toArray();
                    const maxId = lastUser.length > 0 ? lastUser[0].id : 0;
                    await redis.set(NEXT_ID_KEY, maxId);
                    await log(`[Scraper ${workerId}] Initialized counter to ${maxId + 1}`);
                 } catch (e: any) {
                     await log(`[Scraper ${workerId}] CRITICAL: Failed to initialize counter from MongoDB: ${e.message}. Setting to 0.`);
                     await redis.set(NEXT_ID_KEY, 0);
                 } finally {
                     await redis.del(SCRAPER_LOCK_KEY);
                 }
             } else {
                 await delay(2000); // Wait if another worker is initializing
             }
        }
        
        let consecutive404 = 0;
        
        main_loop:
        while(true) {
            // Refresh config before each batch claim
            config = await getConfig();
            const { 
                SCRAPER_BATCH_SIZE, SCRAPER_WRITE_BATCH_SIZE, SCRAPER_CONSECUTIVE_ERROR_LIMIT,
                SCRAPER_PAUSE_DURATION_MS, SCRAPER_BAN_PAUSE_MINUTES,
            } = config;
            
            const status = await redis.get(runStatusKey);
            if (status !== 'running') {
                await log(`[Scraper ${workerId}] Stop signal received ('${status}'). Exiting main loop.`);
                break main_loop;
            }

            const priorityId = await redis.lPop(FAILED_TASKS_KEY);

            let idsToProcess: number[] = [];

            if(priorityId) {
                idsToProcess.push(parseInt(priorityId));
                await log(`[Scraper ${workerId}] Processing priority ID ${priorityId} from failed_tasks queue.`);
            } else {
                let startId = 0;
                let batchSize = Number(SCRAPER_BATCH_SIZE) || 1;
                
                // Acquire lock to get a batch of IDs
                let lockAcquired = false;
                while(!lockAcquired) {
                    lockAcquired = await redis.set(SCRAPER_LOCK_KEY, workerId, { NX: true, EX: LOCK_TTL_SECONDS });
                    if (!lockAcquired) {
                        await delay(100); // Wait and retry
                    }
                }
                
                try {
                    startId = await redis.incrBy(NEXT_ID_KEY, batchSize) - batchSize + 1;
                } finally {
                    await redis.del(SCRAPER_LOCK_KEY); // Always release the lock
                }
                
                await log(`[Scraper ${workerId}] Claimed ID range: ${startId} - ${startId + batchSize - 1}`);
                for (let i = 0; i < batchSize; i++) {
                    idsToProcess.push(startId + i);
                }
            }
            
            for (const currentId of idsToProcess) {
                
                // Re-check status before processing each ID inside the batch loop
                 const currentStatus = await redis.get(runStatusKey);
                 if (currentStatus !== 'running') {
                    await log(`[Scraper ${workerId}] Stop signal received during batch. Exiting inner loop.`);
                    break main_loop; // Exit the main loop directly
                 }

                let profile;
                let attempts = 0;
                const maxAttempts = 3;
                
                while(attempts < maxAttempts) {
                    profile = await scrapeUser(currentId, `[Scraper ${workerId}]`);
                    
                    if (profile.error) {
                        if (profile.status === 404) {
                            await log(`[Scraper ${workerId}] ID ${currentId} not found (404).`);
                            consecutive404++;
                            await redis.incr(ERROR_404_COUNTER_KEY);
                            await redis.set(LAST_ERROR_ID_KEY, currentId);
                            break; // Exit retry loop for 404
                        } else {
                            attempts++;
                            const pauseMinutes = Number(SCRAPER_BAN_PAUSE_MINUTES) || 1;
                            await log(`[Scraper ${workerId}] Error fetching ID ${currentId} (Status: ${profile.status}, Attempt: ${attempts}). Pausing for ${pauseMinutes} min...`);
                            await delay(pauseMinutes * 60 * 1000);
                        }
                    } else {
                        consecutive404 = 0; // Reset counter on success
                        break; // Exit retry loop on success
                    }
                }

                if (profile && !profile.error) {
                    const profileToSave = { ...profile, scrapedBy: workerId, url: `https://funpay.com/users/${profile.id}/` };
                    writeBuffer.push(profileToSave);
                    await log(`[Scraper ${workerId}] Scraped ID ${currentId} (Nick: ${profileToSave.nickname}), buffer size: ${writeBuffer.length}`);
                    
                    if (writeBuffer.length >= (Number(SCRAPER_WRITE_BATCH_SIZE) || 50)) {
                        await saveBuffer();
                    }
                }
                
                if (consecutive404 >= (Number(SCRAPER_CONSECUTIVE_ERROR_LIMIT) || 100)) {
                    await log(`[Scraper ${workerId}] Hit ${SCRAPER_CONSECUTIVE_ERROR_LIMIT} consecutive 404s. Saving buffer and pausing for ${SCRAPER_PAUSE_DURATION_MS / 1000 / 60} minutes.`);
                    await saveBuffer(); // Save whatever is in buffer before long pause
                    await redis.set(runStatusKey, 'paused-404');
                    await delay(Number(SCRAPER_PAUSE_DURATION_MS) || 21600000);
                    consecutive404 = 0; // Reset after pause
                    if(await redis.get(getRunStatusKey(workerId)) !== 'stopping') {
                       await redis.set(runStatusKey, 'running');
                    }
                }
            } // End of for loop for batch

            await delay(500); // Small delay between batches
        } // End of while loop

    } catch (error: any) {
        await log(`[Scraper ${workerId}] CRITICAL ERROR: ${error.message}`);
    } finally {
        if(writeBuffer.length > 0) {
            await log(`[Scraper ${workerId}] Stopping. Saving final buffer of ${writeBuffer.length} profiles.`);
            await saveBuffer();
        }
        await redis.del(runStatusKey);
        if (redis.isOpen) await redis.quit();
        if (mongo) await mongo.close();
        await log(`[Scraper ${workerId}] Stopped.`);
    }

    return NextResponse.json({ message: `Scraper worker ${workerId} has stopped.` });
}

    

    