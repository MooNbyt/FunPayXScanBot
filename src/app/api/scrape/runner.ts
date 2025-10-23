
"use server";
import { createClient } from 'redis';
import { MongoClient } from 'mongodb';
import { getConfig } from '../status/route';
import { log as projectLog } from '../project-logs/route';
import * as cheerio from 'cheerio';
import fs from 'fs/promises';
import path from 'path';


const RUN_STATUS_KEY_PREFIX = 'scraper_status:';
const NEXT_ID_KEY = 'next_funpay_id_to_parse';
const LAST_ERROR_ID_KEY = 'scraper_last_error_id';
const RECENT_PROFILES_KEY = 'recent_profiles';
const STATS_KEY = 'scraping_stats';
const USERS_COLLECTION = 'users';
const FAILED_TASKS_KEY = 'failed_tasks';
const SCRAPER_LOCK_KEY = 'scraper_process_lock';
const LOCK_TTL_SECONDS = 10;
const INTEGRITY_CHECK_LOCK_KEY = 'integrity_check_lock';
const INTEGRITY_LOCK_TTL_SECONDS = 300; // 5 minutes lock for check

// New keys for global 404 handling
const GLOBAL_CONSECUTIVE_404_KEY = 'scraper_global_consecutive_404';
const GLOBAL_404_START_ID_KEY = 'scraper_global_404_start_id';
const GLOBAL_PAUSE_UNTIL_KEY = 'scraper_global_pause_until';

const LOG_FILE_PATH = path.join(process.cwd(), 'logs', 'scraper.log');

async function fileLog(message: string, enabled: boolean) {
    if (!enabled) return;
    const logEntry = `${new Date().toISOString()} - ${message}\n`;
    try {
        await fs.mkdir(path.dirname(LOG_FILE_PATH), { recursive: true });
        await fs.appendFile(LOG_FILE_PATH, logEntry);
    } catch (e) {
        console.error(`Failed to write to log file: ${e}`);
    }
}


export async function scrapeUser(id: number, logPrefix: string, fileLoggingEnabled: boolean): Promise<any> {
    const url = `https://funpay.com/users/${id}/`;
    try {
        const response = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            },
            signal: AbortSignal.timeout(30000) // 30s timeout
        });

        if (response.status === 404) {
            return { error: true, status: 404, message: "Пользователь не найден" };
        }
        
        if (response.status === 429) {
             const msg = `${logPrefix} Достигнут лимит запросов для ID ${id} (статус 429)`;
             await projectLog(msg);
             await fileLog(msg, fileLoggingEnabled);
            return { error: true, status: 429, message: "Слишком много запросов" };
        }

        if (!response.ok) {
             const msg = `${logPrefix} Ошибка сервера для ID ${id} (Статус: ${response.status})`;
             await projectLog(msg);
             await fileLog(msg, fileLoggingEnabled);
            return { error: true, status: response.status, message: `Сервер ответил с ${response.status}` };
        }

        const html = await response.text();
        const $ = cheerio.load(html);

        const titleText = $('title').text();
        if (titleText.includes("Ошибка 404") || titleText.includes("Пользователь не найден") || !titleText) {
             return { error: true, status: 404, message: "Пользователь не найден (проверка по заголовку)" };
        }
        
        const nicknameMatch = titleText.match(/Пользователь (.*?) \//);
        const nickname = nicknameMatch ? nicknameMatch[1] : null;

        if (!nickname) {
            return { error: true, status: 404, message: "Пользователь не найден (пустой никнейм)" };
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
            status: 'found'
        };

    } catch (error: any) {
        if (error.name === 'AbortError') {
             const errorMsg = `[Скрейпер] КРИТИЧЕСКАЯ ОШИБКА в scrapeUser для ID ${id}: Операция прервана по таймауту`;
             await fileLog(errorMsg, fileLoggingEnabled);
             await projectLog(errorMsg);
             return { error: true, status: 500, message: "Операция прервана по таймауту" };
        }
        const errorMsg = `[Скрейпер] КРИТИЧЕСКАЯ ОШИБКА в scrapeUser для ID ${id}: ${error.message}`;
        await fileLog(errorMsg, fileLoggingEnabled);
        await projectLog(errorMsg);
        return { error: true, status: 500, message: error.message };
    }
}


export async function runScraperProcess(workerId: string) {
    const getRunStatusKey = (workerId: string) => `${RUN_STATUS_KEY_PREFIX}${workerId}`;
    const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

    if (!workerId) {
        console.error(`[Scraper Runner] CRITICAL: Scraper started without a workerId. Exiting.`);
        await projectLog(`[Scraper Runner] КРИТИЧЕСКАЯ ОШИБКА: Скрейпер запущен без ID воркера. Выход.`);
        return;
    }
    
    let config = await getConfig();
    const { REDIS_URI, MONGODB_URI, MONGODB_DB_NAME } = config;
    const fileLoggingEnabled = () => config.SCRAPER_FILE_LOGGING_ENABLED;
    
    if (!REDIS_URI || !MONGODB_URI) {
        const errorMsg = `[Скрейпер ${workerId}] КРИТИЧЕСКАЯ ОШИБКА: БД не настроена. Выход.`;
        await fileLog(errorMsg, true);
        await projectLog(errorMsg);
        const redisForCleanup = createClient({ url: REDIS_URI });
        try {
            await redisForCleanup.connect();
            await redisForCleanup.del(getRunStatusKey(workerId));
        } catch(e) {
            console.error(`[Скрейпер ${workerId}] Не удалось подключиться к Redis для очистки.`, e);
        } finally {
            if (redisForCleanup.isOpen) await redisForCleanup.quit();
        }
        return;
    }

    const redis = createClient({ url: REDIS_URI });
    const mongo = new MongoClient(MONGODB_URI);
    const runStatusKey = getRunStatusKey(workerId);
    let writeBuffer: any[] = [];
    
    let recentRequests: (0 | 1)[] = []; // 0 for error, 1 for success
    let isStableMode = false;
    let currentParallelLimit = config.SCRAPER_PARALLEL_REQUEST_LIMIT_MIN || 1;
    let currentDelay = config.SCRAPER_ADAPTIVE_DELAY_MIN_MS || 500;
    let successStreak = 0;

    const saveBuffer = async () => {
        if (writeBuffer.length === 0) return;
        try {
            const collection = mongo.db(new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME).collection(USERS_COLLECTION);
            
            const updates = writeBuffer.map(profile => ({
                updateOne: {
                    filter: { id: profile.id },
                    update: { $set: profile },
                    upsert: true
                }
            }));
            
            await collection.bulkWrite(updates, { ordered: false });

            const successMsg = `[Скрейпер ${workerId}] Успешно сохранена пачка из ${writeBuffer.length} профилей.`;
            await fileLog(successMsg, fileLoggingEnabled());
            await projectLog(successMsg);
            
            const redisMulti = redis.multi();
            const foundProfiles = writeBuffer.filter(p => p.status === 'found');

            if(foundProfiles.length > 0) {
              const currentConfig = await getConfig();
              const recentProfilesLimit = currentConfig.SCRAPER_RECENT_PROFILES_LIMIT || 100;
              
              redisMulti.hIncrBy(STATS_KEY, 'successful', foundProfiles.length);
              
              foundProfiles.slice(0, recentProfilesLimit).forEach(profile => {
                  const profileString = JSON.stringify(profile);
                  redisMulti.lPush(RECENT_PROFILES_KEY, profileString);
              });

              redisMulti.lTrim(RECENT_PROFILES_KEY, 0, recentProfilesLimit - 1);
            }
            
            const supportCount = writeBuffer.filter(p => p.isSupport).length;
            const bannedCount = writeBuffer.filter(p => p.isBanned).length;
            if(supportCount > 0) redisMulti.hIncrBy(STATS_KEY, 'support', supportCount);
            if(bannedCount > 0) redisMulti.hIncrBy(STATS_KEY, 'banned', bannedCount);

            await redisMulti.exec();
        
        } catch (dbError: any) {
            if (dbError.code !== 11000) {
                 const errorMsg = `[Скрейпер ${workerId}] КРИТИЧЕСКАЯ ОШИБКА: Ошибка массовой записи в БД: ${dbError.message}`;
                 await fileLog(errorMsg, fileLoggingEnabled());
                 await projectLog(errorMsg);
            }
        } finally {
            writeBuffer = [];
        }
    };

    try {
        await redis.connect();
        await mongo.connect();
        
        const startMsg = `[Скрейпер ${workerId}] Запущен.`;
        await fileLog(startMsg, fileLoggingEnabled());
        await projectLog(startMsg);
        await redis.set(runStatusKey, 'running');

        // --- Reset adaptive parameters on start ---
        currentParallelLimit = config.SCRAPER_PARALLEL_REQUEST_LIMIT_MIN || 1;
        currentDelay = config.SCRAPER_ADAPTIVE_DELAY_MIN_MS || 500;
        successStreak = 0;
        isStableMode = false;
        recentRequests = [];

        const isFirstWorker = (await redis.keys(`${RUN_STATUS_KEY_PREFIX}*`)).length <= 1;

        if (isFirstWorker) {
            const integrityLock = await redis.set(INTEGRITY_CHECK_LOCK_KEY, workerId, { NX: true, EX: INTEGRITY_LOCK_TTL_SECONDS });
            
            if (integrityLock) {
                const setupMsg = `[Скрейпер ${workerId}] Захвачена блокировка целостности. Выполняется первоначальная настройка...`;
                await fileLog(setupMsg, fileLoggingEnabled());
                await projectLog(setupMsg);
                
                const keyPatterns = [
                    'scraper_global_consecutive_404', 'scraper_global_404_start_id', 'scraper_global_pause_until',
                    'scraper_process_lock', 'writer_lock', 'last_successful_write_id', 'dedicated_writer_worker_id',
                    'scraper_last_error_id'
                ];
                let allKeys: string[] = [];
                for (const pattern of keyPatterns) {
                    const keys = await redis.keys(pattern);
                    allKeys.push(...keys);
                }
                const uniqueKeys = [...new Set(allKeys)];
                if (uniqueKeys.length > 0) {
                    await redis.del(uniqueKeys);
                    const cleanupMsg = `[Скрейпер ${workerId}] Очищено ${uniqueKeys.length} старых ключей Redis.`;
                    await fileLog(cleanupMsg, fileLoggingEnabled());
                    await projectLog(cleanupMsg);
                }
                
                const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
                const collection = mongo.db(dbName).collection(USERS_COLLECTION);
                
                // Ensure unique index on 'id'
                try {
                  await collection.createIndex({ "id": 1 }, { unique: true, name: "id_1" });
                  const indexMsg = `[Скрейпер ${workerId}] Уникальный индекс по полю 'id' обеспечен.`;
                  await fileLog(indexMsg, fileLoggingEnabled());
                  await projectLog(indexMsg);
                } catch(e: any) {
                    // Ignore errors if index already exists with different options or is being built.
                }

                // Ensure text index on 'nickname' for faster searches
                try {
                    await collection.createIndex({ "nickname": "text" }, { name: "nickname_text" });
                    const indexMsg = `[Скрейпер ${workerId}] Текстовый индекс по полю 'nickname' обеспечен.`;
                    await fileLog(indexMsg, fileLoggingEnabled());
                    await projectLog(indexMsg);
                } catch(e: any) {
                    // Ignore errors if index already exists or is being built.
                }


                const lastUser = await collection.find({ status: { $ne: 'not_found' } }).sort({ id: -1 }).limit(1).project({ id: 1 }).toArray();
                const maxId = lastUser.length > 0 ? lastUser[0].id : 0;
                await redis.set(NEXT_ID_KEY, maxId);
                const initMsg = `[Скрейпер ${workerId}] Счетчик инициализирован значением ${maxId}`;
                await fileLog(initMsg, fileLoggingEnabled());
                await projectLog(initMsg);
                
                const integrityStartMsg = `[Скрейпер ${workerId}] Запуск проверки целостности базы данных...`;
                await fileLog(integrityStartMsg, fileLoggingEnabled());
                await projectLog(integrityStartMsg);
                if (maxId > 0) {
                    const existingIdsCursor = collection.find({ id: { $lte: maxId } }, { projection: { id: 1, _id: 0 } });
                    const existingIds = new Set();
                    for await (const doc of existingIdsCursor) {
                        existingIds.add(doc.id);
                    }
                    
                    const missingIds: number[] = [];
                    for (let i = 1; i <= maxId; i++) {
                        if (!existingIds.has(i)) {
                            missingIds.push(i);
                        }
                    }

                    if (missingIds.length > 0) {
                        const missingMsg = `[Скрейпер ${workerId}] Найдено ${missingIds.length} пропущенных ID. Добавление в приоритетную очередь...`;
                        await fileLog(missingMsg, fileLoggingEnabled());
                        await projectLog(missingMsg);
                        const chunkSize = 5000;
                        for (let i = 0; i < missingIds.length; i += chunkSize) {
                           const chunk = missingIds.slice(i, i + chunkSize);
                           await redis.lPush(FAILED_TASKS_KEY, chunk.map(String));
                           const chunkMsg = `[Скрейпер ${workerId}] Добавлено в очередь ${chunk.length} пропущенных ID.`;
                           await fileLog(chunkMsg, fileLoggingEnabled());
                           await projectLog(chunkMsg);
                        }
                        const finishQueueMsg = `[Скрейпер ${workerId}] Завершено добавление всех пропущенных ID в очередь.`;
                         await fileLog(finishQueueMsg, fileLoggingEnabled());
                         await projectLog(finishQueueMsg);
                    } else {
                        const noMissingMsg = `[Скрейпер ${workerId}] Проверка целостности завершена. Пропущенные ID не найдены.`;
                        await fileLog(noMissingMsg, fileLoggingEnabled());
                        await projectLog(noMissingMsg);
                    }
                }
                
                await redis.del(INTEGRITY_CHECK_LOCK_KEY);
                const completeMsg = `[Скрейпер ${workerId}] Первоначальная настройка завершена. Блокировка снята.`;
                await fileLog(completeMsg, fileLoggingEnabled());
                await projectLog(completeMsg);
            } else {
                 const waitMsg = `[Скрейпер ${workerId}] Другой воркер выполняет настройку. Ожидание...`;
                 await fileLog(waitMsg, fileLoggingEnabled());
                 await projectLog(waitMsg);
            }
        }
        
        main_loop:
        while(true) {
            config = await getConfig();
            const { 
                SCRAPER_BATCH_SIZE, SCRAPER_WRITE_BATCH_SIZE, SCRAPER_CONSECUTIVE_ERROR_LIMIT,
                SCRAPER_PAUSE_DURATION_MS, SCRAPER_PARALLEL_REQUEST_LIMIT_MIN, SCRAPER_PARALLEL_REQUEST_LIMIT_MAX,
                SCRAPER_ADAPTIVE_DELAY_MIN_MS, SCRAPER_ADAPTIVE_DELAY_MAX_MS, SCRAPER_ADAPTIVE_DELAY_STEP_MS,
                SCRAPER_SUCCESS_STREAK_TO_INCREASE_LIMIT, SCRAPER_DELAY_COMPENSATION_MS,
                SCRAPER_ANALYSIS_WINDOW, SCRAPER_SUCCESS_THRESHOLD,
            } = config;
            
            const status = await redis.get(runStatusKey);
            if (status !== 'running') {
                const stopSignalMsg = `[Скрейпер ${workerId}] Получен сигнал остановки ('${status}'). Выход из основного цикла.`;
                await fileLog(stopSignalMsg, fileLoggingEnabled());
                await projectLog(stopSignalMsg);
                break main_loop;
            }

            const pauseUntilTimestamp = await redis.get(GLOBAL_PAUSE_UNTIL_KEY);
            const integrityCheckActive = await redis.exists(INTEGRITY_CHECK_LOCK_KEY);

            if (integrityCheckActive) {
                 const integrityWaitMsg = `[Скрейпер ${workerId}] Выполняется проверка целостности. Ожидание...`;
                 await fileLog(integrityWaitMsg, fileLoggingEnabled());
                 await projectLog(integrityWaitMsg);
                 await delay(5000);
                 continue;
            }
            if (pauseUntilTimestamp && Date.now() < parseInt(pauseUntilTimestamp)) {
                const pauseWaitMsg = `[Скрейпер ${workerId}] Активна глобальная пауза. Ожидание...`;
                await fileLog(pauseWaitMsg, fileLoggingEnabled());
                await projectLog(pauseWaitMsg);
                await delay(60000);
                continue;
            } else if (pauseUntilTimestamp) {
                const pauseFinishMsg = `[Скрейпер ${workerId}] Глобальная пауза завершена. Возобновление работы.`;
                await fileLog(pauseFinishMsg, fileLoggingEnabled());
                await projectLog(pauseFinishMsg);
                await redis.del(GLOBAL_PAUSE_UNTIL_KEY);
            }

            let idsToProcess: (number | null)[] = [];
            const priorityQueueSize = await redis.lLen(FAILED_TASKS_KEY);

            if (priorityQueueSize > 0) {
                 const batchSize = Math.min(priorityQueueSize, Number(SCRAPER_BATCH_SIZE) || 20);
                 const multi = redis.multi();
                 for (let i = 0; i < batchSize; i++) {
                     multi.lPop(FAILED_TASKS_KEY);
                 }
                 const idsFromQueue = await multi.exec() as (string | null)[];

                 idsToProcess = idsFromQueue.filter(id => id !== null).map(id => parseInt(id!));
                 if(idsToProcess.length > 0) {
                    const queueTakeMsg = `[Скрейпер ${workerId}] Взято ${idsToProcess.length} ID из приоритетной очереди.`;
                    await fileLog(queueTakeMsg, fileLoggingEnabled());
                    await projectLog(queueTakeMsg);
                 }
            } else {
                const batchSize = Number(SCRAPER_BATCH_SIZE) || 20;
                
                let lockAcquired = false;
                while(!lockAcquired) {
                    lockAcquired = await redis.set(SCRAPER_LOCK_KEY, workerId, { NX: true, EX: LOCK_TTL_SECONDS });
                    if (!lockAcquired) {
                        await delay(100);
                    }
                }
                
                try {
                    const startId = await redis.incrBy(NEXT_ID_KEY, batchSize) - batchSize + 1;
                    const batchTakeMsg = `[Скрейпер ${workerId}] Взята пачка из ${batchSize} ID, начиная с ${startId}.`;
                    await fileLog(batchTakeMsg, fileLoggingEnabled());
                    await projectLog(batchTakeMsg);
                    for (let i = 0; i < batchSize; i++) {
                        idsToProcess.push(startId + i);
                    }
                } finally {
                    await redis.del(SCRAPER_LOCK_KEY);
                }
            }

            if (idsToProcess.length === 0) {
                await delay(1000);
                continue;
            }

            let hadErrorInMainBatch = false;
            let shouldBreakMainLoop = false;

            for (let i = 0; i < idsToProcess.length; i += currentParallelLimit) {
                const subBatch = idsToProcess.slice(i, i + currentParallelLimit).filter(id => id !== null) as number[];
                if (subBatch.length === 0) continue;
                
                await delay(currentDelay);
                
                const subBatchNumber = Math.floor(i / currentParallelLimit) + 1;
                const totalSubBatches = Math.ceil(idsToProcess.length / currentParallelLimit);
                const mode_log = isStableMode ? '(Стабильный режим)' : '(Режим настройки)';
                const subBatchMsg = `[Скрейпер ${workerId}] Обработка под-пачки ${subBatchNumber}/${totalSubBatches} с ${subBatch.length} ID. Задержка: ${currentDelay}ms. Лимит: ${currentParallelLimit}. ${mode_log}`;
                await fileLog(subBatchMsg, fileLoggingEnabled());
                
                const currentStatus = await redis.get(runStatusKey);
                if (currentStatus !== 'running') {
                    const unprocessed = idsToProcess.slice(i).filter(id => id !== null) as number[];
                    if (unprocessed.length > 0) {
                        await redis.lPush(FAILED_TASKS_KEY, unprocessed.map(String));
                        const returnQueueMsg = `[Скрейпер ${workerId}] Получен сигнал остановки. Возвращено ${unprocessed.length} необработанных ID в очередь.`;
                        await fileLog(returnQueueMsg, fileLoggingEnabled());
                        await projectLog(returnQueueMsg);
                    }
                    shouldBreakMainLoop = true;
                    break;
                }
            
                const promises = subBatch.map(id => scrapeUser(id, `[Скрейпер ${workerId}]`, fileLoggingEnabled()));
                const results = await Promise.all(promises);

                for (let j = 0; j < results.length; j++) {
                    const profile = results[j];
                    const currentId = subBatch[j];

                    if (profile.error) {
                        hadErrorInMainBatch = true;
                        successStreak = 0;
                        recentRequests.push(0);

                        if (profile.status === 429) {
                            const oldLimit = currentParallelLimit;
                            const oldDelay = currentDelay;
                            
                            isStableMode = false; // Выход из стабильного режима при любой ошибке 429
                            currentParallelLimit = Math.max(Number(SCRAPER_PARALLEL_REQUEST_LIMIT_MIN) || 1, currentParallelLimit - 1);
                            currentDelay = Math.min(Number(SCRAPER_ADAPTIVE_DELAY_MAX_MS) || 10000, currentDelay + (Number(SCRAPER_ADAPTIVE_DELAY_STEP_MS) || 100));
                            
                            const rollbackMsg = `[Скрейпер ${workerId}] Обнаружен 429. Откат: Новый лимит: ${currentParallelLimit} (был ${oldLimit}), Новая задержка: ${currentDelay}ms (была ${oldDelay}).`;
                            await fileLog(rollbackMsg, fileLoggingEnabled());
                            await projectLog(rollbackMsg);
                            
                            await redis.lPush(FAILED_TASKS_KEY, currentId.toString());

                        } else if (profile.status === 404) {
                            const errorLimit = Number(SCRAPER_CONSECUTIVE_ERROR_LIMIT) || 100;
                            const current404Count = await redis.incr(GLOBAL_CONSECUTIVE_404_KEY);

                            if (current404Count < errorLimit) {
                                const profileToSave = { id: currentId, status: 'not_found', scrapedAt: new Date().toISOString(), scrapedBy: workerId };
                                writeBuffer.push(profileToSave);
                            }
                            
                            if (current404Count === 1) {
                                await redis.set(GLOBAL_404_START_ID_KEY, currentId);
                            }
                            await redis.set(LAST_ERROR_ID_KEY, currentId);

                            if (current404Count >= errorLimit) {
                                const limitReachedMsg = `[Скрейпер ${workerId}] Достигнут ГЛОБАЛЬНЫЙ лимит 404 ошибок (${errorLimit}).`;
                                await fileLog(limitReachedMsg, fileLoggingEnabled());
                                await projectLog(limitReachedMsg);
                                const pauseDuration = Number(SCRAPER_PAUSE_DURATION_MS) || 21600000;
                                const pauseUntil = Date.now() + pauseDuration;
                                await redis.set(GLOBAL_PAUSE_UNTIL_KEY, pauseUntil);

                                const startIdOf404Sequence = await redis.get(GLOBAL_404_START_ID_KEY);
                                if (startIdOf404Sequence) {
                                    const resetCounterMsg = `[Скрейпер ${workerId}] Сброс глобального счетчика на ${startIdOf404Sequence} для следующего запуска.`;
                                    await fileLog(resetCounterMsg, fileLoggingEnabled());
                                    await projectLog(resetCounterMsg);
                                    await redis.set(NEXT_ID_KEY, parseInt(startIdOf404Sequence) - 1);
                                }
                                const pauseMsg = `[Скрейпер ${workerId}] Все воркеры будут приостановлены на ${pauseDuration / 1000 / 60} минут.`;
                                await fileLog(pauseMsg, fileLoggingEnabled());
                                await projectLog(pauseMsg);
                                shouldBreakMainLoop = true;
                                break; 
                            }
                        } else {
                             const errorRequeueMsg = `[Скрейпер ${workerId}] Ошибка при получении ID ${currentId} (Статус: ${profile.status}). Возвращено в очередь.`;
                             await fileLog(errorRequeueMsg, fileLoggingEnabled());
                             await projectLog(errorRequeueMsg);
                             await redis.lPush(FAILED_TASKS_KEY, currentId.toString());
                        }
                    } else {
                        recentRequests.push(1);
                        await redis.del([GLOBAL_CONSECUTIVE_404_KEY, GLOBAL_404_START_ID_KEY]);
                        const profileToSave = { ...profile, scrapedBy: workerId, url: `https://funpay.com/users/${profile.id}/` };
                        writeBuffer.push(profileToSave);
                    }
                }
                
                if (shouldBreakMainLoop) break;
            } 

            // Обрезаем историю запросов до размера окна
            while (recentRequests.length > SCRAPER_ANALYSIS_WINDOW) {
                recentRequests.shift();
            }

            if (!hadErrorInMainBatch) {
                successStreak++;
                
                if (!isStableMode) {
                    // Логика перехода в стабильный режим
                    if (recentRequests.length >= SCRAPER_ANALYSIS_WINDOW) {
                        const successCount = recentRequests.reduce((a, b) => a + b, 0);
                        const successRate = (successCount / recentRequests.length) * 100;
                        if (successRate >= SCRAPER_SUCCESS_THRESHOLD) {
                            isStableMode = true;
                            const stableMsg = `[Скрейпер ${workerId}] Переход в стабильный режим. Успех: ${successRate.toFixed(1)}%. Скорость: ${currentParallelLimit} запросов / ${currentDelay}мс.`;
                            await fileLog(stableMsg, fileLoggingEnabled());
                            await projectLog(stableMsg);
                        }
                    }
                    
                    // Логика оптимизации в режиме настройки
                    const requiredStreak = Number(SCRAPER_SUCCESS_STREAK_TO_INCREASE_LIMIT) || 3;
                    if (successStreak >= requiredStreak) {
                        const oldLimit = currentParallelLimit;
                        const oldDelay = currentDelay;
                        if (currentParallelLimit < (Number(SCRAPER_PARALLEL_REQUEST_LIMIT_MAX) || 10)) {
                            currentParallelLimit++;
                            currentDelay = Math.max(Number(SCRAPER_ADAPTIVE_DELAY_MIN_MS) || 500, currentDelay + (Number(SCRAPER_DELAY_COMPENSATION_MS) || 10));
                            successStreak = 0; // Сбрасываем счетчик после повышения
                            
                            const successMsg = `[Скрейпер ${workerId}] Оптимизация: Новый лимит: ${currentParallelLimit} (был ${oldLimit}), Новая задержка: ${currentDelay}ms (была ${oldDelay}).`;
                            await fileLog(successMsg, fileLoggingEnabled());
                            await projectLog(successMsg);
                        }
                    }
                }
            }


            if (writeBuffer.length >= (Number(SCRAPER_WRITE_BATCH_SIZE) || 20)) {
                await saveBuffer();
            }

            if (shouldBreakMainLoop) break main_loop;

        } // End of main loop (while)

    } catch (error: any) {
        const criticalErrorMsg = `[Скрейпер ${workerId}] КРИТИЧЕСКАЯ ОШИБКА в основном цикле: ${error.message}\n${error.stack}`;
        await fileLog(criticalErrorMsg, fileLoggingEnabled());
        await projectLog(criticalErrorMsg);
    } finally {
        if(writeBuffer.length > 0) {
            const finalSaveMsg = `[Скрейпер ${workerId}] Остановка. Сохранение финального буфера из ${writeBuffer.length} профилей.`;
            await fileLog(finalSaveMsg, fileLoggingEnabled());
            await projectLog(finalSaveMsg);
            await saveBuffer();
        }
        if (fileLoggingEnabled()) {
            const aggregateStartMsg = `[Скрейпер ${workerId}] Агрегация логов из Redis в файл...`;
            await fileLog(aggregateStartMsg, fileLoggingEnabled());
            
            const projectLogsFromRedis = await redis.lRange('project_logs', 0, -1);
            const criticalLogsFromRedis = await redis.lRange('critical_project_logs', 0, -1);
            
            let allLogs = [...projectLogsFromRedis, ...criticalLogsFromRedis]
                .map(log => log ? JSON.parse(log) : null)
                .filter(Boolean)
                .sort((a,b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())
                .map(log => `${log.timestamp} - [Redis Log] ${log.message}`);
            
            if (allLogs.length > 0) {
                 await fs.appendFile(LOG_FILE_PATH, `\n--- Сводка логов из Redis при остановке ---\n${allLogs.join('\n')}\n`);
                 const aggregateFinishMsg = `[Скрейпер ${workerId}] Сведено ${allLogs.length} логов из Redis.`;
                 await fileLog(aggregateFinishMsg, fileLoggingEnabled());
            }
        }
        await redis.del(runStatusKey);
        if (redis.isOpen) await redis.quit();
        if (mongo) await mongo.close();
        
        const stopMsg = `[Скрейпер ${workerId}] Остановлен.`;
        await fileLog(stopMsg, fileLoggingEnabled());
        await projectLog(stopMsg);
    }
}

    