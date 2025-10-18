
"use server";
import { NextResponse, NextRequest } from 'next/server';
import { createClient } from 'redis';
import { getConfig } from '../status/route';
import { log } from '../project-logs/route';
import { headers } from 'next/headers';
import { runScraperProcess } from './runner';

const RUN_STATUS_KEY_PREFIX = 'scraper_status:';
const NEXT_ID_KEY = 'next_funpay_id_to_parse';

const getRunStatusKey = (workerId: string) => `${RUN_STATUS_KEY_PREFIX}${workerId}`;

export async function GET(request: NextRequest) {
    const { searchParams } = new URL(request.url);
    const workerId = searchParams.get('workerId');

    if (!workerId) {
        return NextResponse.json({ error: "Worker ID is required for status check" }, { status: 400 });
    }

    const { REDIS_URI } = await getConfig();
    if (!REDIS_URI) {
        return NextResponse.json({ isRunning: false, workerId: null, error: "Redis not configured" });
    }
    const redis = createClient({ url: REDIS_URI });
    
    try {
        await redis.connect();
        const runStatusKey = getRunStatusKey(workerId);
        const isRunning = await redis.exists(runStatusKey);
        return NextResponse.json({ isRunning, workerId: isRunning ? workerId : null });
    } catch (e: any) {
        return NextResponse.json({ isRunning: false, workerId: null, error: e.message }, { status: 500 });
    } finally {
        if(redis.isOpen) await redis.quit();
    }
}


export async function POST(request: NextRequest) {
    const { action, workerId } = await request.json();
    
    if (!workerId) {
        return NextResponse.json({ error: 'Worker ID is required' }, { status: 400 });
    }

    const config = await getConfig();
    const runStatusKey = getRunStatusKey(workerId);
    const redis = createClient({ url: config.REDIS_URI });
    
    try {
        await redis.connect();

        if (action === 'start') {
            const isAlreadyRunning = await redis.exists(runStatusKey);
            if (isAlreadyRunning) {
                return NextResponse.json({ message: `Scraper for worker ${workerId} is already running.` }, { status: 409 });
            }
            
            const isAnyWorkerRunning = (await redis.keys(`${RUN_STATUS_KEY_PREFIX}*`)).length > 0;
            if (!isAnyWorkerRunning) {
                // Reset counter only if this is the very first worker starting
                await redis.del(NEXT_ID_KEY);
                await log(`[Manager] First worker starting. Resetting ID counter.`);
            }

            await redis.set(runStatusKey, 'running');
            await log(`[Manager] Starting scraper for worker ${workerId}...`);

            // Asynchronously start the scraper process. No `await` here.
            runScraperProcess(workerId).catch(e => log(`[Manager] CRITICAL: Uncaught error in scraper runner for ${workerId}: ${e.message}`));

            return NextResponse.json({ message: `Scraper worker ${workerId} started.` });

        } else if (action === 'stop') {
            await log(`[Manager] Sending stop command to worker ${workerId}...`);
            await redis.set(runStatusKey, 'stopping');
            // The running process will see the 'stopping' status and shut down.
            // We also delete the key to be sure after a small delay.
            setTimeout(async () => {
                const redisClient = createClient({ url: config.REDIS_URI });
                await redisClient.connect();
                await redisClient.del(runStatusKey);
                await redisClient.quit();
            }, 5000); 

            return NextResponse.json({ message: `Stop command sent to worker ${workerId}.` });
        } else {
            return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
        }
    } catch (e: any) {
        await log(`[Manager] Error in POST /api/scrape for worker ${workerId}: ${e.message}`);
        return NextResponse.json({ error: e.message }, { status: 500 });
    } finally {
        if(redis.isOpen) await redis.quit();
    }
}
