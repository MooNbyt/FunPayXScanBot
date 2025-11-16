
"use server";
import { NextResponse, NextRequest } from 'next/server';
import { createClient } from 'redis';
import { getConfig } from '../status/route';
import { log } from '../project-logs/route';
import { headers } from 'next/headers';
import { runScraperProcess } from './runner';
import fs from 'fs/promises';
import path from 'path';

const RUN_STATUS_KEY_PREFIX = 'scraper_status:';
const LOG_FILE_PATH = path.join(process.cwd(), 'logs', 'scraper.log');

const getRunStatusKey = (workerId: string) => `${RUN_STATUS_KEY_PREFIX}${workerId}`;

export async function GET(request: NextRequest) {
    const { searchParams } = new URL(request.url);
    const action = searchParams.get('action');

    if (action === 'download_log') {
        try {
            const logContent = await fs.readFile(LOG_FILE_PATH, 'utf-8');
            const headers = new Headers();
            headers.set('Content-Type', 'text/plain');
            headers.set('Content-Disposition', `attachment; filename="scraper.log"`);
            return new NextResponse(logContent, { headers });
        } catch (error: any) {
            if (error.code === 'ENOENT') {
                return new NextResponse('Log file not found.', { status: 404 });
            }
            return new NextResponse('Error reading log file.', { status: 500 });
        }
    }


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
    
    if (!config.REDIS_URI) {
      await log(`[Manager] CRITICAL: Redis not configured. Cannot process POST request for worker ${workerId}.`);
      return NextResponse.json({ error: 'Redis is not configured' }, { status: 500 });
    }

    const redis = createClient({ url: config.REDIS_URI });
    
    try {
        await redis.connect();

        if (action === 'start') {
            const isAlreadyRunning = await redis.exists(runStatusKey);
            if (isAlreadyRunning) {
                return NextResponse.json({ message: `Scraper for worker ${workerId} is already running.` }, { status: 409 });
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

export async function DELETE(request: NextRequest) {
    const { searchParams } = new URL(request.url);
    const action = searchParams.get('action');

    if (action === 'clear_log') {
        try {
            await fs.writeFile(LOG_FILE_PATH, ''); // Overwrite with empty content
            return NextResponse.json({ message: 'Log file has been cleared.' });
        } catch (error: any) {
             if (error.code === 'ENOENT') {
                // If file doesn't exist, it's already "cleared"
                return NextResponse.json({ message: 'Log file did not exist, nothing to clear.' });
            }
            console.error("Error clearing log file:", error);
            return new NextResponse('Error clearing log file.', { status: 500 });
        }
    }

    return new NextResponse('Invalid DELETE action.', { status: 400 });
}
