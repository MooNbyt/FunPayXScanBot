
"use server";
import { NextResponse } from 'next/server';
import { createClient } from 'redis';
import { getConfig } from '../status/route';

const PROJECT_LOGS_KEY = 'project_logs';
const CRITICAL_PROJECT_LOGS_KEY = 'critical_project_logs';

const MAX_LOG_ENTRIES = 200;

const CRITICAL_KEYWORDS = ['Error', 'CRITICAL', 'Failed', '⚠️'];

export async function log(message: string) {
    const config = await getConfig();
    if (!config.REDIS_URI) {
        console.log(message); // Fallback to console if Redis is not configured
        return;
    }
    let redisClient: any;
    try {
        redisClient = createClient({ url: config.REDIS_URI });
        await redisClient.connect();
        const logEntry = JSON.stringify({ timestamp: new Date().toISOString(), message });
        
        const isCritical = CRITICAL_KEYWORDS.some(keyword => message.includes(keyword));

        if (isCritical) {
            await redisClient.lPush(CRITICAL_PROJECT_LOGS_KEY, logEntry);
            // Критические логи не обрезаются и не имеют TTL
        } else {
            const multi = redisClient.multi();
            multi.lPush(PROJECT_LOGS_KEY, logEntry);
            multi.lTrim(PROJECT_LOGS_KEY, 0, MAX_LOG_ENTRIES - 1);
            
            const ttlSeconds = (config.PROJECT_LOGS_TTL_HOURS || 1) * 3600;
            multi.expire(PROJECT_LOGS_KEY, ttlSeconds);
            
            await multi.exec();
        }

    } catch (error) {
        console.error("Error writing to project log:", error);
        console.log(message); // Log to console as a fallback
    } finally {
        if (redisClient?.isOpen) {
            await redisClient.quit();
        }
    }
}


export async function GET() {
    const { REDIS_URI } = await getConfig();
    if (!REDIS_URI) {
        // If redis is not configured, return a single log entry explaining this.
        const logs = [{ timestamp: new Date().toISOString(), message: "Redis не сконфигурирован. Логи недоступны." }];
        const criticalLogs = [{ timestamp: new Date().toISOString(), message: "Redis не сконфигурирован. Критические ошибки недоступны." }];
        return NextResponse.json({ logs, criticalLogs });
    }
    const redisClient = createClient({ url: REDIS_URI });
    try {
        await redisClient.connect();
        const [logStrings, criticalLogStrings] = await Promise.all([
            redisClient.lRange(PROJECT_LOGS_KEY, 0, -1),
            redisClient.lRange(CRITICAL_PROJECT_LOGS_KEY, 0, -1)
        ]);
        
        const logs = logStrings.map((log) => JSON.parse(log)).reverse(); 
        const criticalLogs = criticalLogStrings.map((log) => JSON.parse(log)).reverse();

        return NextResponse.json({ logs, criticalLogs });
    } catch (error) {
        console.error("Error fetching project logs:", error);
        return NextResponse.json({ error: 'Failed to fetch project logs' }, { status: 500 });
    } finally {
        if (redisClient.isOpen) {
            await redisClient.quit();
        }
    }
}

export async function DELETE(request: Request) {
    const { searchParams } = new URL(request.url);
    const scope = searchParams.get('scope') || 'all'; // 'all', 'critical', 'regular'

    const { REDIS_URI } = await getConfig();
    if (!REDIS_URI) {
        return NextResponse.json({ error: 'Redis не сконфигурирован' }, { status: 500 });
    }
    const redisClient = createClient({ url: REDIS_URI });
    try {
        await redisClient.connect();
        
        let keysToDelete = [];
        if (scope === 'critical' || scope === 'all') {
            keysToDelete.push(CRITICAL_PROJECT_LOGS_KEY);
        }
        if (scope === 'regular' || scope === 'all') {
            keysToDelete.push(PROJECT_LOGS_KEY);
        }

        if (keysToDelete.length > 0) {
            await redisClient.del(keysToDelete);
        }
        
        return NextResponse.json({ message: 'Логи успешно очищены.' });
    } catch (error) {
        console.error("Error deleting project logs:", error);
        return NextResponse.json({ error: 'Не удалось удалить логи проекта' }, { status: 500 });
    } finally {
        if (redisClient.isOpen) {
            await redisClient.quit();
        }
    }
}
