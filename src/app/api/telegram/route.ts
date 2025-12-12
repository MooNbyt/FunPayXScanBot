
"use server";
import { NextResponse, NextRequest } from 'next/server';
import { createClient } from 'redis';
import { getIronSession } from 'iron-session';
import { cookies } from 'next/headers';
import { MongoClient } from 'mongodb';

import { getConfig, updateConfig } from '../status/route';
import { searchProfiles } from '../data/route';
import { setWebhook, setBotCommands, answerCallbackQuery, editMessageText, deleteMessage, sendMessage } from '@/lib/telegram/api';
import { getTelegramLogsKey } from '@/lib/telegram/utils';
import { SessionData, sessionOptions } from '@/lib/session';

import { handleStartCommand, handleTextMessage } from '@/lib/telegram/handlers/message';
import { handlePreCheckout, handleSuccessfulPayment } from '@/lib/telegram/handlers/payment';
import { handleSupportSearch, executeLetterSearch } from '@/lib/telegram/handlers/search';
import { handleShowCategories, handleShowProducts, handleViewProduct, handleAddToCart, handleViewCart, handleClearCart, handleCheckout } from '@/lib/telegram/handlers/shop';
import { handleInitiateConnection, handleConfirmConnection, handleCancelConnection, handleBuyConnections, handleCheckBalance } from '@/lib/telegram/handlers/connection';

const BOT_USERS_COLLECTION = "bot_users";

// Re-export for use in handlers
const localSearch = async (...args: Parameters<typeof searchProfiles>) => searchProfiles(...args);

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

async function updateUserInDb(chatId: number, from: any, config: any): Promise<boolean> {
    let isNewUser = false;
    let client: MongoClient | undefined;
    try {
        const { client: connectedClient, db } = await getDb();
        client = connectedClient;
        const collection = db.collection(BOT_USERS_COLLECTION);
        
        const now = new Date();
        const result = await collection.updateOne(
            { chatId: chatId, ownerId: config.WORKER_ID },
            {
                $set: {
                    username: from.username,
                    firstName: from.first_name,
                    status: 'active',
                    lastSeen: now,
                },
                $setOnInsert: {
                    joinedAt: now,
                    ownerId: config.WORKER_ID, // Ensure ownerId is set on insert
                }
            },
            { upsert: true }
        );
        
        isNewUser = !!result.upsertedId;

    } catch (e) {
        console.error("[Telegram] Failed to update user in DB", e);
    } finally {
        if(client) await client.close();
    }
    return isNewUser;
}


// GET Logs
export async function GET(request: Request) {
    const { REDIS_URI, WORKER_ID, TELEGRAM_LOGS_LIMIT } = await getConfig();
    if (!REDIS_URI) {
        return NextResponse.json({ error: 'Redis not configured' }, { status: 500 });
    }
    const redisClient = createClient({ url: REDIS_URI });
    const TELEGRAM_LOGS_KEY = getTelegramLogsKey(WORKER_ID);
    const maxLogEntries = TELEGRAM_LOGS_LIMIT || 200;
    try {
        await redisClient.connect();
        const logs = await redisClient.lRange(TELEGRAM_LOGS_KEY, 0, maxLogEntries - 1);
        const parsedLogs = logs.map(log => log ? JSON.parse(log) : null).filter(Boolean).reverse();
        return NextResponse.json(parsedLogs);
    } catch (error) {
        console.error("Error fetching telegram logs:", error);
        return NextResponse.json({ error: 'Failed to fetch logs' }, { status: 500 });
    } finally {
        if (redisClient.isOpen) {
            await redisClient.quit();
        }
    }
}

// POST Webhook
export async function POST(request: NextRequest) {
  const { url } = request;
  const urlParts = new URL(url);
  const workerIdFromQuery = urlParts.searchParams.get('worker');
  const config = await getConfig(workerIdFromQuery || undefined);
  
  const { TELEGRAM_TOKEN, REDIS_URI, TELEGRAM_LOGS_LIMIT, WORKER_ID } = config;

  if (!TELEGRAM_TOKEN) {
    console.error(`Telegram token not configured for worker ${WORKER_ID}`);
    return NextResponse.json({ status: 'ok' });
  }
  
  let redisClient: any;
  let chatInfo: any;
  let mongoClient: MongoClient | undefined;

  try {
    const body = await request.json();

    if (REDIS_URI) {
        redisClient = createClient({ url: REDIS_URI });
        await redisClient.connect();
        const logEntry = { timestamp: new Date().toISOString(), payload: body };
        const TELEGRAM_LOGS_KEY = getTelegramLogsKey(WORKER_ID);
        const maxLogEntries = TELEGRAM_LOGS_LIMIT || 200;
        await redisClient.lPush(TELEGRAM_LOGS_KEY, JSON.stringify(logEntry));
        await redisClient.lTrim(TELEGRAM_LOGS_KEY, 0, maxLogEntries - 1);
    }
    
     // --- Ban Check ---
    chatInfo = body.message?.chat || body.callback_query?.message?.chat;
    if (chatInfo) {
        try {
            const { client, db } = await getDb();
            mongoClient = client;
            const user = await db.collection(BOT_USERS_COLLECTION).findOne({ chatId: chatInfo.id, ownerId: WORKER_ID });
            if (user && user.status === 'banned') {
                // Inform the user they are banned, but only if it's a direct message action.
                if(body.message?.chat.id) {
                    await sendMessage(TELEGRAM_TOKEN, body.message.chat.id, "Вы заблокированы и не можете использовать этого бота.");
                } else if (body.callback_query?.id) {
                    await answerCallbackQuery(TELEGRAM_TOKEN, body.callback_query.id, "Вы заблокированы.", true);
                }
                return NextResponse.json({ status: 'ok' }); // Stop further processing
            }
        } catch (e) {
            console.error("Failed to check ban status:", e);
        } finally {
            if(mongoClient) await mongoClient.close();
        }
    }


    // Pass redisClient and other necessary functions/modules into the config for handlers to use
    const handlerConfig = { 
        ...config, 
        redisClient, 
        searchProfiles: localSearch,
        // Pass some api methods to avoid circular dependencies
        answerCallbackQuery,
        answerPreCheckoutQuery: config.answerPreCheckoutQuery,
        sendInvoice: config.sendInvoice,
        deleteMessage,
    };
    
    // Determine chat type and update user DB if it's a private message
    const fromInfo = body.message?.from || body.callback_query?.from;
    let isNewUser = false;
    if (chatInfo?.type === 'private' && fromInfo) {
        isNewUser = await updateUserInDb(chatInfo.id, fromInfo, config);
    }


    // --- Message Handling ---
    if (body.message) {
        const { message } = body;
        const chatId = message.chat.id;
        const text = message.text;
        const chatType = message.chat.type;
        const from = message.from;
        
        if (body.message.successful_payment) {
            await handleSuccessfulPayment(message.successful_payment, chatId, handlerConfig);
            return NextResponse.json({ status: 'ok' });
        }

        if (!text) {
             if (chatType === 'private') {
                 await handleStartCommand(chatId, chatType, handlerConfig, isNewUser);
             }
             return NextResponse.json({ status: 'ok' });
        }

        if (text.startsWith('/')) {
            let command = text.substring(1).split(' ')[0].toLowerCase();
            const botUsernameMatch = command.match(/^(.*?)@/);
            if (botUsernameMatch) command = botUsernameMatch[1];
            
            if (command === 'start') {
                await handleStartCommand(chatId, chatType, handlerConfig, isNewUser);
            } else if (chatType !== 'private') {
                 await handleTextMessage(chatId, text.substring(1), from, handlerConfig);
            } else {
                 await handleTextMessage(chatId, text, from, handlerConfig);
            }
        } else if (chatType === 'private') {
            await handleTextMessage(chatId, text, from, handlerConfig);
        }
    
    // --- Callback Query Handling ---
    } else if (body.callback_query) {
        const { callback_query } = body;
        const chatId = callback_query.message.chat.id;
        const messageId = callback_query.message.message_id;
        const data = callback_query.data;
        const from = callback_query.from;
        
        const fullHandlerConfig = { ...handlerConfig, message: callback_query.message, callback_query };

        await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id);

        const [action, ...params] = data.split(':');

        switch(action) {
            case 'search_support':
                await handleSupportSearch(chatId, parseInt(params[0] || '1', 10), messageId, fullHandlerConfig);
                break;
            case 'search_by_letter_init': {
                const text = "Выберите диапазон:";
                const keyboard = { inline_keyboard: [
                        [{ text: "А-Г", callback_data: "sbl_range:А-Г" }, { text: "Д-И", callback_data: "sbl_range:Д-И" }, { text: "К-О", callback_data: "sbl_range:К-О" }, { text: "П-У", callback_data: "sbl_range:П-У" }, { text: "Ф-Я", callback_data: "sbl_range:Ф-Я" }],
                        [{ text: "A-E", callback_data: "sbl_range:A-E" }, { text: "F-J", callback_data: "sbl_range:F-J" }, { text: "K-O", callback_data: "sbl_range:K-O" }, { text: "P-T", callback_data: "sbl_range:P-T" }, { text: "U-Z", callback_data: "sbl_range:U-Z" }],
                        [{ text: "0-9", callback_data: "sbl_range:0-9" }],
                        [{ text: "⬅️ В главное меню", callback_data: "main_menu" }]
                ]};
                if (callback_query.message.photo) {
                    await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                    await config.sendMessage(TELEGRAM_TOKEN, chatId, text, keyboard);
                } else {
                    await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
                }
                break;
            }
            case 'sbl_range': {
                const range = params[0];
                let chars: string[] = (range === '0-9') ? "0123456789".split('') : [];
                if (range !== '0-9') {
                    const [start, end] = range.split('-');
                    for (let i = start.charCodeAt(0); i <= end.charCodeAt(0); i++) chars.push(String.fromCharCode(i));
                }
                const keyboardRows = [];
                for (let i = 0; i < chars.length; i += 5) {
                    keyboardRows.push(chars.slice(i, i + 5).map(char => ({ text: char, callback_data: `sbl_char:${char}` })));
                }
                keyboardRows.push([{ text: "⬅️ Назад", callback_data: "search_by_letter_init" }]);
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, `Выберите букву/цифру из диапазона *${escapeMarkdown(range)}*:`, { inline_keyboard: keyboardRows });
                break;
            }
            case 'sbl_char':
                await executeLetterSearch(chatId, params[0], 1, messageId, fullHandlerConfig);
                break;
            case 'sbl_page':
                await executeLetterSearch(chatId, params[0], parseInt(params[1], 10), messageId, fullHandlerConfig);
                break;
            case 'get_profile_access':
            case 'search':
                 // This is now handled by successful_payment
                break;
            case 'show_categories':
                await handleShowCategories(chatId, parseInt(params[0] || '1', 10), messageId, fullHandlerConfig);
                break;
            case 'show_products':
                await handleShowProducts(chatId, params[0], parseInt(params[1] || '1', 10), messageId, fullHandlerConfig);
                break;
            case 'view_product':
                await handleViewProduct(chatId, params[0], messageId, fullHandlerConfig);
                break;
            case 'add_to_cart':
                await handleAddToCart(chatId, params[0], callback_query.id, fullHandlerConfig);
                break;
            case 'view_cart':
                await handleViewCart(chatId, messageId, fullHandlerConfig);
                break;
            case 'clear_cart':
                await handleClearCart(chatId, messageId, fullHandlerConfig);
                break;
            case 'buy_now':
            case 'checkout_cart':
                await handleCheckout(chatId, data, callback_query.id, fullHandlerConfig);
                break;
            case 'main_menu':
                await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await handleStartCommand(chatId, 'private', fullHandlerConfig, false);
                break;
            case 'initiate_connection':
                await handleInitiateConnection(chatId, fullHandlerConfig);
                break;
            case 'cancel_flow':
                if (redisClient) await redisClient.del(`telegram_user_state:${chatId}`);
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "Операция отменена\\.");
                break;
            case 'cancel_connection':
                await handleCancelConnection(chatId, messageId, data, fullHandlerConfig);
                break;
            case 'confirm_connection':
                await handleConfirmConnection(chatId, messageId, data, fullHandlerConfig);
                break;
            case 'buy_connections':
                await handleBuyConnections(chatId, fullHandlerConfig);
                break;
            case 'check_balance':
                await handleCheckBalance(chatId, callback_query.id, data, fullHandlerConfig);
                break;
        }

    // --- Pre-Checkout Query Handling ---
    } else if (body.pre_checkout_query) {
        const handlerConfig = { ...config, answerPreCheckoutQuery };
        await handlePreCheckout(body.pre_checkout_query, handlerConfig);
    }

    return NextResponse.json({ status: 'ok' });

  } catch (error: any) {
    console.error("Error handling telegram update:", error);
    // If the error is due to the bot being blocked, update the user status
    if (error.message.includes("bot was blocked by the user") && chatInfo?.id) {
        let client: MongoClient | undefined;
        try {
            const { client: connectedClient, db } = await getDb();
            client = connectedClient;
            await db.collection(BOT_USERS_COLLECTION).updateOne(
                { chatId: chatInfo.id, ownerId: WORKER_ID },
                { $set: { status: 'blocked' } }
            );
        } catch (dbError) {
            console.error("[Telegram] Failed to set user status to 'blocked'", dbError);
        } finally {
            if(client) await client.close();
        }
    }
    return NextResponse.json({ status: 'ok' }); // Always return ok to Telegram
  } finally {
      if (redisClient && redisClient.isOpen) {
          await redisClient.quit();
      }
      if(mongoClient) await mongoClient.close();
  }
}

// PUT Webhook
export async function PUT(request: NextRequest) {
    const { token } = await request.json();
    const config = await getConfig();
    
    let appUrl = config.NEXT_PUBLIC_APP_URL; // Already determined correctly in getConfig

    if (!appUrl) {
        return NextResponse.json({ error: 'App public URL could not be determined. Set NEXT_PUBLIC_APP_URL environment variable.' }, { status: 400 });
    }
    if (!token) {
        return NextResponse.json({ error: 'Telegram token not provided' }, { status: 400 });
    }
    
    const url = new URL(appUrl);
    url.pathname = '/api/telegram';
    url.searchParams.set('worker', config.WORKER_ID);
    const webhookUrl = url.toString();

    try {
        const webhookResult = await setWebhook(token, webhookUrl);
        if (webhookResult.ok) {
            await setBotCommands(token);
            await updateConfig({ TELEGRAM_TOKEN: token });
            return NextResponse.json({ message: `Вебхук успешно установлен на ${webhookUrl}` });
        } else {
            console.error('Webhook Error:', webhookResult);
            const description = webhookResult.description || "Не удалось установить вебхук.";
            if (description.includes("bot token is already taken")) {
                 return NextResponse.json({ error: `Этот токен уже используется другим сервером. Попробуйте сбросить токен у @BotFather.` }, { status: 409 });
            }
             if (description.includes("invalid bot token")) {
                 return NextResponse.json({ error: `Неверный токен. Проверьте правильность введенного токена.` }, { status: 400 });
            }
            throw new Error(description);
        }
    } catch (error: any) {
        console.error('Webhook Exception:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}

// DELETE Logs
export async function DELETE(request: NextRequest) {
    const session = await getIronSession<SessionData>(cookies(), sessionOptions);
    if (!session.isSettingsUnlocked) {
         return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
    }
    
    const config = await getConfig();
    if (!config.REDIS_URI) {
        return NextResponse.json({ error: 'Redis не сконфигурирован' }, { status: 500 });
    }
    
    let redisClient: any;
    try {
        redisClient = createClient({ url: config.REDIS_URI });
        await redisClient.connect();
        const logsKey = getTelegramLogsKey(config.WORKER_ID);
        await redisClient.del(logsKey);
        return NextResponse.json({ message: 'Логи Telegram успешно очищены.' });

    } catch (error: any) {
        console.error("Ошибка при очистке логов Telegram:", error);
        return NextResponse.json({ error: 'Не удалось очистить логи' }, { status: 500 });
    } finally {
        if (redisClient?.isOpen) {
            await redisClient.quit();
        }
    }
}
