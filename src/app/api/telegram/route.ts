
"use server";
import { NextResponse, NextRequest } from 'next/server';
import { getConfig, updateConfig } from '../status/route';
import { searchProfiles } from '../data/route';
import { scrapeUser } from '../scrape/run/route';
import { URL } from 'url';
import { createClient } from 'redis';
import { MongoClient, ObjectId } from 'mongodb';
import { headers } from 'next/headers';

const getTelegramLogsKey = (workerId: string) => `telegram_logs:${workerId}`;
const MAX_LOG_ENTRIES = 200; 
const USER_STATES_KEY_PREFIX = 'telegram_user_state:';
const CONNECTION_REQUEST_PREFIX = 'connect:';
const CONNECTION_CONFIRM_PREFIX = 'confirm:';
const USER_ACTIVE_REQUEST_PREFIX = 'user_active_request:';
const CONNECTION_TTL_SECONDS = 86400; // 24 hours for requests
const USER_CONNECTION_BALANCE_PREFIX = 'telegram_user_connections:';
const TELEGRAM_CART_PREFIX = 'telegram_cart:';
const CART_TTL_SECONDS = 86400; // 24 hours for user cart


const BASE_URL = (token: string) => `https://api.telegram.org/bot${token}`;

// Helper to convert to smallest currency unit (e.g., kopecks, cents)
const toSmallestUnit = (amount: number) => Math.round(amount * 100);


const addUrlToProfile = (profile: any) => ({
    ...profile,
    url: `https://funpay.com/users/${profile.id}/`
});

async function apiCall(token: string, method: string, payload: any) {
    const url = `${BASE_URL(token)}/${method}`;
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const result = await response.json();
        if (!result.ok) {
            console.error(`Telegram API Error (${method}):`, result.description);
        }
        return result;
    } catch (error) {
        console.error(`Failed to call Telegram API method ${method}:`, error);
        return { ok: false, error };
    }
}

async function sendMessage(token: string, chatId: number, text: string, replyMarkup?: any) {
  return apiCall(token, 'sendMessage', {
    chat_id: chatId,
    text: text,
    parse_mode: 'Markdown',
    reply_markup: replyMarkup,
    disable_web_page_preview: true,
  });
}

async function sendInvoice(token: string, chatId: number, title: string, description: string, payload: string, providerToken: string, currency: string, prices: any[]) {
    return apiCall(token, 'sendInvoice', {
        chat_id: chatId,
        title,
        description,
        payload,
        provider_token: providerToken,
        currency,
        prices
    });
}

async function answerPreCheckoutQuery(token: string, preCheckoutQueryId: string, ok: boolean, errorMessage?: string) {
    return apiCall(token, 'answerPreCheckoutQuery', {
        pre_checkout_query_id: preCheckoutQueryId,
        ok,
        error_message: errorMessage
    });
}

async function sendPhoto(token: string, chatId: number, photoUrl: string, caption: string, replyMarkup?: any) {
  return apiCall(token, 'sendPhoto', {
    chat_id: chatId,
    photo: photoUrl,
    caption: caption,
    parse_mode: 'Markdown',
    reply_markup: replyMarkup,
  });
}

async function editMessageText(token: string, chatId: number, messageId: number, text: string, replyMarkup?: any) {
   const result = await apiCall(token, 'editMessageText', {
        chat_id: chatId,
        message_id: messageId,
        text: text,
        parse_mode: 'Markdown',
        reply_markup: replyMarkup,
        disable_web_page_preview: true,
    });
    // If editing fails because the message is the same, it's not a critical error.
    if (!result.ok && result.description && (result.description.includes('message is not modified') || result.description.includes("there is no text in the message to edit"))) {
        return { ...result, ok: true }; // Treat as non-fatal
    }
    return result;
}

async function answerCallbackQuery(token: string, callbackQueryId: string, text?: string) {
    return apiCall(token, 'answerCallbackQuery', {
        callback_query_id: callbackQueryId,
        text: text
    });
}

async function deleteMessage(token: string, chatId: number, messageId: number) {
    return apiCall(token, 'deleteMessage', {
        chat_id: chatId,
        message_id: messageId,
    });
}

const getUserStateKey = (chatId: number) => `${USER_STATES_KEY_PREFIX}${chatId}`;
const getUserConnectionBalanceKey = (chatId: number) => `${USER_CONNECTION_BALANCE_PREFIX}${chatId}`;
const getCartKey = (chatId: number) => `${TELEGRAM_CART_PREFIX}${chatId}`;
const cancelKeyboard = { inline_keyboard: [[{ text: "❌ Отмена", callback_data: `cancel_flow` }]] };


const executeSearch = async (token: string, chatId: number, query: string, config: any) => {
    const isNumeric = /^\d+$/.test(query);
    const searchType = isNumeric ? 'id' : 'nickname';

    const statusMessage = await sendMessage(token, chatId, `⏳ Ищу пользователя по ${searchType === 'id' ? 'ID' : 'никнейму'}: *${query}*...`);
    const loadingMessageId = statusMessage.result?.message_id;

    if (searchType === 'id') {
        const userId = parseInt(query, 10);
        const updatedProfile = await scrapeUser(userId, `[TelegramBot]`);

        if (updatedProfile && !updatedProfile.error) {
            const p = addUrlToProfile(updatedProfile);
            const scrapedAt = new Date(p.scrapedAt);
            const formattedDate = `${scrapedAt.getFullYear()}-${String(scrapedAt.getMonth() + 1).padStart(2, '0')}-${String(scrapedAt.getDate()).padStart(2, '0')} ${String(scrapedAt.getHours()).padStart(2, '0')}:${String(scrapedAt.getMinutes()).padStart(2, '0')}:${String(scrapedAt.getSeconds()).padStart(2, '0')}`;

            let message = `*ID:* \`${p.id}\`\n`;
            message += `*Никнейм:* ${p.nickname}\n`;
            message += `*Дата регистрации:* ${p.regDate} (${p.regDateRelative || 'N/A'})\n`;
            message += `*Кол-во отзывов:* ${p.reviewCount}\n\n`;
            message += `*Бан:* ${p.isBanned ? '✅ Да' : '❌ Нет'}\n`;
            message += `*Саппорт:* ${p.isSupport ? '✅ Да' : '❌ Нет'}\n\n`;
            message += `*Кол-во лотов:* ${p.lotCount}\n`;
            message += `*Ссылка:* [Перейти на профиль](${p.url})\n\n`;
            message += `🕒 *Актуально на:* ${formattedDate}`;

            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, message);
        } else {
            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, `Пользователь с ID ${userId} не найден на FunPay.`);
        }
    } else { // nickname search
        const initialResponse = await searchProfiles(query, 'nickname', config);
        const profiles = await initialResponse.json();

        if (profiles.length > 0) {
            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, `⏳ Найдено ${profiles.length} профилей. Обновляю их данные... (Это может занять время)`);
            
            const updatedProfiles = await Promise.all(profiles.slice(0, 5).map((p: any) => scrapeUser(p.id, `[TelegramBot]`)));

            let message = `*Найденные и обновленные профили по запросу "${query}":*\n\n`;
            updatedProfiles.filter(p => p && !p.error).forEach((p: any) => {
                const profileWithUrl = addUrlToProfile(p);
                message += `*${profileWithUrl.nickname}* (ID: \`${profileWithUrl.id}\`) - [Профиль](${profileWithUrl.url})\n`;
            });
            if (profiles.length > 5) {
                message += `\n... и еще ${profiles.length - 5} профилей (обновлены первые 5).`
            }
            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, message);
        } else {
            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, `Пользователи с никнеймом "${query}" не найдены.`);
        }
    }
};


export async function GET(request: Request) {
    const { REDIS_URI, WORKER_ID } = await getConfig();
    if (!REDIS_URI) {
        return NextResponse.json({ error: 'Redis not configured' }, { status: 500 });
    }
    const redisClient = createClient({ url: REDIS_URI });
    const TELEGRAM_LOGS_KEY = getTelegramLogsKey(WORKER_ID);
    try {
        await redisClient.connect();
        const logs = await redisClient.lRange(TELEGRAM_LOGS_KEY, 0, MAX_LOG_ENTRIES - 1);
        const parsedLogs = logs.map(log => log ? JSON.parse(log) : null).filter(Boolean).reverse(); // Reverse to show newest first
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

export async function POST(request: NextRequest) {
  const { url } = request;
  const urlParts = new URL(url);
  // A way to pass workerId if the webhook URL is unique per worker.
  // Example: /api/telegram?worker=worker-2
  const workerIdFromQuery = urlParts.searchParams.get('worker');
  const config = await getConfig(workerIdFromQuery || undefined);
  
  const { 
      TELEGRAM_TOKEN,
      MONGODB_URI, 
      MONGODB_DB_NAME,
      REDIS_URI, 
      WORKER_ID,
      TELEGRAM_WELCOME_MESSAGE, 
      TELEGRAM_WELCOME_IMAGE_URL,
      TELEGRAM_PAYMENT_ENABLED,
      TELEGRAM_PROVIDER_TOKEN,
      TELEGRAM_SEARCH_COST_STARS,
      TELEGRAM_SEARCH_COST_REAL,
      TELEGRAM_PAYMENT_CURRENCY,
      TELEGRAM_CONNECTION_PAYMENT_ENABLED,
      TELEGRAM_CONNECTION_COST_STARS,
      TELEGRAM_CONNECTION_COST_REAL,
      TELEGRAM_SHOP_BUTTON_NAME,
  } = config;

  if (!TELEGRAM_TOKEN) {
    console.error(`Telegram token not configured for worker ${WORKER_ID}`);
    return NextResponse.json({ status: 'ok' });
  }
  
  let redisClient: any;
  let mongoClient: MongoClient | undefined;
  const TELEGRAM_LOGS_KEY = getTelegramLogsKey(WORKER_ID);

  try {
    const body = await request.json();

    if (REDIS_URI) {
        redisClient = createClient({ url: REDIS_URI });
        await redisClient.connect();
        const logEntry = { timestamp: new Date().toISOString(), payload: body };
        await redisClient.lPush(TELEGRAM_LOGS_KEY, JSON.stringify(logEntry));
        await redisClient.lTrim(TELEGRAM_LOGS_KEY, 0, MAX_LOG_ENTRIES - 1);
    }

    if (body.pre_checkout_query) {
        const { pre_checkout_query } = body;
        const payload = pre_checkout_query.invoice_payload;
        
        if (payload.startsWith('product:')) {
            const productId = payload.split(':')[1];
            if (!MONGODB_URI) {
                 await answerPreCheckoutQuery(TELEGRAM_TOKEN, pre_checkout_query.id, false, "База данных не настроена.");
                 return NextResponse.json({ status: 'ok' });
            }
            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const product = await mongoClient.db(dbName).collection("products").findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });

            if (!product) {
                await answerPreCheckoutQuery(TELEGRAM_TOKEN, pre_checkout_query.id, false, "Товар больше не доступен.");
                return NextResponse.json({ status: 'ok' });
            }

            if (product.type === 'static') {
                const keys = (product.staticKey || '').split('\n').filter((k: string) => k.trim() !== '');
                if (keys.length === 0) {
                     await answerPreCheckoutQuery(TELEGRAM_TOKEN, pre_checkout_query.id, false, "К сожалению, этот товар закончился.");
                     return NextResponse.json({ status: 'ok' });
                }
            }
        }
        
        await answerPreCheckoutQuery(TELEGRAM_TOKEN, pre_checkout_query.id, true);
        return NextResponse.json({ status: 'ok' });
    }

    if (body.message && body.message.successful_payment) {
        const { successful_payment } = body.message;
        const chatId = body.message.chat.id;
        const invoicePayload = successful_payment.invoice_payload;

        if (invoicePayload.startsWith('search:')) {
            const query = invoicePayload.substring('search:'.length);
            await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата прошла успешно! Выполняю поиск по запросу: *${query}*`);
            await executeSearch(TELEGRAM_TOKEN, chatId, query, config);
        } else if (invoicePayload === 'buy_1_connection') {
            const balanceKey = getUserConnectionBalanceKey(chatId);
            await redisClient.incr(balanceKey);
            await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата прошла успешно! Вам зачислена 1 попытка установки связи.`);
        } else if (invoicePayload.startsWith('product:')) {
            const productId = invoicePayload.split(':')[1];
             if (!MONGODB_URI) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Ошибка. База данных не настроена.");
                return NextResponse.json({ status: 'ok' });
            }
            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const productsCollection = mongoClient.db(dbName).collection("products");
            const product = await productsCollection.findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });
            
            if (!product) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Ошибка. Товар не найден после оплаты.");
                return NextResponse.json({ status: 'ok' });
            }

            await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата за *${product.invoiceTitle}* прошла успешно!`);

            if (product.type === 'api') {
                try {
                    const apiResponse = await fetch(product.apiUrl, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': `Bearer ${product.apiToken}`
                        },
                        body: JSON.stringify({ validityDays: product.apiDays })
                    });
                    const data = await apiResponse.json();
                    if (data.success && data.key) {
                        await sendMessage(TELEGRAM_TOKEN, chatId, `Ваш ключ: \`${data.key}\``);
                    } else {
                        throw new Error(data.message || "Не удалось сгенерировать ключ.");
                    }
                } catch (e: any) {
                    console.error("API product error:", e);
                    await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Произошла ошибка при генерации вашего ключа. Свяжитесь с администратором.");
                }

            } else { // static
                const keys = (product.staticKey || '').split('\n').filter((k: string) => k.trim() !== '');
                if (keys.length > 0) {
                    const keyToIssue = keys.shift(); // Get the first key and remove it from the array
                    await sendMessage(TELEGRAM_TOKEN, chatId, `Ваш товар: \`${keyToIssue}\``);
                    
                    // Update the product in the database with the remaining keys
                    const updatedStaticKey = keys.join('\n');
                    await productsCollection.updateOne(
                        { _id: new ObjectId(productId) },
                        { $set: { staticKey: updatedStaticKey } }
                    );
                } else {
                    await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Извините, товар закончился. Пожалуйста, свяжитесь с администратором.");
                }
            }
        }
        
        return NextResponse.json({ status: 'ok' });
    }


    const handleStartCommand = async (chatId: number, chatType: string) => {
        if(redisClient) {
            await redisClient.del(getUserStateKey(chatId));
        }

        let welcomeMessage = TELEGRAM_WELCOME_MESSAGE || "🤖 Привет! Я твой помощник.";
        let userCount = 0;
        if(MONGODB_URI) {
            try {
                mongoClient = new MongoClient(MONGODB_URI);
                await mongoClient.connect();
                const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
                userCount = await mongoClient.db(dbName).collection("users").countDocuments();
            } catch (e) {
                console.error("Failed to get user count for welcome message", e);
            }
        }
        
        welcomeMessage = welcomeMessage.replace(/{user_count}/g, userCount.toLocaleString('ru-RU'));
        
        const commonButtons = [
             [{ text: "💚 Найти саппортов", callback_data: "search_support" }],
        ];
        
        let privateButtons = [
             [{ text: "🤝 Установить связь", callback_data: "initiate_connection" }],
             ...commonButtons
        ];
        
        if (TELEGRAM_SHOP_BUTTON_NAME && chatType === 'private') {
            privateButtons.splice(1, 0, [{ text: `🛍️ ${TELEGRAM_SHOP_BUTTON_NAME}`, callback_data: "show_categories" }]);
        }
        
        if (chatType === 'private') {
            const buttonsToAdd = [];
             if (TELEGRAM_CONNECTION_PAYMENT_ENABLED && redisClient) {
                const balanceKey = getUserConnectionBalanceKey(chatId);
                const balance = await redisClient.get(balanceKey) || 0;
                buttonsToAdd.push({ text: `⭐️ Связей: ${balance}`, callback_data: "check_balance:connection" }, { text: "Купить", callback_data: "buy_connections" });
            }
            if(buttonsToAdd.length > 0) {
                privateButtons.unshift(buttonsToAdd);
            }
        }
        
        const mainMenu = {
          inline_keyboard: privateButtons
        };

        if (TELEGRAM_WELCOME_IMAGE_URL) {
            await sendPhoto(TELEGRAM_TOKEN, chatId, TELEGRAM_WELCOME_IMAGE_URL, welcomeMessage, mainMenu);
        } else {
            await sendMessage(TELEGRAM_TOKEN, chatId, welcomeMessage, mainMenu);
        }
    };
    
    const handleSupportSearch = async (chatId: number) => {
        const statusMessage = await sendMessage(TELEGRAM_TOKEN, chatId, '⏳ Ищу профили поддержки...');
        const loadingMessageId = statusMessage.result?.message_id;

        try {
            const response = await searchProfiles('', 'status', config, 'support');
            const profiles = await response.json();
            
            let responseText = `*Найденные профили поддержки (${profiles.length} шт.):*\n\n`;
            if (profiles.length > 0) {
                profiles.slice(0, 30).forEach((p: any) => { // Limit to 30 to avoid message overflow
                     responseText += `*${p.nickname}* (ID: \`${p.id}\`) - [Профиль](${p.url})\n`;
                });
                 if (profiles.length > 30) {
                    responseText += `\n...и еще ${profiles.length - 30}.`
                }
            } else {
                responseText = "Профили поддержки не найдены.";
            }

            if (loadingMessageId) {
                await editMessageText(TELEGRAM_TOKEN, chatId, loadingMessageId, responseText);
            } else {
                await sendMessage(TELEGRAM_TOKEN, chatId, responseText);
            }
        } catch (e: any) {
            const errorText = "Произошла ошибка при поиске саппортов.";
            if (loadingMessageId) {
                await editMessageText(TELEGRAM_TOKEN, chatId, loadingMessageId, errorText);
            } else {
                await sendMessage(TELEGRAM_TOKEN, chatId, errorText);
            }
        }
    };

    const handleConnectionLogic = async (chatId: number, text: string, from: any) => {
        const stateKey = getUserStateKey(chatId);
        const stateRaw = await redisClient.get(stateKey);
        if (!stateRaw) return;
        
        const state = JSON.parse(stateRaw);

        if (state.step === 'awaiting_my_id') {
            if (!/^\d+$/.test(text)) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Это не похоже на ID. Пожалуйста, введите корректный числовой ID вашего профиля FunPay.", cancelKeyboard);
                return;
            }
            const myId = text;
            const newState = { step: 'awaiting_partner_id', myId: myId, messageId: state.messageId };
            await redisClient.set(stateKey, JSON.stringify(newState), { EX: 300 }); 
            await editMessageText(TELEGRAM_TOKEN, chatId, state.messageId, "✅ Отлично. Теперь введите ID профиля FunPay, с которым хотите связаться.", cancelKeyboard);

        } else if (state.step === 'awaiting_partner_id') {
            if (!/^\d+$/.test(text)) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Это не похоже на ID. Пожалуйста, введите корректный числовой ID партнера.", cancelKeyboard);
                return;
            }
            const myId = state.myId;
            const partnerId = text;
            const flowMessageId = state.messageId;

            if(myId === partnerId) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "😅 Нельзя установить связь с самим собой. Пожалуйста, введите ID другого пользователя.");
                // Reset to previous step
                const newState = { step: 'awaiting_partner_id', myId: myId, messageId: flowMessageId };
                await redisClient.set(stateKey, JSON.stringify(newState), { EX: 300 });
                return;
            }
            
            await redisClient.del(stateKey);
            
            // Decrement connection balance if payment is enabled
            if (TELEGRAM_CONNECTION_PAYMENT_ENABLED) {
                const balanceKey = getUserConnectionBalanceKey(chatId);
                await redisClient.decr(balanceKey);
                const newBalance = await redisClient.get(balanceKey) || 0;
                await sendMessage(TELEGRAM_TOKEN, chatId, `Попытка связи использована. У вас осталось: ${newBalance}.`);
            }

            const myRequestKey = `${CONNECTION_REQUEST_PREFIX}${myId}:${partnerId}`;
            const myRequestData = JSON.stringify({ chatId: chatId, username: from.username || from.first_name || "", messageId: flowMessageId });
            await redisClient.set(myRequestKey, myRequestData, { EX: CONNECTION_TTL_SECONDS });
            
            const userActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${chatId}`;
            await redisClient.set(userActiveRequestKey, myRequestKey, { EX: CONNECTION_TTL_SECONDS });
            
            const partnerRequestKey = `${CONNECTION_REQUEST_PREFIX}${partnerId}:${myId}`;
            const partnerRequestDataRaw = await redisClient.get(partnerRequestKey);

            if (partnerRequestDataRaw) {
                const partnerRequestData = JSON.parse(partnerRequestDataRaw);
                const partnerChatId = partnerRequestData.chatId;

                const myConfirmKey = `${CONNECTION_CONFIRM_PREFIX}${myId}:${partnerId}`;
                const partnerConfirmKey = `${CONNECTION_CONFIRM_PREFIX}${partnerId}:${myId}`;
                await redisClient.set(myConfirmKey, "pending", { EX: CONNECTION_TTL_SECONDS });
                await redisClient.set(partnerConfirmKey, "pending", { EX: CONNECTION_TTL_SECONDS });

                const confirmationKeyboard = (my_id: string, partner_id: string) => ({
                    inline_keyboard: [[
                        { text: "✅ Да, поделиться", callback_data: `confirm_connection:yes:${my_id}:${partner_id}` },
                        { text: "❌ Нет, отменить", callback_data: `confirm_connection:no:${my_id}:${partner_id}` }
                    ]]
                });
                
                const myUsername = from.username ? `@${from.username}` : (from.first_name || 'Скрыт');
                await editMessageText(TELEGRAM_TOKEN, chatId, flowMessageId, `🤝 Произошло соединение с пользователем FunPay \`${partnerId}\`!\n\nВы согласны поделиться с ним вашим профилем Telegram (${myUsername}) для связи?`, confirmationKeyboard(myId, partnerId));
                
                const partnerUsername = partnerRequestData.username ? `@${partnerRequestData.username}` : (partnerRequestData.first_name || 'Скрыт');
                const partnerMessage = `🤝 Произошло соединение с пользователем FunPay \`${myId}\`!\n\nВы согласны поделиться с ним вашим профилем Telegram (${partnerUsername}) для связи?`
                
                // We use sendMessage for the other user as we don't have their messageId to edit
                const sentPartnerMessage = await sendMessage(TELEGRAM_TOKEN, partnerChatId, partnerMessage, confirmationKeyboard(partnerId, myId));
                
                if (sentPartnerMessage.ok && partnerRequestData.messageId) {
                     // We can delete the message from the other user's flow now.
                     await deleteMessage(TELEGRAM_TOKEN, partnerChatId, partnerRequestData.messageId).catch(console.error);
                }


            } else {
                const cancelRequestKeyboard = {
                    inline_keyboard: [[
                        { text: "❌ Отменить запрос", callback_data: `cancel_connection:${myId}:${partnerId}` }
                    ]]
                };
                await editMessageText(TELEGRAM_TOKEN, chatId, flowMessageId, `✅ Ваш запрос на связь с \`${partnerId}\` создан и будет активен 24 часа. Мы сообщим вам, когда пользователь ответит взаимностью.`, cancelRequestKeyboard);
            }
        }
    };

    const handleBuy = async (chatId: number, type: 'connection') => {
        const useStars = !TELEGRAM_PROVIDER_TOKEN;
        const cost = useStars ? TELEGRAM_CONNECTION_COST_STARS : TELEGRAM_CONNECTION_COST_REAL;
        const currency = useStars ? "XTR" : TELEGRAM_PAYMENT_CURRENCY;
        const finalAmount = useStars ? cost : toSmallestUnit(cost);

        const title = "Покупка попытки связи";
        const description = "Покупка 1 попытки для установки связи";
        const payload = "buy_1_connection";

        await sendInvoice(
            TELEGRAM_TOKEN,
            chatId,
            title,
            description,
            payload,
            TELEGRAM_PROVIDER_TOKEN,
            currency,
            [{ label: `1 Связь`, amount: finalAmount }]
        );
    };


    const handleUserInput = async (chatId: number, text: string, from: any) => {
        if (redisClient) {
            const stateKey = getUserStateKey(chatId);
            const stateRaw = await redisClient.get(stateKey);
            
            if (stateRaw) {
                await handleConnectionLogic(chatId, text, from);
                return;
            }
        }

        const isNumeric = /^\d+$/.test(text);
        const searchType = isNumeric ? 'id' : 'nickname';
        
        let profiles = [];
        try {
            const searchResponse = await searchProfiles(text, searchType, config);
            profiles = await searchResponse.json();
        } catch (e) {
            console.error("Error searching profiles in handleUserInput", e);
            await sendMessage(TELEGRAM_TOKEN, chatId, "Произошла ошибка при поиске в базе данных.");
            return;
        }

        if (!profiles || profiles.length === 0) {
             await sendMessage(TELEGRAM_TOKEN, chatId, `Пользователь по запросу "${text}" не найден в базе данных.`);
             return;
        }

        if (TELEGRAM_PAYMENT_ENABLED) {
            await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Профиль по запросу "${text}" найден. Для получения информации, пожалуйста, произведите оплату.`);
            const useStars = !TELEGRAM_PROVIDER_TOKEN;
            const cost = useStars ? TELEGRAM_SEARCH_COST_STARS : TELEGRAM_SEARCH_COST_REAL;
            const currency = useStars ? "XTR" : TELEGRAM_PAYMENT_CURRENCY;
            const finalAmount = useStars ? cost : toSmallestUnit(cost);

            await sendInvoice(
                TELEGRAM_TOKEN,
                chatId,
                `Доступ к профилю: ${text}`,
                `Оплата за получение данных по запросу: "${text}"`,
                `search:${text}`,
                TELEGRAM_PROVIDER_TOKEN,
                currency,
                [{ label: '1 Поиск', amount: finalAmount }]
            );
        } else {
            await executeSearch(TELEGRAM_TOKEN, chatId, text, config);
        }
    }

    if (body.message) {
        const { message } = body;
        const chatId = message.chat.id;
        let text = message.text;
        const chatType = message.chat.type;
        const from = message.from;

        if (!text) {
             if (chatType === 'private') {
                 await handleStartCommand(chatId, chatType);
             }
             return NextResponse.json({ status: 'ok' });
        }

        if (text.startsWith('/')) {
            let command = text.substring(1).split(' ')[0].toLowerCase();
            const botUsernameMatch = command.match(/^(.*?)@/);
            if (botUsernameMatch) {
                command = botUsernameMatch[1];
            }
            
            if (command === 'start') {
                await handleStartCommand(chatId, chatType);
            } else if (chatType !== 'private') {
                 await handleUserInput(chatId, text.substring(1), from);
            } else {
                 await handleUserInput(chatId, text, from);
            }

        } else if (chatType === 'private') {
            await handleUserInput(chatId, text, from);
        }
    } else if (body.callback_query) {
        const { callback_query } = body;
        const chatId = callback_query.message.chat.id;
        const messageId = callback_query.message.message_id;
        const data = callback_query.data;
        const from = callback_query.from;
        
        await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id);

         if (data === 'buy_connections') {
             await handleBuy(chatId, 'connection');
             return NextResponse.json({ status: 'ok' });
        }
        if (data.startsWith('check_balance:')) {
             const type = data.split(':')[1];
             if (type === 'connection' && redisClient) {
                 const balanceKey = getUserConnectionBalanceKey(chatId);
                 const balance = await redisClient.get(balanceKey) || 0;
                 await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id, `У вас ${balance} связей.`);
             }
             return NextResponse.json({ status: 'ok' });
        }
        
        if (data === 'search_support') {
            await handleSupportSearch(chatId);
        } else if (data === 'show_categories' || data === 'back_to_categories') {
            if (!MONGODB_URI) return NextResponse.json({ status: 'ok' });
            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const categories = await mongoClient.db(dbName).collection("products").distinct("category", { ownerId: WORKER_ID });
            
            if (categories.length === 0) {
                 await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "В данный момент товары отсутствуют.", { inline_keyboard: [[{ text: "⬅️ В главное меню", callback_data: "main_menu" }]] });
                 return NextResponse.json({ status: 'ok' });
            }

            const categoryButtons = categories.map(cat => ([{ text: cat || 'Без категории', callback_data: `show_products:${cat || 'none'}` }]));
            
            const keyboard = {
                inline_keyboard: [
                    ...categoryButtons,
                    [{ text: "🛒 Корзина", callback_data: "view_cart" }],
                    [{ text: "⬅️ В главное меню", callback_data: "main_menu" }]
                ]
            };
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "🛍️ Выберите категорию:", keyboard);

        } else if (data.startsWith('show_products:')) {
            const category = data.split(':')[1];
            const findQuery = category === 'none' ? { ownerId: WORKER_ID, $or: [{category: ''}, {category: null}] } : { ownerId: WORKER_ID, category: category };
            if (!MONGODB_URI) return NextResponse.json({ status: 'ok' });

            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const products = await mongoClient.db(dbName).collection("products").find(findQuery).toArray();

            const useStars = !TELEGRAM_PROVIDER_TOKEN;
            const currencySymbol = useStars ? '⭐' : TELEGRAM_PAYMENT_CURRENCY;

            const productButtons = products.map(p => ([{
                text: `${p.buttonName} - ${useStars ? p.price : p.priceReal} ${currencySymbol}`,
                callback_data: `view_product:${p._id}`
            }]));

            const keyboard = { inline_keyboard: [ ...productButtons, [{ text: "⬅️ Назад к категориям", callback_data: "back_to_categories" }]] };
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, `*${category}*`, keyboard);

        } else if (data.startsWith('view_product:')) {
             const productId = data.substring('view_product:'.length);
             if (!MONGODB_URI) return NextResponse.json({ status: 'ok' });
             mongoClient = new MongoClient(MONGODB_URI);
             await mongoClient.connect();
             const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
             const product = await mongoClient.db(dbName).collection("products").findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });

             if (product) {
                const useStars = !TELEGRAM_PROVIDER_TOKEN;
                const price = useStars ? product.price : product.priceReal;
                const currencySymbol = useStars ? '⭐' : TELEGRAM_PAYMENT_CURRENCY;
                
                const text = `*${product.invoiceTitle}*\n\n${product.invoiceDescription}\n\n*Цена:* ${price} ${currencySymbol}`;
                const keyboard = {
                    inline_keyboard: [
                        [
                          { text: `💳 Купить сейчас`, callback_data: `buy_now:${product._id}` },
                          { text: `➕ В корзину`, callback_data: `add_to_cart:${product._id}` }
                        ],
                        [{ text: "⬅️ Назад к товарам", callback_data: `show_products:${product.category || 'none'}` }]
                    ]
                };
                
                if (product.productImageUrl && !callback_query.message.photo) {
                    await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                    await sendPhoto(TELEGRAM_TOKEN, chatId, product.productImageUrl, text, keyboard);
                } else if (product.productImageUrl && callback_query.message.photo) {
                    // To avoid "message is not modified" error, we can't edit a photo with the same photo,
                    // so we delete and send a new one.
                    await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                    await sendPhoto(TELEGRAM_TOKEN, chatId, product.productImageUrl, text, keyboard);
                } else {
                    await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
                }

             } else {
                 await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id, "Товар не найден.");
             }
        }
        else if (data.startsWith('add_to_cart:')) {
            const productId = data.split(':')[1];
            const cartKey = getCartKey(chatId);
            await redisClient.lPush(cartKey, productId);
            await redisClient.expire(cartKey, CART_TTL_SECONDS); // Refresh TTL on activity
            await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id, "✅ Добавлено в корзину");
        }
        else if (data === 'view_cart') {
            const cartKey = getCartKey(chatId);
            const productIds = await redisClient.lRange(cartKey, 0, -1);

            if (productIds.length === 0) {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "🛒 Ваша корзина пуста.", { inline_keyboard: [[{ text: "⬅️ Назад к категориям", callback_data: "back_to_categories" }]] });
                return NextResponse.json({ status: 'ok' });
            }
            if (!MONGODB_URI) return NextResponse.json({ status: 'ok' });
            
            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const productsCollection = mongoClient.db(dbName).collection("products");

            const productObjectIds = productIds.map(id => new ObjectId(id));
            const productsInCart = await productsCollection.find({ _id: { $in: productObjectIds } }).toArray();

            let cartText = "*🛒 Ваша корзина:*\n\n";
            let totalPriceStars = 0;
            let totalPriceReal = 0;
            const useStars = !TELEGRAM_PROVIDER_TOKEN;
            const currencySymbol = useStars ? '⭐' : TELEGRAM_PAYMENT_CURRENCY;
            
            const productCounts: {[key: string]: number} = productIds.reduce((acc: any, id: string) => { acc[id] = (acc[id] || 0) + 1; return acc; }, {});

            productsInCart.forEach(p => {
                const count = productCounts[p._id.toString()];
                cartText += `*${p.invoiceTitle}* (x${count}) - ${useStars ? p.price * count : p.priceReal * count} ${currencySymbol}\n`;
                totalPriceStars += p.price * count;
                totalPriceReal += p.priceReal * count;
            });
            
            const total = useStars ? totalPriceStars : totalPriceReal;
            cartText += `\n*Итого:* ${total} ${currencySymbol}`;
            
            const keyboard = {
                inline_keyboard: [
                    [{ text: `💳 Оплатить ${total} ${currencySymbol}`, callback_data: "checkout_cart" }],
                    [{ text: "🗑️ Очистить корзину", callback_data: "clear_cart" }],
                    [{ text: "⬅️ Назад к категориям", callback_data: "back_to_categories" }]
                ]
            };
            
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, cartText, keyboard);
        }
        else if (data === 'clear_cart') {
            const cartKey = getCartKey(chatId);
            await redisClient.del(cartKey);
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "✅ Корзина очищена.", { inline_keyboard: [[{ text: "⬅️ Назад к категориям", callback_data: "back_to_categories" }]]});
        }
        else if (data.startsWith('buy_now:') || data === 'checkout_cart') {
             const isCartCheckout = data === 'checkout_cart';
             const useStars = !TELEGRAM_PROVIDER_TOKEN;
             const currency = useStars ? "XTR" : TELEGRAM_PAYMENT_CURRENCY;

             if (!MONGODB_URI) return NextResponse.json({ status: 'ok' });
             mongoClient = new MongoClient(MONGODB_URI);
             await mongoClient.connect();
             const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
             const productsCollection = mongoClient.db(dbName).collection("products");

             let productsToBuy: any[] = [];
             let title: string;
             let payload: string;

             if (isCartCheckout) {
                const cartKey = getCartKey(chatId);
                const productIds = await redisClient.lRange(cartKey, 0, -1);
                if (productIds.length === 0) {
                     await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id, "Корзина пуста!");
                     return NextResponse.json({ status: 'ok' });
                }
                const productObjectIds = productIds.map(id => new ObjectId(id));
                const productsInDb = await productsCollection.find({ _id: { $in: productObjectIds } }).toArray();
                
                // Reconstruct the cart with correct quantities
                const productMap = new Map(productsInDb.map(p => [p._id.toString(), p]));
                productsToBuy = productIds.map(id => productMap.get(id)).filter(Boolean);

                title = "Оплата заказа";
                payload = `cart_checkout`;
             } else {
                const productId = data.split(':')[1];
                const product = await productsCollection.findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });
                if (!product) {
                    await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id, "Товар не найден.");
                    return NextResponse.json({ status: 'ok' });
                }
                productsToBuy = [product];
                title = product.invoiceTitle;
                payload = `product:${product._id}:${WORKER_ID}`;
             }

             const prices = productsToBuy.map(p => ({
                label: p.invoiceTitle,
                amount: useStars ? p.price : toSmallestUnit(p.priceReal)
             }));

             // For cart, sum up prices of items with same title
             const consolidatedPrices = Object.values(prices.reduce((acc, price) => {
                if (acc[price.label]) {
                    acc[price.label].amount += price.amount;
                } else {
                    acc[price.label] = { ...price };
                }
                return acc;
             }, {} as {[key: string]: {label: string, amount: number}}));
             
             const totalAmount = consolidatedPrices.reduce((sum, p) => sum + p.amount, 0);

             if (totalAmount <= 0) {
                 await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id, "Нечего оплачивать.");
                 return NextResponse.json({ status: 'ok' });
             }

             await sendInvoice(
                TELEGRAM_TOKEN,
                chatId,
                title,
                `Общая сумма: ${useStars ? totalAmount : totalAmount / 100} ${useStars ? '⭐' : currency}`,
                payload,
                TELEGRAM_PROVIDER_TOKEN,
                currency,
                consolidatedPrices
             );
        }
        else if (data === 'main_menu') {
            await handleStartCommand(chatId, 'private');
            await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
        }
        else if (data === 'initiate_connection') {
             if (redisClient) {
                if (TELEGRAM_CONNECTION_PAYMENT_ENABLED) {
                    const useStars = !TELEGRAM_PROVIDER_TOKEN;
                    const balanceKey = getUserConnectionBalanceKey(chatId);
                    const balance = Number(await redisClient.get(balanceKey) || 0);

                    if (balance <= 0) {
                        const cost = useStars ? TELEGRAM_CONNECTION_COST_STARS : TELEGRAM_CONNECTION_COST_REAL;
                         const currencySymbol = useStars ? 'звезд' : TELEGRAM_PAYMENT_CURRENCY;

                        const keyboard = {
                            inline_keyboard: [[
                                { text: `⭐️ Купить 1 связь за ${cost} ${currencySymbol}`, callback_data: "buy_connections" }
                            ]]
                        };
                        await sendMessage(TELEGRAM_TOKEN, chatId, "❌ У вас закончились попытки установить связь. Пожалуйста, пополните баланс.", keyboard);
                        return NextResponse.json({ status: 'ok' });
                    }
                }

                const userActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${chatId}`;
                const existingRequest = await redisClient.get(userActiveRequestKey);
                
                if (existingRequest) {
                    const [_, myId, partnerId] = existingRequest.split(':');
                    const cancelKeyboard = { inline_keyboard: [[{ text: "❌ Отменить текущий запрос", callback_data: `cancel_connection:${myId}:${partnerId}` }]] };
                    await sendMessage(TELEGRAM_TOKEN, chatId, "У вас уже есть активный запрос на связь. Пожалуйста, сначала отмените его, прежде чем создавать новый.", cancelKeyboard);
                    return NextResponse.json({ status: 'ok' });
                }

                const stateKey = getUserStateKey(chatId);
                const flowMessage = await sendMessage(TELEGRAM_TOKEN, chatId, "▶️ Введите ID вашего профиля FunPay, который вы хотите использовать для связи.", cancelKeyboard);
                if (flowMessage.ok) {
                    const flowMessageId = flowMessage.result.message_id;
                    const initialState = { step: 'awaiting_my_id', messageId: flowMessageId };
                    await redisClient.set(stateKey, JSON.stringify(initialState), { EX: 300 }); // 5 minute timeout
                }
             }
        } else if (data === 'cancel_flow') {
            if (redisClient) {
                const stateKey = getUserStateKey(chatId);
                await redisClient.del(stateKey);
            }
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "Операция отменена.");
        }
        else if (data.startsWith('cancel_connection:')) {
            const [_, myId, partnerId] = data.split(':');
            if (redisClient) {
                const requestKey = `${CONNECTION_REQUEST_PREFIX}${myId}:${partnerId}`;
                const userActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${chatId}`;
                await redisClient.del(requestKey);
                await redisClient.del(userActiveRequestKey);
            }
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "✅ Ваш запрос на связь был успешно отменен.");
        }
        else if (data.startsWith('confirm_connection:')) {
            const [_, decision, myId, partnerId] = data.split(':');
            
            if (!redisClient) {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "⚠️ Ошибка сервера: не удалось подключиться к Redis.");
                return NextResponse.json({ status: 'ok' });
            }

            const myRequestKey = `${CONNECTION_REQUEST_PREFIX}${myId}:${partnerId}`;
            const partnerRequestKey = `${CONNECTION_REQUEST_PREFIX}${partnerId}:${myId}`;
            const myConfirmKey = `${CONNECTION_CONFIRM_PREFIX}${myId}:${partnerId}`;
            const partnerConfirmKey = `${CONNECTION_CONFIRM_PREFIX}${partnerId}:${myId}`;

            const myRequestDataRaw = await redisClient.get(myRequestKey);
            const partnerRequestDataRaw = await redisClient.get(partnerRequestKey);

            if (!myRequestDataRaw || !partnerRequestDataRaw) {
                 await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "⚠️ Этот запрос на связь истек или был отменен.");
                 return NextResponse.json({ status: 'ok' });
            }
             const myRequestData = JSON.parse(myRequestDataRaw);
             const partnerRequestData = JSON.parse(partnerRequestDataRaw);
             const myActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${myRequestData.chatId}`;
             const partnerActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${partnerRequestData.chatId}`;

             const keysToDelete = [myRequestKey, partnerRequestKey, myConfirmKey, partnerConfirmKey, myActiveRequestKey, partnerActiveRequestKey];

            if (decision === 'no') {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "❌ Вы отменили запрос на связь.");
                await sendMessage(TELEGRAM_TOKEN, partnerRequestData.chatId, `❌ Пользователь FunPay \`${myId}\` отменил запрос на связь.`);
                if(redisClient) await redisClient.del(keysToDelete);
                return NextResponse.json({ status: 'ok' });
            }

            // User said YES
            await redisClient.set(myConfirmKey, "confirmed", { EX: CONNECTION_TTL_SECONDS });
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "✅ Вы подтвердили обмен. Ожидаем подтверждения от второго пользователя...");

            const partnerStatus = await redisClient.get(partnerConfirmKey);

            if (partnerStatus === 'confirmed') {
                const myUsername = myRequestData.username ? `@${myRequestData.username}` : (myRequestData.first_name || 'Профиль скрыт');
                const partnerUsername = partnerRequestData.username ? `@${partnerRequestData.username}` : (partnerRequestData.first_name || 'Профиль скрыт');
                
                await sendMessage(myRequestData.chatId, `🎉 Обмен состоялся! \n\nСвяжитесь с пользователем FunPay \`${partnerId}\` через Telegram: ${partnerUsername}`);
                await sendMessage(partnerRequestData.chatId, `🎉 Обмен состоялся! \n\nСвяжитесь с пользователем FunPay \`${myId}\` через Telegram: ${myUsername}`);
                
                // Edit the other user's message as well to confirm
                const partnerOriginalMessageId = partnerRequestData.messageId;
                if(partnerOriginalMessageId) {
                  await editMessageText(TELEGRAM_TOKEN, partnerRequestData.chatId, partnerOriginalMessageId, `🎉 Обмен с FunPay \`${myId}\` состоялся! Контакт отправлен в отдельном сообщении.`).catch(console.error);
                }
                
                // Edit my own original message
                 await editMessageText(TELEGRAM_TOKEN, chatId, messageId, `🎉 Обмен с FunPay \`${partnerId}\` состоялся! Контакт отправлен в отдельном сообщешении.`).catch(console.error);
            
                if(redisClient) await redisClient.del(keysToDelete);
            }
        }
    }


    return NextResponse.json({ status: 'ok' });
  } catch (error: any) {
    console.error("Error handling telegram update:", error);
    // Always return ok to Telegram to prevent retries
    return NextResponse.json({ status: 'ok' });
  } finally {
      if (redisClient && redisClient.isOpen) {
          await redisClient.quit();
      }
      if (mongoClient) {
          await mongoClient.close();
      }
  }
}

async function setWebhook(token: string, webhookUrl: string) {
    let url = new URL(`${BASE_URL(token)}/setWebhook`);
    url.searchParams.append('url', webhookUrl);
    const response = await fetch(url.toString());
    return response.json();
}


export async function PUT(request: NextRequest) {
    const { token } = await request.json();
    const config = await getConfig();

    let appUrl = config.NEXT_PUBLIC_APP_URL;
    if (!appUrl) {
        const requestHeaders = headers();
        const protocol = requestHeaders.get('x-forwarded-proto') || 'http';
        const host = requestHeaders.get('host');
        if (host) {
            appUrl = `${protocol}://${host}`;
        }
    }

    if (!token) {
        return NextResponse.json({ error: 'Telegram token not provided' }, { status: 400 });
    }
    if (!appUrl) {
        return NextResponse.json({ error: 'App public URL could not be determined.' }, { status: 400 });
    }
    if (!appUrl.startsWith('https://')) {
        const errorMsg = `Invalid app URL: ${appUrl}. An HTTPS URL is required.`;
        console.error(errorMsg);
        return NextResponse.json({ error: errorMsg }, { status: 400 });
    }
    
    const url = new URL(appUrl);
    // Append worker ID to webhook to distinguish between tenants
    url.pathname = '/api/telegram';
    url.searchParams.set('worker', config.WORKER_ID);

    const webhookUrl = `${url.protocol}//${url.hostname}${url.pathname}${url.search}`;


    try {
        const result = await setWebhook(token, webhookUrl);
        if (result.ok) {
            await updateConfig({ TELEGRAM_TOKEN: token });
            return NextResponse.json({ message: `Вебхук успешно установлен на ${webhookUrl}` });
        } else {
            console.error('Webhook Error:', result);
            const description = result.description || "Не удалось установить вебхук.";
            if (description.includes("bot token is already taken")) {
                 return NextResponse.json({ error: `Этот токен уже используется другим сервером. Попробуйте сбросить токен у @BotFather.` }, { status: 409 });
            }
             if (description.includes("invalid bot token")) {
                 return NextResponse.json({ error: `Неверный токен. Проверьте правильность введенного токена.` }, { status: 400 });
            }
             if (description.includes("Webhook can be set up only on ports 80, 88, 443 or 8443")) {
                return NextResponse.json({ error: `Неверный порт в URL. Вебхук можно установить только на порты 80, 88, 443 или 8443.`}, { status: 400 });
             }
             if (description.includes("IP address 127.0.0.1 is reserved")) {
                return NextResponse.json({ error: `URL вебхука не может указывать на локальный адрес (127.0.0.1). Укажите публичный HTTPS URL вашего приложения.`}, { status: 400 });
             }
            throw new Error(description);
        }
    } catch (error: any) {
        console.error('Webhook Exception:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}
