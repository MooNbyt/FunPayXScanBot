
"use server";
import { NextResponse, NextRequest } from 'next/server';
import { getConfig, updateConfig } from '../status/route';
import { searchProfiles } from '../data/route';
import { scrapeUser } from '../scrape/runner';
import { URL } from 'url';
import { createClient } from 'redis';
import { MongoClient, ObjectId } from 'mongodb';
import { headers } from 'next/headers';

const getTelegramLogsKey = (workerId: string) => `telegram_logs:${workerId}`;
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

const escapeMarkdown = (text: string | number | null | undefined): string => {
  if (text === null || text === undefined) return '';
  // In MarkdownV2, these characters must be escaped: _ * [ ] ( ) ~ ` > # + - = | { } . !
  return String(text).replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');
};


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
    parse_mode: 'MarkdownV2',
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
    parse_mode: 'MarkdownV2',
    reply_markup: replyMarkup,
  });
}

async function editMessageText(token: string, chatId: number, messageId: number, text: string, replyMarkup?: any) {
   const result = await apiCall(token, 'editMessageText', {
        chat_id: chatId,
        message_id: messageId,
        text: text,
        parse_mode: 'MarkdownV2',
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

    const statusMessage = await sendMessage(token, chatId, `⏳ Ищу пользователя по ${searchType === 'id' ? 'ID' : 'никнейму'}: *${escapeMarkdown(query)}*\\.\\.\\.`);
    const loadingMessageId = statusMessage.result?.message_id;

    if (searchType === 'id') {
        const userId = parseInt(query, 10);
        // For ID search, we always get the latest data.
        const updatedProfile = await scrapeUser(userId, `[TelegramBot]`, false);

        if (updatedProfile && !updatedProfile.error && updatedProfile.status !== 'not_found') {
            const p = addUrlToProfile(updatedProfile);
            const scrapedAt = new Date(p.scrapedAt);
            const formattedDate = `${scrapedAt.getFullYear()}\\-${String(scrapedAt.getMonth() + 1).padStart(2, '0')}\\-${String(scrapedAt.getDate()).padStart(2, '0')} ${String(scrapedAt.getHours()).padStart(2, '0')}:${String(scrapedAt.getMinutes()).padStart(2, '0')}:${String(scrapedAt.getSeconds()).padStart(2, '0')}`;

            let message = `*ID:* \`${p.id}\`\n`;
            message += `*Никнейм:* ${escapeMarkdown(p.nickname)}\n`;
            message += `*Дата регистрации:* ${escapeMarkdown(p.regDate)}\n`;
            message += `*Кол\\-во отзывов:* ${p.reviewCount}\n\n`;
            message += `*Бан:* ${p.isBanned ? '✅ Да' : '❌ Нет'}\n`;
            message += `*Саппорт:* ${p.isSupport ? '✅ Да' : '❌ Нет'}\n\n`;
            message += `*Кол\\-во лотов:* ${p.lotCount}\n`;
            message += `*Ссылка:* [Перейти на профиль](${p.url})\n\n`;
            message += `🕒 *Актуально на:* ${escapeMarkdown(formattedDate)}`;

            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, message);
        } else {
            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, `Пользователь с ID ${userId} не найден на FunPay\\.`);
        }
    } else { // nickname search
        const initialResponse = await searchProfiles(query, 'nickname', config);
        let profiles = await initialResponse.json();
        
        // Filter out duplicates on the fly
        const uniqueProfiles = Array.from(new Map(profiles.map((p: any) => [p.id, p])).values());


        if (uniqueProfiles.length === 1) {
             const p: any = uniqueProfiles[0];
             let message = `*Найден 1 профиль по запросу "${escapeMarkdown(query)}":*\n_\\(Данные из базы, могут быть неактуальны\\)_\n\n`;
             message += `*ID:* \`${p.id}\`\n`;
             message += `*Никнейм:* ${escapeMarkdown(p.nickname)}\n`;
             message += `*Дата регистрации:* ${escapeMarkdown(p.regDate) || 'Неизвестно'}\n`;
             message += `*Кол\\-во отзывов:* ${p.reviewCount || 0}\n\n`;
             message += `*Бан:* ${p.isBanned ? '✅ Да' : '❌ Нет'}\n`;
             message += `*Саппорт:* ${p.isSupport ? '✅ Да' : '❌ Нет'}\n\n`;
             message += `*Кол\\-во лотов:* ${p.lotCount || 0}\n`;
             message += `*Ссылка:* [Перейти на профиль](${p.url})\n`;
             if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, message);

        } else if (uniqueProfiles.length > 0) {
            let message = `*Найдено ${uniqueProfiles.length} профилей по запросу "${escapeMarkdown(query)}":*\n_\\(Данные будут обновлены в фоновом режиме\\)_\n\n`;
            uniqueProfiles.slice(0, 10).forEach((p: any) => {
                const profileWithUrl = addUrlToProfile(p);
                let status = '';
                if(p.isSupport) status = ' \\(Поддержка\\)';
                if(p.isBanned) status = ' \\(Забанен\\)';
                message += `*${escapeMarkdown(profileWithUrl.nickname)}*${escapeMarkdown(status)} \\(ID: \`${profileWithUrl.id}\`\\) \\- [Профиль](${profileWithUrl.url})\n`;
            });
            if (uniqueProfiles.length > 10) {
                message += `\n\\.\\.\\. и еще ${uniqueProfiles.length - 10} профилей\\.`
            }
            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, message);
            
            // Queue profiles for background update
            const profileIds = uniqueProfiles.map((p: any) => p.id);
            const redisClient = createClient({ url: config.REDIS_URI });
            try {
                await redisClient.connect();
                if (profileIds.length > 0) {
                    const multi = redisClient.multi();
                    for (const id of profileIds) {
                        multi.lPush('failed_tasks', id.toString());
                    }
                    await multi.exec();
                }
            } catch (e: any) {
                console.error("Telegram search: Failed to queue profiles for update", e);
            } finally {
                if (redisClient.isOpen) {
                    await redisClient.quit();
                }
            }
        } else {
            if(loadingMessageId) await editMessageText(token, chatId, loadingMessageId, `Пользователи с никнеймом "${escapeMarkdown(query)}" не найдены\\.`);
        }
    }
};

const executeLetterSearch = async (token: string, chatId: number, letter: string, page: number, messageId: number | null, config: any) => {
    
    if (!messageId) {
        const statusMessage = await sendMessage(token, chatId, `⏳ Ищу пользователей на букву *${escapeMarkdown(letter)}*\\.\\.\\.`);
        messageId = statusMessage.result?.message_id;
    }
    
    try {
        const response = await fetch(`${config.NEXT_PUBLIC_APP_URL}/api/data?letter=${encodeURIComponent(letter)}`);
        if (!response.ok) throw new Error("Ошибка при поиске по букве");

        const profiles = await response.json();
        const profilesPerPage = config.TELEGRAM_PAYMENT_ENABLED ? 10 : 30; // Fewer profiles per page if payment is enabled to show buttons
        const totalPages = Math.ceil(profiles.length / profilesPerPage);
        const currentPage = Math.min(Math.max(page, 1), totalPages);
        const startIndex = (currentPage - 1) * profilesPerPage;
        const profilesToShow = profiles.slice(startIndex, startIndex + profilesPerPage);

        let responseText = `*Найденные профили на букву "${escapeMarkdown(letter)}" \\(${escapeMarkdown(profiles.length)} шт\\.\\):*\n\n`;
        const inlineKeyboardRows: any[] = [];

        if (profiles.length > 0) {
            profilesToShow.forEach((p: any) => {
                 let status = '';
                 if(p.isSupport) status = ' \\(Поддержка\\)';
                 if(p.isBanned) status = ' \\(Забанен\\)';
                 if (config.TELEGRAM_PAYMENT_ENABLED) {
                    responseText += `*${escapeMarkdown(p.nickname)}*${escapeMarkdown(status)}\n`;
                    inlineKeyboardRows.push([{ text: `🔗 Получить доступ к ${p.nickname}`, callback_data: `get_profile_access:${p.id}` }]);
                 } else {
                    responseText += `*${escapeMarkdown(p.nickname)}*${escapeMarkdown(status)} \\(ID: \`${p.id}\`\\)\n`;
                 }
            });
            if (!config.TELEGRAM_PAYMENT_ENABLED) {
                responseText = responseText.replace(/\\n$/,""); // remove last newline
            }
        } else {
            responseText = `Профили на букву "${escapeMarkdown(letter)}" не найдены\\.`;
        }
        
        const paginationButtons = [];
        if (currentPage > 1) {
            paginationButtons.push({ text: `⬅️ Назад`, callback_data: `sbl_page:${letter}:${currentPage - 1}` });
        }
        if (totalPages > 1) {
             paginationButtons.push({ text: `${currentPage} / ${totalPages}`, callback_data: `sbl_nop` });
        }
        if (currentPage < totalPages) {
            paginationButtons.push({ text: `Вперед ➡️`, callback_data: `sbl_page:${letter}:${currentPage + 1}` });
        }
        
        if (paginationButtons.length > 0) {
            inlineKeyboardRows.push(paginationButtons);
        }
        inlineKeyboardRows.push([{ text: "🔠 К выбору буквы", callback_data: "search_by_letter_init" }]);

        const keyboard = {
            inline_keyboard: inlineKeyboardRows
        };
        
        const finalKeyboard = inlineKeyboardRows.length > 1 || (inlineKeyboardRows.length === 1 && inlineKeyboardRows[0].length > 0) ? keyboard : undefined;

        if (messageId) {
            await editMessageText(token, chatId, messageId, responseText, finalKeyboard);
        }

    } catch (e: any) {
        const errorText = "Произошла ошибка при поиске\\.";
         if (messageId) {
            await editMessageText(token, chatId, messageId, errorText);
        } else {
            await sendMessage(token, chatId, errorText);
        }
    }
};


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
      TELEGRAM_BOT_LINK,
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
      TELEGRAM_CONNECTION_INFO_MESSAGE,
      TELEGRAM_SHOP_BUTTON_NAME,
      TELEGRAM_CUSTOM_LINKS,
      TELEGRAM_LOGS_LIMIT,
  } = config;

  if (!TELEGRAM_TOKEN) {
    console.error(`Telegram token not configured for worker ${WORKER_ID}`);
    return NextResponse.json({ status: 'ok' });
  }
  
  let redisClient: any;
  let mongoClient: MongoClient | undefined;
  const TELEGRAM_LOGS_KEY = getTelegramLogsKey(WORKER_ID);
  const maxLogEntries = TELEGRAM_LOGS_LIMIT || 200;

  try {
    const body = await request.json();

    if (REDIS_URI) {
        redisClient = createClient({ url: REDIS_URI });
        await redisClient.connect();
        const logEntry = { timestamp: new Date().toISOString(), payload: body };
        await redisClient.lPush(TELEGRAM_LOGS_KEY, JSON.stringify(logEntry));
        await redisClient.lTrim(TELEGRAM_LOGS_KEY, 0, maxLogEntries - 1);
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
            await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата прошла успешно\\! Выполняю поиск по запросу: *${escapeMarkdown(query)}*`);
            await executeSearch(TELEGRAM_TOKEN, chatId, query, config);
        } else if (invoicePayload === 'buy_1_connection') {
            const balanceKey = getUserConnectionBalanceKey(chatId);
            await redisClient.incr(balanceKey);
            await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата прошла успешно\\! Вам зачислена 1 попытка установки связи\\.`);
        } else if (invoicePayload.startsWith('product:')) {
            const productId = invoicePayload.split(':')[1];
             if (!MONGODB_URI) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Ошибка\\. База данных не настроена\\.");
                return NextResponse.json({ status: 'ok' });
            }
            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const productsCollection = mongoClient.db(dbName).collection("products");
            const product = await productsCollection.findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });
            
            if (!product) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Ошибка\\. Товар не найден после оплаты\\.");
                return NextResponse.json({ status: 'ok' });
            }

            await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата за *${escapeMarkdown(product.invoiceTitle)}* прошла успешно\\!`);

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
                        await sendMessage(TELEGRAM_TOKEN, chatId, `Ваш ключ: \`${escapeMarkdown(data.key)}\``);
                    } else {
                        throw new Error(data.message || "Не удалось сгенерировать ключ.");
                    }
                } catch (e: any) {
                    console.error("API product error:", e);
                    await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Произошла ошибка при генерации вашего ключа\\. Свяжитесь с администратором\\.");
                }

            } else { // static
                const keys = (product.staticKey || '').split('\n').filter((k: string) => k.trim() !== '');
                if (keys.length > 0) {
                    const keyToIssue = keys.shift(); // Get the first key and remove it from the array
                    await sendMessage(TELEGRAM_TOKEN, chatId, `Ваш товар: \`${escapeMarkdown(keyToIssue)}\``);
                    
                    // Update the product in the database with the remaining keys
                    const updatedStaticKey = keys.join('\n');
                    await productsCollection.updateOne(
                        { _id: new ObjectId(productId) },
                        { $set: { staticKey: updatedStaticKey } }
                    );
                } else {
                    await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Извините, товар закончился\\. Пожалуйста, свяжитесь с администратором\\.");
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
                userCount = await mongoClient.db(dbName).collection("users").countDocuments({ status: { $ne: 'not_found' } });
            } catch (e) {
                console.error("Failed to get user count for welcome message", e);
            }
        }
        
        welcomeMessage = welcomeMessage.replace(/{user_count}/g, userCount.toLocaleString('ru-RU'));
        welcomeMessage = escapeMarkdown(welcomeMessage);
        
        const commonButtons = [
             [{ text: "💚 Найти саппортов", callback_data: "search_support:1" }],
             [{ text: "🔠 Поиск по букве", callback_data: "search_by_letter_init" }],
        ];
        
        let keyboardRows: any[][] = [];
        
        // Add custom link buttons
        let customLinkButtons = (TELEGRAM_CUSTOM_LINKS || [])
            .filter((link: { text: string; url: string; showInGroups: boolean }) => link.text && link.url);

        if (chatType !== 'private') {
            // Filter links for groups
            customLinkButtons = customLinkButtons.filter((link: any) => link.showInGroups);
        }
        
        const formattedCustomLinks = customLinkButtons.map((link: { text: string; url: string }) => ([{ text: escapeMarkdown(link.text), url: link.url }]));
        keyboardRows.push(...formattedCustomLinks);


        if (chatType === 'private') {
            keyboardRows.push(...commonButtons);
            if (TELEGRAM_SHOP_BUTTON_NAME) {
                keyboardRows.unshift([{ text: `🛍️ ${escapeMarkdown(TELEGRAM_SHOP_BUTTON_NAME)}`, callback_data: "show_categories:1" }]);
            }
            keyboardRows.unshift([{ text: "🤝 Установить связь", callback_data: "initiate_connection" }]);
            
            const balanceButtons = [];
            if (TELEGRAM_CONNECTION_PAYMENT_ENABLED && redisClient) {
                const balanceKey = getUserConnectionBalanceKey(chatId);
                const balance = await redisClient.get(balanceKey) || 0;
                balanceButtons.push({ text: `⭐️ Связей: ${balance}`, callback_data: "check_balance:connection" }, { text: "Купить", callback_data: "buy_connections" });
            }
            if(balanceButtons.length > 0) {
                keyboardRows.unshift(balanceButtons);
            }
        } else { // Group chat
            keyboardRows.push(...commonButtons);
             if (TELEGRAM_BOT_LINK) {
                keyboardRows.unshift([{ text: "🤖 Открыть бота", url: TELEGRAM_BOT_LINK }]);
            }
        }
        

        const mainMenu = {
          inline_keyboard: keyboardRows
        };

        if (TELEGRAM_WELCOME_IMAGE_URL) {
            await sendPhoto(TELEGRAM_TOKEN, chatId, TELEGRAM_WELCOME_IMAGE_URL, welcomeMessage, mainMenu);
        } else {
            await sendMessage(TELEGRAM_TOKEN, chatId, welcomeMessage, mainMenu);
        }
    };
    
    const handleSupportSearch = async (chatId: number, page: number, messageId: number | null) => {
        if (!messageId) {
            const statusMessage = await sendMessage(TELEGRAM_TOKEN, chatId, '⏳ Ищу профили поддержки\\.\\.\\.');
            messageId = statusMessage.result?.message_id;
        }

        try {
            const response = await searchProfiles('', 'status', config, 'support');
            const profiles = await response.json();
            
            const profilesPerPage = 30;
            const totalPages = Math.ceil(profiles.length / profilesPerPage);
            const currentPage = Math.min(Math.max(page, 1), totalPages);
            const startIndex = (currentPage - 1) * profilesPerPage;
            const profilesToShow = profiles.slice(startIndex, startIndex + profilesPerPage);

            let responseText = `*Найденные профили поддержки \\(${escapeMarkdown(profiles.length)} шт\\.\\):*\n\n`;
            if (profiles.length > 0) {
                profilesToShow.forEach((p: any) => {
                     responseText += `*${escapeMarkdown(p.nickname)}* \\(ID: \`${p.id}\`\\) \\- [Профиль](${p.url})\n`;
                });
            } else {
                responseText = "Профили поддержки не найдены\\.";
            }

            const paginationButtons = [];
            if (currentPage > 1) {
                paginationButtons.push({ text: `⬅️ Назад`, callback_data: `search_support:${currentPage - 1}` });
            }
            if (totalPages > 1) {
                paginationButtons.push({ text: `${currentPage} / ${totalPages}`, callback_data: `sbl_nop` });
            }
            if (currentPage < totalPages) {
                paginationButtons.push({ text: `Вперед ➡️`, callback_data: `search_support:${currentPage + 1}` });
            }

            const keyboard = {
                inline_keyboard: [
                    paginationButtons,
                    [{ text: "⬅️ В главное меню", callback_data: "main_menu" }]
                ]
            };
            
            if (messageId) {
              if (messageId) await editMessageText(TELEGRAM_TOKEN, chatId, messageId, responseText, keyboard);
            } else {
               await sendMessage(TELEGRAM_TOKEN, chatId, responseText, keyboard);
            }


        } catch (e: any) {
            const errorText = "Произошла ошибка при поиске саппортов\\.";
            if (messageId) {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, errorText);
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
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Это не похоже на ID\\. Пожалуйста, введите корректный числовой ID вашего профиля FunPay\\.", cancelKeyboard);
                return;
            }
            const myId = text;
            const newState = { step: 'awaiting_partner_id', myId: myId, messageId: state.messageId };
            await redisClient.set(stateKey, JSON.stringify(newState), { EX: 300 }); 
            await editMessageText(TELEGRAM_TOKEN, chatId, state.messageId, "✅ Отлично\\. Теперь введите ID профиля FunPay, с которым хотите связаться\\.", cancelKeyboard);

        } else if (state.step === 'awaiting_partner_id') {
            if (!/^\d+$/.test(text)) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Это не похоже на ID\\. Пожалуйста, введите корректный числовой ID партнера\\.", cancelKeyboard);
                return;
            }
            const myId = state.myId;
            const partnerId = text;
            const flowMessageId = state.messageId;

            if(myId === partnerId) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "😅 Нельзя установить связь с самим собой\\. Пожалуйста, введите ID другого пользователя\\.");
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
                await sendMessage(TELEGRAM_TOKEN, chatId, `Попытка связи использована\\. У вас осталось: ${newBalance}\\.`);
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
                await editMessageText(TELEGRAM_TOKEN, chatId, flowMessageId, `🤝 Произошло соединение с пользователем FunPay \`${partnerId}\`\\!

Вы согласны поделиться с ним вашим профилем Telegram \\(${escapeMarkdown(myUsername)}\\) для связи?`, confirmationKeyboard(myId, partnerId));
                
                const partnerUsername = partnerRequestData.username ? `@${partnerRequestData.username}` : (partnerRequestData.first_name || 'Скрыт');
                const partnerMessage = `🤝 Произошло соединение с пользователем FunPay \`${myId}\`\\!

Вы согласны поделиться с ним вашим профилем Telegram \\(${escapeMarkdown(partnerUsername)}\\) для связи?`
                
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
                await editMessageText(TELEGRAM_TOKEN, chatId, flowMessageId, `✅ Ваш запрос на связь с \`${partnerId}\` создан и будет активен 24 часа\\. Мы сообщим вам, когда пользователь ответит взаимностью\\.`, cancelRequestKeyboard);
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
            await sendMessage(TELEGRAM_TOKEN, chatId, "Произошла ошибка при поиске в базе данных\\.");
            return;
        }

        if (!profiles || profiles.length === 0) {
             await sendMessage(TELEGRAM_TOKEN, chatId, `Пользователь по запросу "${escapeMarkdown(text)}" не найден в базе данных\\.`);
             return;
        }

        if (TELEGRAM_PAYMENT_ENABLED) {
            await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Профиль по запросу "${escapeMarkdown(text)}" найден\\. Для получения информации, пожалуйста, произведите оплату\\.`);
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
        const message = callback_query.message;
        
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
        
        if (data.startsWith('search_support')) {
            const page = parseInt(data.split(':')[1] || '1', 10);
            if (message.photo) {
                await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await handleSupportSearch(chatId, page, null);
            } else {
                await handleSupportSearch(chatId, page, messageId);
            }
        } else if (data === 'search_by_letter_init') {
            const text = "Выберите диапазон:";
            const keyboard = {
                inline_keyboard: [
                    [{ text: "А-Г", callback_data: "sbl_range:А-Г" }, { text: "Д-И", callback_data: "sbl_range:Д-И" }, { text: "К-О", callback_data: "sbl_range:К-О" }, { text: "П-У", callback_data: "sbl_range:П-У" }, { text: "Ф-Я", callback_data: "sbl_range:Ф-Я" }],
                    [{ text: "A-E", callback_data: "sbl_range:A-E" }, { text: "F-J", callback_data: "sbl_range:F-J" }, { text: "K-O", callback_data: "sbl_range:K-O" }, { text: "P-T", callback_data: "sbl_range:P-T" }, { text: "U-Z", callback_data: "sbl_range:U-Z" }],
                    [{ text: "0-9", callback_data: "sbl_range:0-9" }],
                    [{ text: "⬅️ В главное меню", callback_data: "main_menu" }]
                ]
            };
            if (message.photo) {
                await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await sendMessage(TELEGRAM_TOKEN, chatId, text, keyboard);
            } else {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
            }
        } else if (data.startsWith('sbl_range:')) {
            const range = data.split(':')[1];
            let chars: string[] = [];
            if (range === '0-9') {
                chars = "0123456789".split('');
            } else {
                const [start, end] = range.split('-');
                for (let i = start.charCodeAt(0); i <= end.charCodeAt(0); i++) {
                    chars.push(String.fromCharCode(i));
                }
            }
            const keyboardRows = [];
            for (let i = 0; i < chars.length; i += 5) {
                keyboardRows.push(chars.slice(i, i + 5).map(char => ({ text: char, callback_data: `sbl_char:${char}` })));
            }
            keyboardRows.push([{ text: "⬅️ Назад", callback_data: "search_by_letter_init" }]);
            const keyboard = { inline_keyboard: keyboardRows };
            const text = `Выберите букву/цифру из диапазона *${escapeMarkdown(range)}*:`;

             if (message.photo) {
                await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await sendMessage(TELEGRAM_TOKEN, chatId, text, keyboard);
            } else {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
            }

        } else if (data.startsWith('sbl_char:')) {
            const char = data.split(':')[1];
            if (message.photo) {
                await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await executeLetterSearch(TELEGRAM_TOKEN, chatId, char, 1, null, config);
            } else {
                await executeLetterSearch(TELEGRAM_TOKEN, chatId, char, 1, messageId, config);
            }
        
        } else if (data.startsWith('sbl_page:')) {
            const [_, char, pageStr] = data.split(':');
            const page = parseInt(pageStr, 10);
            await executeLetterSearch(TELEGRAM_TOKEN, chatId, char, page, messageId, config);
        
        } else if (data.startsWith('get_profile_access:')) {
            const profileId = data.split(':')[1];
            const useStars = !TELEGRAM_PROVIDER_TOKEN;
            const cost = useStars ? TELEGRAM_SEARCH_COST_STARS : TELEGRAM_SEARCH_COST_REAL;
            const currency = useStars ? "XTR" : TELEGRAM_PAYMENT_CURRENCY;
            const finalAmount = useStars ? cost : toSmallestUnit(cost);

            await sendInvoice(
                TELEGRAM_TOKEN,
                chatId,
                `Доступ к профилю ID: ${profileId}`,
                `Оплата за получение данных по профилю ID: ${profileId}`,
                `search:${profileId}`, // Use the same payload as regular search
                TELEGRAM_PROVIDER_TOKEN,
                currency,
                [{ label: `Доступ к ID ${profileId}`, amount: finalAmount }]
            );
        } else if (data.startsWith('show_categories:')) {
            if (!MONGODB_URI) return NextResponse.json({ status: 'ok' });
            
            const page = parseInt(data.split(':')[1] || '1', 10);

            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const allCategories = await mongoClient.db(dbName).collection("products").distinct("category", { ownerId: WORKER_ID });

            const text = "🛍️ Выберите категорию:";
            let keyboard: any;

            if (allCategories.length === 0) {
                 keyboard = { inline_keyboard: [[{ text: "⬅️ В главное меню", callback_data: "main_menu" }]] };
                 await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "В данный момент товары отсутствуют\\.", keyboard);
                 return NextResponse.json({ status: 'ok' });
            }

            const itemsPerPage = 10;
            const totalPages = Math.ceil(allCategories.length / itemsPerPage);
            const currentPage = Math.min(Math.max(page, 1), totalPages);
            const startIndex = (currentPage - 1) * itemsPerPage;
            const categoriesToShow = allCategories.slice(startIndex, startIndex + itemsPerPage);
            
            const categoryButtons = categoriesToShow.map(cat => ([{ text: escapeMarkdown(cat) || 'Без категории', callback_data: `show_products:${cat || 'none'}:1` }]));

            const paginationButtons = [];
            if (currentPage > 1) paginationButtons.push({ text: `⬅️`, callback_data: `show_categories:${currentPage - 1}` });
            if (totalPages > 1) paginationButtons.push({ text: `${currentPage}/${totalPages}`, callback_data: `sbl_nop` });
            if (currentPage < totalPages) paginationButtons.push({ text: `➡️`, callback_data: `show_categories:${currentPage + 1}` });

            keyboard = {
                inline_keyboard: [
                    ...categoryButtons,
                    paginationButtons,
                    [{ text: "🛒 Корзина", callback_data: "view_cart" }],
                    [{ text: "⬅️ В главное меню", callback_data: "main_menu" }]
                ]
            };
            
            if (message.photo) {
                await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await sendMessage(TELEGRAM_TOKEN, chatId, text, keyboard);
            } else {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
            }

        } else if (data.startsWith('show_products:')) {
            const parts = data.split(':');
            const category = parts[1];
            const page = parseInt(parts[2] || '1', 10);
            
            const findQuery = category === 'none' ? { ownerId: WORKER_ID, $or: [{category: ''}, {category: null}] } : { ownerId: WORKER_ID, category: category };
            if (!MONGODB_URI) return NextResponse.json({ status: 'ok' });

            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const allProducts = await mongoClient.db(dbName).collection("products").find(findQuery).toArray();
            
            const itemsPerPage = 10;
            const totalPages = Math.ceil(allProducts.length / itemsPerPage);
            const currentPage = Math.min(Math.max(page, 1), totalPages);
            const startIndex = (currentPage - 1) * itemsPerPage;
            const productsToShow = allProducts.slice(startIndex, startIndex + itemsPerPage);

            const useStars = !TELEGRAM_PROVIDER_TOKEN;
            const currencySymbol = useStars ? '⭐' : escapeMarkdown(TELEGRAM_PAYMENT_CURRENCY);

            const productButtons = productsToShow.map(p => ([{
                text: `${escapeMarkdown(p.buttonName)} \\- ${escapeMarkdown(useStars ? p.price : p.priceReal)} ${currencySymbol}`,
                callback_data: `view_product:${p._id}`
            }]));

            const paginationButtons = [];
            if (currentPage > 1) paginationButtons.push({ text: `⬅️`, callback_data: `show_products:${category}:${currentPage - 1}` });
            if (totalPages > 1) paginationButtons.push({ text: `${currentPage}/${totalPages}`, callback_data: `sbl_nop` });
            if (currentPage < totalPages) paginationButtons.push({ text: `➡️`, callback_data: `show_products:${category}:${currentPage + 1}` });
            
            const keyboard = { inline_keyboard: [ 
                ...productButtons, 
                paginationButtons,
                [{ text: "⬅️ Назад к категориям", callback_data: "show_categories:1" }]
            ] };
            
            const text = `*${escapeMarkdown(category === 'none' ? 'Без категории' : category)}*`;
            
            if (message.photo) {
                await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await sendMessage(TELEGRAM_TOKEN, chatId, text, keyboard);
            } else {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
            }

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
                const currencySymbol = useStars ? '⭐' : escapeMarkdown(TELEGRAM_PAYMENT_CURRENCY);
                
                const text = `*${escapeMarkdown(product.invoiceTitle)}*\n\n${escapeMarkdown(product.invoiceDescription)}\n\n*Цена:* ${escapeMarkdown(price)} ${currencySymbol}`;
                const keyboard = {
                    inline_keyboard: [
                        [
                          { text: `💳 Купить сейчас`, callback_data: `buy_now:${product._id}` },
                          { text: `➕ В корзину`, callback_data: `add_to_cart:${product._id}` }
                        ],
                        [{ text: "⬅️ Назад к товарам", callback_data: `show_products:${product.category || 'none'}:1` }]
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
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "🛒 Ваша корзина пуста\\.", { inline_keyboard: [[{ text: "⬅️ Назад к категориям", callback_data: "show_categories:1" }]] });
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
            const currencySymbol = useStars ? '⭐' : escapeMarkdown(TELEGRAM_PAYMENT_CURRENCY);
            
            const productCounts: {[key: string]: number} = productIds.reduce((acc: any, id: string) => { acc[id] = (acc[id] || 0) + 1; return acc; }, {});

            productsInCart.forEach(p => {
                const count = productCounts[p._id.toString()];
                cartText += `*${escapeMarkdown(p.invoiceTitle)}* \\(x${count}\\) \\- ${escapeMarkdown((useStars ? p.price : p.priceReal) * count)} ${currencySymbol}\n`;
                totalPriceStars += p.price * count;
                totalPriceReal += p.priceReal * count;
            });
            
            const total = useStars ? totalPriceStars : totalPriceReal;
            cartText += `\n*Итого:* ${escapeMarkdown(total)} ${currencySymbol}`;
            
            const keyboard = {
                inline_keyboard: [
                    [{ text: `💳 Оплатить ${escapeMarkdown(total)} ${currencySymbol}`, callback_data: "checkout_cart" }],
                    [{ text: "🗑️ Очистить корзину", callback_data: "clear_cart" }],
                    [{ text: "⬅️ Назад к категориям", callback_data: "show_categories:1" }]
                ]
            };
            
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, cartText, keyboard);
        }
        else if (data === 'clear_cart') {
            const cartKey = getCartKey(chatId);
            await redisClient.del(cartKey);
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "✅ Корзина очищена\\.", { inline_keyboard: [[{ text: "⬅️ Назад к категориям", callback_data: "show_categories:1" }]]});
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
                payload = `product:${product._id}`;
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

            // Clear cart after creating invoice
            if (isCartCheckout) {
                const cartKey = getCartKey(chatId);
                await redisClient.del(cartKey);
            }
            
             await sendInvoice(
                TELEGRAM_TOKEN,
                chatId,
                title,
                `Общая сумма: ${useStars ? totalAmount : totalAmount / 100} ${useStars ? '⭐' : currency}`,
                isCartCheckout ? payload : `product:${productsToBuy[0]._id}`, // Make sure payload is correct for single/cart
                TELEGRAM_PROVIDER_TOKEN,
                currency,
                consolidatedPrices
             );
        }
        else if (data === 'main_menu') {
            await deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
            await handleStartCommand(chatId, 'private');
        }
        else if (data === 'initiate_connection') {
             if (redisClient) {
                if (TELEGRAM_CONNECTION_PAYMENT_ENABLED) {
                    const useStars = !TELEGRAM_PROVIDER_TOKEN;
                    const balanceKey = getUserConnectionBalanceKey(chatId);
                    const balance = Number(await redisClient.get(balanceKey) || 0);

                    if (balance <= 0) {
                        const cost = useStars ? TELEGRAM_CONNECTION_COST_STARS : TELEGRAM_CONNECTION_COST_REAL;
                         const currencySymbol = useStars ? 'звезд' : escapeMarkdown(TELEGRAM_PAYMENT_CURRENCY);

                        const keyboard = {
                            inline_keyboard: [[
                                { text: `⭐️ Купить 1 связь за ${cost} ${currencySymbol}`, callback_data: "buy_connections" }
                            ]]
                        };
                        await sendMessage(TELEGRAM_TOKEN, chatId, "❌ У вас закончились попытки установить связь\\. Пожалуйста, пополните баланс\\.", keyboard);
                        return NextResponse.json({ status: 'ok' });
                    }
                }

                const userActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${chatId}`;
                const existingRequest = await redisClient.get(userActiveRequestKey);
                
                if (existingRequest) {
                    const [_, myId, partnerId] = existingRequest.split(':');
                    const cancelKeyboard = { inline_keyboard: [[{ text: "❌ Отменить текущий запрос", callback_data: `cancel_connection:${myId}:${partnerId}` }]] };
                    await sendMessage(TELEGRAM_TOKEN, chatId, "У вас уже есть активный запрос на связь\\. Пожалуйста, сначала отмените его, прежде чем создавать новый\\.", cancelKeyboard);
                    return NextResponse.json({ status: 'ok' });
                }

                const stateKey = getUserStateKey(chatId);
                const infoMessage = escapeMarkdown(TELEGRAM_CONNECTION_INFO_MESSAGE);
                await sendMessage(TELEGRAM_TOKEN, chatId, infoMessage);
                
                const flowMessage = await sendMessage(TELEGRAM_TOKEN, chatId, "▶️ Введите ID вашего профиля FunPay, который вы хотите использовать для связи\\.", cancelKeyboard);
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
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "Операция отменена\\.");
        }
        else if (data.startsWith('cancel_connection:')) {
            const [_, myId, partnerId] = data.split(':');
            if (redisClient) {
                const requestKey = `${CONNECTION_REQUEST_PREFIX}${myId}:${partnerId}`;
                const userActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${chatId}`;
                await redisClient.del(requestKey);
                await redisClient.del(userActiveRequestKey);
            }
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "✅ Ваш запрос на связь был успешно отменен\\.");
        }
        else if (data.startsWith('confirm_connection:')) {
            const [_, decision, myId, partnerId] = data.split(':');
            
            if (!redisClient) {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "⚠️ Ошибка сервера: не удалось подключиться к Redis\\.");
                return NextResponse.json({ status: 'ok' });
            }

            const myRequestKey = `${CONNECTION_REQUEST_PREFIX}${myId}:${partnerId}`;
            const partnerRequestKey = `${CONNECTION_REQUEST_PREFIX}${partnerId}:${myId}`;
            const myConfirmKey = `${CONNECTION_CONFIRM_PREFIX}${myId}:${partnerId}`;
            const partnerConfirmKey = `${CONNECTION_CONFIRM_PREFIX}${partnerId}:${myId}`;

            const myRequestDataRaw = await redisClient.get(myRequestKey);
            const partnerRequestDataRaw = await redisClient.get(partnerRequestKey);

            if (!myRequestDataRaw || !partnerRequestDataRaw) {
                 await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "⚠️ Этот запрос на связь истек или был отменен\\.");
                 return NextResponse.json({ status: 'ok' });
            }
             const myRequestData = JSON.parse(myRequestDataRaw);
             const partnerRequestData = JSON.parse(partnerRequestDataRaw);
             const myActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${myRequestData.chatId}`;
             const partnerActiveRequestKey = `${USER_ACTIVE_REQUEST_PREFIX}${partnerRequestData.chatId}`;

             const keysToDelete = [myRequestKey, partnerRequestKey, myConfirmKey, partnerConfirmKey, myActiveRequestKey, partnerActiveRequestKey];

            if (decision === 'no') {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "❌ Вы отменили запрос на связь\\.");
                await sendMessage(TELEGRAM_TOKEN, partnerRequestData.chatId, `❌ Пользователь FunPay \`${myId}\` отменил запрос на связь\\.`);
                if(redisClient) await redisClient.del(keysToDelete);
                return NextResponse.json({ status: 'ok' });
            }

            // User said YES
            await redisClient.set(myConfirmKey, "confirmed", { EX: CONNECTION_TTL_SECONDS });
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "✅ Вы подтвердили обмен\\. Ожидаем подтверждения от второго пользователя\\.\\.\\.");

            const partnerStatus = await redisClient.get(partnerConfirmKey);

            if (partnerStatus === 'confirmed') {
                const myUsername = myRequestData.username ? `@${myRequestData.username}` : (myRequestData.first_name || 'Профиль скрыт');
                const partnerUsername = partnerRequestData.username ? `@${partnerRequestData.username}` : (partnerRequestData.first_name || 'Профиль скрыт');
                
                await sendMessage(myRequestData.chatId, `🎉 Обмен состоялся\\! \n\nСвяжитесь с пользователем FunPay \`${partnerId}\` через Telegram: ${escapeMarkdown(partnerUsername)}`);
                await sendMessage(partnerRequestData.chatId, `🎉 Обмен состоялся\\! \n\nСвяжитесь с пользователем FunPay \`${myId}\` через Telegram: ${escapeMarkdown(myUsername)}`);
                
                // Edit the other user's message as well to confirm
                const partnerOriginalMessageId = partnerRequestData.messageId;
                if(partnerOriginalMessageId) {
                  await editMessageText(TELEGRAM_TOKEN, partnerRequestData.chatId, partnerOriginalMessageId, `🎉 Обмен с FunPay \`${myId}\` состоялся\\! Контакт отправлен в отдельном сообщении\\.`).catch(console.error);
                }
                
                // Edit my own original message
                 await editMessageText(TELEGRAM_TOKEN, chatId, messageId, `🎉 Обмен с FunPay \`${partnerId}\` состоялся\\! Контакт отправлен в отдельном сообщении\\.`).catch(console.error);
            
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

async function setBotCommands(token: string) {
    const commands = [
        { command: 'start', description: 'Запустить/перезапустить бота' }
    ];
    return apiCall(token, 'setMyCommands', { commands });
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
        const webhookResult = await setWebhook(token, webhookUrl);
        if (webhookResult.ok) {
            await setBotCommands(token); // Set commands after webhook is set
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

export async function DELETE(request: NextRequest) {
    const config = await getConfig();
    if (!config.REDIS_URI) {
        return NextResponse.json({ error: 'Redis не сконфигурирован' }, { status: 500 });
    }
    
    const session = await getIronSession<SessionData, any>(cookies(), sessionOptions);
    if (!session.isSettingsUnlocked) {
         return NextResponse.json({ error: 'Доступ запрещен' }, { status: 403 });
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

    