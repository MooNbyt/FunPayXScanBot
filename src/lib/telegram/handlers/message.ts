// src/lib/telegram/handlers/message.ts
import { MongoClient } from 'mongodb';
import { sendMessage, sendPhoto, sendInvoice, apiCall } from '../api';
import { escapeMarkdown, getUserStateKey, toSmallestUnit } from '../utils';
import { executeSearch } from './search';
import { handleConnectionLogic } from './connection';

async function sendActiveCampaignsToNewUser(chatId: number, config: any) {
    const { MONGODB_URI, MONGODB_DB_NAME, TELEGRAM_TOKEN, WORKER_ID } = config;
    if (!MONGODB_URI) return;

    let mongoClient: MongoClient | undefined;
    try {
        mongoClient = new MongoClient(MONGODB_URI);
        await mongoClient.connect();
        const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
        const campaignsCollection = mongoClient.db(dbName).collection("campaigns");

        const now = new Date();
        const activeCampaigns = await campaignsCollection.find({
            ownerId: WORKER_ID,
            status: { $in: ['sending', 'completed'] },
            lifetimeHours: { $gt: 0 }
        }).toArray();

        for (const campaign of activeCampaigns) {
            const campaignEndDate = new Date(new Date(campaign.createdAt).getTime() + campaign.lifetimeHours * 60 * 60 * 1000);
            if (now < campaignEndDate) {
                // Wait a bit before sending promo message
                await new Promise(resolve => setTimeout(resolve, 1500));
                
                try {
                    const payload: any = {
                        chat_id: chatId,
                        parse_mode: 'Markdown',
                    };
                    
                    if (campaign.text) payload.text = campaign.text;
                    if (campaign.text && campaign.imageUrl) payload.caption = campaign.text;

                    if (campaign.imageUrl) {
                        await apiCall(TELEGRAM_TOKEN, 'sendPhoto', {
                            ...payload,
                            photo: campaign.imageUrl,
                        });
                    } else {
                        await apiCall(TELEGRAM_TOKEN, 'sendMessage', payload);
                    }
                } catch (e) {
                    console.error(`[Campaign] Failed to send evergreen message for campaign ${campaign._id} to new user ${chatId}`, e);
                }
            }
        }

    } catch (e) {
        console.error("[Campaign] Error fetching/sending evergreen campaigns for new user:", e);
    } finally {
        if (mongoClient) await mongoClient.close();
    }
}


export async function handleStartCommand(chatId: number, chatType: string, config: any, isNewUser: boolean = false) {
    const { 
        TELEGRAM_WELCOME_MESSAGE, 
        MONGODB_URI, 
        MONGODB_DB_NAME, 
        TELEGRAM_CUSTOM_LINKS, 
        TELEGRAM_SHOP_BUTTON_NAME,
        TELEGRAM_BOT_LINK,
        TELEGRAM_CONNECTION_PAYMENT_ENABLED,
        REDIS_URI,
        TELEGRAM_WELCOME_IMAGE_URL,
        WORKER_ID
    } = config;

    if (config.redisClient) {
        await config.redisClient.del(getUserStateKey(chatId));
    }

    let welcomeMessage = TELEGRAM_WELCOME_MESSAGE || "🤖 Привет! Я твой помощник.";
    let userCount = 0;
    let botUserCount = 0;
    
    if (MONGODB_URI) {
        let mongoClient: MongoClient | undefined;
        try {
            mongoClient = new MongoClient(MONGODB_URI);
            await mongoClient.connect();
            const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
            const db = mongoClient.db(dbName);
            userCount = await db.collection("users").countDocuments({ status: { $ne: 'not_found' } });
            botUserCount = await db.collection("bot_users").countDocuments({ ownerId: WORKER_ID, status: 'active' });

        } catch (e) {
            console.error("Failed to get user count for welcome message", e);
        } finally {
            if (mongoClient) await mongoClient.close();
        }
    }
    
    welcomeMessage = welcomeMessage.replace(/{user_count}/g, userCount.toLocaleString('ru-RU'));
    welcomeMessage = welcomeMessage.replace(/{bot_user_count}/g, botUserCount.toLocaleString('ru-RU'));
    welcomeMessage = escapeMarkdown(welcomeMessage);
    
    const commonButtons = [
         [{ text: "💚 Найти саппортов", callback_data: "search_support:1" }],
         [{ text: "🔠 Поиск по букве", callback_data: "search_by_letter_init" }],
    ];
    
    let keyboardRows: any[][] = [];
    
    let customLinkButtons = (TELEGRAM_CUSTOM_LINKS || [])
        .filter((link: { text: string; url: string; showInGroups: boolean }) => link.text && link.url);

    if (chatType !== 'private') {
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
        if (TELEGRAM_CONNECTION_PAYMENT_ENABLED && config.redisClient) {
            const balanceKey = `telegram_user_connections:${chatId}`;
            const balance = await config.redisClient.get(balanceKey) || 0;
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
        await sendPhoto(config.TELEGRAM_TOKEN, chatId, TELEGRAM_WELCOME_IMAGE_URL, welcomeMessage, mainMenu);
    } else {
        await sendMessage(config.TELEGRAM_TOKEN, chatId, welcomeMessage, mainMenu);
    }

    if (isNewUser && chatType === 'private') {
        await sendActiveCampaignsToNewUser(chatId, config);
    }
}


export async function handleTextMessage(chatId: number, text: string, from: any, config: any) {
    if (config.redisClient) {
        const stateKey = getUserStateKey(chatId);
        const stateRaw = await config.redisClient.get(stateKey);
        
        if (stateRaw) {
            await handleConnectionLogic(chatId, text, from, config);
            return;
        }
    }

    const isNumeric = /^\d+$/.test(text);
    const searchType = isNumeric ? 'id' : 'nickname';
    
    let profiles = [];
    try {
        const searchResponse = await config.searchProfiles(text, searchType, config);
        profiles = await searchResponse.json();
    } catch (e) {
        console.error("Error searching profiles in handleUserInput", e);
        await sendMessage(config.TELEGRAM_TOKEN, chatId, "Произошла ошибка при поиске в базе данных\\.");
        return;
    }

    if (!profiles || profiles.length === 0) {
         await sendMessage(config.TELEGRAM_TOKEN, chatId, `Пользователь по запросу "${escapeMarkdown(text)}" не найден в базе данных\\.`);
         return;
    }

    if (config.TELEGRAM_PAYMENT_ENABLED) {
        await sendMessage(config.TELEGRAM_TOKEN, chatId, `✅ Профиль по запросу "${escapeMarkdown(text)}" найден\\. Для получения информации, пожалуйста, произведите оплату\\.`);
        const useStars = !config.TELEGRAM_PROVIDER_TOKEN;
        const cost = useStars ? config.TELEGRAM_SEARCH_COST_STARS : config.TELEGRAM_SEARCH_COST_REAL;
        const currency = useStars ? "XTR" : config.TELEGRAM_PAYMENT_CURRENCY;
        const finalAmount = useStars ? cost : toSmallestUnit(cost);

        await sendInvoice(
            config.TELEGRAM_TOKEN,
            chatId,
            `Доступ к профилю: ${text}`,
            `Оплата за получение данных по запросу: "${text}"`,
            `search:${text}`,
            config.TELEGRAM_PROVIDER_TOKEN,
            currency,
            [{ label: '1 Поиск', amount: finalAmount }]
        );
    } else {
        await executeSearch(config.TELEGRAM_TOKEN, chatId, text, config);
    }
}
