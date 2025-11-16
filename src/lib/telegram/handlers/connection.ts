// src/lib/telegram/handlers/connection.ts
import { sendMessage, editMessageText, deleteMessage } from '../api';
import { 
    escapeMarkdown, 
    cancelKeyboard, 
    getUserStateKey, 
    getUserConnectionBalanceKey, 
    getUserActiveRequestKey, 
    getConnectionRequestKey, 
    getConfirmKey, 
    CONNECTION_TTL_SECONDS 
} from '../utils';

export async function handleInitiateConnection(chatId: number, config: any) {
    const { redisClient, TELEGRAM_CONNECTION_PAYMENT_ENABLED, TELEGRAM_PROVIDER_TOKEN, TELEGRAM_CONNECTION_COST_STARS, TELEGRAM_CONNECTION_COST_REAL, TELEGRAM_PAYMENT_CURRENCY, TELEGRAM_TOKEN, TELEGRAM_CONNECTION_INFO_MESSAGE } = config;
    
    if (!redisClient) return;

    if (TELEGRAM_CONNECTION_PAYMENT_ENABLED) {
        const useStars = !TELEGRAM_PROVIDER_TOKEN;
        const balanceKey = getUserConnectionBalanceKey(chatId);
        const balance = Number(await redisClient.get(balanceKey) || 0);

        if (balance <= 0) {
            const cost = useStars ? TELEGRAM_CONNECTION_COST_STARS : TELEGRAM_CONNECTION_COST_REAL;
            const currencySymbol = useStars ? 'звезд' : escapeMarkdown(TELEGRAM_PAYMENT_CURRENCY);
            const keyboard = { inline_keyboard: [[{ text: `⭐️ Купить 1 связь за ${cost} ${currencySymbol}`, callback_data: "buy_connections" }]] };
            await sendMessage(TELEGRAM_TOKEN, chatId, "❌ У вас закончились попытки установить связь\\. Пожалуйста, пополните баланс\\.", keyboard);
            return;
        }
    }

    const userActiveRequestKey = getUserActiveRequestKey(chatId);
    const existingRequest = await redisClient.get(userActiveRequestKey);
    
    if (existingRequest) {
        const [_, myId, partnerId] = existingRequest.split(':');
        const cancelExistingKeyboard = { inline_keyboard: [[{ text: "❌ Отменить текущий запрос", callback_data: `cancel_connection:${myId}:${partnerId}` }]] };
        await sendMessage(TELEGRAM_TOKEN, chatId, "У вас уже есть активный запрос на связь\\. Пожалуйста, сначала отмените его, прежде чем создавать новый\\.", cancelExistingKeyboard);
        return;
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

export async function handleConnectionLogic(chatId: number, text: string, from: any, config: any) {
    const { redisClient, TELEGRAM_TOKEN, TELEGRAM_CONNECTION_PAYMENT_ENABLED } = config;
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
            const newState = { step: 'awaiting_partner_id', myId: myId, messageId: flowMessageId };
            await redisClient.set(stateKey, JSON.stringify(newState), { EX: 300 });
            return;
        }
        
        await redisClient.del(stateKey);
        
        if (TELEGRAM_CONNECTION_PAYMENT_ENABLED) {
            const balanceKey = getUserConnectionBalanceKey(chatId);
            await redisClient.decr(balanceKey);
            const newBalance = await redisClient.get(balanceKey) || 0;
            await sendMessage(TELEGRAM_TOKEN, chatId, `Попытка связи использована\\. У вас осталось: ${newBalance}\\.`);
        }

        const myRequestKey = getConnectionRequestKey(myId, partnerId);
        const myRequestData = JSON.stringify({ chatId: chatId, username: from.username || from.first_name || "", messageId: flowMessageId });
        await redisClient.set(myRequestKey, myRequestData, { EX: CONNECTION_TTL_SECONDS });
        
        const userActiveRequestKey = getUserActiveRequestKey(chatId);
        await redisClient.set(userActiveRequestKey, myRequestKey, { EX: CONNECTION_TTL_SECONDS });
        
        const partnerRequestKey = getConnectionRequestKey(partnerId, myId);
        const partnerRequestDataRaw = await redisClient.get(partnerRequestKey);

        if (partnerRequestDataRaw) {
            const partnerRequestData = JSON.parse(partnerRequestDataRaw);
            const partnerChatId = partnerRequestData.chatId;

            const myConfirmKey = getConfirmKey(myId, partnerId);
            const partnerConfirmKey = getConfirmKey(partnerId, myId);
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
            
            const sentPartnerMessage = await sendMessage(TELEGRAM_TOKEN, partnerChatId, partnerMessage, confirmationKeyboard(partnerId, myId));
            
            if (sentPartnerMessage.ok && partnerRequestData.messageId) {
                 await deleteMessage(TELEGRAM_TOKEN, partnerChatId, partnerRequestData.messageId).catch(console.error);
            }

        } else {
            const cancelRequestKeyboard = { inline_keyboard: [[{ text: "❌ Отменить запрос", callback_data: `cancel_connection:${myId}:${partnerId}` }]] };
            await editMessageText(TELEGRAM_TOKEN, chatId, flowMessageId, `✅ Ваш запрос на связь с \`${partnerId}\` создан и будет активен 24 часа\\. Мы сообщим вам, когда пользователь ответит взаимностью\\.`, cancelRequestKeyboard);
        }
    }
}

export async function handleConfirmConnection(chatId: number, messageId: number, data: string, config: any) {
    const { redisClient, TELEGRAM_TOKEN } = config;
    const [_, decision, myId, partnerId] = data.split(':');
    
    if (!redisClient) {
        await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "⚠️ Ошибка сервера: не удалось подключиться к Redis\\.");
        return;
    }

    const myRequestKey = getConnectionRequestKey(myId, partnerId);
    const partnerRequestKey = getConnectionRequestKey(partnerId, myId);
    const myConfirmKey = getConfirmKey(myId, partnerId);
    const partnerConfirmKey = getConfirmKey(partnerId, myId);

    const myRequestDataRaw = await redisClient.get(myRequestKey);
    const partnerRequestDataRaw = await redisClient.get(partnerRequestKey);

    if (!myRequestDataRaw || !partnerRequestDataRaw) {
         await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "⚠️ Этот запрос на связь истек или был отменен\\.");
         return;
    }
     const myRequestData = JSON.parse(myRequestDataRaw);
     const partnerRequestData = JSON.parse(partnerRequestDataRaw);
     const myActiveRequestKey = getUserActiveRequestKey(myRequestData.chatId);
     const partnerActiveRequestKey = getUserActiveRequestKey(partnerRequestData.chatId);

     const keysToDelete = [myRequestKey, partnerRequestKey, myConfirmKey, partnerConfirmKey, myActiveRequestKey, partnerActiveRequestKey];

    if (decision === 'no') {
        await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "❌ Вы отменили запрос на связь\\.");
        await sendMessage(TELEGRAM_TOKEN, partnerRequestData.chatId, `❌ Пользователь FunPay \`${myId}\` отменил запрос на связь\\.`);
        if(redisClient) await redisClient.del(keysToDelete);
        return;
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
        
        const partnerOriginalMessageId = partnerRequestData.messageId;
        if(partnerOriginalMessageId) {
          await editMessageText(TELEGRAM_TOKEN, partnerRequestData.chatId, partnerOriginalMessageId, `🎉 Обмен с FunPay \`${myId}\` состоялся\\! Контакт отправлен в отдельном сообщении\\.`).catch(console.error);
        }
        
         await editMessageText(TELEGRAM_TOKEN, chatId, messageId, `🎉 Обмен с FunPay \`${partnerId}\` состоялся\\! Контакт отправлен в отдельном сообщении\\.`).catch(console.error);
    
        if(redisClient) await redisClient.del(keysToDelete);
    }
}

export async function handleCancelConnection(chatId: number, messageId: number, data: string, config: any) {
    const { redisClient, TELEGRAM_TOKEN } = config;
    const [_, myId, partnerId] = data.split(':');
    if (redisClient) {
        const requestKey = getConnectionRequestKey(myId, partnerId);
        const userActiveRequestKey = getUserActiveRequestKey(chatId);
        await redisClient.del(requestKey);
        await redisClient.del(userActiveRequestKey);
    }
    await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "✅ Ваш запрос на связь был успешно отменен\\.");
}

export async function handleBuyConnections(chatId: number, config: any) {
    const { TELEGRAM_PROVIDER_TOKEN, TELEGRAM_CONNECTION_COST_STARS, TELEGRAM_CONNECTION_COST_REAL, TELEGRAM_PAYMENT_CURRENCY, TELEGRAM_TOKEN } = config;
    const useStars = !TELEGRAM_PROVIDER_TOKEN;
    const cost = useStars ? TELEGRAM_CONNECTION_COST_STARS : TELEGRAM_CONNECTION_COST_REAL;
    const currency = useStars ? "XTR" : TELEGRAM_PAYMENT_CURRENCY;
    const finalAmount = useStars ? cost : toSmallestUnit(cost);

    const title = "Покупка попытки связи";
    const description = "Покупка 1 попытки для установки связи";
    const payload = "buy_1_connection";

    await config.sendInvoice(
        TELEGRAM_TOKEN,
        chatId,
        title,
        description,
        payload,
        TELEGRAM_PROVIDER_TOKEN,
        currency,
        [{ label: `1 Связь`, amount: finalAmount }]
    );
}

export async function handleCheckBalance(chatId: number, callbackQueryId: string, data: string, config: any) {
    const { redisClient, TELEGRAM_TOKEN } = config;
    const type = data.split(':')[1];
    if (type === 'connection' && redisClient) {
        const balanceKey = getUserConnectionBalanceKey(chatId);
        const balance = await redisClient.get(balanceKey) || 0;
        await config.answerCallbackQuery(TELEGRAM_TOKEN, callbackQueryId, `У вас ${balance} связей.`);
    }
}
