// src/lib/telegram/api.ts

const BASE_URL = (token: string) => `https://api.telegram.org/bot${token}`;

export async function apiCall(token: string, method: string, payload: any) {
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

export function sendMessage(token: string, chatId: number, text: string, replyMarkup?: any) {
  return apiCall(token, 'sendMessage', {
    chat_id: chatId,
    text: text,
    parse_mode: 'MarkdownV2',
    reply_markup: replyMarkup,
    disable_web_page_preview: true,
  });
}

export function sendInvoice(token: string, chatId: number, title: string, description: string, payload: string, providerToken: string, currency: string, prices: any[]) {
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

export function answerPreCheckoutQuery(token: string, preCheckoutQueryId: string, ok: boolean, errorMessage?: string) {
    return apiCall(token, 'answerPreCheckoutQuery', {
        pre_checkout_query_id: preCheckoutQueryId,
        ok,
        error_message: errorMessage
    });
}

export function sendPhoto(token: string, chatId: number, photoUrl: string, caption: string, replyMarkup?: any) {
  return apiCall(token, 'sendPhoto', {
    chat_id: chatId,
    photo: photoUrl,
    caption: caption,
    parse_mode: 'MarkdownV2',
    reply_markup: replyMarkup,
  });
}

export async function editMessageText(token: string, chatId: number, messageId: number, text: string, replyMarkup?: any) {
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

export function answerCallbackQuery(token: string, callbackQueryId: string, text?: string, showAlert?: boolean) {
    return apiCall(token, 'answerCallbackQuery', {
        callback_query_id: callbackQueryId,
        text: text,
        show_alert: showAlert
    });
}

export function deleteMessage(token: string, chatId: number, messageId: number) {
    return apiCall(token, 'deleteMessage', {
        chat_id: chatId,
        message_id: messageId,
    });
}

export async function setWebhook(token: string, webhookUrl: string) {
    let url = new URL(`${BASE_URL(token)}/setWebhook`);
    url.searchParams.append('url', webhookUrl);
    const response = await fetch(url.toString());
    return response.json();
}

export async function setBotCommands(token: string) {
    const commands = [
        { command: 'start', description: 'Запустить/перезапустить бота' }
    ];
    return apiCall(token, 'setMyCommands', { commands });
}
