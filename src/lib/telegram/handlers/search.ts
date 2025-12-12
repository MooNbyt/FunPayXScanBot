// src/lib/telegram/handlers/search.ts
import { createClient } from 'redis';
import { sendMessage, editMessageText } from '../api';
import { escapeMarkdown, addUrlToProfile } from '../utils';
import { scrapeUser } from '@/app/api/scrape/runner';

export async function executeSearch(token: string, chatId: number, query: string, config: any) {
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
        const initialResponse = await config.searchProfiles(query, 'nickname', config);
        let profiles = await initialResponse.json();
        
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
}

export async function handleSupportSearch(chatId: number, page: number, messageId: number | null, config: any) {
    const { TELEGRAM_TOKEN } = config;
    if (!messageId) {
        const statusMessage = await sendMessage(TELEGRAM_TOKEN, chatId, '⏳ Ищу профили поддержки\\.\\.\\.');
        messageId = statusMessage.result?.message_id;
    }

    try {
        const response = await config.searchProfiles('', 'status', config, 'support');
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
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, responseText, keyboard);
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
}

export async function executeLetterSearch(chatId: number, letter: string, page: number, messageId: number | null, config: any) {
    const { TELEGRAM_TOKEN, NEXT_PUBLIC_APP_URL, TELEGRAM_PAYMENT_ENABLED } = config;

    if (!messageId) {
        const statusMessage = await sendMessage(TELEGRAM_TOKEN, chatId, `⏳ Ищу пользователей на букву *${escapeMarkdown(letter)}*\\.\\.\\.`);
        messageId = statusMessage.result?.message_id;
    }
    
    try {
        const response = await fetch(`${NEXT_PUBLIC_APP_URL}/api/data?letter=${encodeURIComponent(letter)}`);
        if (!response.ok) throw new Error("Ошибка при поиске по букве");

        const profiles = await response.json();
        const profilesPerPage = TELEGRAM_PAYMENT_ENABLED ? 10 : 30;
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
                 if (TELEGRAM_PAYMENT_ENABLED) {
                    responseText += `*${escapeMarkdown(p.nickname)}*${escapeMarkdown(status)}\n`;
                    inlineKeyboardRows.push([{ text: `🔗 Получить доступ к ${p.nickname}`, callback_data: `get_profile_access:${p.id}` }]);
                 } else {
                    responseText += `*${escapeMarkdown(p.nickname)}*${escapeMarkdown(status)} \\(ID: \`${p.id}\`\\)\n`;
                 }
            });
            if (!TELEGRAM_PAYMENT_ENABLED) {
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

        const keyboard = { inline_keyboard: inlineKeyboardRows };
        const finalKeyboard = inlineKeyboardRows.length > 1 || (inlineKeyboardRows.length === 1 && inlineKeyboardRows[0].length > 0) ? keyboard : undefined;

        if (messageId) {
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, responseText, finalKeyboard);
        }

    } catch (e: any) {
        const errorText = "Произошла ошибка при поиске\\.";
         if (messageId) {
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, errorText);
        } else {
            await sendMessage(TELEGRAM_TOKEN, chatId, errorText);
        }
    }
}
