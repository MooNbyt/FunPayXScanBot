
// src/lib/telegram/handlers/payment.ts
import { MongoClient, ObjectId } from 'mongodb';
import { sendMessage } from '../api';
import { escapeMarkdown, getUserConnectionBalanceKey, toSmallestUnit } from '../utils';
import { executeSearch } from './search';

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

export async function handlePreCheckout(query: any, config: any) {
    const { WORKER_ID, TELEGRAM_TOKEN } = config;
    const payload = query.invoice_payload;
    
    if (payload.startsWith('product:')) {
        const productId = payload.split(':')[1];
        let client: MongoClient | undefined;
        try {
            const { client: connectedClient, db } = await getDb();
            client = connectedClient;
            const product = await db.collection("products").findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });

            if (!product) {
                await config.answerPreCheckoutQuery(TELEGRAM_TOKEN, query.id, false, "Товар больше не доступен.");
                return;
            }

            if (product.type === 'static') {
                const keys = (product.staticKey || '').split('\n').filter((k: string) => k.trim() !== '');
                if (keys.length === 0) {
                     await config.answerPreCheckoutQuery(TELEGRAM_TOKEN, query.id, false, "К сожалению, этот товар закончился.");
                     return;
                }
            }
        } catch(e) {
            console.error("Pre-checkout error:", e);
            await config.answerPreCheckoutQuery(TELEGRAM_TOKEN, query.id, false, "Ошибка на стороне сервера.");
            return;
        } finally {
            if (client) await client.close();
        }
    }
    
    await config.answerPreCheckoutQuery(TELEGRAM_TOKEN, query.id, true);
}


export async function handleSuccessfulPayment(payment: any, chatId: number, config: any) {
    const { WORKER_ID, TELEGRAM_TOKEN } = config;
    const invoicePayload = payment.invoice_payload;

    if (invoicePayload.startsWith('search:')) {
        const query = invoicePayload.substring('search:'.length);
        await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата прошла успешно\\! Выполняю поиск по запросу: *${escapeMarkdown(query)}*`);
        await executeSearch(TELEGRAM_TOKEN, chatId, query, config);
    } else if (invoicePayload === 'buy_1_connection') {
        const balanceKey = getUserConnectionBalanceKey(chatId);
        await config.redisClient.incr(balanceKey);
        await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата прошла успешно\\! Вам зачислена 1 попытка установки связи\\.`);
    } else if (invoicePayload.startsWith('product:')) {
        const productId = invoicePayload.split(':')[1];
        let client: MongoClient | undefined;
        try {
            const { client: connectedClient, db } = await getDb();
            client = connectedClient;
            const productsCollection = db.collection("products");
            const product = await productsCollection.findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });
            
            if (!product) {
                await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Ошибка\\. Товар не найден после оплаты\\.");
                return;
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
                    
                    const updatedStaticKey = keys.join('\n');
                    await productsCollection.updateOne(
                        { _id: new ObjectId(productId) },
                        { $set: { staticKey: updatedStaticKey } }
                    );
                } else {
                    await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Извините, товар закончился\\. Пожалуйста, свяжитесь с администратором\\.");
                }
            }
        } catch(e) {
            console.error("Error handling successful payment:", e);
            await sendMessage(TELEGRAM_TOKEN, chatId, "❌ Произошла ошибка при обработке вашего платежа\\.");
        } finally {
            if (client) await client.close();
        }
    } else if (invoicePayload === 'cart_checkout') {
        const totalAmount = payment.total_amount;
        const currency = payment.currency;

        await sendMessage(TELEGRAM_TOKEN, chatId, `✅ Оплата на сумму ${totalAmount / 100} ${currency} прошла успешно\\! \n\nВыдача товаров из корзины еще не реализована\\.`);
        // TODO: Implement cart items delivery logic here
    }
}
