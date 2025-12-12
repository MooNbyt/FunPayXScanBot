
// src/lib/telegram/handlers/shop.ts
import { ObjectId } from 'mongodb';
import { sendMessage, sendPhoto, sendInvoice, editMessageText, answerCallbackQuery } from '../api';
import { escapeMarkdown, toSmallestUnit, getCartKey, CART_TTL_SECONDS } from '../utils';
import { getDb } from '@/lib/mongodb';

export async function handleShowCategories(chatId: number, page: number, messageId: number, config: any) {
    const { WORKER_ID, TELEGRAM_TOKEN, message } = config;

    try {
        const { db } = await getDb();
        const allCategories = await db.collection("products").distinct("category", { ownerId: WORKER_ID });

        const text = "🛍️ Выберите категорию:";
        let keyboard: any;

        if (allCategories.length === 0) {
             keyboard = { inline_keyboard: [[{ text: "⬅️ В главное меню", callback_data: "main_menu" }]] };
             await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "В данный момент товары отсутствуют\\.", keyboard);
             return;
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
            await config.deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
            await sendMessage(TELEGRAM_TOKEN, chatId, text, keyboard);
        } else {
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
        }
    } catch(e) {
        console.error("Error in handleShowCategories:", e);
    }
}

export async function handleShowProducts(chatId: number, category: string, page: number, messageId: number, config: any) {
    const { WORKER_ID, TELEGRAM_TOKEN, TELEGRAM_PROVIDER_TOKEN, TELEGRAM_PAYMENT_CURRENCY, message } = config;

    try {
        const { db } = await getDb();
        const findQuery = category === 'none' ? { ownerId: WORKER_ID, $or: [{category: ''}, {category: null}] } : { ownerId: WORKER_ID, category: category };
        const allProducts = await db.collection("products").find(findQuery).toArray();
        
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
            await config.deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
            await sendMessage(TELEGRAM_TOKEN, chatId, text, keyboard);
        } else {
            await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
        }
    } catch(e) {
        console.error("Error in handleShowProducts:", e);
    }
}

export async function handleViewProduct(chatId: number, productId: string, messageId: number, config: any) {
    const { WORKER_ID, TELEGRAM_TOKEN, TELEGRAM_PROVIDER_TOKEN, TELEGRAM_PAYMENT_CURRENCY, callback_query } = config;

    try {
        const { db } = await getDb();
        const product = await db.collection("products").findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });

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
                await config.deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await sendPhoto(TELEGRAM_TOKEN, chatId, product.productImageUrl, text, keyboard);
            } else if (product.productImageUrl && callback_query.message.photo) {
                await config.deleteMessage(TELEGRAM_TOKEN, chatId, messageId);
                await sendPhoto(TELEGRAM_TOKEN, chatId, product.productImageUrl, text, keyboard);
            } else {
                await editMessageText(TELEGRAM_TOKEN, chatId, messageId, text, keyboard);
            }

         } else {
             await answerCallbackQuery(TELEGRAM_TOKEN, callback_query.id, "Товар не найден.");
         }
    } catch(e) {
        console.error("Error in handleViewProduct:", e);
    }
}

export async function handleAddToCart(chatId: number, productId: string, callbackQueryId: string, config: any) {
    const cartKey = getCartKey(chatId);
    await config.redisClient.lPush(cartKey, productId);
    await config.redisClient.expire(cartKey, CART_TTL_SECONDS);
    await answerCallbackQuery(config.TELEGRAM_TOKEN, callbackQueryId, "✅ Добавлено в корзину");
}

export async function handleViewCart(chatId: number, messageId: number, config: any) {
    const { redisClient, TELEGRAM_PROVIDER_TOKEN, TELEGRAM_PAYMENT_CURRENCY, TELEGRAM_TOKEN } = config;
    const cartKey = getCartKey(chatId);
    const productIds = await redisClient.lRange(cartKey, 0, -1);

    if (productIds.length === 0) {
        await editMessageText(TELEGRAM_TOKEN, chatId, messageId, "🛒 Ваша корзина пуста\\.", { inline_keyboard: [[{ text: "⬅️ Назад к категориям", callback_data: "show_categories:1" }]] });
        return;
    }
    
    try {
        const { db } = await getDb();
        const productsCollection = db.collection("products");

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
    } catch(e) {
        console.error("Error in handleViewCart:", e);
    }
}

export async function handleClearCart(chatId: number, messageId: number, config: any) {
    const cartKey = getCartKey(chatId);
    await config.redisClient.del(cartKey);
    await editMessageText(config.TELEGRAM_TOKEN, chatId, messageId, "✅ Корзина очищена\\.", { inline_keyboard: [[{ text: "⬅️ Назад к категориям", callback_data: "show_categories:1" }]]});
}

export async function handleCheckout(chatId: number, data: string, callbackQueryId: string, config: any) {
    const { WORKER_ID, TELEGRAM_PROVIDER_TOKEN, TELEGRAM_PAYMENT_CURRENCY, TELEGRAM_TOKEN, redisClient } = config;
    const isCartCheckout = data === 'checkout_cart';
    const useStars = !TELEGRAM_PROVIDER_TOKEN;
    const currency = useStars ? "XTR" : TELEGRAM_PAYMENT_CURRENCY;

    try {
        const { db } = await getDb();
        const productsCollection = db.collection("products");

        let productsToBuy: any[] = [];
        let title: string;
        let payload: string;

        if (isCartCheckout) {
           const cartKey = getCartKey(chatId);
           const productIds = await redisClient.lRange(cartKey, 0, -1);
           if (productIds.length === 0) {
                await answerCallbackQuery(TELEGRAM_TOKEN, callbackQueryId, "Корзина пуста!");
                return;
           }
           const productObjectIds = productIds.map(id => new ObjectId(id));
           const productsInDb = await productsCollection.find({ _id: { $in: productObjectIds } }).toArray();
           
           const productMap = new Map(productsInDb.map(p => [p._id.toString(), p]));
           productsToBuy = productIds.map(id => productMap.get(id)).filter(Boolean);

           title = "Оплата заказа";
           payload = `cart_checkout`;
        } else { // buy_now
           const productId = data.split(':')[1];
           const product = await productsCollection.findOne({ _id: new ObjectId(productId), ownerId: WORKER_ID });
           if (!product) {
               await answerCallbackQuery(TELEGRAM_TOKEN, callbackQueryId, "Товар не найден.");
               return;
           }
           productsToBuy = [product];
           title = product.invoiceTitle;
           payload = `product:${product._id}`;
        }

        const prices = productsToBuy.map(p => ({
           label: p.invoiceTitle,
           amount: useStars ? p.price : toSmallestUnit(p.priceReal)
        }));

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
            await answerCallbackQuery(TELEGRAM_TOKEN, callbackQueryId, "Нечего оплачивать.");
            return;
        }

       if (isCartCheckout) {
           const cartKey = getCartKey(chatId);
           await redisClient.del(cartKey);
       }
       
        await sendInvoice(
           TELEGRAM_TOKEN,
           chatId,
           title,
           `Общая сумма: ${useStars ? totalAmount : totalAmount / 100} ${useStars ? '⭐' : currency}`,
           isCartCheckout ? payload : `product:${productsToBuy[0]._id}`,
           TELEGRAM_PROVIDER_TOKEN,
           currency,
           consolidatedPrices
        );
    } catch(e) {
        console.error("Error in handleCheckout:", e);
    }
}
