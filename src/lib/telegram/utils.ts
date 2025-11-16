// src/lib/telegram/utils.ts

export const escapeMarkdown = (text: string | number | null | undefined): string => {
  if (text === null || text === undefined) return '';
  // In MarkdownV2, these characters must be escaped: _ * [ ] ( ) ~ ` > # + - = | { } . !
  return String(text).replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');
};

export const addUrlToProfile = (profile: any) => ({
    ...profile,
    url: `https://funpay.com/users/${profile.id}/`
});

// Helper to convert to smallest currency unit (e.g., kopecks, cents)
export const toSmallestUnit = (amount: number) => Math.round(amount * 100);

export const getTelegramLogsKey = (workerId: string) => `telegram_logs:${workerId}`;
export const getUserStateKey = (chatId: number) => `telegram_user_state:${chatId}`;
export const getConnectionRequestKey = (myId: string, partnerId: string) => `connect:${myId}:${partnerId}`;
export const getConfirmKey = (myId: string, partnerId: string) => `confirm:${myId}:${partnerId}`;
export const getUserActiveRequestKey = (chatId: number) => `user_active_request:${chatId}`;
export const getUserConnectionBalanceKey = (chatId: number) => `telegram_user_connections:${chatId}`;
export const getCartKey = (chatId: number) => `telegram_cart:${chatId}`;

export const CONNECTION_TTL_SECONDS = 86400; // 24 hours for requests
export const CART_TTL_SECONDS = 86400; // 24 hours for user cart

export const cancelKeyboard = { inline_keyboard: [[{ text: "❌ Отмена", callback_data: `cancel_flow` }]] };
