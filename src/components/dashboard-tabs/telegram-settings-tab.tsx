"use client";

import React from "react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { Separator } from "@/components/ui/separator";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Loader2, Plug, Copy, PlusCircle, Trash2, Save } from "lucide-react";
import type { CustomLink } from "@/types";

interface TelegramSettingsTabProps {
  telegramToken: string;
  setTelegramToken: (val: string) => void;
  handleBlurSave: (value: any, key: string) => void;
  telegramBotLink: string;
  setTelegramBotLink: (val: string) => void;
  telegramProviderToken: string;
  setTelegramProviderToken: (val: string) => void;
  telegramPaymentCurrency: string;
  setTelegramPaymentCurrency: (val: string) => void;
  handleSaveConfig: (config: any) => void;
  telegramShopButtonName: string;
  setTelegramShopButtonName: (val: string) => void;
  telegramPaymentEnabled: boolean;
  setTelegramPaymentEnabled: (val: boolean) => void;
  telegramSearchCost: number;
  setTelegramSearchCost: (val: number) => void;
  telegramSearchCostReal: number;
  setTelegramSearchCostReal: (val: number) => void;
  telegramConnectionPaymentEnabled: boolean;
  setTelegramConnectionPaymentEnabled: (val: boolean) => void;
  telegramConnectionCost: number;
  setTelegramConnectionCost: (val: number) => void;
  telegramConnectionCostReal: number;
  setTelegramConnectionCostReal: (val: number) => void;
  telegramCustomLinks: CustomLink[];
  handleCustomLinkChange: (index: number, field: 'text' | 'url' | 'showInGroups', value: string | boolean) => void;
  handleSaveCustomLinks: () => void;
  handleRemoveCustomLink: (index: number) => void;
  handleAddCustomLink: () => void;
  telegramWelcomeImageUrl: string;
  setTelegramWelcomeImageUrl: (val: string) => void;
  telegramWelcome: string;
  setTelegramWelcome: (val: string) => void;
  handleInsertVariable: (variable: string, textareaId: string) => void;
  telegramConnectionInfoMessage: string;
  setTelegramConnectionInfoMessage: (val: string) => void;
  appUrl: string;
  isSettingWebhook: boolean;
  handleSetWebhook: () => void;
  webhookLog: string;
  copyToClipboard: (text: string) => void;
}

export function TelegramSettingsTab({
  telegramToken, setTelegramToken,
  handleBlurSave,
  telegramBotLink, setTelegramBotLink,
  telegramProviderToken, setTelegramProviderToken,
  telegramPaymentCurrency, setTelegramPaymentCurrency,
  handleSaveConfig,
  telegramShopButtonName, setTelegramShopButtonName,
  telegramPaymentEnabled, setTelegramPaymentEnabled,
  telegramSearchCost, setTelegramSearchCost,
  telegramSearchCostReal, setTelegramSearchCostReal,
  telegramConnectionPaymentEnabled, setTelegramConnectionPaymentEnabled,
  telegramConnectionCost, setTelegramConnectionCost,
  telegramConnectionCostReal, setTelegramConnectionCostReal,
  telegramCustomLinks,
  handleCustomLinkChange,
  handleSaveCustomLinks,
  handleRemoveCustomLink,
  handleAddCustomLink,
  telegramWelcomeImageUrl, setTelegramWelcomeImageUrl,
  telegramWelcome, setTelegramWelcome,
  handleInsertVariable,
  telegramConnectionInfoMessage, setTelegramConnectionInfoMessage,
  appUrl,
  isSettingWebhook, handleSetWebhook,
  webhookLog, copyToClipboard,
}: TelegramSettingsTabProps) {
  return (
    <Card className="bg-secondary">
      <CardHeader>
        <CardTitle>Настройки Telegram Бота</CardTitle>
        <CardDescription>Управление подключением и поведением вашего Telegram бота.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="telegram-token">Токен Telegram Бота</Label>
            <Input id="telegram-token" placeholder="123456:ABC-DEF1234..." value={telegramToken} onChange={(e) => setTelegramToken(e.target.value)} onBlur={() => handleBlurSave(telegramToken, 'TELEGRAM_TOKEN')} className="bg-card" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="telegram-bot-link">Ссылка на бота (для групп)</Label>
            <Input id="telegram-bot-link" placeholder="https://t.me/YourBotName" value={telegramBotLink} onChange={(e) => setTelegramBotLink(e.target.value)} onBlur={() => handleBlurSave(telegramBotLink, 'TELEGRAM_BOT_LINK')} className="bg-card" />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-2">
              <Label htmlFor="telegram-provider-token">Токен провайдера платежей</Label>
              <Input id="telegram-provider-token" placeholder="Live или Test токен от @BotFather" value={telegramProviderToken} onChange={(e) => setTelegramProviderToken(e.target.value)} onBlur={() => handleBlurSave(telegramProviderToken, 'TELEGRAM_PROVIDER_TOKEN')} className="bg-card" />
              <p className="text-xs text-muted-foreground">Для приема платежей в Stars оставьте пустым.</p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="payment-currency">Валюта платежей</Label>
              <Select value={telegramPaymentCurrency} onValueChange={(value) => { setTelegramPaymentCurrency(value); handleSaveConfig({ TELEGRAM_PAYMENT_CURRENCY: value }); }} disabled={!telegramProviderToken}>
                <SelectTrigger className="bg-card">
                  <SelectValue placeholder="Выберите валюту" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="RUB">RUB (Российский рубль)</SelectItem>
                  <SelectItem value="UAH">UAH (Украинская гривна)</SelectItem>
                  <SelectItem value="USD">USD (Доллар США)</SelectItem>
                  <SelectItem value="EUR">EUR (Евро)</SelectItem>
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">Используется, если указан токен провайдера.</p>
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="telegram-shop-button-name">Название кнопки магазина</Label>
            <Input id="telegram-shop-button-name" placeholder="Магазин" value={telegramShopButtonName} onChange={(e) => setTelegramShopButtonName(e.target.value)} onBlur={() => handleBlurSave(telegramShopButtonName, 'TELEGRAM_SHOP_BUTTON_NAME')} className="bg-card" />
          </div>

          <div className="p-4 bg-card rounded-lg border space-y-4">
            <div className="flex flex-row items-center justify-between">
              <div className="space-y-0.5">
                <Label className="text-base">Платный поиск</Label>
                <CardDescription>
                  Если включено, бот будет требовать оплату за каждый поиск.
                </CardDescription>
              </div>
              <Switch
                checked={telegramPaymentEnabled}
                onCheckedChange={(checked) => { setTelegramPaymentEnabled(checked); handleSaveConfig({ TELEGRAM_PAYMENT_ENABLED: checked }); }}
              />
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="telegram-search-cost">Стоимость в Telegram Stars</Label>
                <Input id="telegram-search-cost" type="number" min="1" placeholder="1" value={telegramSearchCost} onChange={(e) => setTelegramSearchCost(Number(e.target.value))} onBlur={(e) => handleBlurSave(e.target.value, 'TELEGRAM_SEARCH_COST_STARS')} className="bg-background" disabled={!telegramPaymentEnabled} />
              </div>
              <div className="space-y-2">
                <Label htmlFor="telegram-search-cost-real">Стоимость в {telegramPaymentCurrency}</Label>
                <Input id="telegram-search-cost-real" type="number" min="1" placeholder="10" value={telegramSearchCostReal} onChange={(e) => setTelegramSearchCostReal(Number(e.target.value))} onBlur={(e) => handleBlurSave(e.target.value, 'TELEGRAM_SEARCH_COST_REAL')} className="bg-background" disabled={!telegramPaymentEnabled || !telegramProviderToken} />
              </div>
            </div>
          </div>

          <div className="p-4 bg-card rounded-lg border space-y-4">
            <div className="flex flex-row items-center justify-between">
              <div className="space-y-0.5">
                <Label className="text-base">Платное установление связи</Label>
                <CardDescription>
                  Если включено, бот будет требовать оплату за связь.
                </CardDescription>
              </div>
              <Switch
                checked={telegramConnectionPaymentEnabled}
                onCheckedChange={(checked) => { setTelegramConnectionPaymentEnabled(checked); handleSaveConfig({ TELEGRAM_CONNECTION_PAYMENT_ENABLED: checked }); }}
              />
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="telegram-connection-cost">Стоимость в Telegram Stars</Label>
                <Input id="telegram-connection-cost" type="number" min="1" placeholder="5" value={telegramConnectionCost} onChange={(e) => setTelegramConnectionCost(Number(e.target.value))} onBlur={(e) => handleBlurSave(e.target.value, 'TELEGRAM_CONNECTION_COST_STARS')} className="bg-background" disabled={!telegramConnectionPaymentEnabled} />
              </div>
              <div className="space-y-2">
                <Label htmlFor="telegram-connection-cost-real">Стоимость в {telegramPaymentCurrency}</Label>
                <Input id="telegram-connection-cost-real" type="number" min="1" placeholder="50" value={telegramConnectionCostReal} onChange={(e) => setTelegramConnectionCostReal(Number(e.target.value))} onBlur={(e) => handleBlurSave(e.target.value, 'TELEGRAM_CONNECTION_COST_REAL')} className="bg-background" disabled={!telegramConnectionPaymentEnabled || !telegramProviderToken} />
              </div>
            </div>
          </div>

          <Separator />

          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <Label className="text-base">Кнопки-ссылки в главном меню</Label>
              <Button variant="outline" onClick={handleSaveCustomLinks}>
                <Save className="mr-2 h-4 w-4" />
                Сохранить ссылки
              </Button>
            </div>
            {telegramCustomLinks.map((link, index) => (
              <div key={index} className="flex items-end gap-2 p-3 bg-card rounded-lg border">
                <div className="flex-grow space-y-2">
                  <Label htmlFor={`link-text-${index}`}>Текст кнопки</Label>
                  <Input id={`link-text-${index}`} value={link.text} onChange={(e) => handleCustomLinkChange(index, 'text', e.target.value)} placeholder="Наш чат" />
                </div>
                <div className="flex-grow space-y-2">
                  <Label htmlFor={`link-url-${index}`}>URL-адрес</Label>
                  <Input id={`link-url-${index}`} value={link.url} onChange={(e) => handleCustomLinkChange(index, 'url', e.target.value)} placeholder="https://t.me/your_chat" />
                </div>
                <div className="flex flex-col items-center space-y-1">
                  <Label htmlFor={`link-show-${index}`} className="text-xs">В группах</Label>
                  <Switch id={`link-show-${index}`} checked={link.showInGroups} onCheckedChange={(checked) => handleCustomLinkChange(index, 'showInGroups', checked)} />
                </div>
                <Button variant="destructive" size="icon" onClick={() => handleRemoveCustomLink(index)}>
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            ))}
            <Button variant="outline" onClick={handleAddCustomLink}>
              <PlusCircle className="mr-2 h-4 w-4" /> Добавить кнопку
            </Button>
          </div>

          <Separator />

          <div className="space-y-2">
            <Label htmlFor="telegram-welcome-image">URL картинки для приветствия</Label>
            <Input id="telegram-welcome-image" placeholder="https://example.com/image.png" value={telegramWelcomeImageUrl} onChange={(e) => setTelegramWelcomeImageUrl(e.target.value)} onBlur={() => handleBlurSave(telegramWelcomeImageUrl, 'TELEGRAM_WELCOME_IMAGE_URL')} className="bg-card" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="telegram-welcome">Приветственное сообщение</Label>
            <Textarea id="telegram-welcome" placeholder="Введите приветствие..." value={telegramWelcome} onChange={(e) => setTelegramWelcome(e.target.value)} onBlur={() => handleBlurSave(telegramWelcome, 'TELEGRAM_WELCOME_MESSAGE')} className="bg-card h-24" />
            <div className="flex gap-2 flex-wrap">
              <Button variant="outline" size="sm" onClick={() => handleInsertVariable('{user_count}', 'telegram-welcome')}>&#123;user_count&#125;</Button>
              <Button variant="outline" size="sm" onClick={() => handleInsertVariable('{bot_user_count}', 'telegram-welcome')}>&#123;bot_user_count&#125;</Button>
            </div>
            <p className="text-xs text-muted-foreground">Это сообщение увидит пользователь при старте бота. Вы можете использовать переменные.</p>
          </div>
          <div className="space-y-2">
            <Label htmlFor="telegram-connection-info">Информационное сообщение для "Установить связь"</Label>
            <Textarea id="telegram-connection-info" value={telegramConnectionInfoMessage} onChange={(e) => setTelegramConnectionInfoMessage(e.target.value)} onBlur={() => handleBlurSave(telegramConnectionInfoMessage, 'TELEGRAM_CONNECTION_INFO_MESSAGE')} className="bg-card h-48" />
            <p className="text-xs text-muted-foreground">Это сообщение будет показано пользователю, когда он нажмет "Установить связь".</p>
          </div>
          <Separator />
          <div className="space-y-2">
            <Label htmlFor="app-url">Публичный URL приложения (для Webhook)</Label>
            <Input id="app-url" placeholder="https://your-app.com" value={appUrl} readOnly disabled className="bg-card cursor-not-allowed" />
            <p className="text-xs text-muted-foreground">Этот URL определяется из переменной окружения `NEXT_PUBLIC_APP_URL`.</p>
          </div>
          <Button onClick={handleSetWebhook} variant="outline" disabled={isSettingWebhook || !telegramToken}>
            {isSettingWebhook ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Plug className="mr-2 h-4 w-4" />}
            Подключить бота (установить Webhook)
          </Button>
          {webhookLog && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label htmlFor="webhook-log">Лог подключения</Label>
                <Button variant="ghost" size="icon" onClick={() => copyToClipboard(webhookLog)} title="Скопировать лог">
                  <Copy className="h-4 w-4" />
                </Button>
              </div>
              <Textarea
                id="webhook-log"
                readOnly
                value={webhookLog}
                className="bg-card h-24 text-xs"
                placeholder="Здесь будет результат подключения..."
              />
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}