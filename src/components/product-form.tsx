
"use client";

import { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { Loader2, ArrowLeft } from "lucide-react";
import type { Product } from './funpay-worker-dashboard';

interface ProductFormProps {
  product: Partial<Product> | null;
  onSave: (product: Partial<Product>) => void;
  onCancel: () => void;
  isSaving: boolean;
  currency: string;
}

export function ProductForm({ product, onSave, onCancel, isSaving, currency }: ProductFormProps) {
  const [currentProduct, setCurrentProduct] = useState<Partial<Product> | null>(null);

  useEffect(() => {
    if (product) {
      setCurrentProduct(product);
    }
  }, [product]);

  if (!currentProduct) {
    return null;
  }

  const handleSave = () => {
    // Basic validation
    if (!currentProduct.buttonName || !currentProduct.invoiceTitle || !currentProduct.invoiceDescription) {
        alert("Пожалуйста, заполните все обязательные поля: Название кнопки, Заголовок счета, Описание счета.");
        return;
    }
    onSave(currentProduct);
  };

  return (
    <Card className="bg-secondary">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Button variant="outline" size="icon" onClick={onCancel}>
              <ArrowLeft className="h-4 w-4" />
            </Button>
            <div>
              <CardTitle>{currentProduct._id ? 'Редактировать товар' : 'Добавить новый товар'}</CardTitle>
              <CardDescription>Заполните информацию о товаре для продажи в боте.</CardDescription>
            </div>
          </div>
          <Button onClick={handleSave} disabled={isSaving}>
            {isSaving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Сохранить
          </Button>
        </div>
      </CardHeader>
      <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-6">
        <div className="space-y-6">
          <div className="space-y-2">
            <Label htmlFor="p-category">Категория</Label>
            <Input id="p-category" value={currentProduct.category || ''} onChange={(e) => setCurrentProduct(p => ({ ...p, category: e.target.value }))} placeholder="Например, Ключи" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="p-buttonName">Название кнопки</Label>
            <Input id="p-buttonName" value={currentProduct.buttonName || ''} onChange={(e) => setCurrentProduct(p => ({ ...p, buttonName: e.target.value }))} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="p-invoiceTitle">Заголовок счета</Label>
            <Input id="p-invoiceTitle" value={currentProduct.invoiceTitle || ''} onChange={(e) => setCurrentProduct(p => ({ ...p, invoiceTitle: e.target.value }))} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="p-invoiceDescription">Описание счета</Label>
            <Textarea id="p-invoiceDescription" value={currentProduct.invoiceDescription || ''} onChange={(e) => setCurrentProduct(p => ({ ...p, invoiceDescription: e.target.value }))} />
          </div>
           <div className="space-y-2">
            <Label htmlFor="p-productImageUrl">URL картинки для товара (необязательно)</Label>
            <Input id="p-productImageUrl" value={currentProduct.productImageUrl || ''} onChange={(e) => setCurrentProduct(p => ({ ...p, productImageUrl: e.target.value }))} placeholder="https://example.com/image.png" />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="p-price">Цена (в Telegram Stars)</Label>
              <Input id="p-price" type="number" min="1" value={currentProduct.price || 1} onChange={(e) => setCurrentProduct(p => ({ ...p, price: Number(e.target.value) }))} />
            </div>
             <div className="space-y-2">
              <Label htmlFor="p-priceReal">Цена (в {currency})</Label>
              <Input id="p-priceReal" type="number" min="1" value={currentProduct.priceReal || 10} onChange={(e) => setCurrentProduct(p => ({ ...p, priceReal: Number(e.target.value) }))} />
            </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="flex items-center space-x-2 pt-2">
            <Switch id="p-type" checked={currentProduct.type === 'api'} onCheckedChange={(checked) => setCurrentProduct(p => ({ ...p, type: checked ? 'api' : 'static' }))} />
            <Label htmlFor="p-type">Генерация через API</Label>
          </div>

          {currentProduct.type === 'api' ? (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="p-apiUrl">URL API</Label>
                <Input id="p-apiUrl" placeholder="https://example.com/api/generate" value={currentProduct.apiUrl || ''} onChange={(e) => setCurrentProduct(p => ({ ...p, apiUrl: e.target.value }))} />
              </div>
              <div className="space-y-2">
                <Label htmlFor="p-apiToken">API Token (Bearer)</Label>
                <Input id="p-apiToken" placeholder="Ваш токен" value={currentProduct.apiToken || ''} onChange={(e) => setCurrentProduct(p => ({ ...p, apiToken: e.target.value }))} />
              </div>
              <div className="space-y-2">
                <Label htmlFor="p-apiDays">Срок действия ключа (дни)</Label>
                <Input id="p-apiDays" type="number" min="1" value={currentProduct.apiDays || 30} onChange={(e) => setCurrentProduct(p => ({ ...p, apiDays: Number(e.target.value) }))} />
              </div>
            </div>
          ) : (
            <div className="space-y-2">
              <Label htmlFor="p-staticKey">Фиксированные ключи/текст (каждый с новой строки)</Label>
              <Textarea
                id="p-staticKey"
                placeholder="Введите по одному ключу в каждой строке..."
                value={currentProduct.staticKey || ''}
                onChange={(e) => setCurrentProduct(p => ({ ...p, staticKey: e.target.value }))}
                className="h-64 font-mono text-xs"
              />
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

    