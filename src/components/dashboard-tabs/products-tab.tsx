"use client";

import React from "react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Loader2, PlusCircle, Edit, Trash2, FolderPlus, Package, ArrowLeft, X } from "lucide-react";
import { ProductForm } from "@/components/product-form";
import type { Product } from "@/types";

type ProductView = 'list' | 'form';

interface ProductsTabProps {
  productView: ProductView;
  currentProduct: Partial<Product> | null;
  handleSaveProduct: (productData: Partial<Product>) => void;
  setProductView: (view: ProductView) => void;
  setCurrentProduct: (product: Partial<Product> | null) => void;
  isSavingProduct: boolean;
  telegramPaymentCurrency: string;
  currentCategoryView: string | null;
  setCurrentCategoryView: (category: string | null) => void;
  openProductForm: (category: string | null) => void;
  isAddingCategory: boolean;
  newCategory: string;
  setNewCategory: (category: string) => void;
  handleAddCategory: () => void;
  productCategories: string[];
  handleDeleteCategory: (categoryName: string) => void;
  products: Product[];
  handleDeleteProduct: (productId: string) => void;
}

export function ProductsTab({
  productView,
  currentProduct,
  handleSaveProduct,
  setProductView,
  setCurrentProduct,
  isSavingProduct,
  telegramPaymentCurrency,
  currentCategoryView,
setCurrentCategoryView,
  openProductForm,
  isAddingCategory,
  newCategory,
  setNewCategory,
  handleAddCategory,
  productCategories,
  handleDeleteCategory,
  products,
  handleDeleteProduct,
}: ProductsTabProps) {

  const productsWithoutCategory = products.filter(p => !p.category || !productCategories.includes(p.category));
  const productsInCategory = currentCategoryView ? products.filter(p => p.category === currentCategoryView) : [];

  if (productView === 'form') {
    return (
      <ProductForm
        product={currentProduct}
        onSave={handleSaveProduct}
        onCancel={() => {
          setProductView('list');
          setCurrentProduct(null);
        }}
        isSaving={isSavingProduct}
        currency={telegramPaymentCurrency}
      />
    );
  }

  return (
    <Card className="bg-secondary">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            {currentCategoryView !== null ? (
              <div className='flex items-center gap-3'>
                <Button variant="outline" size="icon" onClick={() => setCurrentCategoryView(null)}>
                  <ArrowLeft className="h-4 w-4" />
                </Button>
                <div>
                  <CardTitle>Категория: {currentCategoryView}</CardTitle>
                  <CardDescription>Товары в этой категории.</CardDescription>
                </div>
              </div>
            ) : (
              <div>
                <CardTitle>Управление товарами</CardTitle>
                <CardDescription>Создавайте категории и управляйте товарами.</CardDescription>
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            {currentCategoryView === null && (
              <Dialog>
                <DialogTrigger asChild>
                  <Button variant="outline">
                    <FolderPlus className="mr-2 h-4 w-4" />
                    Добавить категорию
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Новая категория</DialogTitle>
                    <DialogDescription>Введите название для новой категории товаров.</DialogDescription>
                  </DialogHeader>
                  <form onSubmit={(e) => { e.preventDefault(); handleAddCategory(); }} className="space-y-4 py-4">
                    <div className="space-y-2">
                      <Label htmlFor="category-name">Название категории</Label>
                      <Input id="category-name" value={newCategory} onChange={(e) => setNewCategory(e.target.value)} />
                    </div>
                    <DialogFooter>
                      <DialogTrigger asChild>
                        <Button type="button" variant="secondary">Отмена</Button>
                      </DialogTrigger>
                      <DialogTrigger asChild>
                        <Button type="submit" disabled={isAddingCategory}>
                          {isAddingCategory && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                          Сохранить
                        </Button>
                      </DialogTrigger>
                    </DialogFooter>
                  </form>
                </DialogContent>
              </Dialog>
            )}
            <Button onClick={() => openProductForm(currentCategoryView)}>
              <PlusCircle className="mr-2 h-4 w-4" />
              Добавить товар
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <ScrollArea className="h-[500px]">
          {currentCategoryView === null ? (
            <div>
              {productCategories.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-lg font-semibold mb-2">Категории</h3>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    {productCategories.map(cat => (
                      <div key={cat} className="relative group">
                        <Button variant="outline" onClick={() => setCurrentCategoryView(cat)} className="h-20 w-full flex-col gap-2">
                          <Package className="h-6 w-6" />
                          <span>{cat}</span>
                        </Button>
                        <AlertDialog>
                          <AlertDialogTrigger asChild>
                            <Button variant="destructive" size="icon" className="absolute top-1 right-1 h-6 w-6">
                              <X className="h-4 w-4" />
                            </Button>
                          </AlertDialogTrigger>
                          <AlertDialogContent>
                            <AlertDialogHeader>
                              <AlertDialogTitle>Удалить категорию "{cat}"?</AlertDialogTitle>
                              <AlertDialogDescription>Все товары в этой категории станут "без категории". Это действие необратимо.</AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Отмена</AlertDialogCancel>
                              <AlertDialogAction onClick={() => handleDeleteCategory(cat)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                            </AlertDialogFooter>
                          </AlertDialogContent>
                        </AlertDialog>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <Separator className="my-4" />

              <h3 className="text-lg font-semibold mb-2">Товары без категории</h3>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Название</TableHead>
                    <TableHead>Тип</TableHead>
                    <TableHead>Цена ({telegramPaymentCurrency})</TableHead>
                    <TableHead className="text-right">Действия</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {productsWithoutCategory.length > 0 ? productsWithoutCategory.map((product) => (
                    <TableRow key={product._id}>
                      <TableCell className="font-medium">{product.buttonName}</TableCell>
                      <TableCell><Badge variant="secondary">{product.type === 'api' ? 'API' : 'Фикс.'}</Badge></TableCell>
                      <TableCell>{product.priceReal}</TableCell>
                      <TableCell className="text-right">
                        <Button variant="ghost" size="icon" onClick={() => { setCurrentProduct(product); setProductView('form'); }}>
                          <Edit className="h-4 w-4" />
                        </Button>
                        <AlertDialog>
                          <AlertDialogTrigger asChild>
                            <Button variant="ghost" size="icon"><Trash2 className="h-4 w-4 text-destructive" /></Button>
                          </AlertDialogTrigger>
                          <AlertDialogContent>
                            <AlertDialogHeader>
                              <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                              <AlertDialogDescription>Это действие необратимо. Товар "{product.buttonName}" будет удален.</AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Отмена</AlertDialogCancel>
                              <AlertDialogAction onClick={() => handleDeleteProduct(product._id!)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                            </AlertDialogFooter>
                          </AlertDialogContent>
                        </AlertDialog>
                      </TableCell>
                    </TableRow>
                  )) : (
                    <TableRow>
                      <TableCell colSpan={4} className="h-24 text-center text-muted-foreground">
                        Нет товаров без категории.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>

            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Название</TableHead>
                  <TableHead>Тип</TableHead>
                  <TableHead>Цена ({telegramPaymentCurrency})</TableHead>
                  <TableHead className="text-right">Действия</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {productsInCategory.length > 0 ? productsInCategory.map((product) => (
                  <TableRow key={product._id}>
                    <TableCell className="font-medium">{product.buttonName}</TableCell>
                    <TableCell><Badge variant="secondary">{product.type === 'api' ? 'API' : 'Фикс.'}</Badge></TableCell>
                    <TableCell>{product.priceReal}</TableCell>
                    <TableCell className="text-right">
                      <Button variant="ghost" size="icon" onClick={() => { setCurrentProduct(product); setProductView('form'); }}>
                        <Edit className="h-4 w-4" />
                      </Button>
                      <AlertDialog>
                        <AlertDialogTrigger asChild>
                          <Button variant="ghost" size="icon"><Trash2 className="h-4 w-4 text-destructive" /></Button>
                        </AlertDialogTrigger>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                            <AlertDialogDescription>Это действие необратимо. Товар "{product.buttonName}" будет удален.</AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Отмена</AlertDialogCancel>
                            <AlertDialogAction onClick={() => handleDeleteProduct(product._id!)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                          </AlertDialogFooter>
                        </AlertDialogContent>
                      </AlertDialog>
                    </TableCell>
                  </TableRow>
                )) : (
                  <TableRow>
                    <TableCell colSpan={4} className="h-24 text-center text-muted-foreground">
                      В этой категории еще нет товаров.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          )}
        </ScrollArea>
      </CardContent>
    </Card>
  );
}
