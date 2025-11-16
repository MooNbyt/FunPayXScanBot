
"use client";

import React from "react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
  CardFooter,
} from "@/components/ui/card";
import {
  Sheet, SheetTrigger, SheetContent, SheetHeader, SheetTitle, SheetDescription, SheetClose 
} from "@/components/ui/sheet";
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
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Send, MessageSquare, PlusCircle, Trash2, StopCircle, Play, Loader2 } from "lucide-react";
import type { Campaign, CampaignTemplate } from "@/types";

interface AdvertisingTabProps {
  currentCampaign: Partial<Campaign>;
  setCurrentCampaign: React.Dispatch<React.SetStateAction<Partial<Campaign>>>;
  campaigns: Campaign[];
  campaignTemplates: CampaignTemplate[];
  setCurrentTemplate: (template: Partial<CampaignTemplate> | null) => void;
  setIsTemplateDialogOpen: (isOpen: boolean) => void;
  handleInsertTemplate: (text: string) => void;
  handleDeleteTemplate: (id: string) => void;
  isCreatingCampaign: boolean;
  handleCreateCampaign: () => void;
  handleCampaignAction: (action: 'restart' | 'stop' | 'delete', campaignId: string) => void;
  isCampaignActionLoading: string | null;
}

export function AdvertisingTab({
  currentCampaign,
  setCurrentCampaign,
  campaigns,
  campaignTemplates,
  setCurrentTemplate,
  setIsTemplateDialogOpen,
  handleInsertTemplate,
  handleDeleteTemplate,
  isCreatingCampaign,
  handleCreateCampaign,
  handleCampaignAction,
  isCampaignActionLoading,
}: AdvertisingTabProps) {
  const getStatusVariant = (status: Campaign['status']) => {
    switch (status) {
        case 'sending': return 'default';
        case 'completed': return 'secondary';
        case 'error': return 'destructive';
        default: return 'outline';
    }
  }

  const isLifetimeEnabled = (currentCampaign.lifetimeHours ?? 0) > 0;

  const handleLifetimeToggle = (enabled: boolean) => {
    setCurrentCampaign(p => ({ ...p, lifetimeHours: enabled ? 24 : 0 }));
  }
  
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div className="space-y-6">
        <Card className="bg-secondary">
          <CardHeader>
            <CardTitle>Создание рассылки</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="campaign-name">Название рассылки (для вас)</Label>
              <Input
                id="campaign-name"
                placeholder="Например, Новогодние скидки"
                className="bg-card"
                value={currentCampaign.name || ''}
                onChange={(e) => setCurrentCampaign(p => ({ ...p, name: e.target.value }))}
              />
            </div>
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label htmlFor="campaign-text">Текст сообщения</Label>
                <Sheet>
                  <SheetTrigger asChild>
                    <Button variant="outline" size="sm">
                      <MessageSquare className="mr-2 h-4 w-4" />
                      Шаблоны
                    </Button>
                  </SheetTrigger>
                  <SheetContent>
                    <SheetHeader>
                      <SheetTitle>Шаблоны сообщений</SheetTitle>
                      <SheetDescription>
                        Вставляйте, создавайте и управляйте вашими шаблонами.
                      </SheetDescription>
                    </SheetHeader>
                    <div className="py-4 space-y-4">
                      <Button className="w-full" onClick={() => { setCurrentTemplate({}); setIsTemplateDialogOpen(true); }}>
                        <PlusCircle className="mr-2 h-4 w-4" />
                        Создать новый шаблон
                      </Button>
                      <ScrollArea className="h-[calc(100vh-12rem)]">
                        <div className="space-y-2 pr-4">
                          {campaignTemplates.map(template => (
                            <Card key={template.id} className="bg-card">
                              <CardHeader className="py-3 px-4">
                                <CardTitle className="text-base">{template.name}</CardTitle>
                              </CardHeader>
                              <CardContent className="py-3 px-4">
                                <p className="text-sm text-muted-foreground truncate">{template.text}</p>
                              </CardContent>
                              <CardFooter className="py-3 px-4 flex justify-end gap-2">
                                <SheetClose asChild>
                                  <Button variant="secondary" size="sm" onClick={() => handleInsertTemplate(template.text)}>Вставить</Button>
                                </SheetClose>
                                <Button variant="outline" size="sm" onClick={() => { setCurrentTemplate(template); setIsTemplateDialogOpen(true); }}>Редактировать</Button>
                                <AlertDialog>
                                  <AlertDialogTrigger asChild>
                                    <Button variant="destructive" size="sm">Удалить</Button>
                                  </AlertDialogTrigger>
                                  <AlertDialogContent>
                                    <AlertDialogHeader>
                                      <AlertDialogTitle>Удалить шаблон "{template.name}"?</AlertDialogTitle>
                                      <AlertDialogDescription>Это действие необратимо.</AlertDialogDescription>
                                    </AlertDialogHeader>
                                    <AlertDialogFooter>
                                      <AlertDialogCancel>Отмена</AlertDialogCancel>
                                      <AlertDialogAction onClick={() => handleDeleteTemplate(template.id)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                    </AlertDialogFooter>
                                  </AlertDialogContent>
                                </AlertDialog>
                              </CardFooter>
                            </Card>
                          ))}
                        </div>
                      </ScrollArea>
                    </div>
                  </SheetContent>
                </Sheet>
              </div>
              <Textarea
                id="campaign-text"
                placeholder="Введите текст вашего сообщения..."
                className="bg-card h-32"
                value={currentCampaign.text || ''}
                onChange={(e) => setCurrentCampaign(p => ({ ...p, text: e.target.value }))}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="campaign-image">URL изображения (необязательно)</Label>
              <Input
                id="campaign-image"
                placeholder="https://example.com/image.png"
                className="bg-card"
                value={currentCampaign.imageUrl || ''}
                onChange={(e) => setCurrentCampaign(p => ({ ...p, imageUrl: e.target.value }))}
              />
            </div>
            <div className="p-4 bg-card rounded-lg border space-y-4">
               <div className="flex flex-row items-center justify-between">
                <div className="space-y-0.5">
                  <Label className="text-base">Ограничить время жизни</Label>
                  <CardDescription>
                    Отправлять пост новым пользователям только в течение определенного времени.
                  </CardDescription>
                </div>
                <Switch
                  checked={isLifetimeEnabled}
                  onCheckedChange={handleLifetimeToggle}
                />
              </div>
              {isLifetimeEnabled && (
                <div className="space-y-2">
                    <Label htmlFor="campaign-lifetime">Время жизни поста для новых пользователей (в часах)</Label>
                    <Input
                        id="campaign-lifetime"
                        type="number"
                        placeholder="24"
                        className="bg-background"
                        value={currentCampaign.lifetimeHours || 0}
                        onChange={(e) => setCurrentCampaign(p => ({ ...p, lifetimeHours: Number(e.target.value) }))}
                    />
                </div>
              )}
            </div>
            <div className="flex gap-2 pt-2">
              <Button onClick={handleCreateCampaign} disabled={isCreatingCampaign}>
                {isCreatingCampaign ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Send className="mr-2 h-4 w-4" />}
                Запустить рассылку
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
      <div className="space-y-6">
        <Card className="bg-secondary">
          <CardHeader>
            <CardTitle>Список кампаний</CardTitle>
            <CardDescription>Отслеживайте и управляйте вашими рассылками.</CardDescription>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[420px]">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Название</TableHead>
                    <TableHead>Статус</TableHead>
                    <TableHead>Прогресс</TableHead>
                    <TableHead className="text-right">Действия</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {campaigns.length > 0 ? campaigns.map(c => (
                    <TableRow key={c._id}>
                      <TableCell className="font-medium">{c.name}</TableCell>
                      <TableCell><Badge variant={getStatusVariant(c.status)}>{c.status}</Badge></TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <span>{c.stats?.sent || 0} / {c.stats?.total || 0}</span>
                          {c.status === 'sending' && <Progress value={((c.stats?.sent || 0) / (c.stats?.total || 1)) * 100} className="h-2 w-16" />}
                        </div>
                      </TableCell>
                      <TableCell className="text-right">
                       {isCampaignActionLoading === c._id ? (
                           <Loader2 className="h-4 w-4 animate-spin ml-auto" />
                       ) : (
                        <div className="flex justify-end">
                            <Button variant="ghost" size="icon" onClick={() => handleCampaignAction('restart', c._id!)} title="Перезапустить">
                                <Play className="h-4 w-4 text-green-400" />
                            </Button>
                            <Button variant="ghost" size="icon" onClick={() => handleCampaignAction('stop', c._id!)} disabled={c.status !== 'sending'} title="Остановить">
                                <StopCircle className="h-4 w-4 text-yellow-400" />
                            </Button>
                            <AlertDialog>
                               <AlertDialogTrigger asChild>
                                    <Button variant="ghost" size="icon" title="Удалить"><Trash2 className="h-4 w-4 text-destructive" /></Button>
                               </AlertDialogTrigger>
                               <AlertDialogContent>
                                    <AlertDialogHeader>
                                        <AlertDialogTitle>Удалить кампанию "{c.name}"?</AlertDialogTitle>
                                        <AlertDialogDescription>Это действие необратимо.</AlertDialogDescription>
                                    </AlertDialogHeader>
                                    <AlertDialogFooter>
                                        <AlertDialogCancel>Отмена</AlertDialogCancel>
                                        <AlertDialogAction onClick={() => handleCampaignAction('delete', c._id!)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                    </AlertDialogFooter>
                               </AlertDialogContent>
                            </AlertDialog>
                        </div>
                       )}
                      </TableCell>
                    </TableRow>
                  )) : (
                    <TableRow>
                      <TableCell colSpan={4} className="h-24 text-center text-muted-foreground">
                        Рассылки еще не созданы.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </ScrollArea>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
