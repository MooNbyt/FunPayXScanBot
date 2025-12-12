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
import { ScrollArea } from "@/components/ui/scroll-area";
import { Copy, Trash2 } from "lucide-react";
import type { TelegramLog, ProjectLog } from "@/types";

interface LogsTabsProps {
    activeTab: "telegram-logs" | "project-logs";
    telegramLogsLimit: number;
    isSettingsUnlocked: boolean;
    handleClearTelegramLogs: () => void;
    telegramLogs: TelegramLog[];
    projectLogs: ProjectLog[];
    criticalLogs: ProjectLog[];
    copyToClipboard: (text: string) => void;
    handleClearProjectLogs: (scope: 'all' | 'critical' | 'regular') => void;
    getLogClass: (message: string) => string;
}

export function LogsTabs({
    activeTab,
    telegramLogsLimit,
    isSettingsUnlocked,
    handleClearTelegramLogs,
    telegramLogs,
    projectLogs,
    criticalLogs,
    copyToClipboard,
    handleClearProjectLogs,
    getLogClass,
}: LogsTabsProps) {

    if (activeTab === "telegram-logs") {
        return (
            <Card className="bg-secondary">
                <CardHeader>
                    <div className="flex items-center justify-between">
                        <div>
                            <CardTitle>Логи вебхуков Telegram</CardTitle>
                            <CardDescription>Здесь отображаются последние {telegramLogsLimit} входящих запросов от Telegram.</CardDescription>
                        </div>
                         <AlertDialog>
                            <AlertDialogTrigger asChild>
                                <Button variant="destructive" disabled={!isSettingsUnlocked}>
                                    <Trash2 className="mr-2 h-4 w-4" />
                                    Очистить логи
                                </Button>
                            </AlertDialogTrigger>
                            <AlertDialogContent>
                            <AlertDialogHeader>
                                <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                <AlertDialogDescription>
                                Это действие необратимо. Все логи вебхуков Telegram будут удалены.
                                </AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                                <AlertDialogCancel>Отмена</AlertDialogCancel>
                                <AlertDialogAction onClick={handleClearTelegramLogs} className="bg-destructive hover:bg-destructive/90">
                                Да, очистить
                                </AlertDialogAction>
                            </AlertDialogFooter>
                            </AlertDialogContent>
                        </AlertDialog>
                    </div>
                </CardHeader>
                <CardContent>
                    <ScrollArea className="h-[500px] w-full bg-card rounded-md border p-4">
                       {telegramLogs.length > 0 ? (
                            telegramLogs.map((log, index) => (
                                <div key={index} className="mb-4 pb-4 border-b border-border last:border-b-0 last:mb-0 last:pb-0">
                                    <p className="text-sm text-muted-foreground mb-2">
                                        {new Date(log.timestamp).toLocaleString()}
                                    </p>
                                    <pre className="text-xs whitespace-pre-wrap break-all bg-background p-3 rounded-md">
                                        {JSON.stringify(log.payload, null, 2)}
                                    </pre>
                                </div>
                            ))
                       ) : (
                           <div className="flex items-center justify-center h-full text-muted-foreground">
                               <p>Ожидание входящих запросов от Telegram...</p>
                           </div>
                       )}
                    </ScrollArea>
                </CardContent>
            </Card>
        )
    }

    if (activeTab === "project-logs") {
        return (
            <Card className="bg-secondary">
              <CardHeader>
                  <div>
                    <CardTitle>Логи проекта</CardTitle>
                    <CardDescription>Системные события, включая запуск, ошибки и действия скрейпера.</CardDescription>
                  </div>
              </CardHeader>
              <CardContent className="space-y-6">
                  <div>
                      <div className="flex items-center justify-between mb-2">
                          <h3 className="text-lg font-semibold text-red-400">Критические ошибки</h3>
                           <div className="flex items-center gap-2">
                            <Button variant="ghost" size="icon" onClick={() => copyToClipboard(criticalLogs.map(l => `${new Date(l.timestamp).toLocaleString()} - ${l.message}`).join('\n'))}>
                              <Copy className="h-4 w-4" />
                            </Button>
                            <AlertDialog>
                                <AlertDialogTrigger asChild>
                                    <Button variant="ghost" size="icon" className="text-destructive hover:text-destructive">
                                        <Trash2 className="h-4 w-4" />
                                    </Button>
                                </AlertDialogTrigger>
                                <AlertDialogContent>
                                <AlertDialogHeader>
                                    <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                    <AlertDialogDescription>
                                    Это действие необратимо. Все критические ошибки будут удалены.
                                    </AlertDialogDescription>
                                </AlertDialogHeader>
                                <AlertDialogFooter>
                                    <AlertDialogCancel>Отмена</AlertDialogCancel>
                                    <AlertDialogAction onClick={() => handleClearProjectLogs('critical')} className="bg-destructive hover:bg-destructive/90">
                                    Удалить
                                    </AlertDialogAction>
                                </AlertDialogFooter>
                                </AlertDialogContent>
                            </AlertDialog>
                          </div>
                      </div>
                      <ScrollArea className="h-64 w-full bg-card rounded-md border p-4 font-mono text-xs">
                         {criticalLogs.length > 0 ? (
                              criticalLogs.map((log, index) => (
                                  <div key={`crit-${index}`} className="flex gap-4 mb-1 last:mb-0">
                                      <p className="shrink-0 text-muted-foreground">
                                          {new Date(log.timestamp).toLocaleTimeString()}
                                      </p>
                                      <p className={`whitespace-pre-wrap break-all text-red-400`}>
                                          {log.message}
                                      </p>
                                  </div>
                              ))
                         ) : (
                             <div className="flex items-center justify-center h-full text-muted-foreground">
                                 <p>Критических ошибок нет.</p>
                             </div>
                         )}
                      </ScrollArea>
                  </div>
                   <div>
                      <div className="flex items-center justify-between mb-2">
                          <h3 className="text-lg font-semibold">Текущие логи</h3>
                          <div className="flex items-center gap-2">
                              <Button variant="ghost" size="icon" onClick={() => copyToClipboard(projectLogs.map(l => `${new Date(l.timestamp).toLocaleString()} - ${l.message}`).join('\n'))}>
                                  <Copy className="h-4 w-4" />
                              </Button>
                               <AlertDialog>
                                <AlertDialogTrigger asChild>
                                    <Button variant="ghost" size="icon" className="text-destructive hover:text-destructive">
                                        <Trash2 className="h-4 w-4" />
                                    </Button>
                                </AlertDialogTrigger>
                                <AlertDialogContent>
                                <AlertDialogHeader>
                                    <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                    <AlertDialogDescription>
                                    Это действие необратимо. Все текущие (не критические) логи будут удалены.
                                    </AlertDialogDescription>
                                </AlertDialogHeader>
                                <AlertDialogFooter>
                                    <AlertDialogCancel>Отмена</AlertDialogCancel>
                                    <AlertDialogAction onClick={() => handleClearProjectLogs('regular')} className="bg-destructive hover:bg-destructive/90">
                                    Удалить
                                    </AlertDialogAction>
                                </AlertDialogFooter>
                                </AlertDialogContent>
                            </AlertDialog>
                          </div>
                      </div>
                      <ScrollArea className="h-96 w-full bg-card rounded-md border p-4 font-mono text-xs">
                         {projectLogs.length > 0 ? (
                              projectLogs.map((log, index) => (
                                  <div key={`proj-${index}`} className="flex gap-4 mb-1 last:mb-0">
                                      <p className="shrink-0 text-muted-foreground">
                                          {new Date(log.timestamp).toLocaleTimeString()}
                                      </p>
                                      <p className={`whitespace-pre-wrap break-all ${getLogClass(log.message)}`}>
                                          {log.message}
                                      </p>
                                  </div>
                              ))
                         ) : (
                             <div className="flex items-center justify-center h-full text-muted-foreground">
                                 <p>Ожидание системных событий...</p>
                             </div>
                         )}
                      </ScrollArea>
                  </div>
              </CardContent>
          </Card>
        );
    }

    return null;
}