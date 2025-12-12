

"use client";

import React from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Progress } from "@/components/ui/progress";
import {
  Play, StopCircle, Database, CheckCircle, AlertTriangle, Users, Ban, Search, Loader2, Server, Users2, HardDrive, Timer,
} from "lucide-react";
import { MongoExplorer } from "@/components/mongo-explorer";
import { RedisExplorer } from "@/components/redis-explorer";
import type { DbStatus, WorkerStatus, Profile } from "@/types";

const StatusCard = ({ title, status, icon, value, description, onClick, isClickable }: { title: string, status: DbStatus, icon: React.ReactNode, value?: string, description?: string, onClick?: () => void, isClickable?: boolean }) => {
  const statusConfig = {
    loading: { text: 'Проверка...', color: 'text-yellow-400' },
    connected: { text: 'Подключено', color: 'text-green-400' },
    error: { text: 'Ошибка', color: 'text-red-400' },
  };

  const currentStatus = statusConfig[status.status];

  return (
    <Card
      className={`bg-secondary h-full ${isClickable ? 'cursor-pointer hover:bg-accent transition-colors' : ''}`}
      onClick={isClickable ? onClick : undefined}
    >
      <CardHeader className="pb-2 flex flex-row items-center justify-between">
        <CardDescription>{title}</CardDescription>
        {icon}
      </CardHeader>
      <CardContent>
        <CardTitle className={`text-xl ${currentStatus.color}`}>{value || currentStatus.text}</CardTitle>
        <p className="text-xs text-muted-foreground pt-1">{status.memory ? `Занято: ${status.memory}` : (description || 'Статус подключения')}</p>
      </CardContent>
    </Card>
  );
};


interface MainDashboardProps {
  stats: {
    processed: number;
    successful: number;
    errors: number;
    support: number;
    banned: number;
    connectionRequests: number;
    activeConnections: number;
    totalUsersInDb: number;
    foundByWorker: number;
    workerStatuses: WorkerStatus[];
  };
  recentProfiles: Profile[];
  filteredProfiles: Profile[];
  listSearchQuery: string;
  setListSearchQuery: (query: string) => void;
  scraperRecentProfilesLimit: number;
  mongoStatus: DbStatus;
  redisStatus: DbStatus;
  isScraping: boolean;
  scraperWorkerId: string | null;
  isActionLoading: boolean;
  isWorkerManagerOpen: boolean;
  setIsWorkerManagerOpen: (isOpen: boolean) => void;
  handleScraperAction: (action: 'start' | 'stop', wId: string) => void;
  isWorkerActionLoading: string | null;
  isSettingsUnlocked: boolean;
  isRecounting: string | null;
  handleStatusCardClick: (status: 'support' | 'banned' | 'total' | 'foundByWorker') => void;
  workerId: string;
  setActiveTab: (tab: string) => void;
  setSearchType: (type: string) => void;
  setDbSearchQuery: (query: string) => void;
  scrapingTarget: number;
  eta: { days: number; hours: number; totalRps: number } | null;
  onRpsClick: () => void;
}

export function MainDashboard({
  stats,
  recentProfiles,
  filteredProfiles,
  listSearchQuery,
  setListSearchQuery,
  scraperRecentProfilesLimit,
  mongoStatus,
  redisStatus,
  isScraping,
  scraperWorkerId,
  isActionLoading,
  isWorkerManagerOpen,
  setIsWorkerManagerOpen,
  handleScraperAction,
  isWorkerActionLoading,
  isSettingsUnlocked,
  isRecounting,
  handleStatusCardClick,
  workerId,
  setActiveTab,
  setSearchType,
  setDbSearchQuery,
  scrapingTarget,
  eta,
  onRpsClick,
}: MainDashboardProps) {

  const runningWorkers = stats.workerStatuses.filter(w => w.status === 'running').length;
  const scrapingStatusText = isScraping ? `Работает (${scraperWorkerId})` : "Остановлен";
  const scrapingStatusColor = isScraping ? "text-green-400" : "text-yellow-400";


  return (
    <>
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4 mb-6">
        <Dialog open={isWorkerManagerOpen} onOpenChange={setIsWorkerManagerOpen}>
          <div
            className={`${isSettingsUnlocked ? 'cursor-pointer' : 'cursor-not-allowed'}`}
            onClick={() => {
              if (isSettingsUnlocked) {
                setIsWorkerManagerOpen(true);
              }
            }}
          >
            <Card className={`bg-secondary h-full ${isSettingsUnlocked ? 'hover:bg-accent transition-colors' : ''}`}>
              <CardHeader className="pb-2 flex flex-row items-center justify-between">
                <CardDescription>Статус проекта</CardDescription>
                <Server className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <CardTitle className={`text-xl ${scrapingStatusColor}`}>{scrapingStatusText}</CardTitle>
                <p className="text-sm text-muted-foreground pt-1">
                  Работает: <span className="font-bold text-green-400">{runningWorkers}</span>
                </p>
              </CardContent>
            </Card>
          </div>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Управление воркерами</DialogTitle>
              <DialogDescription>
                Запускайте и останавливайте скрейперы индивидуально.
              </DialogDescription>
            </DialogHeader>
            <ScrollArea className="max-h-[60vh]">
              <div className="space-y-4 pr-6">
                {stats.workerStatuses.map(worker => (
                  <div key={worker.id} className="flex items-center justify-between p-3 bg-card rounded-lg">
                    <div>
                      <p className="font-mono text-foreground">{worker.id}</p>
                      <Badge variant={worker.status === 'running' ? 'default' : 'secondary'} className={worker.status === 'running' ? 'bg-green-600' : ''}>
                        {worker.status}
                      </Badge>
                    </div>
                    <div className="flex items-center gap-2">
                      {isWorkerActionLoading === worker.id ? <Loader2 className="h-5 w-5 animate-spin" /> : (
                        <Button onClick={() => handleScraperAction(worker.status === 'running' ? 'stop' : 'start', worker.id)} size="sm">
                          {worker.status === 'running' ? <StopCircle className="mr-2 h-4 w-4" /> : <Play className="mr-2 h-4 w-4" />}
                          {worker.status === 'running' ? 'Стоп' : 'Старт'}
                        </Button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </ScrollArea>
          </DialogContent>
        </Dialog>

        <Dialog>
          <DialogTrigger asChild disabled={!isSettingsUnlocked}>
            <div className={!isSettingsUnlocked ? 'cursor-not-allowed' : ''}>
              <StatusCard title="MongoDB" status={mongoStatus} icon={<Database className="h-4 w-4 text-muted-foreground" />} isClickable={isSettingsUnlocked} />
            </div>
          </DialogTrigger>
          <DialogContent className="max-w-4xl h-[90vh]">
            <MongoExplorer />
          </DialogContent>
        </Dialog>

        <Dialog>
          <DialogTrigger asChild disabled={!isSettingsUnlocked}>
            <div className={!isSettingsUnlocked ? 'cursor-not-allowed' : ''}>
              <StatusCard title="Redis" status={redisStatus} icon={<svg className="h-4 w-4 text-muted-foreground" fill="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" width="24" height="24" ><path d="M12.384 1.76a.333.333 0 00-.309.133l-5.616 9.4a.333.333 0 00.288.503l-3.41-.004a.333.333 0 01.309.133l-5.616 9.4a.333.333 0 00.288.503h7.02a.333.333 0 00.309-.133l5.616-9.4a.333.333 0 00-.288-.503l-3.41.004a.333.333 0 01-.309-.133l5.616-9.4a.333.333 0 00-.288-.503h-7.02z" /></svg>} isClickable={isSettingsUnlocked} />
            </div>
          </DialogTrigger>
          <DialogContent className="max-w-4xl h-[90vh]">
            <RedisExplorer />
          </DialogContent>
        </Dialog>
        
        <Card className="bg-secondary h-full">
          <CardHeader className="pb-2">
            <CardDescription>Управление</CardDescription>
          </CardHeader>
          <CardContent className="flex gap-2">
            <Button onClick={() => handleScraperAction('start', workerId)} disabled={isScraping || isActionLoading || !isSettingsUnlocked} className="w-full">
              {isActionLoading && !isScraping ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Play className="mr-2 h-4 w-4" />}
              Вкл
            </Button>
            <Button onClick={() => handleScraperAction('stop', workerId)} variant="destructive" disabled={!isScraping || isActionLoading || !isSettingsUnlocked} className="w-full">
              {isActionLoading && isScraping ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <StopCircle className="mr-2 h-4 w-4" />}
              Выкл
            </Button>
          </CardContent>
        </Card>

        <div className="cursor-pointer h-full" onClick={() => handleStatusCardClick('total')}>
            <Card className="bg-secondary h-full hover:bg-accent transition-colors">
              <CardHeader className="pb-2 flex flex-row items-center justify-between">
                <CardDescription>Всего в БД</CardDescription>
                 {isRecounting === 'total' ? <Loader2 className="h-4 w-4 text-muted-foreground animate-spin" /> : <HardDrive className="h-4 w-4 text-muted-foreground" />}
              </CardHeader>
              <CardContent>
                <CardTitle className="text-4xl font-mono">{stats.totalUsersInDb.toLocaleString()}</CardTitle>
                <p className="text-xs text-muted-foreground pt-1">пользователей (нажмите для пересчета)</p>
              </CardContent>
            </Card>
        </div>
      </div>

       <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4 mb-6">
        <div className="cursor-pointer h-full" onClick={() => handleStatusCardClick('foundByWorker')}>
            <Card className="bg-secondary h-full hover:bg-accent transition-colors">
              <CardHeader className="pb-2 flex flex-row items-center justify-between">
                <CardDescription>Найдено воркером</CardDescription>
                 {isRecounting === 'foundByWorker' ? <Loader2 className="h-4 w-4 text-muted-foreground animate-spin" /> : <CheckCircle className="h-4 w-4 text-muted-foreground" />}
              </CardHeader>
              <CardContent>
                <CardTitle className="text-4xl font-mono">{stats.foundByWorker.toLocaleString()}</CardTitle>
                <p className="text-xs text-muted-foreground pt-1">этим воркером (нажмите для пересчета)</p>
              </CardContent>
            </Card>
        </div>
        <Card className="bg-secondary h-full">
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <CardDescription>Ошибки 404 подряд</CardDescription>
            <AlertTriangle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <CardTitle className="text-4xl font-mono">{stats.errors.toLocaleString()}</CardTitle>
            <p className="text-xs text-muted-foreground pt-1">не найденных профилей</p>
          </CardContent>
        </Card>
        <div className="cursor-pointer h-full" onClick={() => handleStatusCardClick('support')}>
          <Card className="bg-secondary h-full hover:bg-accent transition-colors">
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <CardDescription>Поддержка</CardDescription>
              {isRecounting === 'support' ? <Loader2 className="h-4 w-4 text-muted-foreground animate-spin" /> : <Users className="h-4 w-4 text-muted-foreground" />}
            </CardHeader>
            <CardContent>
              <CardTitle className="text-4xl font-mono">{stats.support.toLocaleString()}</CardTitle>
              <p className="text-xs text-muted-foreground pt-1">профилей в БД (нажмите для пересчета)</p>
            </CardContent>
          </Card>
        </div>
        <div className="cursor-pointer h-full" onClick={() => handleStatusCardClick('banned')}>
          <Card className="bg-secondary h-full hover:bg-accent transition-colors">
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <CardDescription>Бан</CardDescription>
              {isRecounting === 'banned' ? <Loader2 className="h-4 w-4 text-muted-foreground animate-spin" /> : <Ban className="h-4 w-4 text-muted-foreground" />}
            </CardHeader>
            <CardContent>
              <CardTitle className="text-4xl font-mono">{stats.banned.toLocaleString()}</CardTitle>
              <p className="text-xs text-muted-foreground pt-1">профилей в БД (нажмите для пересчета)</p>
            </CardContent>
          </Card>
        </div>
        
        <div className="cursor-pointer" onClick={onRpsClick}>
            <Card className="bg-secondary h-full hover:bg-accent transition-colors">
                <CardHeader className="pb-2 flex flex-row items-center justify-between">
                    <CardDescription>Прогноз до цели: {scrapingTarget.toLocaleString()}</CardDescription>
                    <Timer className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                    {eta && scrapingTarget > 0 ? (
                        <div>
                            <CardTitle className="text-2xl text-primary">{`≈ ${eta.days.toFixed(2)} дней`}</CardTitle>
                            <p className="text-xs text-muted-foreground pt-1">{`Общая скорость: ${eta.totalRps.toFixed(2)} зап/сек`}</p>
                        </div>
                    ) : (
                        <p className="text-sm text-muted-foreground">Введите цель в настройках для расчета.</p>
                    )}
                </CardContent>
            </Card>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-6">
        <Card className="bg-secondary">
          <CardHeader>
            <div className="flex justify-between items-center">
              <div>
                <CardTitle className="flex items-center">
                  Лог сессии
                  <Badge variant="secondary" className="ml-2">{filteredProfiles.length}</Badge>
                </CardTitle>
                <CardDescription>Список последних извлеченных профилей (макс. {scraperRecentProfilesLimit}).</CardDescription>
              </div>
            </div>
            <div className="relative w-full max-w-sm pt-4">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Поиск по никнейму в логе..."
                className="pl-10 bg-card"
                value={listSearchQuery}
                onChange={(e) => setListSearchQuery(e.target.value)}
              />
            </div>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-72">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ID</TableHead>
                    <TableHead>Никнейм</TableHead>
                    <TableHead>Статус</TableHead>
                    <TableHead>Отзывы</TableHead>
                    <TableHead>Оценка</TableHead>
                    <TableHead>Воркер</TableHead>
                    <TableHead>URL Профиля</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredProfiles.length > 0 ? filteredProfiles.map((profile) => (
                    <TableRow key={profile.id}>
                      <TableCell>{profile.id}</TableCell>
                      <TableCell className="font-medium">{profile.nickname}</TableCell>
                      <TableCell>
                        {profile.isSupport && <Badge className="bg-green-500 hover:bg-green-600 mr-2">Поддержка</Badge>}
                        {profile.isBanned && <Badge variant="destructive">Заблокирован</Badge>}
                        {!profile.isSupport && !profile.isBanned && <Badge className="bg-blue-500 hover:bg-blue-600">Пользователь</Badge>}
                      </TableCell>
                      <TableCell>{profile.reviewCount ?? 'N/A'}</TableCell>
                      <TableCell>{profile.rating ?? 'N/A'}</TableCell>
                      <TableCell><Badge variant="outline">{profile.scrapedBy || 'N/A'}</Badge></TableCell>
                      <TableCell><a href={`https://funpay.com/users/${profile.id}/`} target="_blank" rel="noopener noreferrer" className="text-sky-400 hover:underline">{`https://funpay.com/users/${profile.id}/`}</a></TableCell>
                    </TableRow>
                  )) : (
                    <TableRow>
                      <TableCell colSpan={7} className="text-center h-24 text-muted-foreground">
                        Нет данных для отображения.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </ScrollArea>
          </CardContent>
        </Card>
      </div>
    </>
  );
}
