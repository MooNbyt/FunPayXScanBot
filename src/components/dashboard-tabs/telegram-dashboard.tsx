
"use client";

import React from "react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Users,
  UserCheck,
  UserX,
  UserPlus,
  Users2,
  ChevronLeft,
  ChevronRight,
  Search,
  Ban,
} from "lucide-react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "../ui/input";
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from "@/components/ui/alert-dialog";
import type { TelegramStats, BotUser } from "@/types";

interface TelegramDashboardProps {
  stats: TelegramStats;
  activeConnections: number;
  botUsers: BotUser[];
  botUsersPage: number;
  botUsersTotalPages: number;
  onPageChange: (page: number) => void;
  searchQuery: string;
  setSearchQuery: (query: string) => void;
  handleSearch: () => void;
  handleToggleUserBan: (chatId: number, currentStatus: 'active' | 'banned') => void;
}

const StatCard = ({
  title,
  value,
  icon,
  description,
}: {
  title: string;
  value: number;
  icon: React.ReactNode;
  description: string;
}) => (
  <Card className="bg-secondary">
    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
      <CardTitle className="text-sm font-medium">{title}</CardTitle>
      {icon}
    </CardHeader>
    <CardContent>
      <div className="text-2xl font-bold">{value.toLocaleString()}</div>
      <p className="text-xs text-muted-foreground">{description}</p>
    </CardContent>
  </Card>
);

export function TelegramDashboard({
  stats,
  activeConnections,
  botUsers,
  botUsersPage,
  botUsersTotalPages,
  onPageChange,
  searchQuery,
  setSearchQuery,
  handleSearch,
  handleToggleUserBan,
}: TelegramDashboardProps) {

  const formatUserDate = (dateString: string) => {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleString('ru-RU');
  }

  return (
    <div className="space-y-6">
      <Card className="bg-secondary">
        <CardHeader>
          <CardTitle>Сводка по пользователям Telegram</CardTitle>
          <CardDescription>
            Статистика по пользователям, которые взаимодействовали с вашим ботом.
          </CardDescription>
        </CardHeader>
      </Card>
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-5">
        <StatCard
          title="Всего пользователей"
          value={stats.totalUsers}
          icon={<Users className="h-4 w-4 text-muted-foreground" />}
          description="Общее количество уникальных пользователей."
        />
        <StatCard
          title="Активные пользователи"
          value={stats.activeUsers}
          icon={<UserCheck className="h-4 w-4 text-muted-foreground" />}
          description="Пользователи, которые не заблокировали бота."
        />
        <StatCard
          title="Заблокировали бота"
          value={stats.blockedUsers}
          icon={<UserX className="h-4 w-4 text-muted-foreground" />}
          description="Пользователи, остановившие бота."
        />
        <StatCard
          title="Новые за сегодня"
          value={stats.newToday}
          icon={<UserPlus className="h-4 w-4 text-muted-foreground" />}
          description="Присоединились за последние 24 часа."
        />
        <Card className="bg-secondary h-full">
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <CardDescription>Активные соединения</CardDescription>
            <Users2 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <CardTitle className="text-4xl font-mono">
              {activeConnections}
            </CardTitle>
            <p className="text-xs text-muted-foreground pt-1">пар</p>
          </CardContent>
        </Card>
      </div>

       <Card className="bg-secondary">
          <CardHeader>
             <CardTitle>Список пользователей бота</CardTitle>
             <CardDescription>Все пользователи, которые когда-либо запускали вашего бота.</CardDescription>
              <form onSubmit={(e) => { e.preventDefault(); handleSearch(); }} className="flex w-full max-w-sm items-center space-x-2 pt-4">
                <Input 
                    type="text" 
                    placeholder="Поиск по ID, username, имени..."
                    className="bg-card"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                />
                <Button type="submit"><Search className="h-4 w-4" /></Button>
            </form>
          </CardHeader>
          <CardContent>
             <ScrollArea className="h-96">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Chat ID</TableHead>
                    <TableHead>Username</TableHead>
                    <TableHead>Имя</TableHead>
                    <TableHead>Статус</TableHead>
                    <TableHead>Последний визит</TableHead>
                    <TableHead>Дата регистрации</TableHead>
                    <TableHead className="text-right">Действия</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {botUsers.length > 0 ? botUsers.map((user) => (
                    <TableRow key={user._id}>
                      <TableCell>{user.chatId}</TableCell>
                      <TableCell className="font-medium">
                        {user.username ? `@${user.username}` : 'N/A'}
                      </TableCell>
                       <TableCell>{user.firstName}</TableCell>
                      <TableCell>
                        <Badge variant={user.status === 'active' ? 'default' : 'destructive'} className={user.status === 'active' ? 'bg-green-600' : ''}>
                          {user.status === 'banned' ? 'Заблокирован' : user.status}
                        </Badge>
                      </TableCell>
                       <TableCell>{formatUserDate(user.lastSeen)}</TableCell>
                       <TableCell>{formatUserDate(user.joinedAt)}</TableCell>
                       <TableCell className="text-right">
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <Button variant="ghost" size="icon" title={user.status === 'banned' ? 'Разблокировать' : 'Заблокировать'}>
                                  <Ban className={`h-4 w-4 ${user.status === 'banned' ? 'text-green-400' : 'text-destructive'}`} />
                              </Button>
                            </AlertDialogTrigger>
                            <AlertDialogContent>
                                <AlertDialogHeader>
                                    <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                    <AlertDialogDescription>
                                        Вы действительно хотите {user.status === 'banned' ? 'разблокировать' : 'заблокировать'} пользователя {user.username || user.firstName}? 
                                        {user.status !== 'banned' && ' Он не сможет использовать бота.'}
                                    </AlertDialogDescription>
                                </AlertDialogHeader>
                                <AlertDialogFooter>
                                    <AlertDialogCancel>Отмена</AlertDialogCancel>
                                    <AlertDialogAction 
                                      onClick={() => handleToggleUserBan(user.chatId, user.status)}
                                      className={user.status === 'banned' ? '' : 'bg-destructive hover:bg-destructive/90'}
                                    >
                                      {user.status === 'banned' ? 'Разблокировать' : 'Заблокировать'}
                                    </AlertDialogAction>
                                </AlertDialogFooter>
                            </AlertDialogContent>
                          </AlertDialog>
                       </TableCell>
                    </TableRow>
                  )) : (
                    <TableRow>
                      <TableCell colSpan={7} className="text-center h-24 text-muted-foreground">
                       Пользователи бота не найдены.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </ScrollArea>
             {botUsersTotalPages > 1 && (
                <div className="flex items-center justify-center space-x-2 pt-4">
                    <Button
                    variant="outline"
                    size="sm"
                    onClick={() => onPageChange(botUsersPage - 1)}
                    disabled={botUsersPage <= 1}
                    >
                    <ChevronLeft className="h-4 w-4" />
                    Предыдущая
                    </Button>
                    <span className="text-sm text-muted-foreground">
                        Страница {botUsersPage} из {botUsersTotalPages}
                    </span>
                    <Button
                    variant="outline"
                    size="sm"
                    onClick={() => onPageChange(botUsersPage + 1)}
                    disabled={botUsersPage >= botUsersTotalPages}
                    >
                    Следующая
                    <ChevronRight className="h-4 w-4" />
                    </Button>
                </div>
             )}
          </CardContent>
        </Card>
    </div>
  );
}
