"use client";

import React from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Search, Loader2 } from "lucide-react";
import type { Profile } from "@/types";

interface SearchTabProps {
  handleSearchSubmit: (e: React.FormEvent) => void;
  searchType: string;
  setSearchType: (type: string) => void;
  setDbSearchQuery: (query: string) => void;
  dbSearchQuery: string;
  isSearching: boolean;
  searchResults: Profile[];
  setSearchResults: (results: Profile[]) => void;
}

export function SearchTab({
  handleSearchSubmit,
  searchType,
  setSearchType,
  setDbSearchQuery,
  dbSearchQuery,
  isSearching,
  searchResults,
  setSearchResults,
}: SearchTabProps) {
  return (
    <Card className="bg-secondary">
      <CardHeader>
        <CardTitle>Поиск профилей</CardTitle>
        <CardDescription>Поиск пользователей в MongoDB по различным критериям.</CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSearchSubmit} className="flex gap-2 mb-4">
          <Select value={searchType} onValueChange={(value) => { setSearchType(value); setDbSearchQuery(''); setSearchResults([]); }}>
            <SelectTrigger className="w-[180px] bg-card">
              <SelectValue placeholder="Критерий поиска" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="nickname">По никнейму</SelectItem>
              <SelectItem value="id">По ID</SelectItem>
              <SelectItem value="status">По статусу</SelectItem>
            </SelectContent>
          </Select>
          {searchType === 'status' ? (
            <Select value={dbSearchQuery} onValueChange={setDbSearchQuery}>
              <SelectTrigger className="w-full bg-card">
                <SelectValue placeholder="Выберите статус..." />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="support">Поддержка</SelectItem>
                <SelectItem value="banned">Заблокирован</SelectItem>
              </SelectContent>
            </Select>
          ) : (
            <Input
              placeholder={
                searchType === 'id' ? 'Введите ID...' : 'Введите никнейм...'
              }
              className="bg-card"
              value={dbSearchQuery}
              onChange={(e) => setDbSearchQuery(e.target.value)}
            />
          )}
          <Button type="submit" disabled={isSearching}>
            {isSearching ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Search className="mr-2 h-4 w-4" />}
            Поиск
          </Button>
        </form>
        <ScrollArea className="h-96">
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
              {isSearching ? (
                <TableRow>
                  <TableCell colSpan={7} className="text-center h-24">
                    <Loader2 className="mx-auto h-8 w-8 animate-spin text-muted-foreground" />
                  </TableCell>
                </TableRow>
              ) : searchResults.length > 0 ? searchResults.map((profile) => (
                <TableRow key={profile._id || profile.id}>
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
                    {dbSearchQuery ? "Ничего не найдено" : "Введите запрос для начала поиска"}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}