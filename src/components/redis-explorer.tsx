
"use client";
import { useState, useEffect } from 'react';
import { useToast } from '@/hooks/use-toast';
import { DialogHeader, DialogTitle, DialogDescription } from './ui/dialog';
import { ScrollArea } from './ui/scroll-area';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';
import { Loader2, Trash2 } from 'lucide-react';
import { Button } from './ui/button';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from "@/components/ui/alert-dialog"

type RedisData = {
    keyValues: { [key: string]: string | object };
    queueSizes: { [key: string]: number };
};

export function RedisExplorer() {
    const [data, setData] = useState<RedisData | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isDeleting, setIsDeleting] = useState<string | null>(null);
    const [error, setError] = useState<string | null>(null);
    const { toast } = useToast();

    const fetchData = async () => {
        if (!isDeleting) setIsLoading(true);
        setError(null);
        try {
            const response = await fetch('/api/debug?db=redis');
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Не удалось загрузить данные');
            }
            const result = await response.json();
            setData(result);
        } catch (err: any) {
            setError(err.message);
            toast({
                variant: 'destructive',
                title: 'Ошибка',
                description: err.message,
            });
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
        const interval = setInterval(fetchData, 5000); // auto-refresh every 5 seconds
        return () => clearInterval(interval);
    }, []);

    const handleDelete = async (key: string) => {
        setIsDeleting(key);
        try {
            const response = await fetch(`/api/debug?db=redis&key=${key}`, {
                method: 'DELETE',
            });
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.error || 'Не удалось удалить ключ');
            }
            toast({
                title: 'Успех',
                description: result.message,
            });
            await fetchData(); // Refresh data after deletion
        } catch (err: any) {
            toast({
                variant: 'destructive',
                title: 'Ошибка удаления',
                description: err.message,
            });
        } finally {
            setIsDeleting(null);
        }
    };

    if (isLoading && !data) {
        return (
             <>
                <DialogHeader>
                    <DialogTitle>Обозреватель Redis</DialogTitle>
                    <DialogDescription>Загрузка данных...</DialogDescription>
                </DialogHeader>
                <div className="flex items-center justify-center h-64">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
            </>
        );
    }
    
    if (error) {
        return <div className="text-destructive text-center p-8">{error}</div>;
    }

    return (
        <>
            <DialogHeader>
                <DialogTitle>Обозреватель Redis</DialogTitle>
                <DialogDescription>
                    Просмотр ключевых значений и размеров очередей в Redis. Данные обновляются каждые 5 секунд.
                </DialogDescription>
            </DialogHeader>
            <div className="mt-4 h-[calc(90vh-100px)]">
                <ScrollArea className="h-full pr-4">
                    {data ? (
                        <div className="space-y-8">
                            <div>
                                <h3 className="text-lg font-semibold mb-3">Ключевые значения</h3>
                                <div className="border rounded-lg">
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            <TableHead className="w-1/3">Ключ</TableHead>
                                            <TableHead>Значение</TableHead>
                                            <TableHead className="text-right">Действия</TableHead>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {Object.entries(data.keyValues).map(([key, value]) => (
                                            <TableRow key={key}>
                                                <TableCell className="font-mono text-xs text-muted-foreground">{key}</TableCell>
                                                <TableCell className="font-mono text-sm break-all">
                                                    {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                                                </TableCell>
                                                <TableCell className="text-right">
                                                    <AlertDialog>
                                                        <AlertDialogTrigger asChild>
                                                            <Button variant="ghost" size="icon" disabled={isDeleting === key}>
                                                                {isDeleting === key ? <Loader2 className="h-4 w-4 animate-spin"/> : <Trash2 className="h-4 w-4 text-destructive"/>}
                                                            </Button>
                                                        </AlertDialogTrigger>
                                                        <AlertDialogContent>
                                                            <AlertDialogHeader>
                                                                <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                                                <AlertDialogDescription>
                                                                    Это действие необратимо. Ключ <span className="font-mono bg-muted p-1 rounded">{key}</span> будет удален.
                                                                </AlertDialogDescription>
                                                            </AlertDialogHeader>
                                                            <AlertDialogFooter>
                                                                <AlertDialogCancel>Отмена</AlertDialogCancel>
                                                                <AlertDialogAction onClick={() => handleDelete(key)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                                            </AlertDialogFooter>
                                                        </AlertDialogContent>
                                                    </AlertDialog>
                                                </TableCell>
                                            </TableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                                </div>
                            </div>
                            <div>
                                <h3 className="text-lg font-semibold mb-3">Размеры очередей</h3>
                                <div className="border rounded-lg">
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            <TableHead className="w-1/3">Очередь</TableHead>
                                            <TableHead>Размер</TableHead>
                                            <TableHead className="text-right">Действия</TableHead>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {Object.entries(data.queueSizes).map(([key, value]) => (
                                            <TableRow key={key}>
                                                <TableCell className="font-mono text-xs text-muted-foreground">{key}</TableCell>
                                                <TableCell className="font-mono text-sm font-bold">{value.toLocaleString()}</TableCell>
                                                <TableCell className="text-right">
                                                    <AlertDialog>
                                                        <AlertDialogTrigger asChild>
                                                            <Button variant="ghost" size="icon" disabled={isDeleting === key}>
                                                                {isDeleting === key ? <Loader2 className="h-4 w-4 animate-spin"/> : <Trash2 className="h-4 w-4 text-destructive"/>}
                                                            </Button>
                                                        </AlertDialogTrigger>
                                                        <AlertDialogContent>
                                                            <AlertDialogHeader>
                                                                <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                                                <AlertDialogDescription>
                                                                    Это действие необратимо. Очередь <span className="font-mono bg-muted p-1 rounded">{key}</span> будет полностью очищена.
                                                                </AlertDialogDescription>
                                                            </AlertDialogHeader>
                                                            <AlertDialogFooter>
                                                                <AlertDialogCancel>Отмена</AlertDialogCancel>
                                                                <AlertDialogAction onClick={() => handleDelete(key)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                                            </AlertDialogFooter>
                                                        </AlertDialogContent>
                                                    </AlertDialog>
                                                </TableCell>
                                            </TableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                                </div>
                            </div>
                        </div>
                    ) : (
                         <div className="flex items-center justify-center h-full text-muted-foreground">
                            <p>Нет данных для отображения.</p>
                        </div>
                    )}
                </ScrollArea>
            </div>
        </>
    );
}
