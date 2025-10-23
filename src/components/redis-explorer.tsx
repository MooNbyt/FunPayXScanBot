
"use client";
import { useState, useEffect } from 'react';
import { useToast } from '@/hooks/use-toast';
import { DialogHeader, DialogTitle, DialogDescription } from './ui/dialog';
import { ScrollArea } from './ui/scroll-area';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';
import { Loader2 } from 'lucide-react';

type RedisData = {
    keyValues: { [key: string]: string | object };
    queueSizes: { [key: string]: number };
};

export function RedisExplorer() {
    const [data, setData] = useState<RedisData | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const { toast } = useToast();

    useEffect(() => {
        const fetchData = async () => {
            setIsLoading(true);
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

        fetchData();
        const interval = setInterval(fetchData, 5000); // auto-refresh every 5 seconds
        return () => clearInterval(interval);

    }, [toast]);

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
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {Object.entries(data.keyValues).map(([key, value]) => (
                                            <TableRow key={key}>
                                                <TableCell className="font-mono text-xs text-muted-foreground">{key}</TableCell>
                                                <TableCell className="font-mono text-sm break-all">
                                                    {typeof value === 'object' ? JSON.stringify(value) : String(value)}
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
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {Object.entries(data.queueSizes).map(([key, value]) => (
                                            <TableRow key={key}>
                                                <TableCell className="font-mono text-xs text-muted-foreground">{key}</TableCell>
                                                <TableCell className="font-mono text-sm font-bold">{value.toLocaleString()}</TableCell>
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
