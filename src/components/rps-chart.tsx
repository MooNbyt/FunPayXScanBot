"use client";

import { useState, useEffect } from 'react';
import { Area, AreaChart, CartesianGrid, XAxis, Tooltip, YAxis } from 'recharts';
import { ChartContainer, ChartTooltipContent } from '@/components/ui/chart';
import { Loader2 } from 'lucide-react';
import type { RpsDataPoint } from '@/types';

export function RpsChart() {
    const [data, setData] = useState<RpsDataPoint[]>([]);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const response = await fetch('/api/telegram/stats?type=rps');
                if (response.ok) {
                    const result: RpsDataPoint[] = await response.json();
                    setData(result);
                }
            } catch (error) {
                console.error("Failed to fetch RPS data", error);
            } finally {
                setIsLoading(false);
            }
        };

        fetchData();
        const interval = setInterval(fetchData, 60000); // Refresh every minute

        return () => clearInterval(interval);
    }, []);

    if (isLoading) {
        return (
            <div className="flex items-center justify-center h-full">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
        );
    }
    
    if (data.length === 0) {
        return (
             <div className="flex items-center justify-center h-full text-muted-foreground">
                <p>Нет данных для отображения. Запустите скрейпер, чтобы собрать статистику.</p>
            </div>
        )
    }

    return (
        <ChartContainer config={{
            rps: {
                label: "RPS",
                color: "hsl(var(--chart-1))",
            },
        }} className="h-full w-full">
            <AreaChart
                accessibilityLayer
                data={data}
                margin={{
                    left: 12,
                    right: 12,
                    top: 10,
                }}
            >
                <CartesianGrid vertical={false} />
                <XAxis
                    dataKey="time"
                    tickLine={false}
                    axisLine={false}
                    tickMargin={8}
                    tickFormatter={(value) => new Date(value).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}
                />
                 <YAxis
                    tickLine={false}
                    axisLine={false}
                    tickMargin={8}
                    tickFormatter={(value) => `${value}`}
                 />
                <Tooltip
                    cursor={false}
                    content={<ChartTooltipContent
                        indicator="dot"
                        formatter={(value, name, item) => (
                          <>
                            <div className="flex min-w-[120px] items-center gap-2">
                                <div className="flex flex-1 flex-col gap-1">
                                <p className="text-sm text-muted-foreground">Скорость</p>
                                <p className="font-mono font-bold leading-none tracking-tighter">
                                    {typeof value === 'number' ? value.toFixed(2) : 'N/A'} зап/сек
                                </p>
                                </div>
                            </div>
                          </>
                        )}
                    />}
                />
                <Area
                    dataKey="rps"
                    type="natural"
                    fill="var(--color-rps)"
                    fillOpacity={0.4}
                    stroke="var(--color-rps)"
                    stackId="a"
                />
            </AreaChart>
        </ChartContainer>
    );
}
