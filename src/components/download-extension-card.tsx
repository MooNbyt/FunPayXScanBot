
'use client';

import { useState } from 'react';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
  CardFooter
} from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Download, Loader, Archive, GitBranch } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import * as api from '@/lib/api';

export function DownloadExtensionCard() {
    const [isDownloadingExtension, setIsDownloadingExtension] = useState(false);
    const [isDownloadingProject, setIsDownloadingProject] = useState(false);
    const { toast } = useToast();

    const handleDownload = async (type: 'extension' | 'project') => {
        const setIsDownloading = type === 'extension' ? setIsDownloadingExtension : setIsDownloadingProject;
        setIsDownloading(true);

        try {
            const base64 = type === 'extension' ? await api.downloadProject() : await api.downloadEntireProject();
            const link = document.createElement('a');
            link.href = `data:application/zip;base64,${base64}`;
            link.download = type === 'extension' ? 'extension.zip' : 'MooNTooLServerAPI.zip';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        } catch (error) {
            console.error("Download failed:", error);
            toast({
                variant: 'destructive',
                title: 'Ошибка скачивания',
                description: 'Не удалось создать архив. Посмотрите консоль для деталей.',
            });
        } finally {
            setIsDownloading(false);
        }
    };
    
    return (
        <Card>
            <CardHeader>
                <CardTitle>Управление проектом</CardTitle>
                <CardDescription>Скачайте исходный код.</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
                <Button onClick={() => handleDownload('project')} disabled={isDownloadingProject || isDownloadingExtension} variant="outline" className="w-full">
                    {isDownloadingProject ? (
                        <Loader className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                        <Archive className="mr-2 h-4 w-4" />
                    )}
                    {isDownloadingProject ? 'Архивация...' : 'Скачать весь проект (.zip)'}
                </Button>
            </CardContent>
        </Card>
    );
}
