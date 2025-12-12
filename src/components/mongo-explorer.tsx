
"use client";
import { useState, useEffect } from 'react';
import { useToast } from '@/hooks/use-toast';
import { DialogHeader, DialogTitle, DialogDescription, Dialog, DialogTrigger, DialogContent, DialogFooter } from './ui/dialog';
import { ScrollArea } from './ui/scroll-area';
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from './ui/accordion';
import { Loader2, ChevronLeft, ChevronRight, PenTool, Copy, Trash2, CopyPlus, FilePenLine } from 'lucide-react';
import { Badge } from './ui/badge';
import { Button } from './ui/button';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from "@/components/ui/alert-dialog"
import { Table, TableBody, TableCell, TableRow } from './ui/table';
import { Textarea } from './ui/textarea';
import { Input } from './ui/input';

type CollectionData = {
    count: number;
    sample: any[];
    pages: number;
};

type ConnectionInfo = {
    host: string;
    dbName: string;
    version: string;
    storageSize: number;
}

type AllCollectionsData = {
    collections: {
        [collectionName: string]: CollectionData;
    };
    connectionInfo: ConnectionInfo;
};

type CollectionState = {
    [collectionName: string]: {
        page: number;
        jumpToPageInput: string;
    }
};

type EditorState = {
    isOpen: boolean;
    isBulk: boolean;
    collectionName: string;
    content: string;
    docId?: string;
};

const Pagination = ({ currentPage, totalPages, onPageChange }: { currentPage: number, totalPages: number, onPageChange: (page: number) => void }) => {
    const [jumpToPage, setJumpToPage] = useState(currentPage.toString());

    useEffect(() => {
        setJumpToPage(currentPage.toString());
    }, [currentPage]);

    const handleJump = (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();
        const pageNumber = parseInt(jumpToPage, 10);
        if (pageNumber >= 1 && pageNumber <= totalPages) {
            onPageChange(pageNumber);
        }
    };
    
    const getPaginationRange = () => {
        const delta = 2;
        const left = currentPage - delta;
        const right = currentPage + delta + 1;
        const range = [];
        const rangeWithDots = [];
        let l: number | undefined;

        for (let i = 1; i <= totalPages; i++) {
            if (i === 1 || i === totalPages || (i >= left && i < right)) {
                range.push(i);
            }
        }

        for (const i of range) {
            if (l) {
                if (i - l === 2) {
                    rangeWithDots.push(l + 1);
                } else if (i - l !== 1) {
                    rangeWithDots.push('...');
                }
            }
            rangeWithDots.push(i);
            l = i;
        }

        return rangeWithDots;
    };

    return (
        <div className="flex items-center justify-center gap-2 pt-4">
             <Button variant="outline" size="icon" onClick={() => onPageChange(currentPage - 1)} disabled={currentPage <= 1}>
                <ChevronLeft className="h-4 w-4" />
            </Button>

            {getPaginationRange().map((page, index) => (
                typeof page === 'string' 
                ? <span key={`dots-${index}`} className="px-2">...</span>
                : <Button key={page} variant={page === currentPage ? 'default' : 'outline'} size="icon" onClick={() => onPageChange(page)}>{page}</Button>
            ))}

            <Button variant="outline" size="icon" onClick={() => onPageChange(currentPage + 1)} disabled={currentPage >= totalPages}>
                <ChevronRight className="h-4 w-4" />
            </Button>
            
            <form onSubmit={handleJump} className="flex items-center gap-2 ml-4">
                 <Input 
                    type="number"
                    value={jumpToPage}
                    onChange={(e) => setJumpToPage(e.target.value)}
                    className="w-20 h-10 bg-card"
                    min="1"
                    max={totalPages}
                 />
                 <Button type="submit" variant="outline">Перейти</Button>
            </form>
        </div>
    );
};

const formatBytes = (bytes: number, decimals = 2) => {
    if (!+bytes) return '0 Bytes'

    const k = 1024
    const dm = decimals < 0 ? 0 : decimals
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']

    const i = Math.floor(Math.log(bytes) / Math.log(k))

    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`
}


export function MongoExplorer() {
    const [data, setData] = useState<AllCollectionsData | null>(null);
    const [pageState, setPageState] = useState<CollectionState>({});
    const [isLoading, setIsLoading] = useState(true);
    const [isPageLoading, setIsPageLoading] = useState<string | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [editorState, setEditorState] = useState<EditorState | null>(null);
    const [isSaving, setIsSaving] = useState(false);
    const { toast } = useToast();

    const fetchData = async (collectionToRefresh?: string) => {
        if(!collectionToRefresh) setIsLoading(true);
        setError(null);
        try {
            const url = '/api/debug?db=mongo';
            const response = await fetch(url);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Не удалось загрузить данные');
            }
            const result = await response.json();
            
            const initialPageState: CollectionState = {};
            for (const name in result.collections) {
                initialPageState[name] = { page: 1, jumpToPageInput: '1' };
            }
            
            setData(result);
            if (!collectionToRefresh) {
              setPageState(initialPageState);
            }

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
    
    const fetchPage = async (collectionName: string, page: number) => {
        setIsPageLoading(collectionName);
        try {
             const url = `/api/debug?db=mongo&collection=${collectionName}&page=${page}`;
             const response = await fetch(url);
             if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `Не удалось загрузить страницу для ${collectionName}`);
             }
             const result = await response.json();
             setData(prevData => {
                 if (!prevData) return null;
                 return {
                     ...prevData,
                     collections: {
                         ...prevData.collections,
                         [collectionName]: {
                             ...prevData.collections[collectionName],
                             sample: result.sample,
                         }
                     }
                 }
             });
             setPageState(prevPages => ({
                 ...prevPages,
                 [collectionName]: { ...prevPages[collectionName], page: page }
             }));

        } catch (err: any) {
             toast({
                variant: 'destructive',
                title: 'Ошибка',
                description: err.message,
            });
        } finally {
            setIsPageLoading(null);
        }
    };

    useEffect(() => {
        fetchData();
    }, []);
    
    const copyToClipboard = (text: string) => {
        navigator.clipboard.writeText(text);
        toast({
            title: "Скопировано",
            description: "Содержимое скопировано в буфер обмена.",
        });
    };

    const handleOpenEditor = async (collectionName: string, doc?: any) => {
        if (doc) { // Single document edit
             setEditorState({
                isOpen: true,
                isBulk: false,
                collectionName,
                content: JSON.stringify(doc, null, 2),
                docId: doc._id,
            });
        } else { // Bulk edit/copy
            try {
                const response = await fetch(`/api/debug?db=mongo&collection=${collectionName}&all=true`);
                if (!response.ok) throw new Error("Не удалось загрузить все документы");
                const allDocs = await response.json();
                setEditorState({
                    isOpen: true,
                    isBulk: true,
                    collectionName,
                    content: JSON.stringify(allDocs, null, 2),
                });
            } catch (err: any) {
                toast({ variant: 'destructive', title: 'Ошибка', description: err.message });
            }
        }
    };
    
    const handleSave = async () => {
        if (!editorState) return;
        setIsSaving(true);
        try {
            let url = `/api/debug?db=mongo&collection=${editorState.collectionName}`;
            if (!editorState.isBulk && editorState.docId) {
                url += `&id=${editorState.docId}`;
            }

            const method = editorState.isBulk ? 'POST' : 'PUT';
            
            const body = JSON.parse(editorState.content);

            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const result = await response.json();
            if (!response.ok) throw new Error(result.error);
            
            toast({ title: 'Успех', description: result.message });
            setEditorState(null);
            fetchData(editorState.collectionName); // Refresh the specific collection
            fetchPage(editorState.collectionName, pageState[editorState.collectionName].page); // Go back to the same page

        } catch (err: any) {
            toast({ variant: 'destructive', title: 'Ошибка сохранения', description: err.message });
        } finally {
            setIsSaving(false);
        }
    };
    
    const handleDelete = async (collectionName: string, docId?: string) => {
        try {
            let url = `/api/debug?db=mongo&collection=${collectionName}`;
            if (docId) {
                url += `&id=${docId}`;
            }
            const response = await fetch(url, {
                method: 'DELETE'
            });
            const result = await response.json();
            if(!response.ok) throw new Error(result.error);

            toast({ title: 'Успех', description: result.message });
            if (docId) {
                fetchData(collectionName);
                fetchPage(collectionName, pageState[collectionName].page);
            } else {
                 fetchData(); // Full refresh if a whole collection is dropped
            }

        } catch (err: any) {
             toast({ variant: 'destructive', title: 'Ошибка удаления', description: err.message });
        }
    };
    

    const renderValue = (value: any) => {
        if (typeof value === 'boolean') {
            return value ? 'Да' : 'Нет';
        }
        if (typeof value === 'object' && value !== null) {
            return <pre className="text-xs whitespace-pre-wrap bg-background/50 p-2 rounded">{JSON.stringify(value, null, 2)}</pre>;
        }
        if (typeof value === 'string' && value.length > 150) {
            return <span title={value} className="break-all">{value.substring(0, 150)}...</span>;
        }
        return <span className="break-all">{String(value)}</span>;
    };

    if (isLoading) {
        return (
             <>
                <DialogHeader>
                    <DialogTitle>Обозреватель MongoDB</DialogTitle>
                    <DialogDescription>Загрузка данных из базы...</DialogDescription>
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
                <DialogTitle>Обозреватель MongoDB</DialogTitle>
                <DialogDescription>
                    Просмотр и редактирование коллекций и документов в базе данных. Показаны последние записи.
                </DialogDescription>
            </DialogHeader>
             {data?.connectionInfo && (
                <div className="mt-4 p-4 border rounded-lg bg-card">
                    <h4 className="font-semibold mb-2 text-foreground">Информация о подключении</h4>
                    <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                        <p className="text-muted-foreground">Хост:</p> <p className="font-mono">{data.connectionInfo.host}</p>
                        <p className="text-muted-foreground">База данных:</p> <p className="font-mono">{data.connectionInfo.dbName}</p>
                        <p className="text-muted-foreground">Размер:</p> <p className="font-mono">{formatBytes(data.connectionInfo.storageSize)}</p>
                        <p className="text-muted-foreground">Версия сервера:</p> <p className="font-mono">{data.connectionInfo.version}</p>
                    </div>
                </div>
            )}
            <div className="mt-4 h-[calc(90vh-210px)]">
                <ScrollArea className="h-full pr-4">
                    {data && data.collections && Object.keys(data.collections).length > 0 ? (
                        <Accordion type="single" collapsible className="w-full">
                            {Object.entries(data.collections).map(([name, { count, sample, pages }]) => (
                                <AccordionItem value={name} key={name}>
                                    <AccordionTrigger>
                                        <div className="flex items-center justify-between w-full">
                                            <div className="flex items-center gap-4">
                                                <span className="font-semibold">{name}</span>
                                                <Badge variant="secondary">{count.toLocaleString()} док.</Badge>
                                            </div>
                                            <div className="flex items-center gap-1 mr-2" onClick={(e) => e.stopPropagation()}>
                                               <Dialog>
                                                   <DialogTrigger asChild>
                                                      <Button variant="ghost" size="icon" title="Копировать всю коллекцию"><CopyPlus className="h-4 w-4"/></Button>
                                                   </DialogTrigger>
                                                   <DialogContent className="max-w-3xl h-[80vh]">
                                                     <DialogHeader>
                                                       <DialogTitle>Копировать коллекцию: {name}</DialogTitle>
                                                       <DialogDescription>Здесь представлено все содержимое коллекции в формате JSON.</DialogDescription>
                                                     </DialogHeader>
                                                      <Textarea readOnly value={"Загрузка..."} className="h-full flex-grow font-mono text-xs"
                                                         onFocus={async (e) => {
                                                             const response = await fetch(`/api/debug?db=mongo&collection=${name}&all=true`);
                                                             const allDocs = await response.json();
                                                             e.target.value = JSON.stringify(allDocs, null, 2);
                                                         }}
                                                      />
                                                      <DialogFooter>
                                                          <Button onClick={(e) => copyToClipboard((e.currentTarget.parentElement?.previousSibling as HTMLTextAreaElement)?.value || '')}>Копировать</Button>
                                                      </DialogFooter>
                                                   </DialogContent>
                                               </Dialog>
                                                <Button variant="ghost" size="icon" title="Редактировать всю коллекцию" onClick={() => handleOpenEditor(name)}><FilePenLine className="h-4 w-4"/></Button>
                                                <AlertDialog>
                                                    <AlertDialogTrigger asChild>
                                                        <Button variant="ghost" size="icon" title="Удалить коллекцию"><Trash2 className="h-4 w-4 text-destructive"/></Button>
                                                    </AlertDialogTrigger>
                                                    <AlertDialogContent>
                                                        <AlertDialogHeader>
                                                            <AlertDialogTitle>Вы абсолютно уверены?</AlertDialogTitle>
                                                            <AlertDialogDescription>Это действие необратимо. Коллекция "{name}" и все её документы будут навсегда удалены.</AlertDialogDescription>
                                                        </AlertDialogHeader>
                                                        <AlertDialogFooter>
                                                            <AlertDialogCancel>Отмена</AlertDialogCancel>
                                                            <AlertDialogAction onClick={() => handleDelete(name)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                                        </AlertDialogFooter>
                                                    </AlertDialogContent>
                                                </AlertDialog>
                                            </div>
                                        </div>
                                    </AccordionTrigger>
                                    <AccordionContent>
                                     {isPageLoading === name ? (
                                            <div className="flex items-center justify-center h-64">
                                                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                                            </div>
                                        ) : sample.length > 0 ? (
                                            <div className="space-y-4">
                                                {sample.map((doc, index) => (
                                                    <div key={doc._id || index} className="rounded-md border bg-card p-4">
                                                        <div className="flex justify-end items-center -mt-2 -mr-2">
                                                            <Button variant="ghost" size="icon" title="Редактировать" onClick={() => handleOpenEditor(name, doc)}><PenTool className="h-4 w-4"/></Button>
                                                            <Button variant="ghost" size="icon" title="Копировать" onClick={() => copyToClipboard(JSON.stringify(doc, null, 2))}><Copy className="h-4 w-4"/></Button>
                                                            <AlertDialog>
                                                                <AlertDialogTrigger asChild>
                                                                    <Button variant="ghost" size="icon" title="Удалить"><Trash2 className="h-4 w-4 text-destructive"/></Button>
                                                                </AlertDialogTrigger>
                                                                <AlertDialogContent>
                                                                    <AlertDialogHeader>
                                                                        <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                                                        <AlertDialogDescription>Это действие удалит документ с ID: {doc._id}. Отменить будет невозможно.</AlertDialogDescription>
                                                                    </AlertDialogHeader>
                                                                    <AlertDialogFooter>
                                                                        <AlertDialogCancel>Отмена</AlertDialogCancel>
                                                                        <AlertDialogAction onClick={() => handleDelete(name, doc._id)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                                                    </AlertDialogFooter>
                                                                </AlertDialogContent>
                                                            </AlertDialog>
                                                        </div>
                                                        <Table>
                                                            <TableBody>
                                                                {Object.entries(doc).map(([key, value]) => (
                                                                     <TableRow key={key}>
                                                                        <TableCell className="font-medium w-1/5 break-all py-2 align-top text-muted-foreground">{key}</TableCell>
                                                                        <TableCell className="w-4/5 break-all py-2">{renderValue(value)}</TableCell>
                                                                    </TableRow>
                                                                ))}
                                                            </TableBody>
                                                        </Table>
                                                    </div>
                                                ))}

                                                {pages > 1 && (
                                                   <Pagination 
                                                        currentPage={pageState[name]?.page || 1} 
                                                        totalPages={pages} 
                                                        onPageChange={(page) => fetchPage(name, page)}
                                                    />
                                                )}
                                            </div>
                                        ) : (
                                            <p className="text-muted-foreground text-sm p-4 text-center">Коллекция пуста.</p>
                                        )}
                                    </AccordionContent>
                                </AccordionItem>
                            ))}
                        </Accordion>
                    ) : (
                         <div className="flex items-center justify-center h-full text-muted-foreground">
                            <p>База данных пуста или не содержит коллекций.</p>
                        </div>
                    )}
                </ScrollArea>
            </div>
             {editorState && (
                <Dialog open={editorState.isOpen} onOpenChange={(open) => !open && setEditorState(null)}>
                    <DialogContent className="max-w-3xl h-[80vh] flex flex-col">
                        <DialogHeader>
                            <DialogTitle>{editorState.isBulk ? 'Редактировать коллекцию' : 'Редактировать документ'}</DialogTitle>
                            <DialogDescription>
                                {editorState.isBulk ? `Вы редактируете все содержимое коллекции "${editorState.collectionName}". Будьте осторожны, это действие перезапишет все данные.` : `Вы редактируете документ с ID: ${editorState.docId}`}
                            </DialogDescription>
                        </DialogHeader>
                        <Textarea 
                            value={editorState.content}
                            onChange={(e) => setEditorState(s => s ? {...s, content: e.target.value } : null)}
                            className="h-full flex-grow font-mono text-xs"
                            placeholder="Введите валидный JSON..."
                        />
                        <DialogFooter>
                            <Button variant="secondary" onClick={() => setEditorState(null)}>Отмена</Button>
                            <Button onClick={handleSave} disabled={isSaving}>
                                {isSaving && <Loader2 className="mr-2 h-4 w-4 animate-spin"/>}
                                Сохранить
                            </Button>
                        </DialogFooter>
                    </DialogContent>
                </Dialog>
            )}
        </>
    );
}
