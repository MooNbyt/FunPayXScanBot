
"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Play, StopCircle, Trash2, Database, BarChart3, CheckCircle, AlertTriangle, Users, Ban, Download, Search, Save, Settings, Loader2, Upload, FileJson, RefreshCcw, Package, Bot, Link, Plug, Copy, FileText, Archive, Server, Users2, Timer, AlertCircle, HardDrive, Star, Store, PlusCircle, Edit, Key, Cloud, ArrowLeft, BookOpen, RefreshCw, Info, Lock, Unlock, FolderPlus, ShieldCheck, PenTool, Crown, CopyPlus, FilePenLine, Power, PowerOff, DatabaseZap, X, LogIn, MessageSquare } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogFooter,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { Separator } from "./ui/separator";
import { Textarea } from "./ui/textarea";
import { downloadProject, unlockSettings, lockSettings } from "@/app/actions";
import { Switch } from "./ui/switch";
import { ProductForm } from "./product-form";
import { Alert, AlertTitle } from "./ui/alert";
import { MongoExplorer } from "./mongo-explorer";
import { RedisExplorer } from "./redis-explorer";


type Profile = {
  id: number;
  nickname: string;
  isSupport: boolean;
  isBanned: boolean;
  regDate: string;
  url: string;
  scrapedBy: string;
  _id?: any;
};

export type Product = {
  _id?: string;
  category: string; 
  buttonName: string;
  invoiceTitle: string;
  invoiceDescription: string;
  price: number;
  priceReal: number;
  type: 'static' | 'api';
  productImageUrl?: string;
  staticKey?: string;
  apiUrl?: string;
  apiToken?: string;
  apiDays?: number;
};

type DbStatus = {
  status: 'loading' | 'connected' | 'error';
  memory: string | null;
};

type TelegramLog = {
    timestamp: string;
    payload: any;
};

type ProjectLog = {
    timestamp: string;
    message: string;
};

type WorkerStatus = {
  id: string;
  status: string;
};

type ProductView = 'list' | 'form';

type CustomLink = {
  text: string;
  url: string;
  showInGroups: boolean;
};

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


export default function FunPayWorkerDashboard() {
  const [stats, setStats] = useState({
    processed: 0,
    successful: 0,
    errors: 0,
    support: 0,
    banned: 0,
    connectionRequests: 0,
    activeConnections: 0,
    totalUsersInDb: 0,
    foundByWorker: 0,
    workerStatuses: [] as WorkerStatus[],
  });
  const [recentProfiles, setRecentProfiles] = useState<Profile[]>([]);
  const [listSearchQuery, setListSearchQuery] = useState("");
  const [mongoStatus, setMongoStatus] = useState<DbStatus>({ status: 'loading', memory: null });
  const [redisStatus, setRedisStatus] = useState<DbStatus>({ status: 'loading', memory: null });
  const [isScraping, setIsScraping] = useState(false);
  const [scraperWorkerId, setScraperWorkerId] = useState<string | null>(null);
  const [isActionLoading, setIsActionLoading] = useState(false);
  const [isWorkerManagerOpen, setIsWorkerManagerOpen] = useState(false);
  const [isWorkerActionLoading, setIsWorkerActionLoading] = useState<string | null>(null);

  const [activeTab, setActiveTab] = useState("dashboard");

  const [dbSearchQuery, setDbSearchQuery] = useState("");
  const [searchType, setSearchType] = useState("nickname");
  const [searchResults, setSearchResults] = useState<Profile[]>([]);
  const [isSearching, setIsSearching] = useState(false);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const [isExporting, setIsExporting] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [fileToImport, setFileToImport] = useState<File | null>(null);

  const [isDownloadingProject, setIsDownloadingProject] = useState(false);

  const { toast } = useToast();
  const [telegramToken, setTelegramToken] = useState("");
  const [telegramProviderToken, setTelegramProviderToken] = useState("");
  const [telegramPaymentCurrency, setTelegramPaymentCurrency] = useState("RUB");
  const [telegramBotLink, setTelegramBotLink] = useState("");
  const [telegramShopButtonName, setTelegramShopButtonName] = useState("Магазин");
  const [telegramWelcome, setTelegramWelcome] = useState("");
  const [telegramWelcomeImageUrl, setTelegramWelcomeImageUrl] = useState("");
  const [telegramConnectionInfoMessage, setTelegramConnectionInfoMessage] = useState("");
  const [telegramPaymentEnabled, setTelegramPaymentEnabled] = useState(false);
  const [telegramSearchCost, setTelegramSearchCost] = useState(1);
  const [telegramSearchCostReal, setTelegramSearchCostReal] = useState(10);
  const [telegramConnectionPaymentEnabled, setTelegramConnectionPaymentEnabled] = useState(false);
  const [telegramConnectionCost, setTelegramConnectionCost] = useState(5);
  const [telegramConnectionCostReal, setTelegramConnectionCostReal] = useState(50);
  const [telegramCustomLinks, setTelegramCustomLinks] = useState<CustomLink[]>([]);
  const [telegramLogsLimit, setTelegramLogsLimit] = useState(200);
  const [appUrl, setAppUrl] = useState("");
  const [workerId, setWorkerId] = useState("");
  
  const [scraperPauseDuration, setScraperPauseDuration] = useState(6);
  const [scraperConsecutiveErrorLimit, setScraperConsecutiveErrorLimit] = useState(100);
  const [scraperRecentProfilesLimit, setScraperRecentProfilesLimit] = useState(100);
  const [scraperBatchSize, setScraperBatchSize] = useState(25);
  const [scraperWriteBatchSize, setScraperWriteBatchSize] = useState(50);
  const [projectLogsTtl, setProjectLogsTtl] = useState(60);
  const [scraperParallelRequestLimitMin, setScraperParallelRequestLimitMin] = useState(1);
  const [scraperParallelRequestLimitMax, setScraperParallelRequestLimitMax] = useState(10);
  const [fileLoggingEnabled, setFileLoggingEnabled] = useState(true);
  const [scraperAdaptiveDelayMin, setScraperAdaptiveDelayMin] = useState(50);
  const [scraperAdaptiveDelayMax, setScraperAdaptiveDelayMax] = useState(5000);
  const [scraperAdaptiveDelayStep, setScraperAdaptiveDelayStep] = useState(50);
  const [scraperSuccessStreak, setScraperSuccessStreak] = useState(3);
  const [scraperDelayCompensation, setScraperDelayCompensation] = useState(20);
  const [scraperAnalysisWindow, setScraperAnalysisWindow] = useState(200);
  const [scraperSuccessThreshold, setScraperSuccessThreshold] = useState(99);

  const [webhookLog, setWebhookLog] = useState("");
  const [isSettingWebhook, setIsSettingWebhook] = useState(false);
  const [telegramLogs, setTelegramLogs] = useState<TelegramLog[]>([]);
  const [projectLogs, setProjectLogs] = useState<ProjectLog[]>([]);
  const [criticalLogs, setCriticalLogs] = useState<ProjectLog[]>([]);
  
  const [products, setProducts] = useState<Product[]>([]);
  const [productCategories, setProductCategories] = useState<string[]>([]);
  const [productView, setProductView] = useState<ProductView>('list');
  const [currentProduct, setCurrentProduct] = useState<Partial<Product> | null>(null);
  const [isSavingProduct, setIsSavingProduct] = useState(false);
  const [newCategory, setNewCategory] = useState("");
  const [isAddingCategory, setIsAddingCategory] = useState(false);
  const [currentCategoryView, setCurrentCategoryView] = useState<string | null>(null);


  const [isSettingsUnlocked, setIsSettingsUnlocked] = useState(false);
  const [settingsPasswordInput, setSettingsPasswordInput] = useState("");
  const [settingsPasswordError, setSettingsPasswordError] = useState("");
  const [isUnlocking, setIsUnlocking] = useState(false);
  const [isRecounting, setIsRecounting] = useState<string | null>(null);
  const [isCheckingIntegrity, setIsCheckingIntegrity] = useState(false);
  const [integrityCheckResult, setIntegrityCheckResult] = useState<{missingCount: number, missingIds: number[] } | null>(null);
  const [isQueueingMissing, setIsQueueingMissing] = useState(false);
  const [isDeduplicating, setIsDeduplicating] = useState(false);


  const checkDbStatus = async () => {
    try {
      setMongoStatus({ status: 'loading', memory: null });
      setRedisStatus({ status: 'loading', memory: null });
      const response = await fetch('/api/status');
      if (!response.ok) {
        throw new Error('Failed to fetch status');
      }
      const data = await response.json();
      setMongoStatus(data.mongodb);
      setRedisStatus(data.redis);
    } catch (error) {
      console.error('Error fetching status:', error);
      setMongoStatus({ status: 'error', memory: null });
      setRedisStatus({ status: 'error', memory: null });
    }
  };

  const checkScrapingStatus = useCallback(async () => {
    if (!workerId) return;
    try {
        const response = await fetch(`/api/scrape?workerId=${workerId}`);
        const data = await response.json();
        setIsScraping(data.isRunning);
        setScraperWorkerId(data.workerId);
    } catch (error) {
        console.error('Error fetching scraping status:', error);
        setIsScraping(false);
        setScraperWorkerId(null);
    }
  }, [workerId]);
  
  const fetchDashboardData = useCallback(async () => {
    try {
      const response = await fetch('/api/data');
      if (!response.ok) return;
      const data = await response.json();
      if (data.error) {
        console.error('Error fetching dashboard data:', data.error);
        return;
      }
      setStats(data.stats);
      setRecentProfiles(data.recentProfiles);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
    }
  }, []);

  const fetchTelegramLogs = useCallback(async () => {
    try {
      const response = await fetch('/api/telegram');
      if (response.ok) {
        const data: TelegramLog[] = await response.json();
        setTelegramLogs(data); 
      } else {
        console.error('Failed to fetch telegram logs, status:', response.status);
      }
    } catch (error) {
      console.error('Error fetching telegram logs:', error);
    }
  }, []);

  const fetchProjectLogs = useCallback(async () => {
    try {
        const response = await fetch('/api/project-logs');
        if (response.ok) {
            const data: { logs: ProjectLog[], criticalLogs: ProjectLog[] } = await response.json();
            setProjectLogs(data.logs.reverse());
            setCriticalLogs(data.criticalLogs.reverse());
        } else {
            console.error('Failed to fetch project logs, status:', response.status);
        }
    } catch (error) {
        console.error('Error fetching project logs:', error);
    }
  }, []);

  const fetchProductsAndCategories = useCallback(async () => {
    try {
      const [productsRes, categoriesRes] = await Promise.all([
        fetch('/api/products'),
        fetch('/api/products?type=categories')
      ]);

      if (productsRes.ok) {
        const data = await productsRes.json();
        setProducts(data.products);
      } else {
        toast({ variant: 'destructive', title: 'Ошибка', description: 'Не удалось загрузить список товаров.' });
      }

      if (categoriesRes.ok) {
        const data = await categoriesRes.json();
        setProductCategories(data.categories);
      } else {
        toast({ variant: 'destructive', title: 'Ошибка', description: 'Не удалось загрузить список категорий.' });
      }

    } catch (error: any) {
      toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
    }
  }, [toast]);

  const handleSearch = async (query = dbSearchQuery, type = searchType) => {
    if (!query.trim()) {
      setSearchResults([]);
      return;
    }
    setIsSearching(true);
    try {
        const response = await fetch(`/api/data?query=${encodeURIComponent(query)}&type=${type}`);
        if (!response.ok) {
            throw new Error('Search request failed');
        }
        const data = await response.json();
        setSearchResults(data);
    } catch (error: any) {
        toast({
            variant: "destructive",
            title: "Ошибка поиска",
            description: error.message || "Не удалось выполнить поиск по базе данных.",
        });
        setSearchResults([]);
    } finally {
        setIsSearching(false);
    }
  };

  useEffect(() => {
    if (activeTab === 'search' && dbSearchQuery && searchType) {
        handleSearch(dbSearchQuery, searchType);
    }
  }, [activeTab, dbSearchQuery, searchType]);

  const handleSaveConfig = useCallback(async (configToSave: any) => {
    try {
      const response = await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(configToSave),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.error || 'Failed to save config');
      }
      toast({
        title: "Настройки сохранены",
        description: "Конфигурация была успешно обновлена.",
        duration: 2000,
      });
      await checkDbStatus();
    } catch (error: any) {
      console.error('Error saving config:', error);
      toast({
        variant: "destructive",
        title: "Ошибка сохранения",
        description: error.message || "Не удалось сохранить конфигурацию.",
      });
    }
  }, [toast]);

  const fetchConfig = useCallback(async () => {
      try {
        const response = await fetch('/api/config');
        if (response.ok) {
          const data = await response.json();
          setTelegramToken(data.TELEGRAM_TOKEN || "");
          setTelegramProviderToken(data.TELEGRAM_PROVIDER_TOKEN || "");
          setTelegramPaymentCurrency(data.TELEGRAM_PAYMENT_CURRENCY || "RUB");
          setTelegramBotLink(data.TELEGRAM_BOT_LINK || "");
          setTelegramShopButtonName(data.TELEGRAM_SHOP_BUTTON_NAME || "Магазин");
          setTelegramWelcome(data.TELEGRAM_WELCOME_MESSAGE || "");
          setTelegramWelcomeImageUrl(data.TELEGRAM_WELCOME_IMAGE_URL || "");
          setTelegramConnectionInfoMessage(data.TELEGRAM_CONNECTION_INFO_MESSAGE || "");
          setTelegramPaymentEnabled(data.TELEGRAM_PAYMENT_ENABLED || false);
          setTelegramSearchCost(data.TELEGRAM_SEARCH_COST_STARS || 1);
          setTelegramSearchCostReal(data.TELEGRAM_SEARCH_COST_REAL || 10);
          setTelegramConnectionPaymentEnabled(data.TELEGRAM_CONNECTION_PAYMENT_ENABLED || false);
          setTelegramConnectionCost(data.TELEGRAM_CONNECTION_COST_STARS || 5);
          setTelegramConnectionCostReal(data.TELEGRAM_CONNECTION_COST_REAL || 50);
          setTelegramCustomLinks(data.TELEGRAM_CUSTOM_LINKS || []);
          setTelegramLogsLimit(data.TELEGRAM_LOGS_LIMIT || 200);
          setAppUrl(data.NEXT_PUBLIC_APP_URL || "");
          setWorkerId(data.WORKER_ID || "");
          setScraperPauseDuration((data.SCRAPER_PAUSE_DURATION_MS || 21600000) / 1000 / 60 / 60);
          setScraperConsecutiveErrorLimit(data.SCRAPER_CONSECUTIVE_ERROR_LIMIT || 100);
          setScraperRecentProfilesLimit(data.SCRAPER_RECENT_PROFILES_LIMIT || 100);
          setScraperBatchSize(data.SCRAPER_BATCH_SIZE || 25);
          setScraperWriteBatchSize(data.SCRAPER_WRITE_BATCH_SIZE || 50);
          setProjectLogsTtl(data.PROJECT_LOGS_TTL_MINUTES || 60);
          setScraperParallelRequestLimitMin(data.SCRAPER_PARALLEL_REQUEST_LIMIT_MIN || 1);
          setScraperParallelRequestLimitMax(data.SCRAPER_PARALLEL_REQUEST_LIMIT_MAX || 10);
          setFileLoggingEnabled(data.SCRAPER_FILE_LOGGING_ENABLED === undefined ? true : data.SCRAPER_FILE_LOGGING_ENABLED);
          setScraperAdaptiveDelayMin(data.SCRAPER_ADAPTIVE_DELAY_MIN_MS || 50);
          setScraperAdaptiveDelayMax(data.SCRAPER_ADAPTIVE_DELAY_MAX_MS || 5000);
          setScraperAdaptiveDelayStep(data.SCRAPER_ADAPTIVE_DELAY_STEP_MS || 50);
          setIsSettingsUnlocked(data.isSettingsUnlocked || false);
          setScraperSuccessStreak(data.SCRAPER_SUCCESS_STREAK_TO_INCREASE_LIMIT || 3);
          setScraperDelayCompensation(data.SCRAPER_DELAY_COMPENSATION_MS || 20);
          setScraperAnalysisWindow(data.SCRAPER_ANALYSIS_WINDOW || 200);
          setScraperSuccessThreshold(data.SCRAPER_SUCCESS_THRESHOLD || 99);
        } else {
           throw new Error("Failed to fetch config from server.");
        }
      } catch (error) {
        console.error('Error fetching config:', error);
        toast({ variant: "destructive", title: "Ошибка загрузки настроек", description: "Не удалось загрузить конфигурацию с сервера." });
      }
  }, [toast]);


  useEffect(() => {
    fetchConfig();
    checkDbStatus();
    fetchDashboardData();
    fetchProductsAndCategories();
  }, [fetchConfig, fetchProductsAndCategories, fetchDashboardData]);

  useEffect(() => {
    checkScrapingStatus();
  },[workerId, checkScrapingStatus]);

  useEffect(() => {
    let dataInterval: NodeJS.Timeout;
    
    const setupIntervals = () => {
        if (document.visibilityState !== 'visible') return;

        if (activeTab === 'dashboard') {
            checkScrapingStatus();
            fetchDashboardData();
        } else if (activeTab === 'telegram-logs') {
            fetchTelegramLogs();
        } else if (activeTab === 'project-logs') {
            fetchProjectLogs();
        }
    };
    
    setupIntervals(); // Initial fetch on tab change
    dataInterval = setInterval(setupIntervals, 2500);

    if (activeTab === 'products') {
      fetchProductsAndCategories();
      setCurrentCategoryView(null);
      setProductView('list');
    }

    // Auto-switch from protected tab if lock is engaged
    if (activeTab === 'settings' && !isSettingsUnlocked) {
      setActiveTab('access-settings');
    }

    return () => {
        if (dataInterval) clearInterval(dataInterval);
    }
  }, [activeTab, checkScrapingStatus, fetchTelegramLogs, fetchProductsAndCategories, fetchProjectLogs, isSettingsUnlocked, fetchDashboardData]);

  const handleScraperAction = async (action: 'start' | 'stop', wId: string) => {
    setIsWorkerActionLoading(wId);
    setIsActionLoading(true);
    try {
        const response = await fetch('/api/scrape', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action, workerId: wId }),
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || `Failed to ${action} scraper`);
        }
        toast({ title: "Команда отправлена", description: data.message });
        await new Promise(resolve => setTimeout(resolve, 1500));
        await fetchDashboardData();
        await checkScrapingStatus();
    } catch (error: any) {
        toast({ variant: "destructive", title: `Ошибка ${action === 'start' ? 'запуска' : 'остановки'}`, description: error.message });
    } finally {
        setIsWorkerActionLoading(null);
        setIsActionLoading(false);
    }
  };


  const handleClearDB = async () => {
    try {
      const response = await fetch('/api/data', {
        method: 'DELETE',
      });
      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || 'Failed to clear DB');
      }
      toast({
        title: "База данных очищена",
        description: "Все данные скрейпинга были удалены.",
      });
      await fetchDashboardData(); 
    } catch (error: any) {
       toast({
        variant: "destructive",
        title: "Ошибка очистки",
        description: error.message,
      });
    }
  };

  const handleClearProjectLogs = async (scope: 'all' | 'critical' | 'regular') => {
    try {
      const response = await fetch(`/api/project-logs?scope=${scope}`, {
        method: 'DELETE',
      });
      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || 'Не удалось очистить логи');
      }
      toast({
        title: "Логи очищены",
        description: "Выбранные логи были успешно удалены.",
      });
      await fetchProjectLogs(); 
    } catch (error: any) {
       toast({
        variant: "destructive",
        title: "Ошибка очистки",
        description: error.message,
      });
    }
  };

  const handleClearTelegramLogs = async () => {
    try {
      const response = await fetch('/api/telegram', {
        method: 'DELETE',
      });
      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || 'Не удалось очистить логи');
      }
      toast({
        title: "Логи Telegram очищены",
        description: "Логи входящих вебхуков были успешно удалены.",
      });
      await fetchTelegramLogs(); 
    } catch (error: any) {
       toast({
        variant: "destructive",
        title: "Ошибка очистки логов",
        description: error.message,
      });
    }
  };
  
  const handleSearchSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    handleSearch();
  };
  
  const handleStatusCardClick = async (status: 'support' | 'banned') => {
    if (isRecounting) return;
    setIsRecounting(status);
    try {
      const response = await fetch('/api/data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'recount', status }),
      });
      if (!response.ok) {
        throw new Error(`Failed to recount ${status}`);
      }
      const data = await response.json();
      toast({
        title: 'Статистика обновлена',
        description: `Найдено ${data.count} ${status === 'support' ? 'саппортов' : 'забаненных'}.`,
      });
      
      setStats(prev => ({...prev, [status]: data.count }));
      
      setSearchType('status');
      setDbSearchQuery(status);
      setActiveTab('search');

    } catch (error: any) {
      toast({
        variant: "destructive",
        title: "Ошибка пересчета",
        description: error.message,
      });
    } finally {
      setIsRecounting(null);
    }
  };

  const handleExport = async () => {
    setIsExporting(true);
    try {
      const response = await fetch('/api/backup');
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to export database');
      }
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      const contentDisposition = response.headers.get('content-disposition');
      let fileName = 'backup.json';
      if(contentDisposition) {
        const fileNameMatch = contentDisposition.match(/filename="?([^"]+)"?/);
        if(fileNameMatch && fileNameMatch.length > 1) {
          fileName = fileNameMatch[1].replace(/_/g, '');
        }
      }
      a.href = url;
      a.download = fileName;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);

      toast({
        title: "Экспорт завершен",
        description: "Файл с резервной копией был успешно скачан.",
      });
    } catch (error: any) {
      toast({
        variant: "destructive",
        title: "Ошибка экспорта",
        description: error.message || "Не удалось экспортировать базу данных.",
      });
    } finally {
      setIsExporting(false);
    }
  };

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.files && event.target.files.length > 0) {
      setFileToImport(event.target.files[0]);
    } else {
      setFileToImport(null);
    }
  };

  const handleTriggerImport = () => {
    fileInputRef.current?.click();
  };

  const handleConfirmImport = async () => {
    if (!fileToImport) return;

    setIsImporting(true);
    const reader = new FileReader();
    reader.onload = async (e) => {
        try {
            const content = e.target?.result;
            if (typeof content !== 'string') {
                throw new Error("Failed to read file content.");
            }
            const data = JSON.parse(content);
            const response = await fetch('/api/backup', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data),
            });
            
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.error || 'Failed to import database');
            }

            toast({
                title: "Импорт завершен",
                description: "База данных была успешно восстановлена.",
            });
            await fetchDashboardData();
        } catch (error: any) {
            toast({
                variant: "destructive",
                title: "Ошибка импорта",
                description: error.message || "Убедитесь, что это корректный JSON файл.",
            });
        } finally {
            setIsImporting(false);
            setFileToImport(null);
            if(fileInputRef.current) fileInputRef.current.value = "";
        }
    };
    reader.onerror = () => {
        toast({
            variant: "destructive",
            title: "Ошибка чтения файла",
            description: "Не удалось прочитать выбранный файл.",
        });
        setIsImporting(false);
        setFileToImport(null);
    };
    reader.readAsText(fileToImport);
  };

    const handleSetWebhook = async () => {
      setIsSettingWebhook(true);
      setWebhookLog("Подключение...");

      try {
          const response = await fetch('/api/telegram', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ token: telegramToken }),
          });
          const data = await response.json();
          if (!response.ok) {
            throw new Error(data.error || 'Не удалось установить вебхук.');
          }
          toast({
            title: "Вебхук установлен",
            description: data.message,
          });
          setWebhookLog(`Успех: ${data.message}`);
          await fetchConfig();
      } catch (error: any) {
        toast({
          variant: "destructive",
          title: "Ошибка подключения",
          description: error.message,
        });
        setWebhookLog(`Ошибка: ${error.message}`);
      } finally {
        setIsSettingWebhook(false);
      }
    };
    
     const copyToClipboard = (text: string) => {
        navigator.clipboard.writeText(text);
        toast({
            title: "Скопировано",
            description: "Текст скопирован в буфер обмена.",
        });
    };

  const handleDownloadProject = async () => {
    setIsDownloadingProject(true);
    toast({ title: 'Архивация проекта', description: 'Пожалуйста, подождите, это может занять некоторое время...' });
    try {
      const result = await downloadProject();
      if (result.success) {
        const a = document.createElement('a');
        a.href = `data:application/zip;base64,${result.file}`;
        a.download = result.fileName;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        toast({ title: 'Успех', description: 'Проект успешно скачан.' });
      } else {
        throw new Error('Failed to download project');
      }
    } catch (error: any) {
      console.error(error);
      toast({ variant: 'destructive', title: 'Ошибка', description: error.message || 'Не удалось скачать проект.' });
    }
    setIsDownloadingProject(false);
  };
  
    const handleDownloadLogFile = async () => {
    try {
        const response = await fetch('/api/scrape?action=download_log');
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(errorText || 'Не удалось скачать лог-файл');
        }
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'scraper.log';
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);
    } catch (error: any) {
        toast({
            variant: "destructive",
            title: "Ошибка скачивания лога",
            description: error.message,
        });
    }
  };

  const handleClearLogFile = async () => {
     try {
        const response = await fetch('/api/scrape?action=clear_log', { method: 'DELETE' });
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(errorText || 'Не удалось очистить лог-файл');
        }
        const data = await response.json();
        toast({
            title: "Лог-файл очищен",
            description: data.message,
        });
    } catch (error: any) {
        toast({
            variant: "destructive",
            title: "Ошибка очистки лога",
            description: error.message,
        });
    }
  }

  const handleInsertVariable = (variable: string, textareaId: string) => {
    const textarea = document.getElementById(textareaId) as HTMLTextAreaElement;
    if (textarea) {
      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;
      const text = textarea.value;
      const newText = text.substring(0, start) + variable + text.substring(end);
      
      const updater = textareaId === 'telegram-welcome' ? setTelegramWelcome : setTelegramConnectionInfoMessage;
      updater(newText);

      setTimeout(() => {
        textarea.focus();
        textarea.selectionStart = textarea.selectionEnd = start + variable.length;
      }, 0);
    }
  };

  const handleSaveProduct = async (productData: Partial<Product>) => {
    setIsSavingProduct(true);
    try {
      const method = productData._id ? 'PUT' : 'POST';
      const response = await fetch('/api/products', {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(productData),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Не удалось сохранить товар.');
      }
      toast({ title: 'Успех', description: 'Товар успешно сохранен.' });
      setProductView('list');
      setCurrentProduct(null);
      fetchProductsAndCategories();
    } catch (error: any) {
      toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
    } finally {
      setIsSavingProduct(false);
    }
  };

  const handleDeleteProduct = async (productId: string) => {
    try {
      const response = await fetch('/api/products', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ _id: productId }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Не удалось удалить товар.');
      }
      toast({ title: 'Успех', description: 'Товар удален.' });
      fetchProductsAndCategories();
    } catch (error: any) {
      toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
    }
  };

  const openProductForm = (category: string | null) => {
    setCurrentProduct({
      category: category || '',
      buttonName: '',
      invoiceTitle: '',
      invoiceDescription: '',
      price: 1,
      priceReal: 10,
      type: 'static',
      productImageUrl: '',
      staticKey: '',
      apiUrl: '',
      apiToken: '',
      apiDays: 30,
    });
    setProductView('form');
  };
  
  const handleUnlockSettingsSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsUnlocking(true);
    setSettingsPasswordError("");
    const formData = new FormData(e.target as HTMLFormElement);
    
    const response = await unlockSettings(formData);

    if (response?.error) {
        const attemptsKey = `settings_attempts_${workerId}`;
        let attempts = parseInt(localStorage.getItem(attemptsKey) || '0') + 1;
        
        const now = new Date().getTime();
        const blockedUntil = parseInt(localStorage.getItem(`${attemptsKey}_blocked_until`) || '0');

        if (now < blockedUntil) {
            setSettingsPasswordError(`Вы заблокированы. Попробуйте снова через ${Math.ceil((blockedUntil - now) / 60000)} минут.`);
            setIsUnlocking(false);
            return;
        }

        if (attempts >= 6) {
            const blockDuration = 6 * 60 * 60 * 1000; // 6 hours
            localStorage.setItem(`${attemptsKey}_blocked_until`, (now + blockDuration).toString());
            localStorage.removeItem(attemptsKey);
            setSettingsPasswordError("Слишком много неудачных попыток. Доступ заблокирован на 6 часов.");
        } else {
            localStorage.setItem(attemptsKey, attempts.toString());
            setSettingsPasswordError(`${response.error} (Попытка ${attempts} из 6)`);
        }
    }

    if (response?.success) {
        localStorage.removeItem(`settings_attempts_${workerId}`);
        localStorage.removeItem(`settings_attempts_${workerId}_blocked_until`);
        setIsSettingsUnlocked(true);
        setActiveTab('settings');
        setSettingsPasswordInput("");
        await fetchConfig();
        toast({ title: "Доступ разрешен", description: "Настройки системы разблокированы." });
    }
    setIsUnlocking(false);
  };
    
    const handleAddCategory = async () => {
      if (!newCategory.trim()) {
        toast({ variant: 'destructive', title: 'Ошибка', description: 'Название категории не может быть пустым.' });
        return;
      }
      setIsAddingCategory(true);
      try {
        const response = await fetch('/api/products', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'add_category', categoryName: newCategory }),
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.error || 'Не удалось создать категорию.');
        }
        setProductCategories(data.categories);
        toast({ title: 'Успех', description: `Категория "${newCategory}" создана.` });
        setNewCategory('');
      } catch (error: any) {
        toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
      } finally {
        setIsAddingCategory(false);
      }
  };

  const handleDeleteCategory = async (categoryName: string) => {
    try {
        const response = await fetch('/api/products', {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'delete_category', categoryName }),
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.error || 'Не удалось удалить категорию.');
        }
        setProductCategories(data.categories);
        toast({ title: 'Успех', description: `Категория "${categoryName}" удалена.` });
        fetchProductsAndCategories(); // Refresh products as their category might have changed
    } catch (error: any) {
        toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
    }
  };
  
   const handleCheckIntegrity = async () => {
        setIsCheckingIntegrity(true);
        setIntegrityCheckResult(null);
        try {
            const response = await fetch('/api/data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: 'check_integrity' }),
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'Не удалось выполнить проверку');
            }
            
            if (data.missingCount > 0) {
              setIntegrityCheckResult(data);
            }

            toast({
                title: "Проверка целостности",
                description: data.message,
            });
        } catch (error: any) {
            toast({
                variant: "destructive",
                title: "Ошибка проверки",
                description: error.message,
            });
        } finally {
            setIsCheckingIntegrity(false);
        }
    };
    
    const handleQueueMissingIds = async () => {
        if (!integrityCheckResult || integrityCheckResult.missingIds.length === 0) return;
        setIsQueueingMissing(true);
        try {
            const response = await fetch('/api/data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: 'queue_missing', missingIds: integrityCheckResult.missingIds }),
            });
            const data = await response.json();
             if (!response.ok) {
                throw new Error(data.error || 'Не удалось добавить ID в очередь');
            }
            toast({
                title: "Задачи добавлены",
                description: data.message,
            });
        } catch (error: any) {
             toast({
                variant: "destructive",
                title: "Ошибка",
                description: error.message,
            });
        } finally {
            setIsQueueingMissing(false);
            setIntegrityCheckResult(null);
        }
    };

  const handleDeduplicate = async () => {
    setIsDeduplicating(true);
    try {
        const response = await fetch('/api/data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'deduplicate' }),
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || 'Не удалось выполнить удаление дубликатов');
        }
        toast({
            title: "Удаление дубликатов",
            description: data.message,
        });
        await fetchDashboardData(); // Refresh stats
    } catch (error: any) {
        toast({
            variant: "destructive",
            title: "Ошибка",
            description: error.message,
        });
    } finally {
        setIsDeduplicating(false);
    }
  };

  const handleCustomLinkChange = (index: number, field: 'text' | 'url' | 'showInGroups', value: string | boolean) => {
    const newLinks = [...telegramCustomLinks];
    newLinks[index] = { ...newLinks[index], [field]: value };
    setTelegramCustomLinks(newLinks);
  };

  const handleSaveCustomLinks = () => {
    handleSaveConfig({ TELEGRAM_CUSTOM_LINKS: telegramCustomLinks });
  };

  const handleAddCustomLink = () => {
    setTelegramCustomLinks(prev => [...prev, { text: '', url: '', showInGroups: true }]);
  };

  const handleRemoveCustomLink = (index: number) => {
    const newLinks = telegramCustomLinks.filter((_, i) => i !== index);
    setTelegramCustomLinks(newLinks);
    handleSaveConfig({ TELEGRAM_CUSTOM_LINKS: newLinks });
  };

  const handleBlurSave = (value: any, key: string) => {
    let valueToSave = value;
    if (key === 'SCRAPER_PAUSE_DURATION_MS') {
        valueToSave = value * 60 * 60 * 1000;
    }
    handleSaveConfig({ [key]: valueToSave });
  };


  const sortedRecentProfiles = [...recentProfiles].sort((a, b) => b.id - a.id);

  const filteredProfiles = sortedRecentProfiles.filter(profile =>
    profile.nickname.toLowerCase().includes(listSearchQuery.toLowerCase())
  );

  const runningWorkers = stats.workerStatuses.filter(w => w.status === 'running').length;
  const scrapingStatusText = isScraping ? `Работает (${scraperWorkerId})` : "Остановлен";
  const scrapingStatusColor = isScraping ? "text-green-400" : "text-yellow-400";

  const productsWithoutCategory = products.filter(p => !p.category || !productCategories.includes(p.category));
  const productsInCategory = currentCategoryView ? products.filter(p => p.category === currentCategoryView) : [];

  const CRITICAL_KEYWORDS = ['Error', 'CRITICAL', 'Failed', '⚠️', 'КРИТИЧЕСКАЯ ОШИБКА', 'Ошибка'];
  const getLogClass = (message: string) => {
    if (CRITICAL_KEYWORDS.some(keyword => message.includes(keyword))) {
      return 'text-red-400';
    }
    return '';
  };


  return (
    <div className="flex items-center justify-center min-h-screen bg-background text-foreground p-4 sm:p-6 lg:p-8">
      <Card className="w-full max-w-6xl shadow-2xl bg-card border-border">
        <CardHeader>
          <div className="flex flex-col sm:flex-row justify-between sm:items-start gap-4">
            <div>
              <div className="flex items-center gap-3">
                 <svg width="40" height="40" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <path d="M12 2L2 7V17L12 22L22 17V7L12 2Z" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  <path d="M2 7L12 12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  <path d="M12 22V12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  <path d="M22 7L12 12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  <path d="M17 4.5L7 9.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
                <CardTitle className="text-3xl font-headline animate-neon-glow">Funpay Scraper MooNTooL UI</CardTitle>
              </div>
              <CardDescription className="text-muted-foreground pt-2">Панель для извлечения и мониторинга профилей пользователей FunPay.</CardDescription>
            </div>
            
          </div>
        </CardHeader>
        <CardContent>
          <Tabs value={activeTab} onValueChange={setActiveTab}>
            <TabsList className="mb-4 grid w-full grid-cols-3 sm:grid-cols-4 md:grid-cols-7">
              <TabsTrigger value="dashboard">
                <BarChart3 className="mr-2 h-4 w-4" />
                Панель
              </TabsTrigger>
               <TabsTrigger value="search">
                <Search className="mr-2 h-4 w-4" />
                Поиск по БД
              </TabsTrigger>
               <TabsTrigger value="products">
                <Store className="mr-2 h-4 w-4" />
                Товары
              </TabsTrigger>
              <TabsTrigger value="telegram-bot-settings">
                <Bot className="mr-2 h-4 w-4" />
                Настройки ТГ Бота
              </TabsTrigger>
              <TabsTrigger value="telegram-logs">
                <FileText className="mr-2 h-4 w-4" />
                Логи Telegram
              </TabsTrigger>
              <TabsTrigger value="project-logs">
                <BookOpen className="mr-2 h-4 w-4" />
                Логи проекта
              </TabsTrigger>
              {isSettingsUnlocked ? (
                 <TabsTrigger value="settings">
                    <Unlock className="mr-2 h-4 w-4" />
                    Настройки Системы
                 </TabsTrigger>
              ) : (
                 <TabsTrigger value="access-settings">
                    <Lock className="mr-2 h-4 w-4" />
                    Доступ к настройкам
                 </TabsTrigger>
              )}
            </TabsList>
            <TabsContent value="dashboard">
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
                                              <Switch
                                                  checked={worker.status === 'running'}
                                                  onCheckedChange={(checked) => handleScraperAction(checked ? 'start' : 'stop', worker.id)}
                                              />
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
                             <StatusCard title="MongoDB" status={mongoStatus} icon={<Database className="h-4 w-4 text-muted-foreground" />} isClickable={isSettingsUnlocked}/>
                        </div>
                    </DialogTrigger>
                    <DialogContent className="max-w-4xl h-[90vh]">
                         <MongoExplorer />
                    </DialogContent>
                </Dialog>

                 <Dialog>
                    <DialogTrigger asChild disabled={!isSettingsUnlocked}>
                        <div className={!isSettingsUnlocked ? 'cursor-not-allowed' : ''}>
                             <StatusCard title="Redis" status={redisStatus} icon={<svg className="h-4 w-4 text-muted-foreground" fill="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" width="24" height="24" ><path d="M12.384 1.76a.333.333 0 00-.309.133l-5.616 9.4a.333.333 0 00.288.503l-3.41-.004a.333.333 0 01.309.133l-5.616 9.4a.333.333 0 00.288.503h7.02a.333.333 0 00.309-.133l5.616-9.4a.333.333 0 00-.288-.503l-3.41.004a.333.333 0 01-.309-.133l5.616-9.4a.333.333 0 00-.288-.503h-7.02z"/></svg>} isClickable={isSettingsUnlocked} />
                        </div>
                    </DialogTrigger>
                     <DialogContent className="max-w-4xl h-[90vh]">
                         <RedisExplorer />
                    </DialogContent>
                </Dialog>

                <Card className="bg-secondary h-full">
                  <CardHeader className="pb-2 flex flex-row items-center justify-between">
                    <CardDescription>Активные соединения</CardDescription>
                    <Users2 className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <CardTitle className="text-4xl font-mono">{stats.activeConnections}</CardTitle>
                    <p className="text-xs text-muted-foreground pt-1">пар</p>
                  </CardContent>
                </Card>

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

                <Card className="bg-secondary h-full">
                  <CardHeader className="pb-2 flex flex-row items-center justify-between">
                    <CardDescription>Всего в БД</CardDescription>
                    <HardDrive className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <CardTitle className="text-4xl font-mono">{stats.totalUsersInDb}</CardTitle>
                    <p className="text-xs text-muted-foreground pt-1">пользователей</p>
                  </CardContent>
                </Card>

                <Card className="bg-secondary h-full">
                  <CardHeader className="pb-2 flex flex-row items-center justify-between">
                    <CardDescription>Найдено воркером</CardDescription>
                    <CheckCircle className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <CardTitle className="text-4xl font-mono">{stats.foundByWorker}</CardTitle>
                     <p className="text-xs text-muted-foreground pt-1">этим воркером</p>
                  </CardContent>
                </Card>
                 <Card className="bg-secondary h-full">
                    <CardHeader className="pb-2 flex flex-row items-center justify-between">
                    <CardDescription>Ошибки 404 подряд</CardDescription>
                    <AlertTriangle className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                    <CardTitle className="text-4xl font-mono">{stats.errors}</CardTitle>
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
                        <CardTitle className="text-4xl font-mono">{stats.support}</CardTitle>
                        <p className="text-xs text-muted-foreground pt-1">профилей в БД</p>
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
                        <CardTitle className="text-4xl font-mono">{stats.banned}</CardTitle>
                        <p className="text-xs text-muted-foreground pt-1">профилей в БД</p>
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
                              <TableCell><Badge variant="outline">{profile.scrapedBy || 'N/A'}</Badge></TableCell>
                              <TableCell><a href={profile.url} target="_blank" rel="noopener noreferrer" className="text-sky-400 hover:underline">{profile.url}</a></TableCell>
                            </TableRow>
                          )) : (
                            <TableRow>
                                <TableCell colSpan={5} className="text-center h-24 text-muted-foreground">
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

            </TabsContent>
            <TabsContent value="search">
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
                                <TableHead>Воркер</TableHead>
                                <TableHead>URL Профиля</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {isSearching ? (
                                <TableRow>
                                    <TableCell colSpan={5} className="text-center h-24">
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
                                  <TableCell><Badge variant="outline">{profile.scrapedBy || 'N/A'}</Badge></TableCell>
                                  <TableCell><a href={profile.url} target="_blank" rel="noopener noreferrer" className="text-sky-400 hover:underline">{profile.url}</a></TableCell>
                                </TableRow>
                              )) : (
                                <TableRow>
                                    <TableCell colSpan={5} className="text-center h-24 text-muted-foreground">
                                       {dbSearchQuery ? "Ничего не найдено" : "Введите запрос для начала поиска"}
                                    </TableCell>
                                </TableRow>
                              )}
                            </TableBody>
                          </Table>
                        </ScrollArea>
                    </CardContent>
                </Card>
            </TabsContent>
            <TabsContent value="products">
              {productView === 'form' ? (
                  <ProductForm
                    product={currentProduct}
                    onSave={handleSaveProduct}
                    onCancel={() => {
                        setProductView('list');
                        setCurrentProduct(null);
                    }}
                    isSaving={isSavingProduct}
                    currency={telegramPaymentCurrency}
                  />
              ) : (
                <Card className="bg-secondary">
                    <CardHeader>
                      <div className="flex items-center justify-between">
                        <div>
                          {currentCategoryView !== null ? (
                            <div className='flex items-center gap-3'>
                              <Button variant="outline" size="icon" onClick={() => setCurrentCategoryView(null)}>
                                <ArrowLeft className="h-4 w-4" />
                              </Button>
                              <div>
                                <CardTitle>Категория: {currentCategoryView}</CardTitle>
                                <CardDescription>Товары в этой категории.</CardDescription>
                              </div>
                            </div>
                          ) : (
                            <div>
                               <CardTitle>Управление товарами</CardTitle>
                               <CardDescription>Создавайте категории и управляйте товарами.</CardDescription>
                            </div>
                          )}
                        </div>
                         <div className="flex items-center gap-2">
                             {currentCategoryView === null && (
                                <Dialog>
                                    <DialogTrigger asChild>
                                        <Button variant="outline">
                                            <FolderPlus className="mr-2 h-4 w-4" />
                                            Добавить категорию
                                        </Button>
                                    </DialogTrigger>
                                    <DialogContent>
                                        <DialogHeader>
                                            <DialogTitle>Новая категория</DialogTitle>
                                            <DialogDescription>Введите название для новой категории товаров.</DialogDescription>
                                        </DialogHeader>
                                        <form onSubmit={(e) => { e.preventDefault(); handleAddCategory(); }} className="space-y-4 py-4">
                                            <div className="space-y-2">
                                                <Label htmlFor="category-name">Название категории</Label>
                                                <Input id="category-name" value={newCategory} onChange={(e) => setNewCategory(e.target.value)} />
                                            </div>
                                            <DialogFooter>
                                                <DialogTrigger asChild>
                                                    <Button type="button" variant="secondary">Отмена</Button>
                                                </DialogTrigger>
                                                <DialogTrigger asChild>
                                                    <Button type="submit" disabled={isAddingCategory}>
                                                        {isAddingCategory && <Loader2 className="mr-2 h-4 w-4 animate-spin"/>}
                                                        Сохранить
                                                    </Button>
                                                </DialogTrigger>
                                            </DialogFooter>
                                        </form>
                                    </DialogContent>
                                </Dialog>
                            )}
                             <Button onClick={() => openProductForm(currentCategoryView)}>
                                <PlusCircle className="mr-2 h-4 w-4" />
                                Добавить товар
                            </Button>
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent>
                       <ScrollArea className="h-[500px]">
                           {currentCategoryView === null ? (
                                <div>
                                    {productCategories.length > 0 && (
                                      <div className="mb-6">
                                        <h3 className="text-lg font-semibold mb-2">Категории</h3>
                                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                                            {productCategories.map(cat => (
                                                <div key={cat} className="relative group">
                                                  <Button variant="outline" onClick={() => setCurrentCategoryView(cat)} className="h-20 w-full flex-col gap-2">
                                                      <Package className="h-6 w-6"/>
                                                      <span>{cat}</span>
                                                  </Button>
                                                  <AlertDialog>
                                                      <AlertDialogTrigger asChild>
                                                        <Button variant="destructive" size="icon" className="absolute -top-2 -right-2 h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity">
                                                          <X className="h-4 w-4"/>
                                                        </Button>
                                                      </AlertDialogTrigger>
                                                      <AlertDialogContent>
                                                        <AlertDialogHeader>
                                                          <AlertDialogTitle>Удалить категорию "{cat}"?</AlertDialogTitle>
                                                          <AlertDialogDescription>Все товары в этой категории станут "без категории". Это действие необратимо.</AlertDialogDescription>
                                                        </AlertDialogHeader>
                                                        <AlertDialogFooter>
                                                          <AlertDialogCancel>Отмена</AlertDialogCancel>
                                                          <AlertDialogAction onClick={() => handleDeleteCategory(cat)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                                        </AlertDialogFooter>
                                                      </AlertDialogContent>
                                                  </AlertDialog>
                                                </div>
                                            ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    <Separator className="my-4"/>
                                    
                                    <h3 className="text-lg font-semibold mb-2">Товары без категории</h3>
                                     <Table>
                                        <TableHeader>
                                            <TableRow>
                                                <TableHead>Название</TableHead>
                                                <TableHead>Тип</TableHead>
                                                <TableHead>Цена ({telegramPaymentCurrency})</TableHead>
                                                <TableHead className="text-right">Действия</TableHead>
                                            </TableRow>
                                        </TableHeader>
                                        <TableBody>
                                          {productsWithoutCategory.length > 0 ? productsWithoutCategory.map((product) => (
                                              <TableRow key={product._id}>
                                                  <TableCell className="font-medium">{product.buttonName}</TableCell>
                                                  <TableCell><Badge variant="secondary">{product.type === 'api' ? 'API' : 'Фикс.'}</Badge></TableCell>
                                                  <TableCell>{product.priceReal}</TableCell>
                                                  <TableCell className="text-right">
                                                      <Button variant="ghost" size="icon" onClick={()=>{ setCurrentProduct(product); setProductView('form'); }}>
                                                          <Edit className="h-4 w-4" />
                                                      </Button>
                                                      <AlertDialog>
                                                        <AlertDialogTrigger asChild>
                                                          <Button variant="ghost" size="icon"><Trash2 className="h-4 w-4 text-destructive" /></Button>
                                                        </AlertDialogTrigger>
                                                        <AlertDialogContent>
                                                          <AlertDialogHeader>
                                                            <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                                            <AlertDialogDescription>Это действие необратимо. Товар "{product.buttonName}" будет удален.</AlertDialogDescription>
                                                          </AlertDialogHeader>
                                                          <AlertDialogFooter>
                                                            <AlertDialogCancel>Отмена</AlertDialogCancel>
                                                            <AlertDialogAction onClick={() => handleDeleteProduct(product._id!)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                                          </AlertDialogFooter>
                                                        </AlertDialogContent>
                                                      </AlertDialog>
                                                  </TableCell>
                                              </TableRow>
                                          )) : (
                                              <TableRow>
                                                  <TableCell colSpan={4} className="h-24 text-center text-muted-foreground">
                                                      Нет товаров без категории.
                                                  </TableCell>
                                              </TableRow>
                                          )}
                                      </TableBody>
                                    </Table>

                                </div>
                           ) : (
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            <TableHead>Название</TableHead>
                                            <TableHead>Тип</TableHead>
                                            <TableHead>Цена ({telegramPaymentCurrency})</TableHead>
                                            <TableHead className="text-right">Действия</TableHead>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {productsInCategory.length > 0 ? productsInCategory.map((product) => (
                                            <TableRow key={product._id}>
                                                <TableCell className="font-medium">{product.buttonName}</TableCell>
                                                <TableCell><Badge variant="secondary">{product.type === 'api' ? 'API' : 'Фикс.'}</Badge></TableCell>
                                                <TableCell>{product.priceReal}</TableCell>
                                                <TableCell className="text-right">
                                                    <Button variant="ghost" size="icon" onClick={()=>{ setCurrentProduct(product); setProductView('form');}}>
                                                        <Edit className="h-4 w-4" />
                                                    </Button>
                                                     <AlertDialog>
                                                      <AlertDialogTrigger asChild>
                                                        <Button variant="ghost" size="icon"><Trash2 className="h-4 w-4 text-destructive" /></Button>
                                                      </AlertDialogTrigger>
                                                      <AlertDialogContent>
                                                        <AlertDialogHeader>
                                                          <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                                          <AlertDialogDescription>Это действие необратимо. Товар "{product.buttonName}" будет удален.</AlertDialogDescription>
                                                        </AlertDialogHeader>
                                                        <AlertDialogFooter>
                                                          <AlertDialogCancel>Отмена</AlertDialogCancel>
                                                          <AlertDialogAction onClick={() => handleDeleteProduct(product._id!)} className="bg-destructive hover:bg-destructive/90">Удалить</AlertDialogAction>
                                                        </AlertDialogFooter>
                                                      </AlertDialogContent>
                                                    </AlertDialog>
                                                </TableCell>
                                            </TableRow>
                                        )) : (
                                            <TableRow>
                                                <TableCell colSpan={4} className="h-24 text-center text-muted-foreground">
                                                    В этой категории еще нет товаров.
                                                </TableCell>
                                            </TableRow>
                                        )}
                                    </TableBody>
                                </Table>
                           )}
                        </ScrollArea>
                    </CardContent>
                </Card>
              )}
            </TabsContent>
            <TabsContent value="telegram-bot-settings">
                 <Card className="bg-secondary">
                    <CardHeader>
                    <CardTitle>Настройки Telegram Бота</CardTitle>
                    <CardDescription>Управление подключением и поведением вашего Telegram бота.</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-6">
                        <div className="space-y-4">
                        <div className="space-y-2">
                          <Label htmlFor="telegram-token">Токен Telegram Бота</Label>
                          <Input id="telegram-token" placeholder="123456:ABC-DEF1234..." value={telegramToken} onChange={(e) => setTelegramToken(e.target.value)} onBlur={() => handleBlurSave(telegramToken, 'TELEGRAM_TOKEN')} className="bg-card"/>
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor="telegram-bot-link">Ссылка на бота (для групп)</Label>
                          <Input id="telegram-bot-link" placeholder="https://t.me/YourBotName" value={telegramBotLink} onChange={(e) => setTelegramBotLink(e.target.value)} onBlur={() => handleBlurSave(telegramBotLink, 'TELEGRAM_BOT_LINK')} className="bg-card"/>
                        </div>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                          <div className="space-y-2">
                              <Label htmlFor="telegram-provider-token">Токен провайдера платежей</Label>
                              <Input id="telegram-provider-token" placeholder="Live или Test токен от @BotFather" value={telegramProviderToken} onChange={(e) => setTelegramProviderToken(e.target.value)} onBlur={() => handleBlurSave(telegramProviderToken, 'TELEGRAM_PROVIDER_TOKEN')} className="bg-card"/>
                              <p className="text-xs text-muted-foreground">Для приема платежей в Stars оставьте пустым.</p>
                          </div>
                           <div className="space-y-2">
                                <Label htmlFor="payment-currency">Валюта платежей</Label>
                                <Select value={telegramPaymentCurrency} onValueChange={(value) => { setTelegramPaymentCurrency(value); handleSaveConfig({ TELEGRAM_PAYMENT_CURRENCY: value }); }} disabled={!telegramProviderToken}>
                                    <SelectTrigger className="bg-card">
                                        <SelectValue placeholder="Выберите валюту" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="RUB">RUB (Российский рубль)</SelectItem>
                                        <SelectItem value="UAH">UAH (Украинская гривна)</SelectItem>
                                        <SelectItem value="USD">USD (Доллар США)</SelectItem>
                                        <SelectItem value="EUR">EUR (Евро)</SelectItem>
                                    </SelectContent>
                                </Select>
                                <p className="text-xs text-muted-foreground">Используется, если указан токен провайдера.</p>
                            </div>
                        </div>
                         <div className="space-y-2">
                          <Label htmlFor="telegram-shop-button-name">Название кнопки магазина</Label>
                          <Input id="telegram-shop-button-name" placeholder="Магазин" value={telegramShopButtonName} onChange={(e) => setTelegramShopButtonName(e.target.value)} onBlur={() => handleBlurSave(telegramShopButtonName, 'TELEGRAM_SHOP_BUTTON_NAME')} className="bg-card"/>
                        </div>
                        
                        <div className="p-4 bg-card rounded-lg border space-y-4">
                            <div className="flex flex-row items-center justify-between">
                              <div className="space-y-0.5">
                                <Label className="text-base">Платный поиск</Label>
                                <CardDescription>
                                  Если включено, бот будет требовать оплату за каждый поиск.
                                </CardDescription>
                              </div>
                              <Switch
                                checked={telegramPaymentEnabled}
                                onCheckedChange={(checked) => {setTelegramPaymentEnabled(checked); handleSaveConfig({ TELEGRAM_PAYMENT_ENABLED: checked });}}
                              />
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                               <div className="space-y-2">
                                 <Label htmlFor="telegram-search-cost">Стоимость в Telegram Stars</Label>
                                 <Input id="telegram-search-cost" type="number" min="1" placeholder="1" value={telegramSearchCost} onChange={(e) => setTelegramSearchCost(Number(e.target.value))} onBlur={(e) => handleBlurSave(e.target.value, 'TELEGRAM_SEARCH_COST_STARS')} className="bg-background" disabled={!telegramPaymentEnabled}/>
                               </div>
                                <div className="space-y-2">
                                 <Label htmlFor="telegram-search-cost-real">Стоимость в {telegramPaymentCurrency}</Label>
                                 <Input id="telegram-search-cost-real" type="number" min="1" placeholder="10" value={telegramSearchCostReal} onChange={(e) => setTelegramSearchCostReal(Number(e.target.value))} onBlur={(e) => handleBlurSave(e.target.value, 'TELEGRAM_SEARCH_COST_REAL')} className="bg-background" disabled={!telegramPaymentEnabled || !telegramProviderToken}/>
                               </div>
                            </div>
                        </div>

                        <div className="p-4 bg-card rounded-lg border space-y-4">
                            <div className="flex flex-row items-center justify-between">
                              <div className="space-y-0.5">
                                <Label className="text-base">Платное установление связи</Label>
                                <CardDescription>
                                  Если включено, бот будет требовать оплату за связь.
                                </CardDescription>
                              </div>
                              <Switch
                                checked={telegramConnectionPaymentEnabled}
                                onCheckedChange={(checked) => {setTelegramConnectionPaymentEnabled(checked); handleSaveConfig({ TELEGRAM_CONNECTION_PAYMENT_ENABLED: checked });}}
                              />
                            </div>
                             <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                               <div className="space-y-2">
                                 <Label htmlFor="telegram-connection-cost">Стоимость в Telegram Stars</Label>
                                 <Input id="telegram-connection-cost" type="number" min="1" placeholder="5" value={telegramConnectionCost} onChange={(e) => setTelegramConnectionCost(Number(e.target.value))} onBlur={(e) => handleBlurSave(e.target.value, 'TELEGRAM_CONNECTION_COST_STARS')} className="bg-background" disabled={!telegramConnectionPaymentEnabled}/>
                               </div>
                               <div className="space-y-2">
                                 <Label htmlFor="telegram-connection-cost-real">Стоимость в {telegramPaymentCurrency}</Label>
                                 <Input id="telegram-connection-cost-real" type="number" min="1" placeholder="50" value={telegramConnectionCostReal} onChange={(e) => setTelegramConnectionCostReal(Number(e.target.value))} onBlur={(e) => handleBlurSave(e.target.value, 'TELEGRAM_CONNECTION_COST_REAL')} className="bg-background" disabled={!telegramConnectionPaymentEnabled || !telegramProviderToken}/>
                               </div>
                            </div>
                        </div>

                        <Separator/>
                        
                        <div className="space-y-4">
                            <div className="flex items-center justify-between">
                                <Label className="text-base">Кнопки-ссылки в главном меню</Label>
                                <Button variant="outline" onClick={handleSaveCustomLinks}>
                                    <Save className="mr-2 h-4 w-4" />
                                    Сохранить ссылки
                                </Button>
                            </div>
                             {telegramCustomLinks.map((link, index) => (
                                <div key={index} className="flex items-end gap-2 p-3 bg-card rounded-lg border">
                                    <div className="flex-grow space-y-2">
                                        <Label htmlFor={`link-text-${index}`}>Текст кнопки</Label>
                                        <Input id={`link-text-${index}`} value={link.text} onChange={(e) => handleCustomLinkChange(index, 'text', e.target.value)} placeholder="Наш чат" />
                                    </div>
                                    <div className="flex-grow space-y-2">
                                        <Label htmlFor={`link-url-${index}`}>URL-адрес</Label>
                                        <Input id={`link-url-${index}`} value={link.url} onChange={(e) => handleCustomLinkChange(index, 'url', e.target.value)} placeholder="https://t.me/your_chat" />
                                    </div>
                                    <div className="flex flex-col items-center space-y-1">
                                        <Label htmlFor={`link-show-${index}`} className="text-xs">В группах</Label>
                                        <Switch id={`link-show-${index}`} checked={link.showInGroups} onCheckedChange={(checked) => handleCustomLinkChange(index, 'showInGroups', checked)} />
                                    </div>
                                    <Button variant="destructive" size="icon" onClick={() => handleRemoveCustomLink(index)}>
                                        <Trash2 className="h-4 w-4" />
                                    </Button>
                                </div>
                            ))}
                            <Button variant="outline" onClick={handleAddCustomLink}>
                                <PlusCircle className="mr-2 h-4 w-4" /> Добавить кнопку
                            </Button>
                        </div>
                        
                        <Separator/>

                        <div className="space-y-2">
                           <Label htmlFor="telegram-welcome-image">URL картинки для приветствия</Label>
                           <Input id="telegram-welcome-image" placeholder="https://example.com/image.png" value={telegramWelcomeImageUrl} onChange={(e) => setTelegramWelcomeImageUrl(e.target.value)} onBlur={() => handleBlurSave(telegramWelcomeImageUrl, 'TELEGRAM_WELCOME_IMAGE_URL')} className="bg-card"/>
                         </div>
                         <div className="space-y-2">
                            <Label htmlFor="telegram-welcome">Приветственное сообщение</Label>
                            <Textarea id="telegram-welcome" placeholder="Введите приветствие..." value={telegramWelcome} onChange={(e) => setTelegramWelcome(e.target.value)} onBlur={() => handleBlurSave(telegramWelcome, 'TELEGRAM_WELCOME_MESSAGE')} className="bg-card h-24"/>
                             <div className="flex gap-2 flex-wrap">
                                <Button variant="outline" size="sm" onClick={() => handleInsertVariable('{user_count}', 'telegram-welcome')}>&#123;user_count&#125;</Button>
                            </div>
                            <p className="text-xs text-muted-foreground">Это сообщение увидит пользователь при старте бота. Вы можете использовать переменные.</p>
                        </div>
                        <div className="space-y-2">
                            <Label htmlFor="telegram-connection-info">Информационное сообщение для "Установить связь"</Label>
                            <Textarea id="telegram-connection-info" value={telegramConnectionInfoMessage} onChange={(e) => setTelegramConnectionInfoMessage(e.target.value)} onBlur={() => handleBlurSave(telegramConnectionInfoMessage, 'TELEGRAM_CONNECTION_INFO_MESSAGE')} className="bg-card h-48"/>
                            <p className="text-xs text-muted-foreground">Это сообщение будет показано пользователю, когда он нажмет "Установить связь".</p>
                        </div>
                        <Separator/>
                        <div className="space-y-2">
                          <Label htmlFor="app-url">Публичный URL приложения (для Webhook)</Label>                          
                          <Input id="app-url" placeholder="https://your-app.com" value={appUrl} readOnly disabled className="bg-card cursor-not-allowed" />
                          <p className="text-xs text-muted-foreground">Этот URL определяется из переменной окружения `NEXT_PUBLIC_APP_URL`.</p>
                        </div>
                        <Button onClick={handleSetWebhook} variant="outline" disabled={isSettingWebhook || !telegramToken}>
                            {isSettingWebhook ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Plug className="mr-2 h-4 w-4" />}
                            Подключить бота (установить Webhook)
                        </Button>
                        {webhookLog && (
                          <div className="space-y-2">
                              <div className="flex items-center justify-between">
                                <Label htmlFor="webhook-log">Лог подключения</Label>
                                <Button variant="ghost" size="icon" onClick={() => copyToClipboard(webhookLog)} title="Скопировать лог">
                                    <Copy className="h-4 w-4" />
                                </Button>
                              </div>
                              <Textarea
                                  id="webhook-log"
                                  readOnly
                                  value={webhookLog}
                                  className="bg-card h-24 text-xs"
                                  placeholder="Здесь будет результат подключения..."
                              />
                          </div>
                        )}
                    </div>
                    </CardContent>
                 </Card>
            </TabsContent>
            <TabsContent value="telegram-logs">
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
            </TabsContent>
             <TabsContent value="project-logs">
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
            </TabsContent>
            <TabsContent value="settings">
                  <Card className="bg-secondary">
                    <CardHeader>
                        <div className="flex items-center justify-between">
                          <div>
                            <CardTitle>Настройки Системы</CardTitle>
                            <CardDescription>Управление подключениями, скрейпером и глобальными данными.</CardDescription>
                          </div>
                           <form action={lockSettings}>
                                <Button variant="outline">
                                    <Lock className="mr-2 h-4 w-4" />
                                    Заблокировать
                                </Button>
                            </form>
                        </div>
                    </CardHeader>
                    <CardContent className="space-y-6">
                      <div className="space-y-2">
                        <Label htmlFor="worker-id">Идентификатор воркера (из env)</Label>
                        <Input id="worker-id" placeholder="worker-1" value={workerId} readOnly disabled className="bg-card cursor-not-allowed"/>
                        <p className="text-xs text-muted-foreground">Этот ID берется из переменной окружения `WORKER_ID` и не может быть изменен здесь.</p>
                      </div>
                      <Separator />

                      <div>
                        <h3 className="text-lg font-medium text-foreground mb-4">Настройки Скрейпера (Глобальные)</h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                            <div className="space-y-2">
                                <Label htmlFor="scraper-batch-size">Размер пачки для парсинга</Label>
                                <Input id="scraper-batch-size" type="number" placeholder="25" value={scraperBatchSize} onChange={(e) => setScraperBatchSize(Number(e.target.value))} onBlur={() => handleBlurSave(scraperBatchSize, 'SCRAPER_BATCH_SIZE')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Кол-во ID, которое воркер берет из Redis за раз.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="scraper-write-batch-size">Размер пачки для записи в БД</Label>
                                <Input id="scraper-write-batch-size" type="number" placeholder="50" value={scraperWriteBatchSize} onChange={(e) => setScraperWriteBatchSize(Number(e.target.value))} onBlur={() => handleBlurSave(scraperWriteBatchSize, 'SCRAPER_WRITE_BATCH_SIZE')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Кол-во профилей для накопления перед записью в MongoDB.</p>
                            </div>
                             <div className="space-y-2">
                                <Label htmlFor="recent-profiles-limit">Лимит лога сессии</Label>
                                <Input id="recent-profiles-limit" type="number" placeholder="100" value={scraperRecentProfilesLimit} onChange={(e) => setScraperRecentProfilesLimit(Number(e.target.value))} onBlur={() => handleBlurSave(scraperRecentProfilesLimit, 'SCRAPER_RECENT_PROFILES_LIMIT')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Макс. кол-во профилей в логе на главной.</p>
                            </div>
                             <div className="space-y-2">
                                <Label htmlFor="telegram-logs-limit">Лимит логов Telegram</Label>
                                <Input id="telegram-logs-limit" type="number" placeholder="200" value={telegramLogsLimit} onChange={(e) => setTelegramLogsLimit(Number(e.target.value))} onBlur={() => handleBlurSave(telegramLogsLimit, 'TELEGRAM_LOGS_LIMIT')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Макс. кол-во запросов от Telegram для хранения.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="consecutive-error-limit">Лимит ошибок 404</Label>
                                <Input id="consecutive-error-limit" type="number" placeholder="100" value={scraperConsecutiveErrorLimit} onChange={(e) => setScraperConsecutiveErrorLimit(Number(e.target.value))} onBlur={() => handleBlurSave(scraperConsecutiveErrorLimit, 'SCRAPER_CONSECUTIVE_ERROR_LIMIT')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Кол-во ошибок "не найдено" подряд.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="pause-duration">Пауза после лимита 404 (часы)</Label>
                                <Input id="pause-duration" type="number" placeholder="6" value={scraperPauseDuration} onChange={(e) => setScraperPauseDuration(Number(e.target.value))} onBlur={() => handleBlurSave(scraperPauseDuration, 'SCRAPER_PAUSE_DURATION_MS')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">На сколько часов остановиться после лимита 404.</p>
                            </div>
                             <div className="space-y-2">
                                <Label htmlFor="project-logs-ttl">Время жизни обычных логов (минуты)</Label>
                                <Input id="project-logs-ttl" type="number" placeholder="60" value={projectLogsTtl} onChange={(e) => setProjectLogsTtl(Number(e.target.value))} onBlur={() => handleBlurSave(projectLogsTtl, 'PROJECT_LOGS_TTL_MINUTES')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Через сколько минут удалять обычные логи проекта.</p>
                            </div>
                        </div>
                      </div>
                      
                      <Separator/>
                      <div>
                        <h3 className="text-lg font-medium text-foreground mb-4">Настройки производительности и адаптации (Глобальные)</h3>
                          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                              <div className="space-y-2">
                                  <Label htmlFor="parallel-limit-min">Мин. параллельных запросов</Label>
                                  <Input id="parallel-limit-min" type="number" value={scraperParallelRequestLimitMin} onChange={(e) => setScraperParallelRequestLimitMin(Number(e.target.value))} onBlur={() => handleBlurSave(scraperParallelRequestLimitMin, 'SCRAPER_PARALLEL_REQUEST_LIMIT_MIN')} className="bg-card"/>
                                  <p className="text-xs text-muted-foreground">С какого кол-ва начинать.</p>
                              </div>
                               <div className="space-y-2">
                                  <Label htmlFor="parallel-limit-max">Макс. параллельных запросов</Label>
                                  <Input id="parallel-limit-max" type="number" value={scraperParallelRequestLimitMax} onChange={(e) => setScraperParallelRequestLimitMax(Number(e.target.value))} onBlur={() => handleBlurSave(scraperParallelRequestLimitMax, 'SCRAPER_PARALLEL_REQUEST_LIMIT_MAX')} className="bg-card"/>
                                  <p className="text-xs text-muted-foreground">"Потолок" лимита.</p>
                              </div>
                               <div className="space-y-2">
                                <Label htmlFor="adaptive-delay-min">Мин. адаптивная задержка (мс)</Label>
                                <Input id="adaptive-delay-min" type="number" value={scraperAdaptiveDelayMin} onChange={(e) => setScraperAdaptiveDelayMin(Number(e.target.value))} onBlur={() => handleBlurSave(scraperAdaptiveDelayMin, 'SCRAPER_ADAPTIVE_DELAY_MIN_MS')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Начальная пауза между пачками.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="adaptive-delay-max">Макс. адаптивная задержка (мс)</Label>
                                <Input id="adaptive-delay-max" type="number" value={scraperAdaptiveDelayMax} onChange={(e) => setScraperAdaptiveDelayMax(Number(e.target.value))} onBlur={() => handleBlurSave(scraperAdaptiveDelayMax, 'SCRAPER_ADAPTIVE_DELAY_MAX_MS')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">"Потолок" паузы.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="adaptive-delay-step">Шаг изменения задержки (мс)</Label>
                                <Input id="adaptive-delay-step" type="number" value={scraperAdaptiveDelayStep} onChange={(e) => setScraperAdaptiveDelayStep(Number(e.target.value))} onBlur={() => handleBlurSave(scraperAdaptiveDelayStep, 'SCRAPER_ADAPTIVE_DELAY_STEP_MS')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">На сколько менять паузу при адаптации.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="success-streak">Пачек для повышения лимита</Label>
                                <Input id="success-streak" type="number" value={scraperSuccessStreak} onChange={(e) => setScraperSuccessStreak(Number(e.target.value))} onBlur={() => handleBlurSave(scraperSuccessStreak, 'SCRAPER_SUCCESS_STREAK_TO_INCREASE_LIMIT')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Сколько успешных пачек нужно перед повышением лимита.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="delay-compensation">Компенсация задержки (мс)</Label>
                                <Input id="delay-compensation" type="number" value={scraperDelayCompensation} onChange={(e) => setScraperDelayCompensation(Number(e.target.value))} onBlur={() => handleBlurSave(scraperDelayCompensation, 'SCRAPER_DELAY_COMPENSATION_MS')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Доп. пауза за +1 к лимиту запросов.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="analysis-window">Окно для анализа (шт)</Label>
                                <Input id="analysis-window" type="number" value={scraperAnalysisWindow} onChange={(e) => setScraperAnalysisWindow(Number(e.target.value))} onBlur={() => handleBlurSave(scraperAnalysisWindow, 'SCRAPER_ANALYSIS_WINDOW')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Кол-во последних запросов для анализа.</p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="success-threshold">Процент успеха для стабилизации (%)</Label>
                                <Input id="success-threshold" type="number" value={scraperSuccessThreshold} onChange={(e) => setScraperSuccessThreshold(Number(e.target.value))} onBlur={() => handleBlurSave(scraperSuccessThreshold, 'SCRAPER_SUCCESS_THRESHOLD')} className="bg-card"/>
                                <p className="text-xs text-muted-foreground">Процент успеха для перехода в стабильный режим.</p>
                            </div>
                          </div>
                      </div>

                      <Separator />
                      <div>
                        <h3 className="text-lg font-medium text-foreground mb-4">Управление данными</h3>
                        <div className="space-y-4">
                            <div className="p-4 bg-card rounded-lg border space-y-4">
                                <div className="flex flex-row items-center justify-between">
                                  <div className="space-y-0.5">
                                    <Label className="text-base">Логирование скрейпера</Label>
                                    <CardDescription>
                                      Включает или отключает запись логов скрейпера в файл `logs/scraper.log`.
                                    </CardDescription>
                                  </div>
                                  <Switch
                                    checked={fileLoggingEnabled}
                                    onCheckedChange={(checked) => { setFileLoggingEnabled(checked); handleSaveConfig({ SCRAPER_FILE_LOGGING_ENABLED: checked }); }}
                                  />
                                </div>
                                <div className="flex flex-wrap gap-4 pt-2">
                                     <Button variant="outline" onClick={handleDownloadLogFile}>
                                        <Download className="mr-2 h-4 w-4" />
                                        Скачать лог-файл
                                    </Button>
                                    <AlertDialog>
                                        <AlertDialogTrigger asChild>
                                             <Button variant="destructive"><Trash2 className="mr-2 h-4 w-4" />Очистить лог-файл</Button>
                                        </AlertDialogTrigger>
                                        <AlertDialogContent>
                                            <AlertDialogHeader>
                                                <AlertDialogTitle>Вы уверены?</AlertDialogTitle>
                                                <AlertDialogDescription>
                                                    Это действие необратимо. Файл `scraper.log` будет полностью очищен.
                                                </AlertDialogDescription>
                                            </AlertDialogHeader>
                                            <AlertDialogFooter>
                                                <AlertDialogCancel>Отмена</AlertDialogCancel>
                                                <AlertDialogAction onClick={handleClearLogFile} className="bg-destructive hover:bg-destructive/90">
                                                    Да, очистить
                                                </AlertDialogAction>
                                            </AlertDialogFooter>
                                        </AlertDialogContent>
                                    </AlertDialog>
                                </div>
                            </div>
                            <div className="flex flex-wrap gap-4 pt-2">
                                 <AlertDialog onOpenChange={(open) => !open && setIntegrityCheckResult(null)}>
                                    <AlertDialogTrigger asChild>
                                         <Button variant="outline" onClick={handleCheckIntegrity} disabled={isCheckingIntegrity}>
                                            {isCheckingIntegrity ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <ShieldCheck className="mr-2 h-4 w-4" />}
                                            Проверить целостность БД
                                        </Button>
                                    </AlertDialogTrigger>
                                    {integrityCheckResult && (
                                        <AlertDialogContent>
                                            <AlertDialogHeader>
                                                <AlertDialogTitle>Результат проверки</AlertDialogTitle>
                                                <AlertDialogDescription>
                                                    {integrityCheckResult.missingCount > 0 
                                                        ? `Обнаружено ${integrityCheckResult.missingCount} пропущенных ID. Добавить их в приоритетную очередь для обработки?`
                                                        : "Проверка завершена. Все ID на месте!"}
                                                </AlertDialogDescription>
                                            </AlertDialogHeader>
                                            <AlertDialogFooter>
                                                <AlertDialogCancel onClick={() => setIntegrityCheckResult(null)}>Закрыть</AlertDialogCancel>
                                                {integrityCheckResult.missingCount > 0 && (
                                                    <AlertDialogAction onClick={handleQueueMissingIds} disabled={isQueueingMissing}>
                                                        {isQueueingMissing ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                                                        Да, обработать
                                                    </AlertDialogAction>
                                                )}
                                            </AlertDialogFooter>
                                        </AlertDialogContent>
                                    )}
                                </AlertDialog>
                                 <Button variant="outline" onClick={handleDeduplicate} disabled={isDeduplicating}>
                                    {isDeduplicating ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <DatabaseZap className="mr-2 h-4 w-4" />}
                                    Удалить дубликаты
                                </Button>
                                <Button onClick={handleExport} disabled={isExporting}>
                                {isExporting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Download className="mr-2 h-4 w-4" />}
                                Экспорт БД
                                </Button>
                                <AlertDialog open={!!fileToImport} onOpenChange={(open) => !open && setFileToImport(null)}>
                                <AlertDialogTrigger asChild>
                                    <Button variant="outline" onClick={handleTriggerImport} disabled={isImporting}>
                                        {isImporting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Upload className="mr-2 h-4 w-4" />}
                                        Импорт БД
                                    </Button>
                                </AlertDialogTrigger>
                                <AlertDialogContent>
                                    <AlertDialogHeader>
                                    <AlertDialogTitle>Подтверждение импорта</AlertDialogTitle>
                                    <AlertDialogDescription>
                                        Вы уверены, что хотите импортировать файл <span className="font-bold text-foreground">{fileToImport?.name}</span>? Это действие перезапишет все существующие данные в коллекции `users`. Это действие необратимо.
                                    </AlertDialogDescription>
                                    </AlertDialogHeader>
                                    <AlertDialogFooter>
                                    <AlertDialogCancel onClick={()=>{ setFileToImport(null)}}>Отмена</AlertDialogCancel>
                                    <AlertDialogAction onClick={handleConfirmImport} disabled={isImporting} className="bg-destructive hover:bg-destructive/90">
                                        {isImporting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                                        Да, импортировать
                                    </AlertDialogAction>
                                    </AlertDialogFooter>
                                </AlertDialogContent>
                                </AlertDialog>
                                <input 
                                type="file" 
                                ref={fileInputRef} 
                                className="hidden" 
                                accept=".json"
                                onChange={handleFileSelect} 
                                />
                                <AlertDialog>
                                <AlertDialogTrigger asChild>
                                    <Button variant="destructive"><Trash2 className="mr-2 h-4 w-4" />Очистить БД</Button>
                                </AlertDialogTrigger>
                                <AlertDialogContent>
                                    <AlertDialogHeader>
                                    <AlertDialogTitle>Вы абсолютно уверены?</AlertDialogTitle>
                                    <AlertDialogDescription>
                                        Это действие необратимо. Все данные скрейпинга, включая
                                        статистику и найденные профили, будут навсегда удалены.
                                    </AlertDialogDescription>
                                    </AlertDialogHeader>
                                    <AlertDialogFooter>
                                    <AlertDialogCancel>Отмена</AlertDialogCancel>
                                    <AlertDialogAction onClick={handleClearDB} className="bg-destructive hover:bg-destructive/90">
                                        Да, очистить
                                    </AlertDialogAction>
                                    </AlertDialogFooter>
                                </AlertDialogContent>
                                </AlertDialog>
                                <Button variant="outline" onClick={handleDownloadProject} disabled={isDownloadingProject}>
                                <Archive className="mr-2 h-4 w-4" />
                                {isDownloadingProject ? 'Архивация...' : 'Скачать проект'}
                                </Button>
                            </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
              
            </TabsContent>
             <TabsContent value="access-settings">
                <Card className="bg-secondary">
                    <CardHeader>
                        <CardTitle>Доступ к настройкам системы</CardTitle>
                        <CardDescription>
                         Для доступа к глобальным настройкам системы введите пароль, указанный в переменной окружения SETTINGS_PASSWORD.
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <form onSubmit={handleUnlockSettingsSubmit} className="max-w-sm space-y-4">
                            {settingsPasswordError && (
                                <Alert variant="destructive">
                                    <AlertTriangle className="h-4 w-4" />
                                    <AlertTitle>{settingsPasswordError}</AlertTitle>
                                </Alert>
                            )}
                            <div className="space-y-2">
                                <Label htmlFor="settings-password">Пароль администратора</Label>
                                <Input 
                                    id="settings-password" 
                                    name="password"
                                    type="password" 
                                    value={settingsPasswordInput}
                                    onChange={(e) => setSettingsPasswordInput(e.target.value)}
                                />
                            </div>
                            <Button type="submit" disabled={isUnlocking}>
                                {isUnlocking ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                                Разблокировать
                            </Button>
                        </form>
                    </CardContent>
                </Card>
              </TabsContent>
          </Tabs>
        </CardContent>
      </Card>
    </div>
  );
}

    