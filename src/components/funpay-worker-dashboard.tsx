

"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
  DialogClose,
} from "@/components/ui/dialog";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { useToast } from "@/hooks/use-toast";
import { downloadProject, unlockSettings, lockSettings, syncWithGitHub } from "@/app/actions";
import * as api from "@/lib/api";
import { MainDashboard } from "@/components/dashboard-tabs/main-dashboard";
import { TelegramDashboard } from "@/components/dashboard-tabs/telegram-dashboard";
import { SearchTab } from "@/components/dashboard-tabs/search-tab";
import { ProductsTab } from "@/components/dashboard-tabs/products-tab";
import { AdvertisingTab } from "@/components/dashboard-tabs/advertising-tab";
import { TelegramSettingsTab } from "@/components/dashboard-tabs/telegram-settings-tab";
import { LogsTabs } from "@/components/dashboard-tabs/logs-tabs";
import { SystemSettingsTab } from "@/components/dashboard-tabs/system-settings-tab";
import { AccessSettingsTab } from "@/components/dashboard-tabs/access-settings-tab";
import { RpsChart } from "@/components/rps-chart";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";


import {
  BarChart3,
  Search,
  Settings,
  FileText,
  Bot,
  Store,
  Send,
  Lock,
  Unlock,
  Users,
} from "lucide-react";
import type { Profile, Product, DbStatus, TelegramLog, ProjectLog, WorkerStatus, ProductView, Campaign, CampaignTemplate, CustomLink, BackgroundExportStatus, TelegramStats, BotUser, RpsDataPoint } from "@/types";

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
  const [telegramStats, setTelegramStats] = useState<TelegramStats>({
    totalUsers: 0,
    activeUsers: 0,
    blockedUsers: 0,
    newToday: 0,
    newThisWeek: 0,
  });
   const [botUsers, setBotUsers] = useState<BotUser[]>([]);
  const [botUsersPage, setBotUsersPage] = useState(1);
  const [botUsersTotalPages, setBotUsersTotalPages] = useState(1);
  const [botUsersSearchQuery, setBotUsersSearchQuery] = useState("");

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
  const tgFileInputRef = useRef<HTMLInputElement>(null);

  const [isImporting, setIsImporting] = useState(false);
  const [fileToImport, setFileToImport] = useState<File | null>(null);
  const [isImportingTg, setIsImportingTg] = useState(false);
  const [fileToImportTg, setFileToImportTg] = useState<File | null>(null);

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

  const [scrapingTarget, setScrapingTarget] = useState(0);
  const [scraperPauseDuration, setScraperPauseDuration] = useState(6);
  const [scraperConsecutiveErrorLimit, setScraperConsecutiveErrorLimit] = useState(100);
  const [scraperRecentProfilesLimit, setScraperRecentProfilesLimit] = useState(100);
  const [scraperBatchSize, setScraperBatchSize] = useState(25);
  const [scraperWriteBatchSize, setScraperWriteBatchSize] = useState(50);
  const [projectLogsTtl, setProjectLogsTtl] = useState(60);
  const [scraperParallelRequestLimitMin, setScraperParallelRequestLimitMin] = useState(1);
  const [scraperParallelRequestLimitMax, setScraperParallelRequestLimitMax] = useState(10);
  const [fileLoggingEnabled, setFileLoggingEnabled] = useState(true);
  const [scraperSkipIntegrityCheck, setScraperSkipIntegrityCheck] = useState(false);
  const [scraperAdaptiveDelayMin, setScraperAdaptiveDelayMin] = useState(50);
  const [scraperAdaptiveDelayMax, setScraperAdaptiveDelayMax] = useState(5000);
  const [scraperAdaptiveDelayStep, setScraperAdaptiveDelayStep] = useState(50);
  const [scraperSuccessStreak, setScraperSuccessStreak] = useState(3);
  const [scraperAnalysisWindow, setScraperAnalysisWindow] = useState(200);
  const [scraperSuccessThreshold, setScraperSuccessThreshold] = useState(99);
  const [scraperIntegrityCheckBatchSize, setScraperIntegrityCheckBatchSize] = useState(50000);
  const [githubRepoUrl, setGithubRepoUrl] = useState("");
  const [githubToken, setGithubToken] = useState("");
  const [isSyncing, setIsSyncing] = useState(false);


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

  const [bgExportStatus, setBgExportStatus] = useState<BackgroundExportStatus>({ status: 'idle', progress: 0, total: 0, filePath: null, error: null });
  const [isBgExportActionLoading, setIsBgExportActionLoading] = useState(false);
  const [tgBgExportStatus, setTgBgExportStatus] = useState<BackgroundExportStatus>({ status: 'idle', progress: 0, total: 0, filePath: null, error: null });
  const [isTgBgExportActionLoading, setIsTgBgExportActionLoading] = useState(false);

  const [campaigns, setCampaigns] = useState<Campaign[]>([]);
  const [currentCampaign, setCurrentCampaign] = useState<Partial<Campaign>>({ name: '', text: '', imageUrl: '', lifetimeHours: 24 });
  const [isCreatingCampaign, setIsCreatingCampaign] = useState(false);
  const [isCampaignActionLoading, setIsCampaignActionLoading] = useState<string | null>(null);

  const [campaignTemplates, setCampaignTemplates] = useState<CampaignTemplate[]>([]);
  const [isTemplateDialogOpen, setIsTemplateDialogOpen] = useState(false);
  const [currentTemplate, setCurrentTemplate] = useState<Partial<CampaignTemplate> | null>(null);
  
  const [eta, setEta] = useState<{ days: number; hours: number; totalRps: number } | null>(null);
  const [isRpsModalOpen, setIsRpsModalOpen] = useState(false);


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
  
  const fetchTelegramStats = useCallback(async () => {
    try {
      const response = await fetch('/api/telegram/stats');
      if (response.ok) {
        const data = await response.json();
        setTelegramStats(data);
      }
    } catch (error) {
      console.error('Error fetching Telegram stats:', error);
    }
  }, []);

  const fetchBotUsers = useCallback(async (page = 1, search = botUsersSearchQuery) => {
    try {
        const response = await fetch(`/api/telegram/stats?type=users&page=${page}&limit=15&search=${encodeURIComponent(search)}`);
        if(response.ok) {
            const data = await response.json();
            setBotUsers(data.users);
            setBotUsersPage(data.currentPage);
            setBotUsersTotalPages(data.totalPages);
        }
    } catch (error) {
        console.error("Error fetching bot users", error);
    }
  }, [botUsersSearchQuery]);

  const handleBotUsersSearch = () => {
    setBotUsersPage(1);
    fetchBotUsers(1, botUsersSearchQuery);
  }

  const handleBotUserPageChange = (newPage: number) => {
      setBotUsersPage(newPage);
      fetchBotUsers(newPage, botUsersSearchQuery);
  }


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
        if (!response.ok) {
            console.error('Failed to fetch project logs, status:', response.status);
            return;
        }

        const data: { logs: ProjectLog[], criticalLogs: ProjectLog[] } = await response.json();
        setProjectLogs(data.logs.reverse());
        setCriticalLogs(data.criticalLogs.reverse());
    } catch (error) {
        console.error('Error fetching project logs:', error);
    }
}, []);

  const calculateEta = useCallback(async () => {
    try {
      const response = await fetch('/api/telegram/stats?type=rps');
      if (response.ok) {
        const rpsData: RpsDataPoint[] = await response.json();
        if (rpsData.length > 0) {
          // Get the most recent total RPS
          const latestRps = rpsData[rpsData.length - 1].rps;

          if (scrapingTarget > 0 && latestRps > 0 && stats.totalUsersInDb > 0) {
            const profilesNeeded = scrapingTarget - stats.totalUsersInDb;
            if (profilesNeeded > 0) {
              const seconds = profilesNeeded / latestRps;
              const hours = seconds / 3600;
              const days = hours / 24;
              setEta({ days, hours, totalRps: latestRps });
            } else {
              setEta(null); // Target met
            }
          } else {
            setEta(null);
          }
        } else {
          setEta(null);
        }
      }
    } catch (e) {
      console.error("Failed to calculate ETA", e);
      setEta(null);
    }
  }, [scrapingTarget, stats.totalUsersInDb]);


  const fetchCampaigns = useCallback(async () => {
    try {
      const response = await fetch('/api/campaigns');
      if (response.ok) {
        const data = await response.json();
        setCampaigns(data.campaigns);
      } else {
        const errorData = await response.json();
        if (response.status !== 403) {
          toast({ variant: 'destructive', title: 'Ошибка', description: `Не удалось загрузить кампании: ${errorData.error}` });
        }
      }
    } catch (error: any) {
      toast({ variant: 'destructive', title: 'Ошибка', description: `Не удалось загрузить кампании: ${error.message}` });
    }
  }, [toast]);


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
        setScrapingTarget(data.SCRAPER_TARGET_GOAL || 0);
        setScraperPauseDuration((data.SCRAPER_PAUSE_DURATION_MS || 21600000) / 1000 / 60 / 60);
        setScraperConsecutiveErrorLimit(data.SCRAPER_CONSECUTIVE_ERROR_LIMIT || 100);
        setScraperRecentProfilesLimit(data.SCRAPER_RECENT_PROFILES_LIMIT || 100);
        setScraperBatchSize(data.SCRAPER_BATCH_SIZE || 25);
        setScraperWriteBatchSize(data.SCRAPER_WRITE_BATCH_SIZE || 50);
        setProjectLogsTtl(data.PROJECT_LOGS_TTL_MINUTES || 60);
        setScraperParallelRequestLimitMin(data.SCRAPER_PARALLEL_REQUEST_LIMIT_MIN || 1);
        setScraperParallelRequestLimitMax(data.SCRAPER_PARALLEL_REQUEST_LIMIT_MAX || 10);
        setFileLoggingEnabled(data.SCRAPER_FILE_LOGGING_ENABLED === undefined ? true : data.SCRAPER_FILE_LOGGING_ENABLED);
        setScraperSkipIntegrityCheck(data.SCRAPER_SKIP_INTEGRITY_CHECK || false);
        setScraperAdaptiveDelayMin(data.SCRAPER_ADAPTIVE_DELAY_MIN_MS || 50);
        setScraperAdaptiveDelayMax(data.SCRAPER_ADAPTIVE_DELAY_MAX_MS || 5000);
        setScraperAdaptiveDelayStep(data.SCRAPER_ADAPTIVE_DELAY_STEP_MS || 50);
        setIsSettingsUnlocked(data.isSettingsUnlocked || false);
        setScraperSuccessStreak(data.SCRAPER_SUCCESS_STREAK_TO_INCREASE_LIMIT || 3);
        setScraperAnalysisWindow(data.SCRAPER_ANALYSIS_WINDOW || 200);
        setScraperSuccessThreshold(data.SCRAPER_SUCCESS_THRESHOLD || 99);
        setScraperIntegrityCheckBatchSize(data.SCRAPER_INTEGRITY_CHECK_BATCH_SIZE || 50000);
        setGithubRepoUrl(data.GITHUB_REPO_URL || "");
        setGithubToken(data.GITHUB_TOKEN || "");
      } else {
        throw new Error("Failed to fetch config from server.");
      }
    } catch (error) {
      console.error('Error fetching config:', error);
      toast({ variant: "destructive", title: "Ошибка загрузки настроек", description: "Не удалось загрузить конфигурацию с сервера." });
    }
  }, [toast]);

  const fetchBgExportStatus = useCallback(async () => {
    try {
      const response = await fetch('/api/backup/background');
      if (response.ok) {
        const data = await response.json();
        setBgExportStatus(data);
      }
    } catch (error) {
      // Silently fail, as this is a background check
      console.error("Failed to fetch bg export status", error);
    }
  }, []);

  const fetchTgBgExportStatus = useCallback(async () => {
    try {
      const response = await fetch('/api/backup/telegram');
      if (response.ok) {
        const data = await response.json();
        setTgBgExportStatus(data);
      }
    } catch (error) {
      console.error("Failed to fetch tg bg export status", error);
    }
  }, []);

  useEffect(() => {
    fetchConfig();
    checkDbStatus();
    fetchDashboardData();
    fetchTelegramStats();
    fetchProductsAndCategories();
    fetchBgExportStatus();
    fetchTgBgExportStatus();
  }, [fetchConfig, fetchProductsAndCategories, fetchDashboardData, fetchTelegramStats, fetchBgExportStatus, fetchTgBgExportStatus]);

  useEffect(() => {
    let dataInterval: NodeJS.Timeout;

    const setupIntervals = () => {
      if (document.visibilityState !== 'visible') return;

      if (activeTab === 'dashboard') {
        checkScrapingStatus();
        fetchDashboardData();
        calculateEta();
      } else if (activeTab === 'telegram-panel') {
        fetchTelegramStats();
        fetchDashboardData(); // For active connections
        fetchBotUsers(botUsersPage, botUsersSearchQuery);
      } else if (activeTab === 'telegram-logs' || activeTab === 'project-logs') {
        if (activeTab === 'telegram-logs') fetchTelegramLogs();
        if (activeTab === 'project-logs') fetchProjectLogs();
      } else if (activeTab === 'settings' || activeTab === 'telegram-bot-settings') {
        fetchBgExportStatus();
        fetchTgBgExportStatus();
      } else if (activeTab === 'advertising') {
        fetchCampaigns();
      }
    };

    setupIntervals(); // Initial fetch on tab change
    dataInterval = setInterval(setupIntervals, 5000);

    if (activeTab === 'products') {
      fetchProductsAndCategories();
      setCurrentCategoryView(null);
      setProductView('list');
    }

    if (activeTab === 'advertising') {
      fetchCampaigns();
    }

    // Auto-switch from protected tab if lock is engaged
    if (activeTab === 'settings' && !isSettingsUnlocked) {
      setActiveTab('access-settings');
    }

    return () => {
      if (dataInterval) clearInterval(dataInterval);
    }
  }, [activeTab, checkScrapingStatus, fetchTelegramLogs, fetchProjectLogs, isSettingsUnlocked, fetchDashboardData, fetchTelegramStats, fetchBgExportStatus, fetchTgBgExportStatus, fetchCampaigns, fetchProductsAndCategories, fetchBotUsers, botUsersPage, botUsersSearchQuery, calculateEta]);

  useEffect(() => {
    if (activeTab === 'search' && dbSearchQuery && searchType) {
        handleSearch(dbSearchQuery, searchType);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, dbSearchQuery, searchType]);

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
        let errorMsg = 'Не удалось очистить логи';
        if (response.status === 403) {
          errorMsg = "Доступ запрещен. Разблокируйте настройки.";
        } else {
          try {
            const data = await response.json();
            errorMsg = data.error || errorMsg;
          } catch (e) {
            // Ignore if response is not JSON
          }
        }
        throw new Error(errorMsg);
      }
      const data = await response.json();
      toast({
        title: "Логи Telegram очищены",
        description: data.message,
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

  const handleStatusCardClick = async (status: 'support' | 'banned' | 'total' | 'foundByWorker') => {
    if (isRecounting) return;
    setIsRecounting(status);
    try {
      const response = await fetch('/api/data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'recount', status }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.error || `Failed to recount ${status}`);
      }
      
      const toastMessageMap = {
        successful: 'общее количество',
        support: 'саппортов',
        banned: 'забаненных',
        foundByWorker: 'найденных этим воркером'
      };
      
      toast({
        title: 'Статистика обновлена',
        description: `Найдено ${result.count.toLocaleString()} ${toastMessageMap[result.field as keyof typeof toastMessageMap]}.`,
      });
      
      if(result.field === 'successful') {
        setStats(prev => ({ ...prev, totalUsersInDb: result.count }));
      } else {
        setStats(prev => ({ ...prev, [result.field]: result.count }));
      }
      
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
        if (fileInputRef.current) fileInputRef.current.value = "";
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
  
  const handleFileSelectTg = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.files && event.target.files.length > 0) {
      setFileToImportTg(event.target.files[0]);
    } else {
      setFileToImportTg(null);
    }
  };

  const handleTriggerImportTg = () => {
    tgFileInputRef.current?.click();
  };

  const handleConfirmImportTg = async () => {
    if (!fileToImportTg) return;

    setIsImportingTg(true);
    const reader = new FileReader();
    reader.onload = async (e) => {
      try {
        const content = e.target?.result;
        if (typeof content !== 'string') throw new Error("Failed to read file content.");
        const data = JSON.parse(content);
        
        const response = await fetch('/api/backup/telegram?action=import', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(data),
        });

        const result = await response.json();
        if (!response.ok) throw new Error(result.error || 'Failed to import Telegram database');

        toast({ title: "Импорт завершен", description: result.message });
        await fetchTelegramStats();
        
      } catch (error: any) {
        toast({
          variant: "destructive",
          title: "Ошибка импорта БД ТГ",
          description: error.message || "Убедитесь, что это корректный JSON файл.",
        });
      } finally {
        setIsImportingTg(false);
        setFileToImportTg(null);
        if (tgFileInputRef.current) tgFileInputRef.current.value = "";
      }
    };
    reader.onerror = () => {
      toast({ variant: "destructive", title: "Ошибка чтения файла", description: "Не удалось прочитать выбранный файл." });
      setIsImportingTg(false);
      setFileToImportTg(null);
    };
    reader.readAsText(fileToImportTg);
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
    if (!isSettingsUnlocked) {
      toast({
        variant: 'destructive',
        title: 'Доступ запрещен',
        description: 'Скачивание проекта доступно только с разблокированными настройками.',
      });
      return;
    }
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
  
  const handleSyncWithGitHub = async () => {
    setIsSyncing(true);
    toast({
        title: 'Синхронизация с GitHub запущена...',
        description: 'Это может занять некоторое время. Пожалуйста, подождите.',
    });
    try {
        const result = await syncWithGitHub({
            repoUrl: githubRepoUrl,
            token: githubToken,
        });

        if (result.success) {
            toast({
                title: 'Синхронизация успешна',
                description: result.message || 'Все изменения отправлены в ваш GitHub репозиторий.',
                duration: 10000,
            });
        } else {
             toast({
                variant: 'destructive',
                title: 'Ошибка синхронизации',
                description: result.error,
                duration: 15000,
            });
        }
    } catch (error: any) {
         toast({
            variant: 'destructive',
            title: 'Критическая ошибка',
            description: error.message || 'Не удалось выполнить синхронизацию.',
            duration: 15000,
        });
    } finally {
        setIsSyncing(false);
    }
  }

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

  const handleBgExportAction = async (action: 'start' | 'stop' | 'clear') => {
    setIsBgExportActionLoading(true);
    try {
      const response = await fetch('/api/backup/background', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Не удалось выполнить действие');
      }
      toast({ title: "Успех", description: data.message });
      fetchBgExportStatus();
    } catch (error: any) {
      toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
    } finally {
      setIsBgExportActionLoading(false);
    }
  };

  const handleTgBgExportAction = async (action: 'start' | 'stop' | 'clear') => {
    setIsTgBgExportActionLoading(true);
    try {
      const response = await fetch('/api/backup/telegram', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Не удалось выполнить действие');
      }
      toast({ title: "Успех", description: data.message });
      fetchTgBgExportStatus();
    } catch (error: any) {
      toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
    } finally {
      setIsTgBgExportActionLoading(false);
    }
  };

  const handleSaveTemplate = () => {
    if (!currentTemplate || !currentTemplate.name || !currentTemplate.text) {
      toast({ variant: 'destructive', title: 'Ошибка', description: 'Название и текст шаблона не могут быть пустыми.' });
      return;
    }

    if (currentTemplate.id) { // Editing existing
      setCampaignTemplates(prev => prev.map(t => t.id === currentTemplate.id ? t : t));
      toast({ title: 'Шаблон обновлен' });
    } else { // Creating new
      const newTemplate: CampaignTemplate = { ...currentTemplate, id: Date.now().toString() } as CampaignTemplate;
      setCampaignTemplates(prev => [...prev, newTemplate]);
      toast({ title: 'Шаблон создан' });
    }

    setIsTemplateDialogOpen(false);
    setCurrentTemplate(null);
  };

  const handleDeleteTemplate = (id: string) => {
    setCampaignTemplates(prev => prev.filter(t => t.id !== id));
    toast({ variant: 'destructive', title: 'Шаблон удален' });
  };

  const handleInsertTemplate = (text: string) => {
    setCurrentCampaign(prev => ({ ...prev, text: (prev.text || '') + text }));
    toast({ title: 'Шаблон вставлен' });
  }
  
  const handleCampaignAction = async (action: 'restart' | 'stop' | 'delete', campaignId: string) => {
    setIsCampaignActionLoading(campaignId);
    try {
        const isDelete = action === 'delete';
        const method = isDelete ? 'DELETE' : (action === 'stop' ? 'PUT' : 'POST');
        const body = isDelete 
            ? { campaignId } 
            : { action, campaignId };

        const response = await fetch('/api/campaigns', {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });

        const result = await response.json();
        if (!response.ok) {
            throw new Error(result.error || `Не удалось выполнить действие: ${action}`);
        }
        
        toast({ title: 'Успех', description: result.message || `Действие "${action}" выполнено.` });
        
        if (isDelete) {
            setCampaigns(prev => prev.filter(c => c._id !== campaignId));
        } else {
            fetchCampaigns(); // Refresh the list
        }

    } catch (error: any) {
        toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
    } finally {
        setIsCampaignActionLoading(null);
    }
};

  const handleCreateCampaign = async () => {
    if (!currentCampaign.name || !currentCampaign.text) {
      toast({ variant: 'destructive', title: 'Ошибка', description: 'Название и текст рассылки обязательны.' });
      return;
    }
    setIsCreatingCampaign(true);
    try {
      const response = await fetch('/api/campaigns', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(currentCampaign)
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.error || 'Не удалось создать кампанию.');
      }
      toast({ title: 'Успех', description: 'Рассылка запущена.' });
      setCampaigns(prev => [result.campaign, ...prev]);
      setCurrentCampaign({ name: '', text: '', lifetimeHours: 24, imageUrl: '' });
    } catch (error: any) {
      toast({ variant: 'destructive', title: 'Ошибка создания кампании', description: error.message });
    } finally {
      setIsCreatingCampaign(false);
    }
  };

  const handleToggleUserBan = async (chatId: number, currentStatus: 'active' | 'banned') => {
      const newStatus = currentStatus === 'active' ? 'banned' : 'active';
      try {
        const response = await fetch('/api/telegram/stats', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chatId, status: newStatus }),
        });
        const result = await response.json();
        if (!response.ok) {
            throw new Error(result.error || 'Не удалось обновить статус');
        }
        toast({ title: 'Успех', description: `Статус пользователя обновлен на "${newStatus}".` });
        fetchBotUsers(botUsersPage, botUsersSearchQuery); // Refresh the user list
        fetchTelegramStats(); // Refresh the stats
      } catch (error: any) {
         toast({ variant: 'destructive', title: 'Ошибка', description: error.message });
      }
  };


  const sortedRecentProfiles = [...recentProfiles].sort((a, b) => b.id - a.id);

  const filteredProfiles = sortedRecentProfiles.filter(profile =>
    profile.nickname.toLowerCase().includes(listSearchQuery.toLowerCase())
  );

  const CRITICAL_KEYWORDS = ['Error', 'CRITICAL', 'Failed', '⚠️', 'КРИТИЧЕСКАЯ ОШИБКА', 'Ошибка'];
  const getLogClass = (message: string) => {
    if (CRITICAL_KEYWORDS.some(keyword => message.includes(keyword))) {
      return 'text-red-400';
    }
    return '';
  };


  const renderTabContent = () => {
    switch (activeTab) {
      case "dashboard":
        return <MainDashboard
          stats={stats}
          recentProfiles={recentProfiles}
          filteredProfiles={filteredProfiles}
          listSearchQuery={listSearchQuery}
          setListSearchQuery={setListSearchQuery}
          scraperRecentProfilesLimit={scraperRecentProfilesLimit}
          mongoStatus={mongoStatus}
          redisStatus={redisStatus}
          isScraping={isScraping}
          scraperWorkerId={scraperWorkerId}
          isActionLoading={isActionLoading}
          isWorkerManagerOpen={isWorkerManagerOpen}
          setIsWorkerManagerOpen={setIsWorkerManagerOpen}
          handleScraperAction={handleScraperAction}
          isWorkerActionLoading={isWorkerActionLoading}
          isSettingsUnlocked={isSettingsUnlocked}
          isRecounting={isRecounting}
          handleStatusCardClick={handleStatusCardClick}
          workerId={workerId}
          setActiveTab={setActiveTab}
          setSearchType={setSearchType}
          setDbSearchQuery={setDbSearchQuery}
          scrapingTarget={scrapingTarget}
          eta={eta}
          onRpsClick={() => setIsRpsModalOpen(true)}
        />;
    case "telegram-panel":
        return <TelegramDashboard 
          stats={telegramStats} 
          activeConnections={stats.activeConnections}
          botUsers={botUsers}
          botUsersPage={botUsersPage}
          botUsersTotalPages={botUsersTotalPages}
          onPageChange={handleBotUserPageChange}
          searchQuery={botUsersSearchQuery}
          setSearchQuery={setBotUsersSearchQuery}
          handleSearch={handleBotUsersSearch}
          handleToggleUserBan={handleToggleUserBan}
        />;
      case "search":
        return <SearchTab
          handleSearchSubmit={handleSearchSubmit}
          searchType={searchType}
          setSearchType={setSearchType}
          dbSearchQuery={dbSearchQuery}
          setDbSearchQuery={setDbSearchQuery}
          isSearching={isSearching}
          searchResults={searchResults}
          setSearchResults={setSearchResults}
        />;
      case "products":
        return <ProductsTab
          productView={productView}
          currentProduct={currentProduct}
          handleSaveProduct={handleSaveProduct}
          setProductView={setProductView}
          setCurrentProduct={setCurrentProduct}
          isSavingProduct={isSavingProduct}
          telegramPaymentCurrency={telegramPaymentCurrency}
          currentCategoryView={currentCategoryView}
          setCurrentCategoryView={setCurrentCategoryView}
          openProductForm={openProductForm}
          isAddingCategory={isAddingCategory}
          newCategory={newCategory}
          setNewCategory={setNewCategory}
          handleAddCategory={handleAddCategory}
          productCategories={productCategories}
          handleDeleteCategory={handleDeleteCategory}
          products={products}
          handleDeleteProduct={handleDeleteProduct}
        />;
      case "advertising":
        return <AdvertisingTab
          currentCampaign={currentCampaign}
          setCurrentCampaign={setCurrentCampaign}
          campaigns={campaigns}
          campaignTemplates={campaignTemplates}
          setCurrentTemplate={setCurrentTemplate}
          setIsTemplateDialogOpen={setIsTemplateDialogOpen}
          handleInsertTemplate={handleInsertTemplate}
          handleDeleteTemplate={handleDeleteTemplate}
          isCreatingCampaign={isCreatingCampaign}
          handleCreateCampaign={handleCreateCampaign}
          handleCampaignAction={handleCampaignAction}
          isCampaignActionLoading={isCampaignActionLoading}
        />;
      case "telegram-bot-settings":
        return <TelegramSettingsTab
          telegramToken={telegramToken}
          setTelegramToken={setTelegramToken}
          handleBlurSave={handleBlurSave}
          telegramBotLink={telegramBotLink}
          setTelegramBotLink={setTelegramBotLink}
          telegramProviderToken={telegramProviderToken}
          setTelegramProviderToken={setTelegramProviderToken}
          telegramPaymentCurrency={telegramPaymentCurrency}
          setTelegramPaymentCurrency={setTelegramPaymentCurrency}
          handleSaveConfig={handleSaveConfig}
          telegramShopButtonName={telegramShopButtonName}
          setTelegramShopButtonName={setTelegramShopButtonName}
          telegramPaymentEnabled={telegramPaymentEnabled}
          setTelegramPaymentEnabled={setTelegramPaymentEnabled}
          telegramSearchCost={telegramSearchCost}
          setTelegramSearchCost={setTelegramSearchCost}
          telegramSearchCostReal={telegramSearchCostReal}
          setTelegramSearchCostReal={setTelegramSearchCostReal}
          telegramConnectionPaymentEnabled={telegramConnectionPaymentEnabled}
          setTelegramConnectionPaymentEnabled={setTelegramConnectionPaymentEnabled}
          telegramConnectionCost={telegramConnectionCost}
          setTelegramConnectionCost={setTelegramConnectionCost}
          telegramConnectionCostReal={telegramConnectionCostReal}
          setTelegramConnectionCostReal={setTelegramConnectionCostReal}
          telegramCustomLinks={telegramCustomLinks}
          handleCustomLinkChange={handleCustomLinkChange}
          handleSaveCustomLinks={handleSaveCustomLinks}
          handleRemoveCustomLink={handleRemoveCustomLink}
          handleAddCustomLink={handleAddCustomLink}
          telegramWelcomeImageUrl={telegramWelcomeImageUrl}
          setTelegramWelcomeImageUrl={setTelegramWelcomeImageUrl}
          telegramWelcome={telegramWelcome}
          setTelegramWelcome={setTelegramWelcome}
          handleInsertVariable={handleInsertVariable}
          telegramConnectionInfoMessage={telegramConnectionInfoMessage}
          setTelegramConnectionInfoMessage={setTelegramConnectionInfoMessage}
          appUrl={appUrl}
          isSettingWebhook={isSettingWebhook}
          handleSetWebhook={handleSetWebhook}
          webhookLog={webhookLog}
          copyToClipboard={copyToClipboard}
        />;
      case "telegram-logs":
      case "project-logs":
        return <LogsTabs
          activeTab={activeTab}
          telegramLogsLimit={telegramLogsLimit}
          isSettingsUnlocked={isSettingsUnlocked}
          handleClearTelegramLogs={handleClearTelegramLogs}
          telegramLogs={telegramLogs}
          projectLogs={projectLogs}
          criticalLogs={criticalLogs}
          copyToClipboard={copyToClipboard}
          handleClearProjectLogs={handleClearProjectLogs}
          getLogClass={getLogClass}
        />;
      case "settings":
        return <SystemSettingsTab
          workerId={workerId}
          scrapingTarget={scrapingTarget}
          setScrapingTarget={setScrapingTarget}
          scraperBatchSize={scraperBatchSize}
          setScraperBatchSize={setScraperBatchSize}
          scraperIntegrityCheckBatchSize={scraperIntegrityCheckBatchSize}
          setScraperIntegrityCheckBatchSize={setScraperIntegrityCheckBatchSize}
          scraperWriteBatchSize={scraperWriteBatchSize}
          setScraperWriteBatchSize={setScraperWriteBatchSize}
          scraperRecentProfilesLimit={scraperRecentProfilesLimit}
          setScraperRecentProfilesLimit={setScraperRecentProfilesLimit}
          telegramLogsLimit={telegramLogsLimit}
          setTelegramLogsLimit={setTelegramLogsLimit}
          scraperConsecutiveErrorLimit={scraperConsecutiveErrorLimit}
          setScraperConsecutiveErrorLimit={setScraperConsecutiveErrorLimit}
          scraperPauseDuration={scraperPauseDuration}
          setScraperPauseDuration={setScraperPauseDuration}
          projectLogsTtl={projectLogsTtl}
          setProjectLogsTtl={setProjectLogsTtl}
          handleBlurSave={handleBlurSave}
          scraperParallelRequestLimitMin={scraperParallelRequestLimitMin}
          setScraperParallelRequestLimitMin={setScraperParallelRequestLimitMin}
          scraperParallelRequestLimitMax={scraperParallelRequestLimitMax}
          setScraperParallelRequestLimitMax={setScraperParallelRequestLimitMax}
          scraperAdaptiveDelayMin={scraperAdaptiveDelayMin}
          setScraperAdaptiveDelayMin={setScraperAdaptiveDelayMin}
          scraperAdaptiveDelayMax={scraperAdaptiveDelayMax}
          setScraperAdaptiveDelayMax={setScraperAdaptiveDelayMax}
          scraperAdaptiveDelayStep={scraperAdaptiveDelayStep}
          setScraperAdaptiveDelayStep={setScraperAdaptiveDelayStep}
          scraperSuccessStreak={scraperSuccessStreak}
          setScraperSuccessStreak={setScraperSuccessStreak}
          scraperAnalysisWindow={scraperAnalysisWindow}
          setScraperAnalysisWindow={setScraperAnalysisWindow}
          scraperSuccessThreshold={scraperSuccessThreshold}
          setScraperSuccessThreshold={setScraperSuccessThreshold}
          fileLoggingEnabled={fileLoggingEnabled}
          setFileLoggingEnabled={setFileLoggingEnabled}
          scraperSkipIntegrityCheck={scraperSkipIntegrityCheck}
          setScraperSkipIntegrityCheck={setScraperSkipIntegrityCheck}
          handleSaveConfig={handleSaveConfig}
          handleDownloadLogFile={handleDownloadLogFile}
          handleClearLogFile={handleClearLogFile}
          isImporting={isImporting}
          handleTriggerImport={handleTriggerImport}
          isImportingTg={isImportingTg}
          handleTriggerImportTg={handleTriggerImportTg}
          isDownloadingProject={isDownloadingProject}
          handleDownloadProject={handleDownloadProject}
          handleClearDB={handleClearDB}
          bgExportStatus={bgExportStatus}
          handleBgExportAction={handleBgExportAction}
          isBgExportActionLoading={isBgExportActionLoading}
          tgBgExportStatus={tgBgExportStatus}
          handleTgBgExportAction={handleTgBgExportAction}
          isTgBgExportActionLoading={isTgBgExportActionLoading}
          githubRepoUrl={githubRepoUrl}
          setGithubRepoUrl={setGithubRepoUrl}
          githubToken={githubToken}
          setGithubToken={setGithubToken}
          handleSyncWithGitHub={handleSyncWithGitHub}
          isSyncing={isSyncing}
        />;
      case "access-settings":
        return <AccessSettingsTab
          handleUnlockSettingsSubmit={handleUnlockSettingsSubmit}
          settingsPasswordError={settingsPasswordError}
          settingsPasswordInput={settingsPasswordInput}
          setSettingsPasswordInput={setSettingsPasswordInput}
          isUnlocking={isUnlocking}
        />;
      default:
        return null;
    }
  }


  return (
    <div className="flex items-center justify-center min-h-screen bg-background text-foreground p-4 sm:p-6 lg:p-8">
      <Card className="w-full max-w-6xl shadow-2xl bg-card border-border">
        <CardHeader>
          <div className="flex flex-col sm:flex-row justify-between sm:items-start gap-4">
            <div>
              <div className="flex items-center gap-3">
                <svg width="40" height="40" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <path d="M12 2L2 7V17L12 22L22 17V7L12 2Z" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                  <path d="M2 7L12 12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                  <path d="M12 22V12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                  <path d="M22 7L12 12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                  <path d="M17 4.5L7 9.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
                <CardTitle className="text-3xl font-headline animate-neon-glow">Funpay Scraper MooNTooL UI</CardTitle>
              </div>
              <CardDescription className="text-muted-foreground pt-2">Панель для извлечения и мониторинга профилей пользователей FunPay.</CardDescription>
            </div>

          </div>
        </CardHeader>
        <CardContent>
          <Tabs value={activeTab} onValueChange={setActiveTab}>
            <TabsList className="mb-4 grid w-full grid-cols-3 sm:grid-cols-5 md:grid-cols-9">
              <TabsTrigger value="dashboard">
                <BarChart3 className="mr-2 h-4 w-4" />
                Скрейпер
              </TabsTrigger>
              <TabsTrigger value="telegram-panel">
                <Users className="mr-2 h-4 w-4" />
                Панель ТГ
              </TabsTrigger>
              <TabsTrigger value="search">
                <Search className="mr-2 h-4 w-4" />
                Поиск по БД
              </TabsTrigger>
              <TabsTrigger value="products">
                <Store className="mr-2 h-4 w-4" />
                Товары
              </TabsTrigger>
              <TabsTrigger value="advertising">
                <Send className="mr-2 h-4 w-4" />
                Реклама
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
                <FileText className="mr-2 h-4 w-4" />
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
            <TabsContent value="dashboard">{renderTabContent()}</TabsContent>
            <TabsContent value="telegram-panel">{renderTabContent()}</TabsContent>
            <TabsContent value="search">{renderTabContent()}</TabsContent>
            <TabsContent value="products">{renderTabContent()}</TabsContent>
            <TabsContent value="advertising">{renderTabContent()}</TabsContent>
            <TabsContent value="telegram-bot-settings">{renderTabContent()}</TabsContent>
            <TabsContent value="telegram-logs">{renderTabContent()}</TabsContent>
            <TabsContent value="project-logs">{renderTabContent()}</TabsContent>
            <TabsContent value="settings">{renderTabContent()}</TabsContent>
            <TabsContent value="access-settings">{renderTabContent()}</TabsContent>
          </Tabs>
        </CardContent>
      </Card>
      
      {/* File import dialog and logic */}
      <input
        type="file"
        ref={fileInputRef}
        className="hidden"
        accept=".json"
        onChange={handleFileSelect}
      />
      <input
        type="file"
        ref={tgFileInputRef}
        className="hidden"
        accept=".json"
        onChange={handleFileSelectTg}
      />
      <Dialog open={!!fileToImport} onOpenChange={(open) => !open && setFileToImport(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Подтверждение импорта</DialogTitle>
            <DialogDescription>
              Вы уверены, что хотите импортировать файл <span className="font-bold text-foreground">{fileToImport?.name}</span>? Это действие перезапишет все существующие данные в коллекции `users`. Это действие необратимо.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button onClick={() => setFileToImport(null)} variant="secondary">Отмена</Button>
            <Button onClick={handleConfirmImport} disabled={isImporting} variant="destructive">Да, импортировать</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
       <Dialog open={!!fileToImportTg} onOpenChange={(open) => !open && setFileToImportTg(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Подтверждение импорта БД Telegram</DialogTitle>
            <DialogDescription>
              Вы уверены, что хотите импортировать файл <span className="font-bold text-foreground">{fileToImportTg?.name}</span>? Это действие удалит всех текущих пользователей бота для воркера <span className="font-bold text-foreground">{workerId}</span> и заменит их данными из файла. Это действие необратимо.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button onClick={() => setFileToImportTg(null)} variant="secondary">Отмена</Button>
            <Button onClick={handleConfirmImportTg} disabled={isImportingTg} variant="destructive">Да, импортировать</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      
      {/* Template Editor Dialog */}
      <Dialog open={isTemplateDialogOpen} onOpenChange={setIsTemplateDialogOpen}>
        <DialogContent>
            <DialogHeader>
                <DialogTitle>{currentTemplate?.id ? 'Редактировать шаблон' : 'Создать новый шаблон'}</DialogTitle>
            </DialogHeader>
            <div className="space-y-4 py-4">
                <div className="space-y-2">
                    <Label htmlFor="template-name">Название шаблона</Label>
                    <Input 
                        id="template-name" 
                        value={currentTemplate?.name || ''} 
                        onChange={(e) => setCurrentTemplate(t => t ? ({...t, name: e.target.value}) : null)} 
                    />
                </div>
                <div className="space-y-2">
                    <Label htmlFor="template-text">Текст шаблона</Label>
                    <Textarea 
                        id="template-text" 
                        className="h-32"
                        value={currentTemplate?.text || ''} 
                        onChange={(e) => setCurrentTemplate(t => t ? ({...t, text: e.target.value}) : null)} 
                    />
                </div>
            </div>
            <DialogFooter>
                <Button variant="secondary" onClick={() => setIsTemplateDialogOpen(false)}>Отмена</Button>
                <Button onClick={handleSaveTemplate}>Сохранить</Button>
            </DialogFooter>
        </DialogContent>
       </Dialog>
       
       {/* RPS Chart Modal */}
       <Dialog open={isRpsModalOpen} onOpenChange={setIsRpsModalOpen}>
            <DialogContent className="max-w-3xl h-[60vh] flex flex-col">
                <DialogHeader>
                    <DialogTitle>Динамика скорости парсинга</DialogTitle>
                    <DialogDescription>
                        Общая скорость работы всех скрейперов (зап/сек) за последние 24 часа.
                    </DialogDescription>
                </DialogHeader>
                <div className="h-full">
                  <RpsChart />
                </div>
            </DialogContent>
       </Dialog>
    </div>
  );
}
