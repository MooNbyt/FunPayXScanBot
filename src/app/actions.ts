
"use server";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { getIronSession } from "iron-session";
import { SessionData, sessionOptions } from "@/lib/session";
import fs from 'fs/promises';
import path from 'path';
import JSZip from 'jszip';
import { updateConfig } from "./api/status/route";

export async function login(formData: FormData) {
  const session = await getIronSession<SessionData>(cookies(), sessionOptions);

  const username = formData.get("username");
  const password = formData.get("password");

  if (
    username === process.env.ADMIN_USERNAME &&
    password === process.env.ADMIN_PASSWORD
  ) {
    session.isLoggedIn = true;
    await session.save();
    redirect("/");
  } else {
    redirect("/login?error=Invalid credentials");
  }
}

export async function logout() {
  const session = await getIronSession<SessionData>(cookies(), sessionOptions);
  session.destroy();
  redirect("/login");
}

export async function unlockSettings(formData: FormData) {
  const session = await getIronSession<SessionData>(cookies(), sessionOptions);
  const password = formData.get("password") as string;
  
  if (!password) {
    return { error: "Пароль не может быть пустым." };
  }

  const settingsPassword = process.env.SETTINGS_PASSWORD;

  if (!settingsPassword) {
      return { error: "Пароль настроек не задан в переменных окружения." };
  }
  
  if (password === settingsPassword) {
    session.isSettingsUnlocked = true;
    await session.save();
    return { success: true };
  } else {
    return { error: "Неверный пароль." };
  }
}

export async function lockSettings() {
    const session = await getIronSession<SessionData>(cookies(), sessionOptions);
    session.isSettingsUnlocked = false;
    await session.save();
    redirect('/');
}

async function readFiles(dir: string, zip: JSZip, root: string) {
  try {
    const dirents = await fs.readdir(dir, { withFileTypes: true });
    for (const dirent of dirents) {
      const fullPath = path.join(dir, dirent.name);
      if (['node_modules', '.next', '.git', 'logs'].includes(dirent.name)) {
        continue;
      }
      const relativePath = path.relative(root, fullPath);
      if (dirent.isDirectory()) {
        await readFiles(fullPath, zip, root);
      } else {
        const content = await fs.readFile(fullPath);
        zip.file(relativePath, content);
      }
    }
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return;
    }
    throw error;
  }
}

export async function downloadProject(): Promise<{ success: boolean; file: string; fileName: string; }> {
  const session = await getIronSession<SessionData>(cookies(), sessionOptions);
  if (!session.isSettingsUnlocked) {
      throw new Error("Доступ запрещен. Вы должны разблокировать настройки.");
  }
  
  const zip = new JSZip();
  const projectRoot = process.cwd();
  
  await readFiles(projectRoot, zip, projectRoot);

  const zipAsBase64 = await zip.generateAsync({ type: "base64" });

  return {
    success: true,
    file: zipAsBase64,
    fileName: `Funpay Scraper MooNTooL UI-${new Date().toISOString().split('T')[0]}.zip`
  };
}
