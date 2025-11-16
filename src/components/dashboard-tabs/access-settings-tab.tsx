"use client";

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Alert, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle, Loader2 } from "lucide-react";
import React from "react";

interface AccessSettingsTabProps {
  handleUnlockSettingsSubmit: (e: React.FormEvent) => void;
  settingsPasswordError: string;
  settingsPasswordInput: string;
  setSettingsPasswordInput: (value: string) => void;
  isUnlocking: boolean;
}

export function AccessSettingsTab({
  handleUnlockSettingsSubmit,
  settingsPasswordError,
  settingsPasswordInput,
  setSettingsPasswordInput,
  isUnlocking,
}: AccessSettingsTabProps) {
  return (
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
  );
}
