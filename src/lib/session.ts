import { SessionOptions } from "iron-session";
import { randomBytes } from "crypto";

export interface SessionData {
  isLoggedIn: boolean;
  isSettingsUnlocked?: boolean;
}

// Generate a secret if it's not in the environment variables
const secret = process.env.AUTH_SECRET || randomBytes(32).toString("hex");

if (!process.env.AUTH_SECRET) {
  if (process.env.NODE_ENV === 'production') {
    console.warn(
      '⚠️ WARNING: No AUTH_SECRET environment variable found. A temporary secret has been generated. ' +
      'Sessions will not persist across server restarts. ' +
      'For production, it is STRONGLY recommended to set a permanent AUTH_SECRET in your environment variables.'
    );
  }
}

export const sessionOptions: SessionOptions = {
  password: secret,
  cookieName: "funpay-scraper-session",
  cookieOptions: {
    secure: process.env.NODE_ENV === "production",
    maxAge: 3600, // 60 minutes
  },
};
