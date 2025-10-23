import { SessionOptions } from "iron-session";

export interface SessionData {
  isLoggedIn: boolean;
  isSettingsUnlocked?: boolean;
}

export const sessionOptions: SessionOptions = {
  password: process.env.AUTH_SECRET as string,
  cookieName: "funpay-scraper-session",
  cookieOptions: {
    secure: process.env.NODE_ENV === "production",
  },
};
