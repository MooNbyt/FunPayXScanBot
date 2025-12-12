import { NextResponse, type NextRequest } from "next/server";
import { getIronSession } from "iron-session";
import { SessionData, sessionOptions } from "@/lib/session";
import { cookies } from "next/headers";

export async function middleware(request: NextRequest) {
  const session = await getIronSession<SessionData>(cookies(), sessionOptions);
  const { pathname } = request.nextUrl;

  // If user is logged in and tries to access login page, redirect to home
  if (session.isLoggedIn && pathname === "/login") {
    return NextResponse.redirect(new URL("/", request.url));
  }

  // If user is not logged in and tries to access a protected page, redirect to login
  if (!session.isLoggedIn && !pathname.startsWith("/login") && !pathname.startsWith("/api/")) {
    return NextResponse.redirect(new URL("/login", request.url));
  }

  return NextResponse.next();
}

export const config = {
  // Matcher ignoring `/api/`, `/_next/` and other static files
  matcher: ["/((?!api|_next/static|_next/image|favicon.ico).*)"],
};
