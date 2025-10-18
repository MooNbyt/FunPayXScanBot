
"use server";
import { NextResponse } from 'next/server';

// This file is intentionally left blank. The scraping logic is now consolidated in run/route.ts.

export async function GET(request: Request) {
     return NextResponse.json({ message: "This endpoint is deprecated." }, { status: 410 });
}
