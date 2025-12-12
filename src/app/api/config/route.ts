import { NextResponse } from 'next/server';
import { updateConfig, getConfig } from '../status/route';

export async function GET() {
  try {
    const config = await getConfig();
    return NextResponse.json(config);
  } catch (error) {
     console.error("Error fetching config:", error);
     return new NextResponse('Error fetching configuration', { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    await updateConfig(body);
    return NextResponse.json({ message: 'Configuration updated successfully' });
  } catch (error) {
    console.error("Error updating config:", error);
    return new NextResponse('Error updating configuration', { status: 500 });
  }
}
