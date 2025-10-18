
"use server";
import { NextResponse } from 'next/server';
import { MongoClient } from 'mongodb';
import { getConfig } from '../status/route';
import { Readable } from 'stream';

const COLLECTION_NAME = "users";

export async function GET() {
    const { MONGODB_URI, MONGODB_DB_NAME } = await getConfig();
    if (!MONGODB_URI) {
        return NextResponse.json({ error: 'DB not configured' }, { status: 500 });
    }

    const mongoClient = new MongoClient(MONGODB_URI);
    try {
        await mongoClient.connect();
        const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
        const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);
        // Добавлена сортировка по ID для гарантированного порядка в выгрузке
        const data = await collection.find({}).project({_id: 0}).sort({ id: 1 }).toArray();

        const jsonString = JSON.stringify(data, null, 2);
        const headers = new Headers();
        headers.set('Content-Type', 'application/json');
        headers.set('Content-Disposition', `attachment; filename="funpay_users_backup_${new Date().toISOString()}.json"`);
        
        return new NextResponse(jsonString, { headers });

    } catch (error: any) {
        console.error("Error exporting data:", error);
        return NextResponse.json({ error: 'Failed to export data' }, { status: 500 });
    } finally {
        await mongoClient.close();
    }
}

export async function POST(request: Request) {
    const { MONGODB_URI, MONGODB_DB_NAME } = await getConfig();
    if (!MONGODB_URI) {
        return NextResponse.json({ error: 'DB not configured' }, { status: 500 });
    }

    const mongoClient = new MongoClient(MONGODB_URI);
    try {
        const data = await request.json();

        if (!Array.isArray(data)) {
            return NextResponse.json({ error: 'Invalid data format. Expected an array of user profiles.' }, { status: 400 });
        }

        await mongoClient.connect();
        const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
        const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);

        // Clear the collection before importing
        await collection.deleteMany({});

        if(data.length > 0){
            await collection.insertMany(data);
        }

        return NextResponse.json({ message: 'Database imported successfully.' });

    } catch (error: any) {
        console.error("Error importing data:", error);
        return NextResponse.json({ error: 'Failed to import data' }, { status: 500 });
    } finally {
        await mongoClient.close();
    }
}
