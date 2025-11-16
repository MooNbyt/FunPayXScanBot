
"use server";
import { NextResponse } from 'next/server';
import { MongoClient } from 'mongodb';
import { getConfig } from '../status/route';

const COLLECTION_NAME = "users";

export async function GET(request: Request) {
    const { MONGODB_URI, MONGODB_DB_NAME } = await getConfig();
    if (!MONGODB_URI) {
        return NextResponse.json({ error: 'DB not configured' }, { status: 500 });
    }

    const { searchParams } = new URL(request.url);
    const type = searchParams.get('type');
    const page = parseInt(searchParams.get('page') || '1', 10);
    const limit = parseInt(searchParams.get('limit') || '4000', 10);

    const mongoClient = new MongoClient(MONGODB_URI);
    
    try {
        await mongoClient.connect();
        const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
        const collection = mongoClient.db(dbName).collection(COLLECTION_NAME);

        if (type === 'count') {
            const count = await collection.countDocuments();
            return NextResponse.json({ count });
        }
        
        const skip = (page - 1) * limit;
        const users = await collection.find({}).project({_id: 0}).sort({ id: 1 }).skip(skip).limit(limit).toArray();

        return NextResponse.json(users);

    } catch (error: any) {
        console.error("Error exporting data:", error);
        return NextResponse.json({ error: 'Failed to export data' }, { status: 500 });
    } finally {
        if (mongoClient) {
            await mongoClient.close();
        }
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
