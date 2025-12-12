
"use server";
import { NextResponse } from 'next/server';
import { MongoClient } from 'mongodb';

const COLLECTION_NAME = "users";

async function getDb() {
    const mongoUri = process.env.MONGODB_URI;
    const dbName = process.env.MONGODB_DB_NAME;
    if (!mongoUri || !dbName) {
        throw new Error("MongoDB URI или имя БД не сконфигурированы.");
    }
    const client = new MongoClient(mongoUri);
    await client.connect();
    return { client, db: client.db(dbName) };
}

export async function GET(request: Request) {
    let client: MongoClient | undefined;
    try {
        const { searchParams } = new URL(request.url);
        const type = searchParams.get('type');
        const page = parseInt(searchParams.get('page') || '1', 10);
        const limit = parseInt(searchParams.get('limit') || '4000', 10);
        
        const { client: connectedClient, db } = await getDb();
        client = connectedClient;
        const collection = db.collection(COLLECTION_NAME);

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
        if(client) await client.close();
    }
}


export async function POST(request: Request) {
    let client: MongoClient | undefined;
    try {
        const data = await request.json();

        if (!Array.isArray(data)) {
            return NextResponse.json({ error: 'Invalid data format. Expected an array of user profiles.' }, { status: 400 });
        }
        
        const { client: connectedClient, db } = await getDb();
        client = connectedClient;
        const collection = db.collection(COLLECTION_NAME);

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
        if(client) await client.close();
    }
}
