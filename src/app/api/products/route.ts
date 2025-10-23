
"use server";
import { NextResponse } from 'next/server';
import { MongoClient, ObjectId } from 'mongodb';
import { getConfig, updateConfig } from '../status/route';

const PRODUCTS_COLLECTION = "products";

async function getDb(config: any) {
  const { MONGODB_URI, MONGODB_DB_NAME } = config;
  if (!MONGODB_URI) {
    throw new Error('DB not configured');
  }
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const dbName = new URL(MONGODB_URI).pathname.substring(1) || MONGODB_DB_NAME;
  return { db: client.db(dbName), client };
}

// GET all products for the current worker OR categories
export async function GET(request: Request) {
  let client: MongoClient | undefined;
  try {
    const { searchParams } = new URL(request.url);
    const fetchType = searchParams.get('type');
    const config = await getConfig();
    const { WORKER_ID } = config;

    if (fetchType === 'categories') {
        return NextResponse.json({ categories: config.productCategories || [] });
    }

    const { db, client: connectedClient } = await getDb(config);
    client = connectedClient;
    const products = await db.collection(PRODUCTS_COLLECTION).find({ ownerId: WORKER_ID }).toArray();
    return NextResponse.json({ products });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  } finally {
    if (client) await client.close();
  }
}

// POST a new product OR a new category for the current worker
export async function POST(request: Request) {
  let client: MongoClient | undefined;
  try {
    const config = await getConfig();
    const { WORKER_ID } = config;

    const body = await request.json();

    // Handle Category Creation
    if (body.action === 'add_category') {
        const { categoryName } = body;
        if (!categoryName) {
            return NextResponse.json({ error: 'Category name is required' }, { status: 400 });
        }
        const currentCategories = config.productCategories || [];
        if (currentCategories.includes(categoryName)) {
            return NextResponse.json({ error: 'Category already exists' }, { status: 409 });
        }
        const newCategories = [...currentCategories, categoryName];
        await updateConfig({ productCategories: newCategories });
        return NextResponse.json({ success: true, categories: newCategories });
    }

    // Handle Product Creation
    const product = body;
    delete product._id; 
    product.ownerId = WORKER_ID; // Assign ownership
    
    const { db, client: connectedClient } = await getDb(config);
    client = connectedClient;
    
    if (product.price) product.price = Number(product.price);
    if (product.priceReal) product.priceReal = Number(product.priceReal);
    if (product.apiDays) product.apiDays = Number(product.apiDays);

    const result = await db.collection(PRODUCTS_COLLECTION).insertOne(product);
    return NextResponse.json({ success: true, insertedId: result.insertedId });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  } finally {
    if (client) await client.close();
  }
}

// PUT (update) a product, checking ownership
export async function PUT(request: Request) {
  let client: MongoClient | undefined;
  try {
    const config = await getConfig();
    const { WORKER_ID } = config;

    const { _id, ...productData } = await request.json();
    if (!_id) {
      return NextResponse.json({ error: 'Product ID is required for update.' }, { status: 400 });
    }

    const { db, client: connectedClient } = await getDb(config);
    client = connectedClient;

    if (productData.price) productData.price = Number(productData.price);
    if (productData.priceReal) productData.priceReal = Number(productData.priceReal);
    if (productData.apiDays) productData.apiDays = Number(productData.apiDays);

    const result = await db.collection(PRODUCTS_COLLECTION).updateOne(
      { _id: new ObjectId(_id), ownerId: WORKER_ID }, // Ensure owner matches
      { $set: productData }
    );

    if (result.matchedCount === 0) {
      return NextResponse.json({ error: 'Product not found or you do not have permission to edit it.' }, { status: 404 });
    }

    return NextResponse.json({ success: true, modifiedCount: result.modifiedCount });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  } finally {
    if (client) await client.close();
  }
}

// DELETE a product OR a category, checking ownership
export async function DELETE(request: Request) {
  let client: MongoClient | undefined;
  try {
    const config = await getConfig();

    const body = await request.json();
    
    // Handle Category Deletion
    if (body.action === 'delete_category') {
        const { categoryName } = body;
        if (categoryName === undefined) { // Allow empty string but not undefined
            return NextResponse.json({ error: 'Category name is required' }, { status: 400 });
        }
        const currentCategories = config.productCategories || [];
        if (!currentCategories.includes(categoryName)) {
            return NextResponse.json({ error: 'Category not found' }, { status: 404 });
        }
        const newCategories = currentCategories.filter((c: string) => c !== categoryName);
        await updateConfig({ productCategories: newCategories });
        
        // Also remove this category from all products
        const { db, client: connectedClient } = await getDb(config);
        client = connectedClient;
        await db.collection(PRODUCTS_COLLECTION).updateMany({ ownerId: config.WORKER_ID, category: categoryName }, { $set: { category: '' } });

        return NextResponse.json({ success: true, categories: newCategories });
    }


    // Handle Product Deletion
    const { _id } = body;
    if (!_id) {
      return NextResponse.json({ error: 'Product ID is required for deletion.' }, { status: 400 });
    }
    
    const { db, client: connectedClient } = await getDb(config);
    client = connectedClient;
    
    const result = await db.collection(PRODUCTS_COLLECTION).deleteOne({ _id: new ObjectId(_id), ownerId: config.WORKER_ID }); // Ensure owner matches

    if (result.deletedCount === 0) {
       return NextResponse.json({ error: 'Product not found or you do not have permission to delete it.' }, { status: 404 });
    }

    return NextResponse.json({ success: true, deletedCount: result.deletedCount });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  } finally {
    if (client) await client.close();
  }
}
