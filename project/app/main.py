import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncpg
import redis.asyncio as aioredis

dbUrl = os.getenv("DATABASE_URL")
redisHost = os.getenv("REDIS_HOST", "redis")
redisPort = int(os.getenv("REDIS_PORT", 6379))

dbPool = None
redisClient = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global dbPool, redisClient
    
    # Ждем, пока база данных полностью запустится
    print("Connecting to database...")
    dbPool = await asyncpg.create_pool(dsn=dbUrl)
    
    async with dbPool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )
            """
        )
    
    redisClient = aioredis.Redis(
        host=redisHost,
        port=redisPort,
        decode_responses=True,
    )
    print("Application startup complete!")
    yield
    await dbPool.close()
    await redisClient.close()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "It works!"}

@app.get("/health")
async def health():
    try:
        async with dbPool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        await redisClient.ping()
        return {"status": "ok", "database": "connected", "redis": "connected"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/items")
async def listItems():
    async with dbPool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name FROM items ORDER BY id")
    return [{"id": row["id"], "name": row["name"]} for row in rows]

@app.post("/items")
async def createItem(name: str):
    async with dbPool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO items (name) VALUES ($1) RETURNING id, name",
            name,
        )
    await redisClient.incr("items_count")
    return {"id": row["id"], "name": row["name"]}

@app.get("/counter")
async def getCounter():
    value = await redisClient.get("items_count")
    return {"items_count": int(value) if value else 0}