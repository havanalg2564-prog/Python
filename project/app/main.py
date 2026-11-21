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

async def connectDb(maxAttempts: int = 5, delay: float = 2.0):
    url = dbUrl.replace("postgresql://", "postgres://") if dbUrl else None
    
    for attempt in range(1, maxAttempts + 1):
        try:
            pool = await asyncpg.create_pool(dsn=url, timeout=5.0)
            print(f"--> Database connected on attempt {attempt}", flush=True)
            return pool
        except Exception as e:
            print(f"--> Database attempt {attempt} failed: {repr(e)}", flush=True)
            if attempt == maxAttempts:
                raise
            await asyncio.sleep(delay)

async def connectRedis(maxAttempts: int = 5, delay: float = 2.0):
    for attempt in range(1, maxAttempts + 1):
        try:
            client = aioredis.Redis(
                host=redisHost,
                port=redisPort,
                decode_responses=True,
                socket_connect_timeout=5.0
            )
            await client.ping()
            print(f"--> Redis connected on attempt {attempt}", flush=True)
            return client
        except Exception as e:
            print(f"--> Redis attempt {attempt} failed: {repr(e)}", flush=True)
            if attempt == maxAttempts:
                raise
            await asyncio.sleep(delay)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global dbPool, redisClient
    
    print(f"DEBUG: DB_URL prefix: {str(dbUrl)[:15] if dbUrl else 'NONE'}", flush=True)
    print(f"DEBUG: REDIS_HOST: {redisHost}", flush=True)
    print(f"DEBUG: REDIS_PORT: {redisPort}", flush=True)

    print("Connecting to database...", flush=True)
    dbPool = await connectDb()
    
    async with dbPool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )
            """
        )
    
    print("Connecting to Redis...", flush=True)
    redisClient = await connectRedis()
    
    print("Application startup complete!", flush=True)
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