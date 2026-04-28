from psycopg2.pool import ThreadedConnectionPool
from fastapi import FastAPI

import redis
from shapely.wkb import loads
import dotenv
import psycopg2
import time
import os

dotenv.load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"), # Put your actual password here exactly as it is
    "port": os.getenv("DB_PORT")
}

ENABLE_DB_WARMUP = os.getenv("ENABLE_DB_WARMUP", "False") == "True"

app = FastAPI()
boot_time = time.perf_counter_ns()

# --- Get Redis URL from Environment Variables ---
REDIS_URL = os.getenv('REDIS_URL')

# --- Initialization ---
if REDIS_URL:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)

times = []
pool = None

query = """
    
    SELECT road_name, way_id, custom_prompt, geom,
    ST_Distance(geom, ST_MakePoint(%s, %s)::geography) as distance_meters
    FROM roads_geojson
    WHERE ST_DWithin(geom, ST_MakePoint(%s, %s)::geography, 20)
    ORDER BY geom <-> ST_MakePoint(%s, %s)::geography
    LIMIT 1;
"""

def api_query_count():
    COUNTER_KEY = 'api:closest_road:call_count'
    current_count = redis_client.incr(COUNTER_KEY) 
    ## app.logger.warning(f"Endpoint '/closest-road' has been called {current_count} times.")
    return current_count
   
def initialize_and_warmup_db():
    global pool
    boot_start = time.perf_counter_ns()
    # Create the connection ONCE
    
    minimium_connections = 1
    maximum_connections = 3
    pool = ThreadedConnectionPool(minconn=minimium_connections,maxconn=maximum_connections,**DB_CONFIG)
 
    for i in range(minimium_connections):
        conn = pool.getconn()
        
        try:
        # The FIRST time you run this, planning time will be ~28ms
            with conn.cursor() as cur:
                start = time.perf_counter_ns()
                cur.execute(query, (-46.8521364,-23.4926477,-46.8521364,-23.4926477, -46.8521364,-23.4926477))
                results, query_time = cur.fetchone(), (time.perf_counter_ns() - start) / 1e6
                print(results)
                app.logger.warning(f"Query time: {query_time:.2f} ms")
                
        except Exception as e:
            app.logger.warning(f"Warmup failed on connection {i+1}: {e}")
        
        finally:
            try:
                cur.close()
                if conn:
                    try:
                        pool.putconn(conn) # This is the only time we put it back
                    except psycopg2.pool.PoolError as e:
                        print(f"Warning: Failed to return connection {i+1} to pool: {e}")
            
            except Exception as e:
                app.logger.warning(f"Error closing resources during warmup on connection {i+1}: {e}")
                pass
                
    boot_end = time.perf_counter_ns()
    app.logger.warning(f"Boot time (including initial query): {(boot_end - boot_start) / 1e6:.4f} ms")

async def fetch_closest_road(lat, lon):
    global pool
    local_pool = pool
    try:
        conn = pool.getconn()
        with conn.cursor() as cur:
            start = time.perf_counter_ns()
            cur.execute(query, (lon, lat, lon, lat, lon, lat))
            finish = time.perf_counter_ns()
            times.append((finish - start)/ 1e6)
            data = cur.fetchone()
            return data, (finish - start) / 1e6
        
    finally:
        if conn:
            local_pool.putconn(conn) # Return connection to pool

async def database_search(lat, lon):
    database_start = time.perf_counter_ns()
    results, query_time = await fetch_closest_road(lat, lon)
    if results is None:
        database_stop = time.perf_counter_ns()
        api_query_count()
        
        return {
            "road_name": None,
            "custom_prompt": None,
            "road_id": None,
            "coordinates": None,
            "distance_meters": 0,
            "query_time_ms": query_time,
            "processing_time_ms": round((database_stop - database_start) / 1e6 - query_time, 4)
        }
        
    else:
        road_name, road_id, custom_prompt, coordinates, distance = results
        byte_data = bytes.fromhex(coordinates)
        
        # 2. Load geometry from bytes
        geom_object = loads(byte_data) 
        out_lon = geom_object.x
        out_lat = geom_object.y
        
        database_stop = time.perf_counter_ns()
        await api_query_count()
        
        return {
            "road_name": road_name,
            "custom_prompt": custom_prompt,
            "road_id": road_id,
            "coordinates": [out_lon, out_lat],
            "distance_meters": distance,
            "query_time_ms": query_time,
            "processing_time_ms": round((database_stop - database_start) / 1e6 - query_time, 4)
        }

@app.get('/api/closest-road')
async def closest_road(lat: float, lon: float):
    if lat and lon:
        search_query = await database_search(lat, lon)
        print(search_query)
        return search_query
    else:
        return {"error": "Invalid or missing 'lat' and 'lon' parameters"}

@app.get('/')
def response():
    return {"status": "Success"}

@app.get('/api/health')
def health():
    api_count_variable = api_query_count()
    now = time.perf_counter_ns()
    uptime_seconds = (now - boot_time) / 1e9
    uptime_days = uptime_seconds / (24 * 3600)
    
    return {
    "status": "Healthy",
    "queries_served": api_count_variable,
    "uptime_days": uptime_days
    }

if ENABLE_DB_WARMUP:
    initialize_and_warmup_db()
    print(app.url_map)

else:
    pool = ThreadedConnectionPool(minconn=1,maxconn=3,**DB_CONFIG)

if __name__ == "__main__":
    import uvicorn
    print("Starting Waitress server. Listening on all interfaces @ port 5000")
    # Waitress handles concurrency itself, similar to Gunicorn's worker concept
    uvicorn.run(app, host='0.0.0.0', port=5000)







