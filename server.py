from flask import request, Flask, jsonify
from psycopg2.pool import ThreadedConnectionPool
from flask_limiter import Limiter
from flask_limiter import Limiter
from flask_cors import CORS


import redis
from shapely.wkb import loads
import dotenv
import psycopg2
import time
import json
import os

dotenv.load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"), # Put your actual password here exactly as it is
    "port": 5432
}



app = Flask(__name__)
CORS(app)

# --- Function to correctly get Client IP on Vercel/Proxies ---
def get_client_ip():
    """
    Fetches the actual client IP from X-Forwarded-For header,
    which is necessary when running behind a proxy like Vercel.
    """
    if request.headers.get('x-forwarded-for'):
        # Take the first IP in the list, which is typically the client's IP
        return request.headers.getlist("x-forwarded-for")[0]
    else:
        # Fallback for direct connections (e.g., local development)
        return request.remote_addr


# --- Get Redis URL from Environment Variables ---
REDIS_URL = os.getenv('REDIS_URL')

# --- Initialization ---
if REDIS_URL:
    # The credentials (user/pass) are parsed *from* the URL string here
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)

    # Initialize Limiter passing the pre-configured Redis CLIENT INSTANCE
    limiter = Limiter(
        key_func=get_client_ip, # Use the correct IP function
        app=app,
        storage_uri=REDIS_URL, # Pass the connected client object
        default_limits=["60 per minute"]
    )
else:
    # Fallback for local development
    print("WARNING: REDIS_URL not found. Limiter will use in-memory storage.")
    limiter = Limiter(
        key_func=get_client_ip,
        app=app,
        storage_uri="memory://",
        default_limits=["60 per minute"]
    )

times = []
pool = None

query = """
    
    SELECT road_name, way_id, geom,
    ST_Distance(geom, ST_MakePoint(%s, %s)::geography) as distance_meters
    FROM roads_geojson
    WHERE ST_DWithin(geom, ST_MakePoint(%s, %s)::geography, 20)
    ORDER BY geom <-> ST_MakePoint(%s, %s)::geography
    LIMIT 1;
"""

def api_query_count():
    COUNTER_KEY = 'api:closest_road:call_count'
    current_count = redis_client.incr(COUNTER_KEY) 
    app.logger.warning(f"Endpoint '/closest-road' has been called {current_count} times.")
    return current_count

    
def initialize_and_warmup_db():
    global pool
    boot_start = time.perf_counter_ns()
    # Create the connection ONCE
    
    minimium_connections = 1
    maximum_connections = 5
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

def fetch_closest_road(lat, lon):
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

def database_search(lat, lon):
    database_start = time.perf_counter_ns()
    results, query_time = fetch_closest_road(lat, lon)
    print(results)
    if results is None:
        database_stop = time.perf_counter_ns()
        api_query_count()
        
        return {
            "road_name": None,
            "road_id": None,
            "coordinates": None,
            "distance_meters": 0,
            "query_time_ms": query_time,
            "processing_time_ms": round((database_stop - database_start) / 1e6 - query_time, 4)
        }
        
    else:
        road_name, road_id, coordinates, distance = results
        byte_data = bytes.fromhex(coordinates)
        
        # 2. Load geometry from bytes
        geom_object = loads(byte_data) 
        out_lon = geom_object.x
        out_lat = geom_object.y
        
        database_stop = time.perf_counter_ns()
        api_query_count()
        
        return {
            "road_name": road_name,
            "road_id": road_id,
            "coordinates": [out_lon, out_lat],
            "distance_meters": distance,
            "query_time_ms": query_time,
            "processing_time_ms": round((database_stop - database_start) / 1e6 - query_time, 4)
        }

@app.route('/closest-road', methods=['GET'])
@limiter.limit("60 per minute")
def closest_road():
    request_start = time.perf_counter_ns()
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return {"error": "Invalid or missing 'lat' and 'lon' parameters"}, 400
    
    search_query = database_search(lat, lon)
    #if result["coordinates"] is None:
     #   return {"error": "Out of Range"}, 404
    request_end = time.perf_counter_ns()
    app.logger.warning(f"Total request time: {(request_end - request_start) / 1e6:.4f} ms")
    return search_query

@app.route('/', methods=['GET'])
def response():
    return "Success"

@app.route('/health', methods=['GET'])
def health():
    api_count_variable = api_query_count()
    now = time.perf_counter_ns()
    uptime_seconds = (now - boot_time) / 1e9
    uptime_days = uptime_seconds / (24 * 3600)
    return f"Healthy. Global Gateways have served {api_count_variable} queries. Uptime: {uptime_days:.2f} days"


if __name__ == "__main__":
    initialize_and_warmup_db()  # Set up the pool and warm up connections
    boot_time = time.perf_counter_ns()
    
    from waitress import serve
    print("Starting Waitress server. Listening on all interfaces @ port 5000")
    CORS(app)  # Enable CORS for all routes and origins
    # Waitress handles concurrency itself, similar to Gunicorn's worker concept
    serve(app, host='0.0.0.0', port=5000)
