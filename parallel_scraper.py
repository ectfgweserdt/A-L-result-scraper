import asyncio
import aiohttp
import time
import os
import re
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from datetime import datetime

# --- CONFIGURATION FROM GITHUB ACTIONS ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI environment variable is missing!")
    exit(1)

# Get inputs from Workflow (with defaults just in case)
WORKER_ID = os.getenv("WORKER_ID", "default_worker")
START_INDEX = int(os.getenv("START_INDEX", "1000000"))
END_INDEX = int(os.getenv("END_INDEX", "9999999"))

DB_NAME = "exam_database"
COLLECTION_NAME = "results_2024_al_rescrutiny"
STATE_COLLECTION = "scraper_state"

# This is the magic fix: Every worker gets its own save file!
STATE_DOCUMENT_ID = f"scrape_state_{WORKER_ID}"

EXAM_SESSION_ID = "607"
EXAM_TITLE = "G.C.E. (A/L) Examination - 2024 (After Rescrutiny)"
EXAM_YEAR = "2024"

MAX_RUNTIME_SECONDS = (5 * 3600) + (45 * 60) 
CONCURRENCY_LIMIT = 150     
BATCH_SIZE = 1000          

url = "http://www.results.exams.gov.lk/viewresults.htm"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "http://www.results.exams.gov.lk/viewresultsforexam.htm",
    "Content-Type": "application/x-www-form-urlencoded"
}

client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
state_db = db[STATE_COLLECTION]

async def fetch_and_parse(session, index_number, semaphore):
    index_str = str(index_number).zfill(7)
    data = {
        "examSessionId": EXAM_SESSION_ID,
        "year": EXAM_YEAR,
        "typeTitle": EXAM_TITLE,
        "isAddIndexNeeded": "N",
        "additionalFieldName": "comment",
        "indexNumber": index_str
    }

    async with semaphore:
        try:
            async with session.post(url, data=data, headers=headers, timeout=15) as response:
                if response.status != 200: return None
                html = await response.text()
                if "invalid" in html.lower() or "not found" in html.lower() or "<table" not in html: return None

                soup = BeautifulSoup(html, "lxml")
                details = {}
                for row in soup.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) == 3:
                        label_raw = cols[0].get_text(strip=True).replace(":", "").lower()
                        value = cols[2].get_text(strip=True)
                        if label_raw and value:
                            key = label_raw[:3]
                            if "z-" in label_raw: key = "zsc"
                            elif "dist" in label_raw: key = "dis"
                            elif "isl" in label_raw: key = "isl"
                            details[key] = value 

                if not details: return None

                subjects = []
                seen_subjects = set()
                for table in soup.find_all("table"):
                    for row in table.find_all("tr"):
                        cols = row.find_all("td")
                        if len(cols) == 2:
                            subject = cols[0].get_text(strip=True)
                            result = cols[1].get_text(strip=True)
                            if not re.match(r'^[A-Z\s\.\&\(\)]+$', subject): continue
                            if subject.lower() in ["subject", "home", "search again", "close", "print"]: continue
                            
                            uid = f"{subject}|{result}"
                            if uid not in seen_subjects:
                                subjects.append({"s": subject, "g": result})
                                seen_subjects.add(uid)

                if not subjects: return None
                return {"_id": index_str, "d": details, "r": subjects}
        except: return None

async def get_start_index():
    state = await state_db.find_one({"_id": STATE_DOCUMENT_ID})
    # Only resume if the saved index is within our current assigned range
    if state and "last_index" in state and state["last_index"] >= START_INDEX:
        return state["last_index"] + 1
    return START_INDEX

async def save_state(last_index, total_found, total_checked):
    await state_db.update_one(
        {"_id": STATE_DOCUMENT_ID},
        {
            "$set": {"last_index": last_index, "worker_id": WORKER_ID}, 
            "$inc": {"total_found": total_found, "total_checked": total_checked}
        },
        upsert=True
    )

def generate_report(start_idx, end_idx, total_checked, total_found, time_taken, reason):
    speed = total_checked / time_taken if time_taken > 0 else 0
    report = f"""## 🚀 Worker '{WORKER_ID}' Summary
* **Status:** {reason}
* **Assigned Range:** `{str(START_INDEX).zfill(7)}` to `{str(END_INDEX).zfill(7)}`
* **Stopped At:** `{str(end_idx).zfill(7)}`
* **Found:** {total_found:,} | **Checked:** {total_checked:,}
* **Speed:** {speed:.2f} indices/sec
"""
    with open("report.md", "w") as f: f.write(report)
    if "GITHUB_STEP_SUMMARY" in os.environ:
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f: f.write(report)
    print(report)

async def main():
    start_time = time.time()
    current_index = await get_start_index()
    initial_index = current_index
    
    print(f"🚀 [Worker: {WORKER_ID}] Resuming from: {str(current_index).zfill(7)} (Target: {str(END_INDEX).zfill(7)})")
    
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT, enable_cleanup_closed=True)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    total_found_this_run = 0
    total_checked_this_run = 0
    stop_reason = "Completed Assigned Range"

    async with aiohttp.ClientSession(connector=connector) as session:
        while current_index <= END_INDEX:
            if (time.time() - start_time) > MAX_RUNTIME_SECONDS:
                stop_reason = "Graceful Timeout (6 hours limit)"
                break

            batch_end = min(current_index + BATCH_SIZE, END_INDEX + 1)
            tasks = [fetch_and_parse(session, idx, semaphore) for idx in range(current_index, batch_end)]
            results = [res for res in await asyncio.gather(*tasks) if res]
            
            if results:
                ops = [UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True) for doc in results]
                await collection.bulk_write(ops, ordered=False)
                total_found_this_run += len(results)

            total_checked_this_run += (batch_end - current_index)
            current_index = batch_end
            
            if total_checked_this_run % (BATCH_SIZE * 5) == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Checked: {current_index-1} | Found: {total_found_this_run}")
                await save_state(current_index - 1, 0, 0)

    await save_state(current_index - 1, total_found_this_run, total_checked_this_run)
    generate_report(initial_index, current_index - 1, total_checked_this_run, total_found_this_run, time.time() - start_time, stop_reason)

if __name__ == "__main__":
    asyncio.run(main())
