"""
Kill switch logic — stops executors and marks candidates.

Only marks candidates as EMERGENCY_STOPPED if the executor was successfully
stopped. Failed stops keep TESTNET_ACTIVE with emergency_stop_attempted flag.
"""
import asyncio
import json
import logging
import os
import sys

import aiohttp
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

HB_API = os.environ.get("HUMMINGBOT_API_HOST", "http://localhost:8000")
HB_USER = os.environ.get("HUMMINGBOT_API_USERNAME", "admin")
HB_PASS = os.environ.get("HUMMINGBOT_API_PASSWORD", "admin")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.environ.get("MONGO_DATABASE", "quants_lab")


async def stop_executors():
    """Stop all active executors. Returns (succeeded_ids, failed_ids)."""
    succeeded = []
    failed = []

    auth = aiohttp.BasicAuth(HB_USER, HB_PASS)
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as session:
        # Fetch active executors
        try:
            async with session.post(
                f"{HB_API}/executors/search",
                json={"status": "active"},
            ) as resp:
                if resp.status >= 400:
                    logger.warning(f"  Could not fetch executors: HTTP {resp.status}")
                    return succeeded, failed
                data = await resp.json()
                if not isinstance(data, list):
                    logger.warning(f"  Unexpected executor response: {type(data)}")
                    return succeeded, failed
        except Exception as e:
            logger.warning(f"  HB API unreachable: {e}")
            return succeeded, failed

        if not data:
            logger.info("  No active executors")
            return succeeded, failed

        # Stop each executor individually
        for executor in data:
            eid = executor.get("executor_id") or executor.get("id")
            if not eid:
                continue
            try:
                async with session.post(f"{HB_API}/executors/{eid}/stop") as resp:
                    if resp.status < 400:
                        succeeded.append(eid)
                        logger.info(f"  Stopped executor {eid}")
                    else:
                        text = await resp.text()
                        failed.append((eid, f"HTTP {resp.status}: {text}"))
                        logger.warning(f"  Failed to stop {eid}: HTTP {resp.status}")
            except Exception as e:
                failed.append((eid, str(e)))
                logger.warning(f"  Failed to stop {eid}: {e}")

    return succeeded, failed


def mark_candidates(succeeded_ids: list, failed_ids: list):
    """Mark MongoDB candidates based on executor stop results."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    now_ms = int(asyncio.get_event_loop().time() * 1000)

    import time
    now_ms = int(time.time() * 1000)

    # Find all active/placing candidates
    active = list(db.candidates.find({
        "disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]},
    }))

    if not active:
        logger.info("  No active candidates in MongoDB")
        return

    succeeded_set = set(succeeded_ids)
    failed_set = {eid for eid, _ in failed_ids}

    stopped_count = 0
    kept_count = 0

    for doc in active:
        cid = doc.get("candidate_id")
        eid = doc.get("executor_id")

        if eid and eid in failed_set:
            # Executor stop failed — keep active with flag
            db.candidates.update_one(
                {"candidate_id": cid},
                {"$set": {
                    "emergency_stop_attempted": True,
                    "emergency_stop_error": dict(failed_ids).get(eid, "unknown"),
                }},
            )
            kept_count += 1
            logger.warning(f"  {cid}: kept TESTNET_ACTIVE (stop failed for {eid})")
        else:
            # Executor stopped successfully, or no executor (orphan/placing)
            db.candidates.update_one(
                {"candidate_id": cid},
                {"$set": {
                    "disposition": "EMERGENCY_STOPPED",
                    "stopped_at": now_ms,
                }},
            )
            stopped_count += 1

    logger.info(f"  Marked {stopped_count} EMERGENCY_STOPPED, "
                f"{kept_count} kept active (stop failed)")
    client.close()


async def main():
    logger.info("[2/4] Stopping active executors...")
    succeeded, failed = await stop_executors()

    logger.info("[3/4] Marking candidates in MongoDB...")
    mark_candidates(succeeded, failed)

    if failed:
        logger.warning(f"  WARNING: {len(failed)} executor(s) could not be stopped!")
        for eid, err in failed:
            logger.warning(f"    {eid}: {err}")


if __name__ == "__main__":
    asyncio.run(main())
