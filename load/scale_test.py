"""Measure worker drain throughput: submit N jobs, time until all complete."""
import asyncio, httpx, random, time, urllib.request, json, sys

N = int(sys.argv[1]) if len(sys.argv) > 1 else 900
BASE = "http://localhost:8000"

async def submit(client, i):
    w = random.choice([64, 96, 128, 160, 192])
    await client.post(
        f"{BASE}/jobs",
        json={"image_url": f"synthetic://img-{i}.jpg", "width": w, "height": w},
        headers={"X-API-Key": "scale"},
    )

async def flood():
    async with httpx.AsyncClient(timeout=60) as c:
        await asyncio.gather(*[submit(c, i) for i in range(N)])

def stats():
    return json.load(urllib.request.urlopen(f"{BASE}/stats"))

if __name__ == "__main__":
    asyncio.run(flood())
    start = time.time()
    while True:
        if stats()["completed_total"] >= N:
            break
        if time.time() - start > 180:
            print("TIMEOUT"); break
        time.sleep(0.2)
    elapsed = time.time() - start
    print(f"{N} jobs drained in {elapsed:.1f}s -> {N/elapsed:.1f} jobs/sec")