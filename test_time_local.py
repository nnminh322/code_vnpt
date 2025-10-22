import time
N = 1_000_000
t0 = time.perf_counter()
c = 0
for _ in range(N):
    c += 1
dt = time.perf_counter() - t0
print(f"PY  done={c}, elapsed={dt:.4f}s")
