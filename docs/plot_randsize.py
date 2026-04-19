"""
Generate a comparison plot of the legacy log2 distribution vs the new
lognormal distribution used by --obj.randsize in warp-replay.

Plotted as histograms with SIZE on the X axis (log2 scale) and object
count on the Y axis.  Parameters: maxSize=100MB, 50,000 samples, sigma=1.0.
"""

import math
import random
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# --- Parameters -------------------------------------------------------
MAX_SIZE = 100 * 1024 * 1024   # 100 MB
MIN_SIZE = 1
SIGMA    = 1.0
N        = 50_000
rng      = random.Random(42)

def fmt_bytes(n):
    for unit, div in [("GB", 1<<30), ("MB", 1<<20), ("KB", 1<<10)]:
        if n >= div:
            return f"{n/div:.0f} {unit}"
    return f"{n} B"

# --- Log2 (legacy) sampler -------------------------------------------
def legacy_exp_rand_size(min_size, max_size):
    if max_size - min_size < 10:
        return 1 + min_size
    log_max = math.log2(max_size - 1)
    log_min = max(7.0, log_max - 8)
    if min_size > 0:
        log_min = math.log2(max(2, min_size) - 1)
    delta  = log_max - log_min
    r      = rng.random()
    log_s  = r * delta
    if log_s > 1:
        return 1 + int(2 ** (log_s + log_min))
    return 1 + min_size + int(r * 2 ** (log_min + 1))

# --- Lognormal (new) sampler -----------------------------------------
def lognormal_rand_size(min_size, max_size, sigma=1.0):
    mu = math.log(max_size / 10.0)
    for _ in range(100):
        s = round(math.exp(mu + sigma * rng.gauss(0, 1)))
        if min_size <= s <= max_size:
            return s
    s = round(math.exp(mu + sigma * rng.gauss(0, 1)))
    return max(min_size, min(max_size, s))

# --- Sample -----------------------------------------------------------
legacy_sizes    = [legacy_exp_rand_size(MIN_SIZE, MAX_SIZE)       for _ in range(N)]
lognormal_sizes = [lognormal_rand_size(MIN_SIZE, MAX_SIZE, SIGMA)  for _ in range(N)]

legacy_mean    = sum(legacy_sizes)    / N
lognormal_mean = sum(lognormal_sizes) / N

# Bins: one per doubling, from 128 B to 100 MB
bin_edges = [2**k for k in range(7, 28) if 2**k <= MAX_SIZE * 1.01]
bin_edges = sorted(set(bin_edges + [MAX_SIZE]))

# --- Plot -------------------------------------------------------------
fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=False)
fig.suptitle(
    f"warp-replay --obj.randsize: Object Size Distribution  |  maxSize=100 MB, n={N:,}",
    fontsize=12, fontweight="bold"
)

tick_bytes  = [128, 1<<10, 8<<10, 64<<10, 512<<10, 4<<20, 32<<20, MAX_SIZE]
tick_labels = ["128 B", "1 KB", "8 KB", "64 KB", "512 KB", "4 MB", "32 MB", "100 MB"]

def make_panel(ax, data, title, color, mean_val, annot):
    counts, edges = np.histogram(data, bins=bin_edges)
    centres = [math.sqrt(edges[i] * edges[i+1]) for i in range(len(edges)-1)]
    widths  = [edges[i+1] - edges[i]             for i in range(len(edges)-1)]
    ax.bar(centres, counts, width=widths, color=color, edgecolor="white",
           linewidth=0.4, alpha=0.85, align="center")

    ax.set_xscale("log", base=2)
    ax.set_xticks(tick_bytes)
    ax.set_xticklabels(tick_labels, rotation=35, ha="right", fontsize=8.5)
    ax.set_xlim(100, MAX_SIZE * 1.4)
    ax.set_xlabel("Object Size  (log₂ scale)", fontsize=10)
    ax.set_ylabel("Number of Objects", fontsize=10)
    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.grid(axis="y", alpha=0.3)

    ax.axvline(mean_val, color="black", linewidth=1.2, linestyle="--")
    ymax = ax.get_ylim()[1]
    ax.text(mean_val * 1.15, ymax * 0.82,
            f"mean\n{fmt_bytes(int(mean_val))}", fontsize=8)

    ax.text(0.03, 0.95, annot, transform=ax.transAxes, fontsize=8,
            va="top", bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.85))

make_panel(axes[0], legacy_sizes,
           "--obj.rand-log2  (legacy log₂)", "#e07b54", legacy_mean,
           "Equal count per doubling\n(flat in log₂-space)\nNo \"typical\" object size")
make_panel(axes[1], lognormal_sizes,
           f"--obj.rand-logn  (lognormal, σ={SIGMA})", "#4c8cbf", lognormal_mean,
           f"Bell curve in log-space\nMedian = maxSize/10 = 10 MB\nσ={SIGMA} → ~9 doublings span")

plt.tight_layout()
out = "docs/randsize_distribution.png"
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"Saved: {out}")
print(f"Legacy mean:    {fmt_bytes(int(legacy_mean))}  ({legacy_mean/MAX_SIZE*100:.1f}% of max)")
print(f"Lognormal mean: {fmt_bytes(int(lognormal_mean))}  ({lognormal_mean/MAX_SIZE*100:.1f}% of max)")
