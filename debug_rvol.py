"""
Run from the ross-pullback directory to verify whether the RVOL leaderboard
would have detected a mid-session spike (e.g. RHI Magnesita at 13:09).

Usage:
    /path/to/venv/bin/python3 debug_rvol.py RHIMAGNSITA
    /path/to/venv/bin/python3 debug_rvol.py          # lists all cached symbols
"""
import json
import os
import sys
from datetime import date

from config import Config

SESSION_START = 555   # 9:15 IST in minutes-from-midnight
SESSION_LEN   = 375   # minutes in full session


def ist(ts: int) -> str:
    m = (ts // 60 + 330) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


def slot(ts: int) -> int:
    """0-indexed minute slot from 9:15 (slot 0 = 9:15, slot 234 = 13:09)."""
    m = (ts // 60 + 330) % (24 * 60)
    return m - SESSION_START


def main():
    if not os.path.isdir(Config.BARS_CACHE_DIR):
        print(f"No bars cache found at {Config.BARS_CACHE_DIR!r} — run the scanner first.")
        return

    files = sorted(os.listdir(Config.BARS_CACHE_DIR))

    if len(sys.argv) < 2:
        print(f"Cached symbols ({len(files)}):")
        for f in files[:50]:
            print(" ", f.replace(".json", ""))
        if len(files) > 50:
            print(f"  ... and {len(files) - 50} more")
        return

    target = sys.argv[1].upper()
    match = next((f for f in files if target in f.upper()), None)
    if not match:
        print(f"Symbol {target!r} not found. Available: {[f.replace('.json','') for f in files if target[:3] in f.upper()]}")
        return

    symbol = match.replace(".json", "")
    with open(os.path.join(Config.BARS_CACHE_DIR, match)) as f:
        cache = json.load(f)

    today = date.today().isoformat()
    bars = cache.get(today, [])
    bars.sort(key=lambda b: b["ts"])

    # Avg daily volume from universe cache
    avg_daily = 0.0
    if os.path.exists(Config.UNIVERSE_CACHE):
        with open(Config.UNIVERSE_CACHE) as f:
            uni = json.load(f)
        for s in uni.get("symbols", []):
            if s["symbol"] == symbol:
                avg_daily = s.get("avg_daily_volume", 0.0)
                break

    # Per-slot historical averages (from all cached days except today)
    per_slot: dict[int, list[float]] = {}
    for day, day_bars in cache.items():
        if day == today:
            continue
        for b in day_bars:
            sl = slot(b["ts"])
            if 0 <= sl < SESSION_LEN and b["volume"] > 0:
                per_slot.setdefault(sl, []).append(b["volume"])
    slot_avg: dict[int, float] = {sl: sum(v) / len(v) for sl, v in per_slot.items()}

    print(f"\nSymbol      : {symbol}")
    print(f"Avg daily vol: {avg_daily:>14,.0f} shares  (from universe cache)")
    print(f"Today bars  : {len(bars)}")
    print(f"History days: {len(cache) - (1 if today in cache else 0)}")
    print()
    print(f"{'Time':>5}  {'Bar Vol':>10}  {'Cum Vol':>12}  "
          f"{'Cumul Ratio':>11}  {'Bar RVOL':>9}  {'Bar Ratio note'}")
    print("-" * 80)

    cum_vol = 0.0
    avg_bar = avg_daily / SESSION_LEN if avg_daily > 0 else 0.0

    for b in bars:
        vol = b["volume"]
        cum_vol += vol
        ts = b["ts"]
        sl = slot(ts)
        elapsed = max(sl + 1, 1)
        elapsed_frac = elapsed / SESSION_LEN

        # Formula 1: cumulative ratio (what we currently show on the leaderboard)
        cumul_ratio = (cum_vol / (avg_daily * elapsed_frac)) if avg_daily > 0 else 0.0

        # Formula 2: per-minute bar RVOL (Ross's approach)
        ref_vol = slot_avg.get(sl, avg_bar)
        bar_rvol = (vol / ref_vol) if ref_vol > 0 else 0.0

        # Only print if something notable is happening
        notable = bar_rvol >= 2.0 or cumul_ratio >= 3.0 or sl == 234   # 234 = 13:09
        if notable:
            note = f"← {bar_rvol:.1f}× the normal {ist(ts)} bar" if bar_rvol >= 2.0 else ""
            print(f"{ist(ts):>5}  {vol:>10,.0f}  {cum_vol:>12,.0f}  "
                  f"{cumul_ratio:>10.2f}×  {bar_rvol:>8.1f}×  {note}")

    print()
    print("Cumul Ratio = today cumulative ÷ (avg_daily × elapsed_fraction)  ← what leaderboard shows now")
    print("Bar RVOL    = this bar ÷ historical avg for same time slot         ← Ross's approach")


if __name__ == "__main__":
    main()
