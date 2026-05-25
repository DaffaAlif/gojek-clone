# -*- coding: utf-8 -*-
"""
run_all.py - Jalankan semua node sekaligus:
  1. Dashboard (Streamlit) -> http://localhost:8501
  2. Simulasi 1 ride (60 detik)

Usage:
    python run_all.py
    python run_all.py --duration 120
"""
import argparse
import os
import subprocess
import sys
import time

# Force UTF-8 output on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE = os.path.dirname(os.path.abspath(__file__))
PY   = sys.executable


def start(label, cmd, **kwargs):
    print(f"[START] {label}")
    return subprocess.Popen(cmd, cwd=BASE, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", "-d", type=int, default=60,
                        help="Durasi simulasi dalam detik (default: 60)")
    args = parser.parse_args()

    procs = []
    print("=" * 60)
    print("  Ride-Hailing Kafka - Node Launcher")
    print("=" * 60)

    try:
        # 1. Data server (port 8502) — harus start sebelum dashboard & simulasi
        data_srv = start(
            "Data server -> port 8502",
            [PY, os.path.join(BASE, "dashboard", "data_server.py")],
        )
        procs.append(("DataServer", data_srv))
        time.sleep(1)

        # 2. Dashboard
        dash = start(
            "Dashboard  -> http://localhost:8501",
            [PY, "-m", "streamlit", "run",
             os.path.join(BASE, "dashboard", "app.py"),
             "--server.port", "8501",
             "--server.headless", "true",
             "--server.runOnSave", "false"],
        )
        procs.append(("Dashboard", dash))

        print("   Menunggu dashboard siap...", end="", flush=True)
        time.sleep(4)
        print(" OK")

        # 2. Simulasi
        sim = start(
            f"Simulasi   -> 1 driver + 1 rider ({args.duration}s)",
            [PY, os.path.join(BASE, "simulate_one_ride.py"),
             "--duration", str(args.duration)],
        )
        procs.append(("Simulasi", sim))

        print("-" * 60)
        print("  Buka browser: http://localhost:8501")
        print("  Ctrl+C untuk menghentikan semua node")
        print("-" * 60)

        sim.wait()
        print("\n[OK] Simulasi selesai. Dashboard tetap berjalan.")
        print("     Ctrl+C untuk stop.\n")
        dash.wait()

    except KeyboardInterrupt:
        print("\n[STOP] Menghentikan semua node...")
        for name, p in procs:
            p.terminate()
            print(f"  - {name} dihentikan")


if __name__ == "__main__":
    main()
