"""
crexi_watcher_app.py

Tkinter desktop UI for the Crexi land-deal watcher.
Runs crexi_watcher.py as a subprocess and streams log output in real time.
Shows deal counters broken down by market/state.

Launch:  python crexi_watcher_app.py
         (or double-click Launch Crexi Watcher.bat)
"""

import glob
import json
import os
import queue
import subprocess
import sys
import threading
import tkinter as tk
import webbrowser
from tkinter import ttk

# ── Path setup ───────────────────────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(PROJECT_DIR, "data")
DEDUP_PATH  = os.path.join(DATA_DIR, "seen_deals.json")
REPORTS_DIR = os.path.join(PROJECT_DIR, "reports")

sys.path.insert(0, PROJECT_DIR)
from crexi.scraper import STATE_ABBREVIATIONS

MARKETS = sorted(k.title() for k in STATE_ABBREVIATIONS)

# ── Theme ─────────────────────────────────────────────────────────────────────
BG       = "#0d1117"
BG2      = "#161b22"
FG       = "#e6edf3"
ACCENT   = "#238636"
DANGER   = "#da3633"
MUTED    = "#8b949e"
BORDER   = "#30363d"
FONT     = ("Consolas", 10)
FONT_HDR = ("Consolas", 10, "bold")
FONT_SM  = ("Consolas", 9)
FONT_TIT = ("Consolas", 14, "bold")


# ── Counter data ──────────────────────────────────────────────────────────────
def load_counters() -> dict:
    """
    Read all per-market seen_deals_*.json files (plus legacy seen_deals.json)
    and return per-market counters.
    Returns { market_name: {total, processed, skipped, pending}, "_total_": {...} }
    """
    # Per-market files are authoritative — read them first so their entries
    # win the de-dup check over the legacy seen_deals.json.
    per_market = sorted(glob.glob(os.path.join(DATA_DIR, "seen_deals_*.json")))
    legacy     = glob.glob(os.path.join(DATA_DIR, "seen_deals.json"))
    paths = per_market + legacy
    if not paths:
        return {}

    seen_ids: set = set()   # guard against double-counting across legacy + market files
    counters: dict = {}

    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        for lid, entry in data.items():
            if lid in seen_ids:
                continue
            seen_ids.add(lid)
            market = (entry.get("market") or "").strip() or "(unknown)"
            c = counters.setdefault(market, {"total": 0, "processed": 0, "skipped": 0, "pending": 0})
            c["total"] += 1
            if entry.get("processed"):
                c["processed"] += 1
            elif entry.get("skip_reason"):
                c["skipped"] += 1
            else:
                c["pending"] += 1

    if not counters:
        return {}

    grand = {"total": 0, "processed": 0, "skipped": 0, "pending": 0}
    for c in counters.values():
        for k in grand:
            grand[k] += c[k]
    counters["_total_"] = grand
    return counters


# ── App ───────────────────────────────────────────────────────────────────────
class CrexiWatcherApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Crexi Deal Watcher")
        self.configure(bg=BG)
        self.geometry("760x680")
        self.minsize(640, 520)

        self._proc: subprocess.Popen | None = None
        self._flask_proc: subprocess.Popen | None = None
        self._log_queue: queue.Queue = queue.Queue()
        self._run_queue: list = []      # [(market, max_deals), ...] for sequential fallback
        self._all_procs: list = []      # all active procs during Run All Markets
        self._active_procs: int = 0     # count of still-running procs
        self._running: bool = False     # True while any subprocess is active

        self._apply_styles()
        self._build_ui()
        self._refresh_counters()
        self._poll_log()
        self._live_refresh()

    # ── Styles ─────────────────────────────────────────────────────────────
    def _apply_styles(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Vertical.TScrollbar",
                        background=BORDER, troughcolor=BG2, arrowcolor=MUTED,
                        borderwidth=0)
        style.configure("TCombobox",
                        fieldbackground=BG2, background=BG2, foreground=FG,
                        arrowcolor=MUTED, selectbackground=BG2, selectforeground=FG)
        style.map("TCombobox", fieldbackground=[("readonly", BG2)])
        style.configure("TSpinbox",
                        fieldbackground=BG2, background=BG2, foreground=FG,
                        arrowcolor=MUTED)

    # ── UI construction ─────────────────────────────────────────────────────
    def _build_ui(self):
        # Title bar
        tk.Label(self, text="Crexi Deal Watcher", font=FONT_TIT,
                 bg=BG, fg=FG).pack(fill="x", padx=14, pady=(12, 6))

        self._build_controls()
        self._build_counters()
        self._build_log()

    def _build_controls(self):
        outer = tk.Frame(self, bg=BG2, highlightthickness=1, highlightbackground=BORDER)
        outer.pack(fill="x", padx=14, pady=(0, 8))

        # Row 1: market / max-deals / dry-run
        r1 = tk.Frame(outer, bg=BG2)
        r1.pack(fill="x", padx=10, pady=(10, 4))

        tk.Label(r1, text="Market", font=FONT, bg=BG2, fg=MUTED).pack(side="left")
        self._market_var = tk.StringVar(value="Washington")
        ttk.Combobox(r1, textvariable=self._market_var, values=MARKETS,
                     state="readonly", width=18, font=FONT).pack(side="left", padx=(6, 24))

        tk.Label(r1, text="Max Deals", font=FONT, bg=BG2, fg=MUTED).pack(side="left")
        self._max_deals_var = tk.IntVar(value=3)
        ttk.Spinbox(r1, from_=0, to=9999, textvariable=self._max_deals_var,
                    width=5, font=FONT).pack(side="left", padx=(6, 4))
        tk.Button(r1, text="All", font=FONT_SM, bg=BORDER, fg=MUTED,
                  activebackground=ACCENT, activeforeground=FG,
                  relief="flat", cursor="hand2", padx=4, pady=1,
                  command=self._set_all_pending).pack(side="left", padx=(0, 20))

        self._dry_run_var = tk.BooleanVar(value=False)
        tk.Checkbutton(r1, text="Dry Run", variable=self._dry_run_var,
                       font=FONT, bg=BG2, fg=FG, selectcolor=BG,
                       activebackground=BG2, activeforeground=FG,
                       highlightthickness=0).pack(side="left")

        # Row 2: action buttons
        r2 = tk.Frame(outer, bg=BG2)
        r2.pack(fill="x", padx=10, pady=(4, 10))

        self._run_btn = tk.Button(
            r2, text="▶  Run", font=FONT_HDR, width=10,
            bg=ACCENT, fg=FG, activebackground="#2ea043", activeforeground=FG,
            relief="flat", cursor="hand2", bd=0, padx=8, pady=4,
            command=self._start_run)
        self._run_btn.pack(side="left", padx=(0, 6))

        self._run_all_btn = tk.Button(
            r2, text="▶▶  Run All Markets", font=FONT_HDR,
            bg=ACCENT, fg=FG, activebackground="#2ea043", activeforeground=FG,
            relief="flat", cursor="hand2", bd=0, padx=8, pady=4,
            command=self._start_run_all)
        self._run_all_btn.pack(side="left", padx=(0, 6))

        self._stop_btn = tk.Button(
            r2, text="⏹  Stop", font=FONT_HDR, width=10,
            bg=BORDER, fg=MUTED, activebackground=DANGER, activeforeground=FG,
            relief="flat", cursor="hand2", bd=0, padx=8, pady=4,
            state="disabled", command=self._stop_run)
        self._stop_btn.pack(side="left", padx=(0, 24))

        tk.Button(
            r2, text="📂  Open Reports", font=FONT,
            bg=BG2, fg=MUTED, activebackground=BORDER, activeforeground=FG,
            relief="flat", cursor="hand2", bd=0, padx=6, pady=4,
            command=self._open_reports).pack(side="left")

        # Row 3: dashboard controls
        r3 = tk.Frame(outer, bg=BG2)
        r3.pack(fill="x", padx=10, pady=(0, 10))

        self._dash_dot = tk.Label(r3, text="●", fg=MUTED, bg=BG2, font=("Consolas", 10))
        self._dash_dot.pack(side="left", padx=(0, 4))

        tk.Button(
            r3, text="🌐  Open Dashboard", font=FONT,
            bg=ACCENT, fg=FG, activebackground="#2ea043", activeforeground=FG,
            relief="flat", cursor="hand2", bd=0, padx=8, pady=4,
            command=self._open_dashboard).pack(side="left", padx=(0, 6))

        tk.Button(
            r3, text="Stop Dashboard", font=FONT_SM,
            bg=BORDER, fg=MUTED, activebackground=DANGER, activeforeground=FG,
            relief="flat", cursor="hand2", bd=0, padx=6, pady=4,
            command=self._stop_dashboard).pack(side="left")

    def _build_counters(self):
        outer = tk.Frame(self, bg=BG2, highlightthickness=1, highlightbackground=BORDER)
        outer.pack(fill="x", padx=14, pady=(0, 8))

        hdr = tk.Frame(outer, bg=BG2)
        hdr.pack(fill="x", padx=10, pady=(8, 4))
        tk.Label(hdr, text="DEAL COUNTERS", font=FONT_HDR, bg=BG2, fg=MUTED).pack(side="left")
        self._refresh_ts = tk.Label(hdr, text="", font=FONT_SM, bg=BG2, fg=BORDER)
        self._refresh_ts.pack(side="right")

        self._counter_text = tk.Text(
            outer, bg=BG2, fg=FG, font=FONT, state="disabled",
            relief="flat", bd=0, padx=10, pady=4,
            height=8, wrap="none", cursor="arrow")
        self._counter_text.pack(fill="x", padx=0, pady=(0, 6))
        self._counter_text.tag_config("hdr",  foreground=MUTED)
        self._counter_text.tag_config("bold", font=FONT_HDR)
        self._counter_text.tag_config("sep",  foreground=BORDER)

    def _build_log(self):
        hdr = tk.Frame(self, bg=BG)
        hdr.pack(fill="x", padx=14)
        tk.Label(hdr, text="LOG", font=FONT_HDR, bg=BG, fg=MUTED).pack(side="left")
        tk.Button(hdr, text="Clear", font=FONT_SM, bg=BG, fg=MUTED,
                  relief="flat", cursor="hand2",
                  command=lambda: (self._log.config(state="normal"),
                                   self._log.delete("1.0", "end"),
                                   self._log.config(state="disabled"))
                  ).pack(side="right")

        outer = tk.Frame(self, bg=BG2, highlightthickness=1, highlightbackground=BORDER)
        outer.pack(fill="both", expand=True, padx=14, pady=(4, 14))

        self._log = tk.Text(outer, bg=BG2, fg=FG, font=FONT_SM, wrap="word",
                            state="disabled", relief="flat", bd=0,
                            padx=8, pady=6, insertbackground=FG)
        sb = ttk.Scrollbar(outer, orient="vertical", command=self._log.yview)
        self._log.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._log.pack(side="left", fill="both", expand=True)

    # ── Counter refresh ──────────────────────────────────────────────────────
    def _refresh_counters(self):
        from datetime import datetime
        counters = load_counters()
        markets  = sorted(k for k in counters if k != "_total_") if counters else []
        t        = counters.get("_total_", {})

        self._refresh_ts.config(
            text=f"{datetime.now().strftime('%H:%M:%S')}  "
                 f"{t.get('total',0)}T / {t.get('pending',0)}P")

        W = 22   # market column width
        N =  8   # number column width

        def row(market, c, bold=False):
            tag = "bold" if bold else ""
            line = f"{market:<{W}} {c.get('total',0):>{N}} {c.get('processed',0):>{N}} {c.get('skipped',0):>{N}} {c.get('pending',0):>{N}}\n"
            return (line, tag)

        self._counter_text.config(state="normal")
        self._counter_text.delete("1.0", "end")

        if not markets:
            self._counter_text.insert("end", "No data yet — run the watcher to populate counters.\n", "hdr")
        else:
            # Header
            hdr = f"{'Market':<{W}} {'Total':>{N}} {'Processed':>{N}} {'Skipped':>{N}} {'Pending':>{N}}\n"
            self._counter_text.insert("end", hdr, "hdr")
            self._counter_text.insert("end", "-" * (W + N*4 + 4) + "\n", "sep")

            for market in markets:
                line, tag = row(market, counters[market])
                self._counter_text.insert("end", line, tag)

            # Total
            self._counter_text.insert("end", "-" * (W + N*4 + 4) + "\n", "sep")
            line, tag = row("TOTAL", t, bold=True)
            self._counter_text.insert("end", line, tag)

        self._counter_text.config(state="disabled")

    def _set_all_pending(self):
        """Set Max Deals to the current pending count for the selected market."""
        market = self._market_var.get()
        counters = load_counters()
        pending = counters.get(market, {}).get("pending", 0)
        self._max_deals_var.set(pending)

    # ── Run / Stop ───────────────────────────────────────────────────────────
    def _start_run_all(self):
        """Launch all markets with pending deals simultaneously (parallel)."""
        counters = load_counters()
        markets = [
            (m, counters[m]["pending"]) for m in sorted(counters)
            if m != "_total_" and counters[m].get("pending", 0) > 0
        ]
        if not markets:
            self._log_append("\nNo pending deals in any market.\n")
            return

        self._all_procs.clear()
        self._active_procs = len(markets)
        self._running = True
        self._run_btn.config(state="disabled", bg=BORDER, fg=MUTED)
        self._run_all_btn.config(state="disabled", bg=BORDER, fg=MUTED)
        self._stop_btn.config(state="normal", bg=DANGER, fg=FG)

        market_names = ", ".join(m for m, _ in markets)
        self._log_append(
            f"\n{'='*60}\n"
            f"▶▶  Run All Markets (parallel): {market_names}\n"
            f"{'='*60}\n"
        )

        dry_run = self._dry_run_var.get()
        for market, pending in markets:
            cmd = [sys.executable, "crexi_watcher.py",
                   "--market", market, "--max-deals", str(pending)]
            if dry_run:
                cmd.append("--dry-run")
            self._log_append(f"\n--- Launching {market} ({pending} pending) ---\n")
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, encoding="utf-8", errors="replace", cwd=PROJECT_DIR,
                )
            except Exception as exc:
                self._log_append(f"ERROR launching {market}: {exc}\n")
                self._active_procs -= 1
                if self._active_procs <= 0:
                    self._on_run_finished()
                continue
            self._proc = proc
            self._all_procs.append(proc)
            threading.Thread(target=self._read_output_proc, args=(proc,), daemon=True).start()

    def _start_run(self):
        market    = self._market_var.get()
        max_deals = self._max_deals_var.get()
        dry_run   = self._dry_run_var.get()

        cmd = [sys.executable, "crexi_watcher.py",
               "--market", market, "--max-deals", str(max_deals)]
        if dry_run:
            cmd.append("--dry-run")

        self._log_append(f"\n{'='*60}\n"
                         f"▶  {market}  |  max-deals={max_deals}"
                         f"  |  dry-run={'yes' if dry_run else 'no'}\n"
                         f"{'='*60}\n")

        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=PROJECT_DIR,
            )
        except Exception as exc:
            self._log_append(f"ERROR starting process: {exc}\n")
            return

        self._running = True
        self._run_btn.config(state="disabled", bg=BORDER, fg=MUTED)
        self._stop_btn.config(state="normal", bg=DANGER, fg=FG)
        threading.Thread(target=self._read_output, daemon=True).start()

    def _stop_run(self):
        procs_to_stop = list(self._all_procs) or ([self._proc] if self._proc else [])
        for proc in procs_to_stop:
            if proc and proc.poll() is None:
                proc.terminate()
        self._log_append("\n⏹  Stopped by user.\n")

    def _read_output(self):
        """Read output from self._proc (single-market run)."""
        try:
            for line in self._proc.stdout:
                self._log_queue.put(line)
        except Exception:
            pass
        finally:
            self._proc.wait()
            self._log_queue.put(None)  # sentinel — run finished

    def _read_output_proc(self, proc: subprocess.Popen):
        """Read output from a specific proc (parallel multi-market run)."""
        try:
            for line in proc.stdout:
                self._log_queue.put(line)
        except Exception:
            pass
        finally:
            proc.wait()
            self._log_queue.put(None)  # sentinel — this proc finished

    def _poll_log(self):
        try:
            while True:
                item = self._log_queue.get_nowait()
                if item is None:
                    # One subprocess finished
                    self._refresh_counters()
                    if self._active_procs > 0:
                        self._active_procs -= 1
                        if self._active_procs == 0:
                            self._on_run_finished()
                    else:
                        # Single-market run (active_procs was never set)
                        self._on_run_finished()
                else:
                    self._log_append(item)
        except queue.Empty:
            pass
        self.after(100, self._poll_log)

    def _on_run_finished(self):
        self._running = False
        self._all_procs.clear()
        self._run_btn.config(state="normal", bg=ACCENT, fg=FG)
        self._run_all_btn.config(state="normal", bg=ACCENT, fg=FG)
        self._stop_btn.config(state="disabled", bg=BORDER, fg=MUTED)
        self._refresh_counters()
        self._log_append("\n✓  All done.\n")

    def _live_refresh(self):
        """Refresh counters every 3 s. Loop is bulletproof — never dies on error."""
        try:
            self._refresh_counters()
        except Exception as exc:
            self._log_append(f"[counter refresh error] {exc}\n")
        self._update_dash_dot()
        self.after(3000, self._live_refresh)

    # ── Dashboard ────────────────────────────────────────────────────────────
    def _open_dashboard(self):
        if self._flask_proc is None or self._flask_proc.poll() is not None:
            self._flask_proc = subprocess.Popen(
                [sys.executable, "app.py"],
                cwd=PROJECT_DIR,
            )
            self.after(1500, lambda: webbrowser.open("http://localhost:5000"))
        else:
            webbrowser.open("http://localhost:5000")
        self._update_dash_dot()

    def _stop_dashboard(self):
        if self._flask_proc and self._flask_proc.poll() is None:
            self._flask_proc.terminate()
            self._flask_proc = None
        self._update_dash_dot()

    def _update_dash_dot(self):
        alive = self._flask_proc is not None and self._flask_proc.poll() is None
        self._dash_dot.config(fg=ACCENT if alive else MUTED)

    def _log_append(self, text: str):
        self._log.config(state="normal")
        self._log.insert("end", text)
        self._log.see("end")
        self._log.config(state="disabled")

    # ── Utility ──────────────────────────────────────────────────────────────
    def _open_reports(self):
        os.makedirs(REPORTS_DIR, exist_ok=True)
        os.startfile(REPORTS_DIR)


if __name__ == "__main__":
    app = CrexiWatcherApp()
    app.mainloop()
