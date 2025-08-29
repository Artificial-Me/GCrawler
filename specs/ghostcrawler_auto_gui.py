"""
GhostCrawler GUI - Modern Dashboard Interface with ttkbootstrap
"""

import asyncio
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty
from tkinter import filedialog, messagebox
from typing import Dict, List, Optional, Any
import tkinter as tk

import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from ttkbootstrap.scrolled import ScrolledText
from ttkbootstrap.dialogs import Messagebox
import psutil

from ghostcrawler_auto_specs import (
    GhostCrawler, CrawlerConfig, 
    load_proxies_from_file, parse_proxy_url,
    validate_and_deduplicate_urls, resolve_proxy_config
)
from utils.url_processor import check_and_filter_urls, filter_urls_from_file


class SystemMonitor(ttk.Frame):
    """System resource monitoring widget"""
    
    def __init__(self, parent):
        super().__init__(parent)
        self.setup_ui()
        self.update_stats()
        
    def setup_ui(self):
        # CPU Usage
        self.cpu_frame = ttk.Frame(self)
        self.cpu_frame.pack(fill=X, pady=2)
        ttk.Label(self.cpu_frame, text="CPU:", width=10).pack(side=LEFT)
        self.cpu_var = ttk.DoubleVar(value=0)
        self.cpu_progress = ttk.Progressbar(
            self.cpu_frame, variable=self.cpu_var, 
            maximum=100, bootstyle="success-striped"
        )
        self.cpu_progress.pack(side=LEFT, fill=X, expand=True, padx=(5, 10))
        self.cpu_label = ttk.Label(self.cpu_frame, text="0%", width=8)
        self.cpu_label.pack(side=LEFT)
        
        # Memory Usage
        self.mem_frame = ttk.Frame(self)
        self.mem_frame.pack(fill=X, pady=2)
        ttk.Label(self.mem_frame, text="Memory:", width=10).pack(side=LEFT)
        self.mem_var = ttk.DoubleVar(value=0)
        self.mem_progress = ttk.Progressbar(
            self.mem_frame, variable=self.mem_var,
            maximum=100, bootstyle="warning-striped"
        )
        self.mem_progress.pack(side=LEFT, fill=X, expand=True, padx=(5, 10))
        self.mem_label = ttk.Label(self.mem_frame, text="0%", width=8)
        self.mem_label.pack(side=LEFT)
        
        # Disk Usage
        self.disk_frame = ttk.Frame(self)
        self.disk_frame.pack(fill=X, pady=2)
        ttk.Label(self.disk_frame, text="Disk:", width=10).pack(side=LEFT)
        self.disk_var = ttk.DoubleVar(value=0)
        self.disk_progress = ttk.Progressbar(
            self.disk_frame, variable=self.disk_var,
            maximum=100, bootstyle="info-striped"
        )
        self.disk_progress.pack(side=LEFT, fill=X, expand=True, padx=(5, 10))
        self.disk_label = ttk.Label(self.disk_frame, text="0%", width=8)
        self.disk_label.pack(side=LEFT)
        
    def update_stats(self):
        """Update system statistics"""
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=0.1)
            self.cpu_var.set(cpu_percent)
            self.cpu_label.config(text=f"{cpu_percent:.1f}%")
            self.cpu_progress.configure(
                bootstyle="danger-striped" if cpu_percent > 80 else 
                "warning-striped" if cpu_percent > 60 else "success-striped"
            )
            
            # Memory
            mem = psutil.virtual_memory()
            self.mem_var.set(mem.percent)
            self.mem_label.config(text=f"{mem.percent:.1f}%")
            self.mem_progress.configure(
                bootstyle="danger-striped" if mem.percent > 80 else
                "warning-striped" if mem.percent > 60 else "success-striped"
            )
            
            # Disk
            disk = psutil.disk_usage('/')
            self.disk_var.set(disk.percent)
            self.disk_label.config(text=f"{disk.percent:.1f}%")
            self.disk_progress.configure(
                bootstyle="danger-striped" if disk.percent > 90 else
                "warning-striped" if disk.percent > 75 else "info-striped"
            )
            
        except Exception as e:
            pass
        
        # Schedule next update
        self.after(2000, self.update_stats)


class CrawlerStats(ttk.Frame):
    """Crawler statistics display widget"""
    
    def __init__(self, parent):
        super().__init__(parent)
        self.setup_ui()
        
    def setup_ui(self):
        # Stats grid
        stats_frame = ttk.Frame(self)
        stats_frame.pack(fill=BOTH, expand=True)
        
        # Row 1
        row1 = ttk.Frame(stats_frame)
        row1.pack(fill=X, pady=2)
        
        ttk.Label(row1, text="URLs Processed:", font=("", 9, "bold")).pack(side=LEFT, padx=(0, 5))
        self.processed_label = ttk.Label(row1, text="0", bootstyle="success")
        self.processed_label.pack(side=LEFT, padx=(0, 15))
        
        ttk.Label(row1, text="Failed:", font=("", 9, "bold")).pack(side=LEFT, padx=(0, 5))
        self.failed_label = ttk.Label(row1, text="0", bootstyle="danger")
        self.failed_label.pack(side=LEFT, padx=(0, 15))
        
        ttk.Label(row1, text="Success Rate:", font=("", 9, "bold")).pack(side=LEFT, padx=(0, 5))
        self.success_rate_label = ttk.Label(row1, text="0%", bootstyle="info")
        self.success_rate_label.pack(side=LEFT)
        
        # Row 2
        row2 = ttk.Frame(stats_frame)
        row2.pack(fill=X, pady=2)
        
        ttk.Label(row2, text="Speed:", font=("", 9, "bold")).pack(side=LEFT, padx=(0, 5))
        self.speed_label = ttk.Label(row2, text="0 URLs/min", bootstyle="warning")
        self.speed_label.pack(side=LEFT, padx=(0, 15))
        
        ttk.Label(row2, text="Elapsed:", font=("", 9, "bold")).pack(side=LEFT, padx=(0, 5))
        self.elapsed_label = ttk.Label(row2, text="00:00:00")
        self.elapsed_label.pack(side=LEFT, padx=(0, 15))
        
        ttk.Label(row2, text="Status:", font=("", 9, "bold")).pack(side=LEFT, padx=(0, 5))
        self.status_label = ttk.Label(row2, text="Idle", bootstyle="secondary")
        self.status_label.pack(side=LEFT)
        
    def update_stats(self, processed=0, failed=0, speed=0, elapsed=0, status="Idle"):
        """Update crawler statistics"""
        self.processed_label.config(text=str(processed))
        self.failed_label.config(text=str(failed))
        
        total = processed + failed
        success_rate = (processed / total * 100) if total > 0 else 0
        self.success_rate_label.config(text=f"{success_rate:.1f}%")
        
        self.speed_label.config(text=f"{speed:.1f} URLs/min")
        
        hours, remainder = divmod(int(elapsed), 3600)
        minutes, seconds = divmod(remainder, 60)
        self.elapsed_label.config(text=f"{hours:02d}:{minutes:02d}:{seconds:02d}")
        
        self.status_label.config(text=status)
        if status == "Running":
            self.status_label.configure(bootstyle="success")
        elif status == "Stopped":
            self.status_label.configure(bootstyle="danger")
        else:
            self.status_label.configure(bootstyle="secondary")


class GhostCrawlerGUI:
    """Main GUI Application for GhostCrawler"""
    
    def __init__(self):
        self.root = ttk.Window(themename="solar")
        self.root.title("GhostCrawler Dashboard")
        self.root.geometry("1400x900")
        
        # Make window responsive
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
        # Settings file
        self.settings_file = Path("crawler_settings.json")
        
        # Crawler instance and thread
        self.crawler = None
        self.crawler_thread = None
        self.stop_event = threading.Event()
        self.message_queue = Queue()
        
        # Variables
        self.input_file_var = ttk.StringVar()
        self.output_dir_var = ttk.StringVar(value="Specs")
        self.proxy_var = ttk.StringVar(value="No proxy")
        
        # Load saved settings
        self.load_settings()
        
        # Setup UI
        self.setup_ui()
        
        # Start message processor
        self.process_messages()
        
    def setup_ui(self):
        """Setup the main UI"""
        # Main container
        main_container = ttk.Frame(self.root, padding=10)
        main_container.grid(row=0, column=0, sticky="nsew")
        main_container.columnconfigure(0, weight=1)
        main_container.columnconfigure(1, weight=1)
        main_container.columnconfigure(2, weight=1)
        main_container.rowconfigure(2, weight=1)
        
        # Top Section - File Selection and Quick Actions
        self.setup_top_section(main_container)
        
        # Middle Section - Settings
        self.setup_settings_section(main_container)
        
        # Bottom Section - Console and Stats
        self.setup_bottom_section(main_container)
        
        # Control Buttons
        self.setup_control_buttons(main_container)
        
    def setup_top_section(self, parent):
        """Setup file selection and quick actions"""
        top_frame = ttk.LabelFrame(parent, text="Input/Output Configuration", padding=10)
        top_frame.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 10))
        top_frame.columnconfigure(1, weight=1)
        
        # Input file selection
        ttk.Label(top_frame, text="Input File:").grid(row=0, column=0, sticky=W, padx=(0, 10))
        ttk.Entry(top_frame, textvariable=self.input_file_var).grid(row=0, column=1, sticky="ew", padx=(0, 5))
        ttk.Button(
            top_frame, text="Browse", 
            command=self.browse_input_file,
            bootstyle="primary-outline"
        ).grid(row=0, column=2, padx=(0, 10))
        
        # Output directory selection
        ttk.Label(top_frame, text="Output Dir:").grid(row=1, column=0, sticky=W, padx=(0, 10), pady=(5, 0))
        ttk.Entry(top_frame, textvariable=self.output_dir_var).grid(row=1, column=1, sticky="ew", padx=(0, 5), pady=(5, 0))
        ttk.Button(
            top_frame, text="Browse",
            command=self.browse_output_dir,
            bootstyle="primary-outline"
        ).grid(row=1, column=2, padx=(0, 10), pady=(5, 0))
        
        # Quick info
        info_frame = ttk.Frame(top_frame)
        info_frame.grid(row=0, column=3, rowspan=2, padx=(20, 0))
        
        self.url_count_label = ttk.Label(info_frame, text="URLs to process: 0", font=("", 10, "bold"))
        self.url_count_label.pack(anchor=W)
        
        self.existing_files_label = ttk.Label(info_frame, text="Existing files: 0")
        self.existing_files_label.pack(anchor=W)
        
    def setup_settings_section(self, parent):
        """Setup crawler settings"""
        # Left column - Basic Settings
        basic_frame = ttk.LabelFrame(parent, text="Basic Settings", padding=10)
        basic_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 5))
        
        # Browsers
        ttk.Label(basic_frame, text="Max Browsers:").grid(row=0, column=0, sticky=W, pady=2)
        self.browsers_var = ttk.IntVar(value=5)
        browsers_spin = ttk.Spinbox(basic_frame, from_=1, to=20, textvariable=self.browsers_var, width=10)
        browsers_spin.grid(row=0, column=1, sticky=W, pady=2, padx=(10, 0))
        
        # Batch Size
        ttk.Label(basic_frame, text="Batch Size:").grid(row=1, column=0, sticky=W, pady=2)
        self.batch_size_var = ttk.IntVar(value=20)
        batch_spin = ttk.Spinbox(basic_frame, from_=1, to=100, textvariable=self.batch_size_var, width=10)
        batch_spin.grid(row=1, column=1, sticky=W, pady=2, padx=(10, 0))
        
        # URL Delay
        ttk.Label(basic_frame, text="URL Delay (s):").grid(row=2, column=0, sticky=W, pady=2)
        self.delay_var = ttk.DoubleVar(value=0.5)
        delay_spin = ttk.Spinbox(basic_frame, from_=0, to=10, increment=0.1, textvariable=self.delay_var, width=10)
        delay_spin.grid(row=2, column=1, sticky=W, pady=2, padx=(10, 0))
        
        # Max URLs
        ttk.Label(basic_frame, text="Max URLs:").grid(row=3, column=0, sticky=W, pady=2)
        self.max_urls_var = ttk.IntVar(value=1000000)
        max_urls_spin = ttk.Spinbox(basic_frame, from_=1, to=9999999, textvariable=self.max_urls_var, width=10)
        max_urls_spin.grid(row=3, column=1, sticky=W, pady=2, padx=(10, 0))
        
        # Checkboxes
        self.headless_var = ttk.BooleanVar(value=True)
        ttk.Checkbutton(basic_frame, text="Headless Mode", variable=self.headless_var).grid(row=4, column=0, columnspan=2, sticky=W, pady=2)
        
        self.force_recrawl_var = ttk.BooleanVar(value=False)
        ttk.Checkbutton(basic_frame, text="Force Recrawl", variable=self.force_recrawl_var).grid(row=5, column=0, columnspan=2, sticky=W, pady=2)
        
        self.auto_mode_var = ttk.BooleanVar(value=True)
        ttk.Checkbutton(basic_frame, text="Auto Mode (process all)", variable=self.auto_mode_var).grid(row=6, column=0, columnspan=2, sticky=W, pady=2)
        
        self.new_filter_var = ttk.BooleanVar(value=False)
        ttk.Checkbutton(basic_frame, text="Use New URL Processor", variable=self.new_filter_var).grid(row=7, column=0, columnspan=2, sticky=W, pady=2)
        
        # Middle column - Advanced Settings
        advanced_frame = ttk.LabelFrame(parent, text="Advanced Settings", padding=10)
        advanced_frame.grid(row=1, column=1, sticky="nsew", padx=5)
        
        # Timeouts
        ttk.Label(advanced_frame, text="Request Timeout (ms):").grid(row=0, column=0, sticky=W, pady=2)
        self.request_timeout_var = ttk.IntVar(value=120000)
        ttk.Spinbox(advanced_frame, from_=10000, to=300000, increment=10000, textvariable=self.request_timeout_var, width=12).grid(row=0, column=1, sticky=W, pady=2, padx=(10, 0))
        
        ttk.Label(advanced_frame, text="Navigation Timeout (ms):").grid(row=1, column=0, sticky=W, pady=2)
        self.nav_timeout_var = ttk.IntVar(value=60000)
        ttk.Spinbox(advanced_frame, from_=10000, to=300000, increment=10000, textvariable=self.nav_timeout_var, width=12).grid(row=1, column=1, sticky=W, pady=2, padx=(10, 0))
        
        ttk.Label(advanced_frame, text="Turnstile Timeout (ms):").grid(row=2, column=0, sticky=W, pady=2)
        self.turnstile_timeout_var = ttk.IntVar(value=20000)
        ttk.Spinbox(advanced_frame, from_=5000, to=60000, increment=5000, textvariable=self.turnstile_timeout_var, width=12).grid(row=2, column=1, sticky=W, pady=2, padx=(10, 0))
        
        ttk.Label(advanced_frame, text="Memory Threshold (MB):").grid(row=3, column=0, sticky=W, pady=2)
        self.memory_threshold_var = ttk.IntVar(value=49152)
        ttk.Spinbox(advanced_frame, from_=1000, to=65536, increment=1000, textvariable=self.memory_threshold_var, width=12).grid(row=3, column=1, sticky=W, pady=2, padx=(10, 0))
        
        # More checkboxes
        self.humanize_var = ttk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="Humanize Behavior", variable=self.humanize_var).grid(row=4, column=0, columnspan=2, sticky=W, pady=2)
        
        self.geoip_var = ttk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="GeoIP Emulation", variable=self.geoip_var).grid(row=5, column=0, columnspan=2, sticky=W, pady=2)
        
        self.block_webrtc_var = ttk.BooleanVar(value=False)
        ttk.Checkbutton(advanced_frame, text="Block WebRTC", variable=self.block_webrtc_var).grid(row=6, column=0, columnspan=2, sticky=W, pady=2)
        
        self.aggressive_wait_var = ttk.BooleanVar(value=False)
        ttk.Checkbutton(advanced_frame, text="Aggressive Wait Mode", variable=self.aggressive_wait_var).grid(row=7, column=0, columnspan=2, sticky=W, pady=2)
        
        # Right column - Proxy & System
        system_frame = ttk.LabelFrame(parent, text="Proxy & System", padding=10)
        system_frame.grid(row=1, column=2, sticky="nsew", padx=(5, 0))
        
        # Proxy selection
        ttk.Label(system_frame, text="Proxy:").grid(row=0, column=0, sticky=W, pady=2)
        self.proxy_combo = ttk.Combobox(system_frame, textvariable=self.proxy_var, width=20, state="readonly")
        self.proxy_combo.grid(row=0, column=1, sticky=W, pady=2, padx=(10, 0))
        ttk.Button(system_frame, text="Refresh", command=self.refresh_proxies, bootstyle="secondary-outline").grid(row=0, column=2, padx=(5, 0))
        
        # System Monitor
        ttk.Label(system_frame, text="System Resources:", font=("", 10, "bold")).grid(row=1, column=0, columnspan=3, sticky=W, pady=(10, 5))
        self.system_monitor = SystemMonitor(system_frame)
        self.system_monitor.grid(row=2, column=0, columnspan=3, sticky="ew", pady=5)
        
        # Crawler Stats
        ttk.Label(system_frame, text="Crawler Statistics:", font=("", 10, "bold")).grid(row=3, column=0, columnspan=3, sticky=W, pady=(10, 5))
        self.crawler_stats = CrawlerStats(system_frame)
        self.crawler_stats.grid(row=4, column=0, columnspan=3, sticky="ew", pady=5)
        
        # Load proxies
        self.refresh_proxies()
        
    def setup_bottom_section(self, parent):
        """Setup console output and progress"""
        console_frame = ttk.LabelFrame(parent, text="Console Output", padding=10)
        console_frame.grid(row=2, column=0, columnspan=3, sticky="nsew", pady=10)
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)
        
        # Console output
        self.console = ScrolledText(console_frame, height=15, wrap=tk.WORD, autohide=True)
        self.console.grid(row=0, column=0, sticky="nsew")
        
        # Progress bar
        self.progress_var = ttk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            console_frame, variable=self.progress_var,
            maximum=100, bootstyle="success-striped"
        )
        self.progress_bar.grid(row=1, column=0, sticky="ew", pady=(5, 0))
        
    def setup_control_buttons(self, parent):
        """Setup control buttons"""
        button_frame = ttk.Frame(parent)
        button_frame.grid(row=3, column=0, columnspan=3, sticky="ew")
        
        # Left side - main controls
        left_buttons = ttk.Frame(button_frame)
        left_buttons.pack(side=LEFT)
        
        self.start_button = ttk.Button(
            left_buttons, text="🚀 Start Crawling",
            command=self.start_crawling,
            bootstyle="success", width=20
        )
        self.start_button.pack(side=LEFT, padx=2)
        
        self.stop_button = ttk.Button(
            left_buttons, text="⏹ Stop",
            command=self.stop_crawling,
            bootstyle="danger", width=15,
            state=DISABLED
        )
        self.stop_button.pack(side=LEFT, padx=2)
        
        ttk.Button(
            left_buttons, text="🔍 Analyze URLs",
            command=self.analyze_urls,
            bootstyle="info-outline", width=15
        ).pack(side=LEFT, padx=2)
        
        # Right side - settings controls
        right_buttons = ttk.Frame(button_frame)
        right_buttons.pack(side=RIGHT)
        
        ttk.Button(
            right_buttons, text="💾 Save Settings",
            command=self.save_settings,
            bootstyle="primary-outline"
        ).pack(side=LEFT, padx=2)
        
        ttk.Button(
            right_buttons, text="📂 Load Settings",
            command=self.load_settings,
            bootstyle="secondary-outline"
        ).pack(side=LEFT, padx=2)
        
        ttk.Button(
            right_buttons, text="🔄 Reset",
            command=self.reset_settings,
            bootstyle="warning-outline"
        ).pack(side=LEFT, padx=2)
        
        ttk.Button(
            right_buttons, text="❌ Exit",
            command=self.on_closing,
            bootstyle="danger-outline"
        ).pack(side=LEFT, padx=2)
        
    def browse_input_file(self):
        """Browse for input file"""
        filename = filedialog.askopenfilename(
            title="Select URL file",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialdir="input"
        )
        if filename:
            self.input_file_var.set(filename)
            self.analyze_urls()
            
    def browse_output_dir(self):
        """Browse for output directory"""
        dirname = filedialog.askdirectory(
            title="Select output directory",
            initialdir="."
        )
        if dirname:
            self.output_dir_var.set(dirname)
            
    def refresh_proxies(self):
        """Refresh proxy list"""
        proxies = ["No proxy"]
        proxy_data = load_proxies_from_file()
        for proxy in proxy_data:
            proxies.append(f"{proxy['name']} (ID: {proxy['id']})")
        self.proxy_combo['values'] = proxies
        if not self.proxy_var.get():
            self.proxy_var.set("No proxy")
            
    def analyze_urls(self):
        """Analyze URLs in the input file"""
        input_file = self.input_file_var.get()
        if not input_file or not os.path.exists(input_file):
            self.url_count_label.config(text="URLs to process: 0")
            return
            
        try:
            # Read URLs from file
            with open(input_file, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            # Validate URLs
            valid_urls = validate_and_deduplicate_urls(urls)
            
            # Check existing files
            if self.new_filter_var.get():
                filtered_urls = check_and_filter_urls(
                    input_file,
                    output_dir=self.output_dir_var.get(),
                    force_recrawl=self.force_recrawl_var.get()
                )
            else:
                filtered_urls = filter_urls_from_file(
                    input_file,
                    output_base_dir=self.output_dir_var.get(),
                    specs_dir="Specs",
                    force_recrawl=self.force_recrawl_var.get()
                )
            
            existing = len(valid_urls) - len(filtered_urls)
            
            self.url_count_label.config(text=f"URLs to process: {len(filtered_urls)}")
            self.existing_files_label.config(text=f"Existing files: {existing}")
            
            self.log_message(f"Found {len(urls)} total URLs")
            self.log_message(f"Valid URLs: {len(valid_urls)}")
            self.log_message(f"Already crawled: {existing}")
            self.log_message(f"To be processed: {len(filtered_urls)}")
            
        except Exception as e:
            self.log_message(f"Error analyzing URLs: {e}", "error")
            
    def save_settings(self):
        """Save current settings to file"""
        settings = {
            "input_file": self.input_file_var.get(),
            "output_dir": self.output_dir_var.get(),
            "max_browsers": self.browsers_var.get(),
            "batch_size": self.batch_size_var.get(),
            "url_delay": self.delay_var.get(),
            "max_urls": self.max_urls_var.get(),
            "headless": self.headless_var.get(),
            "force_recrawl": self.force_recrawl_var.get(),
            "auto_mode": self.auto_mode_var.get(),
            "new_filter": self.new_filter_var.get(),
            "request_timeout": self.request_timeout_var.get(),
            "navigation_timeout": self.nav_timeout_var.get(),
            "turnstile_timeout": self.turnstile_timeout_var.get(),
            "memory_threshold": self.memory_threshold_var.get(),
            "humanize": self.humanize_var.get(),
            "geoip": self.geoip_var.get(),
            "block_webrtc": self.block_webrtc_var.get(),
            "aggressive_wait": self.aggressive_wait_var.get(),
            "proxy": self.proxy_var.get()
        }
        
        try:
            with open(self.settings_file, 'w') as f:
                json.dump(settings, f, indent=2)
            self.log_message("Settings saved successfully")
            Messagebox.show_info("Settings saved successfully!", "Success")
        except Exception as e:
            self.log_message(f"Error saving settings: {e}", "error")
            Messagebox.show_error(f"Failed to save settings: {e}", "Error")
            
    def load_settings(self):
        """Load settings from file"""
        if not self.settings_file.exists():
            return
            
        try:
            with open(self.settings_file, 'r') as f:
                settings = json.load(f)
            
            # Apply settings
            self.input_file_var.set(settings.get("input_file", ""))
            self.output_dir_var.set(settings.get("output_dir", "Specs"))
            self.browsers_var.set(settings.get("max_browsers", 5))
            self.batch_size_var.set(settings.get("batch_size", 20))
            self.delay_var.set(settings.get("url_delay", 0.5))
            self.max_urls_var.set(settings.get("max_urls", 1000000))
            self.headless_var.set(settings.get("headless", True))
            self.force_recrawl_var.set(settings.get("force_recrawl", False))
            self.auto_mode_var.set(settings.get("auto_mode", True))
            self.new_filter_var.set(settings.get("new_filter", False))
            self.request_timeout_var.set(settings.get("request_timeout", 120000))
            self.nav_timeout_var.set(settings.get("navigation_timeout", 60000))
            self.turnstile_timeout_var.set(settings.get("turnstile_timeout", 20000))
            self.memory_threshold_var.set(settings.get("memory_threshold", 49152))
            self.humanize_var.set(settings.get("humanize", True))
            self.geoip_var.set(settings.get("geoip", True))
            self.block_webrtc_var.set(settings.get("block_webrtc", False))
            self.aggressive_wait_var.set(settings.get("aggressive_wait", False))
            self.proxy_var.set(settings.get("proxy", "No proxy"))
            
            self.log_message("Settings loaded successfully")
        except Exception as e:
            self.log_message(f"Error loading settings: {e}", "error")
            
    def reset_settings(self):
        """Reset all settings to default"""
        result = Messagebox.show_question(
            "Are you sure you want to reset all settings to default?",
            "Reset Settings"
        )
        if result == "Yes":
            self.input_file_var.set("")
            self.output_dir_var.set("Specs")
            self.browsers_var.set(5)
            self.batch_size_var.set(20)
            self.delay_var.set(0.5)
            self.max_urls_var.set(1000000)
            self.headless_var.set(True)
            self.force_recrawl_var.set(False)
            self.auto_mode_var.set(True)
            self.new_filter_var.set(False)
            self.request_timeout_var.set(120000)
            self.nav_timeout_var.set(60000)
            self.turnstile_timeout_var.set(20000)
            self.memory_threshold_var.set(49152)
            self.humanize_var.set(True)
            self.geoip_var.set(True)
            self.block_webrtc_var.set(False)
            self.aggressive_wait_var.set(False)
            self.proxy_var.set("No proxy")
            self.log_message("Settings reset to default")
            
    def log_message(self, message, level="info"):
        """Log message to console"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Color coding based on level
        if level == "error":
            formatted_msg = f"[{timestamp}] ❌ {message}\n"
        elif level == "warning":
            formatted_msg = f"[{timestamp}] ⚠️ {message}\n"
        elif level == "success":
            formatted_msg = f"[{timestamp}] ✅ {message}\n"
        else:
            formatted_msg = f"[{timestamp}] ℹ️ {message}\n"
        
        self.console.insert(tk.END, formatted_msg)
        self.console.see(tk.END)
        self.root.update_idletasks()
        
    def start_crawling(self):
        """Start the crawling process"""
        # Validate input
        if not self.input_file_var.get():
            Messagebox.show_error("Please select an input file!", "Error")
            return
            
        if not os.path.exists(self.input_file_var.get()):
            Messagebox.show_error("Input file does not exist!", "Error")
            return
            
        # Disable start button, enable stop
        self.start_button.config(state=DISABLED)
        self.stop_button.config(state=NORMAL)
        
        # Clear console
        self.console.delete(1.0, tk.END)
        
        # Reset stop event
        self.stop_event.clear()
        
        # Start crawler in thread
        self.crawler_thread = threading.Thread(target=self.run_crawler, daemon=True)
        self.crawler_thread.start()
        
        self.log_message("Starting crawler...", "success")
        self.crawler_stats.update_stats(status="Running")
        
    def stop_crawling(self):
        """Stop the crawling process"""
        self.log_message("Stopping crawler...", "warning")
        self.stop_event.set()
        self.crawler_stats.update_stats(status="Stopping")
        
    def run_crawler(self):
        """Run the crawler in a separate thread"""
        try:
            # Get proxy configuration
            proxy_config = None
            if self.proxy_var.get() != "No proxy":
                proxy_name = self.proxy_var.get().split(" (ID:")[0]
                proxy_data = load_proxies_from_file()
                for proxy in proxy_data:
                    if proxy['name'] == proxy_name:
                        proxy_config = parse_proxy_url(proxy['url'])
                        break
            
            # Create crawler configuration
            config_kwargs = {
                'max_browsers': self.browsers_var.get(),
                'headless': self.headless_var.get(),
                'batch_size': self.batch_size_var.get(),
                'url_delay': self.delay_var.get(),
                'output_dir': self.output_dir_var.get(),
                'humanize': self.humanize_var.get(),
                'geoip': self.geoip_var.get(),
                'block_webrtc': self.block_webrtc_var.get(),
                'request_timeout': self.request_timeout_var.get(),
                'navigation_timeout': self.nav_timeout_var.get(),
                'turnstile_timeout': self.turnstile_timeout_var.get(),
                'memory_threshold_mb': self.memory_threshold_var.get(),
                'aggressive_wait_mode': self.aggressive_wait_var.get(),
                'max_total_urls': self.max_urls_var.get() if not self.auto_mode_var.get() else 9999999
            }
            
            if proxy_config:
                config_kwargs['proxy_server'] = proxy_config.get('server')
                config_kwargs['proxy_username'] = proxy_config.get('username')
                config_kwargs['proxy_password'] = proxy_config.get('password')
            
            # Get filtered URLs
            if self.new_filter_var.get():
                urls = check_and_filter_urls(
                    self.input_file_var.get(),
                    output_dir=self.output_dir_var.get(),
                    force_recrawl=self.force_recrawl_var.get()
                )
            else:
                urls = filter_urls_from_file(
                    self.input_file_var.get(),
                    output_base_dir=self.output_dir_var.get(),
                    specs_dir="Specs",
                    force_recrawl=self.force_recrawl_var.get()
                )
            
            if not urls:
                self.message_queue.put(("log", "All URLs have already been crawled!", "warning"))
                return
            
            # Validate URLs
            urls = validate_and_deduplicate_urls(urls)
            
            # Update progress bar max
            self.progress_bar.configure(maximum=len(urls))
            
            # Create and run crawler
            config = CrawlerConfig(**config_kwargs)
            self.crawler = GhostCrawler(config)
            
            # Run async crawler
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Initialize crawler
            loop.run_until_complete(self.crawler.initialize())
            
            # Start crawling with monitoring
            start_time = time.time()
            
            # Create crawl task
            crawl_task = loop.create_task(self.crawler.crawl(urls))
            
            # Monitor progress
            while not crawl_task.done():
                if self.stop_event.is_set():
                    crawl_task.cancel()
                    break
                    
                # Update stats
                elapsed = time.time() - start_time
                processed = self.crawler.urls_processed
                failed = self.crawler.urls_failed
                speed = (processed / elapsed * 60) if elapsed > 0 else 0
                
                self.message_queue.put(("stats", {
                    "processed": processed,
                    "failed": failed,
                    "speed": speed,
                    "elapsed": elapsed
                }))
                
                self.message_queue.put(("progress", processed + failed))
                
                # Small delay to prevent excessive updates
                loop.run_until_complete(asyncio.sleep(0.5))
            
            # Cleanup
            loop.run_until_complete(self.crawler.cleanup())
            loop.close()
            
            self.message_queue.put(("log", "Crawling completed!", "success"))
            self.message_queue.put(("status", "Completed"))
            
        except Exception as e:
            self.message_queue.put(("log", f"Crawler error: {e}", "error"))
            self.message_queue.put(("status", "Error"))
        finally:
            self.message_queue.put(("enable_start", True))
            
    def process_messages(self):
        """Process messages from crawler thread"""
        try:
            while True:
                msg_type, data = self.message_queue.get_nowait()
                
                if msg_type == "log":
                    if len(data) == 2:
                        self.log_message(data[0], data[1])
                    else:
                        self.log_message(data)
                elif msg_type == "stats":
                    self.crawler_stats.update_stats(**data, status="Running")
                elif msg_type == "progress":
                    self.progress_var.set(data)
                elif msg_type == "status":
                    self.crawler_stats.update_stats(status=data)
                elif msg_type == "enable_start":
                    self.start_button.config(state=NORMAL)
                    self.stop_button.config(state=DISABLED)
                    
        except Empty:
            pass
        finally:
            # Schedule next check
            self.root.after(100, self.process_messages)
            
    def on_closing(self):
        """Handle window closing"""
        if self.crawler_thread and self.crawler_thread.is_alive():
            result = Messagebox.show_question(
                "Crawler is still running. Do you want to stop it and exit?",
                "Exit Confirmation"
            )
            if result == "Yes":
                self.stop_event.set()
                self.root.after(1000, self.root.destroy)
            return
        self.root.destroy()
        
    def run(self):
        """Run the GUI application"""
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.root.mainloop()


if __name__ == "__main__":
    app = GhostCrawlerGUI()
    app.run()