"""
抖音罗盘数据抓取器 - GUI 主界面
使用 tkinter 构建，包含：账号配置、调度设置、日志查看、系统托盘
跨平台支持：Windows / macOS (Apple Silicon & Intel)
"""

import logging
import shutil
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, font as tkfont, ttk, messagebox, scrolledtext
from datetime import datetime

from config_manager import (
    APP_DIR,
    COOKIE_FILE,
    DATA_DIR,
    DOWNLOAD_DIR,
    IS_MACOS,
    IS_WINDOWS,
    load_config,
    save_config,
)
from scraper import DouyinCompassScraper, parse_user_date_text

logger = logging.getLogger("douyin_rpa")

UI_FONT = "SF Pro Text" if IS_MACOS else "Microsoft YaHei UI"
UI_FONT_BOLD = "SF Pro Display" if IS_MACOS else "Microsoft YaHei UI"
MONO_FONT = ("Menlo", 10) if IS_MACOS else ("Cascadia Mono", 10)


def _resource_path(relative_path: str) -> Path:
    """返回开发环境或 PyInstaller 打包后的资源路径。"""
    base_path = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    return base_path / relative_path

# ============================================================
# 日志 Handler：将日志输出到 tkinter Text 组件
# ============================================================
class TextHandler(logging.Handler):
    """将 logging 输出重定向到 tkinter ScrolledText。"""

    def __init__(self, text_widget: scrolledtext.ScrolledText):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record) + "\n"
        # 线程安全写入
        self.text_widget.after(0, self._append, msg)

    def _append(self, msg):
        self.text_widget.configure(state="normal")
        self.text_widget.insert(tk.END, msg)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state="disabled")


# ============================================================
# 主窗口
# ============================================================
class MainWindow:
    APP_TITLE = "抖音罗盘数据抓取器"
    WINDOW_SIZE = "860x700"

    def __init__(self):
        self.config = load_config()
        self.scraper = None
        self.task_thread = None
        self.scheduler_thread = None
        self.scheduler_running = False
        self.tray_icon = None
        self.latest_export_path = self._find_latest_export_path()

        # --- 主窗口 ---
        self.root = tk.Tk()
        self.root.title(self.APP_TITLE)
        self.root.geometry(self.WINDOW_SIZE)
        self.root.minsize(820, 660)
        self.root.resizable(True, True)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.app_icon_photo = None
        self._apply_platform_scaling()
        self._setup_styles()

        # macOS: 使用原生外观
        if IS_MACOS:
            try:
                self.root.tk.call("::tk::unsupported::MacWindowStyle",
                                  "style", self.root._w, "document", "closeBox collapseBox")
            except tk.TclError:
                pass

        self._apply_window_icon()

        # --- Tabs ---
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=8, pady=(8, 0))

        self.tab_account = ttk.Frame(self.notebook)
        self.tab_schedule = ttk.Frame(self.notebook)
        self.tab_log = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_account, text="  账号配置  ")
        self.notebook.add(self.tab_schedule, text="  调度设置  ")
        self.notebook.add(self.tab_log, text="  运行日志  ")

        self._build_account_tab()
        self._build_schedule_tab()
        self._build_log_tab()
        self._build_bottom_bar()
        self._setup_log_handler()

    def _setup_styles(self):
        self._configure_fonts()
        style = ttk.Style()
        try:
            style.theme_use("vista" if IS_WINDOWS else "clam")
        except tk.TclError:
            pass

        self.root.configure(bg="#f3f6fb")
        style.configure("TNotebook", background="#f3f6fb", borderwidth=0)
        style.configure("TNotebook.Tab", padding=(20, 12), font=(UI_FONT_BOLD, 10, "bold"))
        style.map("TNotebook.Tab", background=[("selected", "#ffffff")])
        style.configure("Card.TFrame", background="#ffffff")
        style.configure("TFrame", background="#f3f6fb")
        style.configure("TLabel", font=(UI_FONT, 10), background="#ffffff")
        style.configure("Section.TLabel", font=(UI_FONT_BOLD, 13, "bold"), foreground="#1f2a44")
        style.configure("Hint.TLabel", font=(UI_FONT, 10), foreground="#667085", background="#ffffff")
        style.configure("Primary.TButton", font=(UI_FONT_BOLD, 10, "bold"), padding=(16, 9))
        style.configure("TButton", font=(UI_FONT, 10), padding=(12, 7))
        style.configure("TEntry", font=(UI_FONT, 10))
        style.configure("TCheckbutton", background="#ffffff")
        style.configure("TRadiobutton", background="#ffffff")
        style.configure("TLabelframe", background="#ffffff")
        style.configure("TLabelframe.Label", font=(UI_FONT_BOLD, 10, "bold"))

    def _apply_platform_scaling(self):
        if not IS_WINDOWS:
            return

        try:
            scale = self.root.winfo_fpixels("1i") / 96.0
            if scale > 0:
                self.root.tk.call("tk", "scaling", scale)
        except tk.TclError:
            pass

    def _configure_fonts(self):
        try:
            default_font = tkfont.nametofont("TkDefaultFont")
            text_font = tkfont.nametofont("TkTextFont")
            heading_font = tkfont.nametofont("TkHeadingFont")
        except tk.TclError:
            return

        default_font.configure(family=UI_FONT, size=10)
        text_font.configure(family=UI_FONT, size=10)
        heading_font.configure(family=UI_FONT_BOLD, size=10, weight="bold")

    # ----------------------------------------------------------
    # 账号配置 Tab（Cookie 登录模式）
    # ----------------------------------------------------------
    def _build_account_tab(self):
        f = ttk.Frame(self.tab_account, style="Card.TFrame", padding=16)
        f.pack(fill="both", expand=True, padx=10, pady=10)
        pad = {"padx": 15, "pady": 8}

        ttk.Label(f, text="登录与抓取设置", style="Section.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w", **pad
        )

        ttk.Label(f, text="抖音罗盘使用手机验证码登录，首次运行时会打开浏览器，\n"
                          "请在浏览器中手动完成登录。浏览器会自动记住登录状态，\n"
                          "后续运行无需重复登录（和你日常用的浏览器一样）。",
                  style="Hint.TLabel", justify="left").grid(
            row=1, column=0, columnspan=2, sticky="w", padx=15, pady=4
        )

        # 登录状态
        self.label_cookie_status = ttk.Label(f, text="检查中...", foreground="gray")
        self.label_cookie_status.grid(row=2, column=0, columnspan=2, sticky="w", **pad)
        self._update_cookie_status()

        btn_row = ttk.Frame(f)
        btn_row.grid(row=3, column=0, columnspan=2, sticky="w", padx=15, pady=8)

        ttk.Button(btn_row, text="切换账号", command=self._switch_account).pack(
            side="left", padx=(0, 10)
        )
        ttk.Button(btn_row, text="清除浏览器数据（重新登录）", command=self._clear_cookies).pack(
            side="left"
        )

        ttk.Separator(f, orient="horizontal").grid(
            row=4, column=0, columnspan=2, sticky="ew", padx=15, pady=12
        )

        ttk.Label(f, text="高级设置", style="Section.TLabel").grid(
            row=5, column=0, columnspan=2, sticky="w", **pad
        )

        # 入口类型选择
        ttk.Label(f, text="入口类型：").grid(row=6, column=0, sticky="e", **pad)
        portal_frame = ttk.Frame(f)
        portal_frame.grid(row=6, column=1, sticky="w", **pad)

        self.var_portal = tk.StringVar(
            value=self.config.get("portal_type", "creator")
        )
        ttk.Radiobutton(
            portal_frame, text="达人入口", variable=self.var_portal, value="creator"
        ).pack(side="left", padx=(0, 15))
        ttk.Radiobutton(
            portal_frame, text="店铺入口", variable=self.var_portal, value="shop"
        ).pack(side="left")

        ttk.Label(f, text="达人入口: 直播 → 直播复盘\n店铺入口: 直播 → 实时直播数据",
                  style="Hint.TLabel", justify="left").grid(
            row=7, column=1, sticky="w", padx=15, pady=0
        )

        ttk.Label(f, text="罗盘地址：").grid(row=8, column=0, sticky="e", **pad)
        self.entry_url = ttk.Entry(f, width=35)
        self.entry_url.grid(row=8, column=1, sticky="w", **pad)
        self.entry_url.insert(0, self.config.get("compass_url", ""))

        self.var_headless = tk.BooleanVar(value=self.config.get("headless", False))
        ttk.Checkbutton(f, text="无头模式（Cookie 有效时可用，登录时自动关闭）",
                        variable=self.var_headless).grid(
            row=9, column=1, sticky="w", padx=15, pady=4
        )

        ttk.Separator(f, orient="horizontal").grid(
            row=10, column=0, columnspan=2, sticky="ew", padx=15, pady=12
        )

        ttk.Label(f, text="抓取日期范围", style="Section.TLabel").grid(
            row=11, column=0, columnspan=2, sticky="w", **pad
        )

        date_frame = ttk.Frame(f, style="Card.TFrame")
        date_frame.grid(row=12, column=0, columnspan=2, sticky="w", padx=15, pady=(0, 6))

        self.var_date_mode = tk.StringVar(value=self.config.get("date_mode", "last_7_days"))
        ttk.Radiobutton(
            date_frame, text="近期七天（默认）", variable=self.var_date_mode,
            value="last_7_days", command=self._toggle_custom_date
        ).pack(side="left", padx=(0, 14))
        ttk.Radiobutton(
            date_frame, text="近一天", variable=self.var_date_mode,
            value="last_1_day", command=self._toggle_custom_date
        ).pack(side="left", padx=(0, 14))
        ttk.Radiobutton(
            date_frame, text="自定义日期", variable=self.var_date_mode,
            value="custom_date", command=self._toggle_custom_date
        ).pack(side="left")

        ttk.Label(f, text="日期输入：").grid(row=13, column=0, sticky="e", **pad)
        self.entry_custom_date = ttk.Entry(f, width=35)
        self.entry_custom_date.grid(row=13, column=1, sticky="w", **pad)
        self.entry_custom_date.insert(0, self.config.get("custom_date_text", ""))

        ttk.Label(
            f,
            text="支持：2026-03-28、2026年3月28日、3月28日、昨天。自定义日期时会自动尝试选中对应日期。",
            style="Hint.TLabel",
            justify="left",
        ).grid(row=14, column=1, sticky="w", padx=15, pady=(0, 10))

        self._toggle_custom_date()

        ttk.Button(f, text="保存配置", command=self._save_account, style="Primary.TButton").grid(
            row=15, column=1, sticky="w", padx=15, pady=15
        )

    # ----------------------------------------------------------
    # 调度设置 Tab
    # ----------------------------------------------------------
    def _build_schedule_tab(self):
        f = ttk.Frame(self.tab_schedule, style="Card.TFrame", padding=16)
        f.pack(fill="both", expand=True, padx=10, pady=10)
        pad = {"padx": 15, "pady": 8}

        ttk.Label(f, text="定时任务", style="Section.TLabel").grid(
            row=0, column=0, columnspan=3, sticky="w", **pad
        )

        self.var_schedule_on = tk.BooleanVar(value=self.config.get("schedule_enabled", True))
        ttk.Checkbutton(f, text="启用每日定时抓取", variable=self.var_schedule_on).grid(
            row=1, column=0, columnspan=3, sticky="w", padx=15, pady=4
        )

        ttk.Label(f, text="执行时间：").grid(row=2, column=0, sticky="e", **pad)

        time_frame = ttk.Frame(f)
        time_frame.grid(row=2, column=1, sticky="w", **pad)

        self.spin_hour = ttk.Spinbox(time_frame, from_=0, to=23, width=4, format="%02.0f")
        self.spin_hour.set(f"{self.config.get('schedule_hour', 8):02d}")
        self.spin_hour.pack(side="left")
        ttk.Label(time_frame, text=" 时 ").pack(side="left")

        self.spin_minute = ttk.Spinbox(time_frame, from_=0, to=59, width=4, format="%02.0f")
        self.spin_minute.set(f"{self.config.get('schedule_minute', 0):02d}")
        self.spin_minute.pack(side="left")
        ttk.Label(time_frame, text=" 分").pack(side="left")

        ttk.Separator(f, orient="horizontal").grid(
            row=3, column=0, columnspan=3, sticky="ew", padx=15, pady=12
        )

        ttk.Label(f, text="调度状态", style="Section.TLabel").grid(
            row=4, column=0, columnspan=3, sticky="w", **pad
        )

        self.label_scheduler_status = ttk.Label(f, text="未启动", foreground="gray")
        self.label_scheduler_status.grid(row=5, column=0, columnspan=3, sticky="w", padx=15)

        btn_frame = ttk.Frame(f)
        btn_frame.grid(row=6, column=0, columnspan=3, sticky="w", padx=15, pady=15)

        self.btn_start_scheduler = ttk.Button(
            btn_frame, text="启动调度", command=self._start_scheduler
        )
        self.btn_start_scheduler.pack(side="left", padx=(0, 10))

        self.btn_stop_scheduler = ttk.Button(
            btn_frame, text="停止调度", command=self._stop_scheduler, state="disabled"
        )
        self.btn_stop_scheduler.pack(side="left")

        ttk.Button(f, text="保存设置", command=self._save_schedule).grid(
            row=7, column=0, columnspan=3, sticky="w", padx=15, pady=5
        )

    # ----------------------------------------------------------
    # 日志 Tab
    # ----------------------------------------------------------
    def _build_log_tab(self):
        f = self.tab_log

        self.log_text = scrolledtext.ScrolledText(
            f, state="disabled", wrap="word", font=MONO_FONT, height=22
        )
        self.log_text.pack(fill="both", expand=True, padx=8, pady=8)

        btn_frame = ttk.Frame(f)
        btn_frame.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btn_frame, text="清空日志", command=self._clear_log).pack(side="right")

    # ----------------------------------------------------------
    # 底部操作栏
    # ----------------------------------------------------------
    def _build_bottom_bar(self):
        bar = ttk.Frame(self.root)
        bar.pack(fill="x", padx=8, pady=8)

        self.btn_run_once = ttk.Button(bar, text="立即执行一次", command=self._run_once)
        self.btn_run_once.pack(side="left")

        self.btn_cancel = ttk.Button(bar, text="取消任务", command=self._cancel_task, state="disabled")
        self.btn_cancel.pack(side="left", padx=10)

        self.btn_export = ttk.Button(bar, text="导出最近一次数据", command=self._export_latest_data)
        self.btn_export.pack(side="left")

        # 切换账号确认按钮（仅在切换账号过程中显示）
        self.btn_confirm_switch = ttk.Button(
            bar, text="确认已切换", command=self._confirm_switch
        )
        # 默认不显示，切换账号时才 pack

        self.btn_minimize = ttk.Button(bar, text="最小化到托盘", command=self._minimize_to_tray)
        self.btn_minimize.pack(side="right")

        self.label_status = ttk.Label(bar, text="就绪", foreground="green")
        self.label_status.pack(side="right", padx=15)

        self._refresh_export_button()

    def _apply_window_icon(self):
        """给主窗口设置应用图标。"""
        icon_path = _resource_path("assets/app_icon.png")
        if not icon_path.exists():
            return

        try:
            self.app_icon_photo = tk.PhotoImage(file=str(icon_path))
            self.root.iconphoto(True, self.app_icon_photo)
        except Exception as e:
            logger.warning(f"窗口图标加载失败: {e}")

    # ----------------------------------------------------------
    # 日志 Handler 设置
    # ----------------------------------------------------------
    def _setup_log_handler(self):
        handler = TextHandler(self.log_text)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)

    # ----------------------------------------------------------
    # 配置保存
    # ----------------------------------------------------------
    def _save_account(self):
        compass_url = self.entry_url.get().strip()
        headless = self.var_headless.get()
        portal_type = self.var_portal.get()
        date_mode = self.var_date_mode.get()
        custom_date_text = self.entry_custom_date.get().strip()

        if date_mode == "custom_date" and not custom_date_text:
            messagebox.showwarning("提示", "你已选择自定义日期，请先填写日期，例如 2026-03-28。")
            return

        parsed_date = None
        if date_mode == "custom_date":
            try:
                parsed_date = parse_user_date_text(custom_date_text)
            except RuntimeError as e:
                messagebox.showwarning("提示", str(e))
                return

        new_config = {
            **self.config,
            "compass_url": compass_url,
            "headless": headless,
            "portal_type": portal_type,
            "date_mode": date_mode,
            "custom_date_text": custom_date_text,
        }
        save_config(new_config)
        self.config = new_config

        portal_name = "达人入口" if portal_type == "creator" else "店铺入口"
        mode_map = {
            "last_7_days": "近期七天",
            "last_1_day": "近一天",
            "custom_date": f"自定义日期：{parsed_date:%Y-%m-%d}" if parsed_date else "自定义日期",
        }
        messagebox.showinfo("提示", f"配置已保存\n入口类型: {portal_name}\n抓取日期: {mode_map[date_mode]}")

    def _toggle_custom_date(self):
        is_custom = self.var_date_mode.get() == "custom_date"
        self.entry_custom_date.config(state="normal" if is_custom else "disabled")

    def _update_cookie_status(self):
        """更新登录状态显示。"""
        profile_dir = APP_DIR / "browser_profile"
        if profile_dir.exists() and any(profile_dir.iterdir()):
            self.label_cookie_status.config(
                text="浏览器配置已保存（登录状态会自动保持）",
                foreground="green"
            )
        else:
            self.label_cookie_status.config(
                text="首次运行 — 执行时会打开浏览器让你登录",
                foreground="orange"
            )

    def _clear_cookies(self):
        """清除浏览器持久化数据，下次需要重新登录。"""
        import shutil
        profile_dir = APP_DIR / "browser_profile"
        if profile_dir.exists():
            try:
                shutil.rmtree(profile_dir)
                logger.info("浏览器数据已清除")
            except Exception as e:
                logger.warning(f"清除浏览器数据失败: {e}")
                messagebox.showwarning("提示", f"清除失败: {e}\n请先关闭正在运行的任务")
                return
        # 同时清除旧的 cookie 文件（如果存在）
        if COOKIE_FILE.exists():
            COOKIE_FILE.unlink()
        self._update_cookie_status()
        messagebox.showinfo("提示", "浏览器数据已清除，下次执行时需要重新登录")

    def _switch_account(self):
        """打开浏览器让用户切换账号。"""
        if self.task_thread and self.task_thread.is_alive():
            messagebox.showwarning("提示", "任务正在执行中，请等待完成后再切换账号")
            return

        self.btn_run_once.config(state="disabled")
        self.btn_cancel.config(state="normal")
        self.label_status.config(text="切换账号中 — 请在浏览器中操作", foreground="orange")
        self.notebook.select(self.tab_log)

        # 显示「确认已切换」按钮
        self.btn_confirm_switch.pack(side="left", padx=10)

        self.scraper = DouyinCompassScraper(self.config)
        self.task_thread = threading.Thread(target=self._switch_account_worker, daemon=True)
        self.task_thread.start()

    def _switch_account_worker(self):
        result = self.scraper.run_switch_account()
        self.root.after(0, self._on_switch_done, result)

    def _confirm_switch(self):
        """用户点击确认按钮，通知 scraper 切换完成。"""
        if self.scraper:
            self.scraper.confirm_switch()
            self.label_status.config(text="正在保存...", foreground="orange")

    def _on_switch_done(self, result: dict):
        self.btn_run_once.config(state="normal")
        self.btn_cancel.config(state="disabled")
        # 隐藏「确认已切换」按钮
        self.btn_confirm_switch.pack_forget()
        self._update_cookie_status()
        if result["success"]:
            self.label_status.config(text="账号已切换", foreground="green")
            messagebox.showinfo("提示", "账号切换成功！现在可以点击「立即执行一次」抓取新账号的数据。")
        else:
            self.label_status.config(text="切换失败", foreground="red")

    def _save_schedule(self):
        self.config["schedule_enabled"] = self.var_schedule_on.get()
        self.config["schedule_hour"] = int(self.spin_hour.get())
        self.config["schedule_minute"] = int(self.spin_minute.get())
        save_config(self.config)
        messagebox.showinfo("提示", "调度设置已保存")

    # ----------------------------------------------------------
    # 立即执行 / 取消
    # ----------------------------------------------------------
    def _run_once(self):
        if self.task_thread and self.task_thread.is_alive():
            messagebox.showwarning("提示", "任务正在执行中")
            return

        self.btn_run_once.config(state="disabled")
        self.btn_cancel.config(state="normal")
        self.label_status.config(text="执行中...", foreground="orange")
        self.notebook.select(self.tab_log)

        self.scraper = DouyinCompassScraper(self.config)
        self.task_thread = threading.Thread(target=self._task_worker, daemon=True)
        self.task_thread.start()

    def _task_worker(self):
        result = self.scraper.run()
        self.root.after(0, self._on_task_done, result)

    def _on_task_done(self, result: dict):
        self.btn_run_once.config(state="normal")
        self.btn_cancel.config(state="disabled")
        self._update_cookie_status()
        if result["success"]:
            self.latest_export_path = self._resolve_export_source(result)
            self._refresh_export_button()
            account = result.get("account_name", "")
            status_text = f"完成 ({result['rows']}行)"
            if account:
                status_text += f" [{account}]"
            self.label_status.config(text=status_text, foreground="green")
            if self.latest_export_path:
                messagebox.showinfo(
                    "抓取完成",
                    f"{result['message']}\n\n现在可以点击“导出最近一次数据”直接导出文件。"
                )
        else:
            self.label_status.config(text="失败", foreground="red")

    def _cancel_task(self):
        if self.scraper:
            self.scraper.cancel()
            self.label_status.config(text="取消中...", foreground="gray")

    # ----------------------------------------------------------
    # 调度器
    # ----------------------------------------------------------
    def _start_scheduler(self):
        self._save_schedule()
        if self.scheduler_running:
            return

        self.scheduler_running = True
        self.btn_start_scheduler.config(state="disabled")
        self.btn_stop_scheduler.config(state="normal")

        hour = self.config["schedule_hour"]
        minute = self.config["schedule_minute"]
        self.label_scheduler_status.config(
            text=f"运行中 — 每天 {hour:02d}:{minute:02d} 执行", foreground="green"
        )
        logger.info(f"调度已启动: 每天 {hour:02d}:{minute:02d}")

        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()

    def _scheduler_loop(self):
        """简易调度循环：每 30 秒检查一次是否到达执行时间。"""
        import time
        last_run_date = None

        while self.scheduler_running:
            now = datetime.now()
            target_hour = self.config["schedule_hour"]
            target_minute = self.config["schedule_minute"]

            if (now.hour == target_hour and now.minute == target_minute
                    and last_run_date != now.date()):
                last_run_date = now.date()
                logger.info("定时触发任务...")
                self.root.after(0, self._run_once)

            time.sleep(30)

    def _stop_scheduler(self):
        self.scheduler_running = False
        self.btn_start_scheduler.config(state="normal")
        self.btn_stop_scheduler.config(state="disabled")
        self.label_scheduler_status.config(text="已停止", foreground="gray")
        logger.info("调度已停止")

    # ----------------------------------------------------------
    # 系统托盘（跨平台：Windows 用 pystray，macOS 用 pystray + rumps 后端）
    # ----------------------------------------------------------
    def _minimize_to_tray(self):
        try:
            import pystray
            from PIL import Image
        except ImportError:
            # macOS 提示需要额外安装 rumps
            extra = "\npip install rumps  # macOS 必需" if IS_MACOS else ""
            messagebox.showwarning(
                "提示",
                f"托盘功能需要安装依赖：\npip install pystray Pillow{extra}"
            )
            return

        self.root.withdraw()

        icon_path = _resource_path("assets/app_icon.png")
        if icon_path.exists():
            img = Image.open(icon_path).convert("RGBA")
        else:
            img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))

        menu = pystray.Menu(
            pystray.MenuItem("显示主窗口", self._restore_from_tray, default=True),
            pystray.MenuItem("立即执行", lambda icon, item: self.root.after(0, self._run_once)),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("退出", self._quit_from_tray),
        )

        self.tray_icon = pystray.Icon(self.APP_TITLE, img, self.APP_TITLE, menu)

        # macOS 上 pystray.run() 需要在非主线程运行（主线程留给 tkinter）
        threading.Thread(target=self.tray_icon.run, daemon=True).start()
        logger.info("已最小化到系统托盘")

    def _restore_from_tray(self, icon=None, item=None):
        if self.tray_icon:
            self.tray_icon.stop()
            self.tray_icon = None
        self.root.after(0, self.root.deiconify)

    def _quit_from_tray(self, icon=None, item=None):
        if self.tray_icon:
            self.tray_icon.stop()
        self.root.after(0, self.root.destroy)

    # ----------------------------------------------------------
    # 其他
    # ----------------------------------------------------------
    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")

    def _find_latest_export_path(self) -> Path | None:
        patterns = ["live_*.csv", "*.xlsx"]
        candidates = []

        for pattern in patterns[:1]:
            candidates.extend(DATA_DIR.glob(pattern))
        for pattern in patterns[1:]:
            candidates.extend(DOWNLOAD_DIR.glob(pattern))

        files = [path for path in candidates if path.is_file()]
        if not files:
            return None
        return max(files, key=lambda path: path.stat().st_mtime)

    def _resolve_export_source(self, result: dict) -> Path | None:
        for key in ("csv_path", "filepath"):
            value = result.get(key)
            if not value:
                continue
            path = Path(value)
            if path.exists() and path.is_file():
                return path
        return self._find_latest_export_path()

    def _refresh_export_button(self):
        state = "normal" if self.latest_export_path and self.latest_export_path.exists() else "disabled"
        self.btn_export.config(state=state)

    def _export_latest_data(self):
        source = self.latest_export_path
        if not source or not source.exists():
            source = self._find_latest_export_path()
            self.latest_export_path = source
            self._refresh_export_button()

        if not source or not source.exists():
            messagebox.showwarning("提示", "还没有可导出的抓取结果，请先执行一次抓取。")
            return

        initial_name = source.name
        filetypes = [("CSV 文件", "*.csv")] if source.suffix.lower() == ".csv" else [("Excel 文件", "*.xlsx")]
        target = filedialog.asksaveasfilename(
            title="导出抓取数据",
            initialfile=initial_name,
            defaultextension=source.suffix,
            filetypes=filetypes + [("所有文件", "*.*")],
        )
        if not target:
            return

        try:
            shutil.copy2(source, target)
            logger.info(f"数据已导出到: {target}")
            messagebox.showinfo("导出成功", f"文件已导出到：\n{target}")
        except Exception as e:
            logger.exception(f"导出数据失败: {e}")
            messagebox.showerror("导出失败", f"导出失败：{e}")

    def _on_close(self):
        if self.task_thread and self.task_thread.is_alive():
            if not messagebox.askyesno("确认", "任务正在执行中，确定退出？"):
                return
            if self.scraper:
                self.scraper.cancel()
        self.scheduler_running = False
        self.root.destroy()

    def run(self):
        """启动 GUI 主循环。"""
        logger.info(f"{self.APP_TITLE} 已启动")
        self.root.mainloop()
