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
from tkinter import filedialog, font as tkfont, messagebox, scrolledtext
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
from scraper import DouyinCompassScraper
from scraper_logic import (
    DATE_MODE_LAST_1_DAY,
    DATE_MODE_LAST_7_DAYS,
    SCENE_DISPLAY_NAMES,
    TASK_STATUS_CANCELLED,
    TASK_STATUS_EXPORTING,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_PRECHECKING,
    TASK_STATUS_SELECTING_DATE,
    TASK_STATUS_SUCCESS,
    resolve_target_date_range,
)

logger = logging.getLogger("douyin_rpa")

UI_FONT = "PingFang SC" if IS_MACOS else "Microsoft YaHei UI"
UI_FONT_BOLD = "PingFang SC" if IS_MACOS else "Microsoft YaHei UI"
MONO_FONT = ("Menlo", 10) if IS_MACOS else ("Cascadia Mono", 10)


def _resource_path(relative_path: str) -> Path:
    """返回开发环境或 PyInstaller 打包后的资源路径。"""
    base_path = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    return base_path / relative_path


def compute_scroll_units(delta: int | None = None, num: int | None = None, platform: str = "other") -> int:
    """将不同平台的滚轮事件统一转换成 Tk 可识别的滚动步数。"""
    if num == 4:
        return -1
    if num == 5:
        return 1
    if not delta:
        return 0

    if platform == "windows":
        step = max(1, abs(int(delta)) // 120)
    else:
        step = 1

    return -step if delta > 0 else step


def build_scene_options(portal_type: str) -> list[tuple[str, str]]:
    """统一抓取模式：一次抓取渠道、直播、短视频全部三类数据，输出到同一张 Excel 表"""
    return [
        ("全部数据（渠道 + 直播 + 短视频）", "unified"),
    ]


def build_date_mode_options() -> list[tuple[str, str]]:
    return [
        ("近期七天", DATE_MODE_LAST_7_DAYS),
        ("近一天", DATE_MODE_LAST_1_DAY),
    ]


def describe_scene_selection(config: dict) -> str:
    """统一抓取：渠道 + 直播 + 短视频全部数据"""
    return "全部数据（渠道 + 直播 + 短视频）"


def resolve_runtime_config(
    saved_config: dict,
    form_config: dict | None,
    *,
    use_saved_task: bool,
) -> dict:
    source = saved_config if use_saved_task or not form_config else form_config
    return dict(source)


def build_task_status_badge(task_status: str) -> tuple[str, str]:
    mapping = {
        TASK_STATUS_PENDING: ("就绪", "success"),
        TASK_STATUS_PRECHECKING: ("检查中", "warning"),
        TASK_STATUS_SELECTING_DATE: ("选择日期中", "warning"),
        TASK_STATUS_EXPORTING: ("导出中", "warning"),
        TASK_STATUS_SUCCESS: ("已完成", "success"),
        TASK_STATUS_FAILED: ("执行失败", "danger"),
        TASK_STATUS_CANCELLED: ("已取消", "muted"),
    }
    return mapping.get(task_status, ("未知状态", "muted"))


class TextHandler(logging.Handler):
    """将 logging 输出重定向到 tkinter ScrolledText。"""

    def __init__(self, text_widget: scrolledtext.ScrolledText):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record) + "\n"
        self.text_widget.after(0, self._append, msg)

    def _append(self, msg):
        self.text_widget.configure(state="normal")
        self.text_widget.insert(tk.END, msg)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state="disabled")


class AppButton(tk.Label):
    """使用 Label 实现的跨平台自绘按钮，避免 macOS 原生按钮样式失控。"""

    def __init__(
        self,
        parent,
        text: str,
        command=None,
        normal_palette: dict | None = None,
        hover_palette: dict | None = None,
        disabled_palette: dict | None = None,
        state: str = "normal",
        **kwargs,
    ):
        self.command = command
        self._state = state
        self._hovered = False
        self._normal_palette = normal_palette or {}
        self._hover_palette = hover_palette or self._normal_palette
        self._disabled_palette = disabled_palette or self._normal_palette
        kwargs.setdefault("padx", 16)
        kwargs.setdefault("pady", 10)
        kwargs.setdefault("cursor", "hand2")
        kwargs.setdefault("highlightthickness", 1)
        kwargs.setdefault("bd", 0)
        kwargs.setdefault("justify", "center")
        super().__init__(parent, text=text, **kwargs)
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<Button-1>", self._on_click)
        self._apply_palette()

    def set_palettes(self, normal: dict, hover: dict | None = None, disabled: dict | None = None):
        self._normal_palette = normal
        self._hover_palette = hover or normal
        self._disabled_palette = disabled or normal
        self._apply_palette()

    def _palette_for_state(self) -> dict:
        if self._state == "disabled":
            return self._disabled_palette
        if self._hovered:
            return self._hover_palette
        return self._normal_palette

    def _apply_palette(self):
        palette = self._palette_for_state()
        border = palette.get("border", palette.get("bg", self.cget("bg")))
        cursor = "arrow" if self._state == "disabled" else "hand2"
        super().configure(
            bg=palette.get("bg", self.cget("bg")),
            fg=palette.get("fg", self.cget("fg")),
            font=palette.get("font", self.cget("font")),
            highlightbackground=border,
            highlightcolor=border,
            cursor=cursor,
        )

    def _on_enter(self, _event):
        if self._state != "disabled":
            self._hovered = True
            self._apply_palette()

    def _on_leave(self, _event):
        self._hovered = False
        self._apply_palette()

    def _on_click(self, _event):
        if self._state != "disabled" and self.command:
            self.command()

    def configure(self, cnf=None, **kwargs):
        if cnf is not None and not isinstance(cnf, str):
            kwargs = {**cnf, **kwargs}
        elif isinstance(cnf, str):
            return super().configure(cnf)

        if "state" in kwargs:
            self._state = kwargs.pop("state")
        if "command" in kwargs:
            self.command = kwargs.pop("command")
        if "normal_palette" in kwargs:
            self._normal_palette = kwargs.pop("normal_palette")
        if "hover_palette" in kwargs:
            self._hover_palette = kwargs.pop("hover_palette")
        if "disabled_palette" in kwargs:
            self._disabled_palette = kwargs.pop("disabled_palette")

        result = super().configure(**kwargs) if kwargs else None
        self._apply_palette()
        return result

    config = configure


class MainWindow:
    APP_TITLE = "抖音罗盘数据抓取器"
    WINDOW_SIZE = "1220x820"
    COLORS = {
        "bg": "#ecf2f8",
        "surface": "#ffffff",
        "surface_alt": "#f4f7fb",
        "panel": "#e7eef7",
        "text": "#14233b",
        "muted": "#5d6b82",
        "border": "#d7e0eb",
        "accent": "#165dff",
        "accent_hover": "#0f4dd0",
        "accent_soft": "#e7f0ff",
        "nav_bg": "#eef3f9",
        "nav_active": "#14233b",
        "nav_active_hover": "#0f1b2d",
        "nav_text": "#53657d",
        "success_fg": "#0f7b4c",
        "success_bg": "#e7f8ef",
        "warning_fg": "#b26b00",
        "warning_bg": "#fff4e4",
        "danger_fg": "#b42318",
        "danger_bg": "#fdecec",
    }

    def __init__(self):
        self.config = load_config()
        self.scraper = None
        self.task_thread = None
        self.scheduler_thread = None
        self.scheduler_running = False
        self.tray_icon = None
        self.latest_export_path = self._find_latest_export_path()
        self.last_task_result = None
        self.last_precheck_result = None
        self.nav_buttons: dict[str, tk.Button] = {}
        self.pages: dict[str, tk.Frame] = {}
        self.app_icon_photo = None
        self.header_icon_photo = None
        self.current_page_key = "account"
        self.page_scroll_canvases: dict[str, tk.Canvas] = {}

        self.root = tk.Tk()
        self.root.title(self.APP_TITLE)
        self.root.geometry(self.WINDOW_SIZE)
        self.root.minsize(1120, 760)
        self.root.resizable(True, True)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._apply_platform_scaling()
        self._setup_styles()

        if IS_MACOS:
            try:
                self.root.tk.call(
                    "::tk::unsupported::MacWindowStyle",
                    "style",
                    self.root._w,
                    "document",
                    "closeBox collapseBox",
                )
            except tk.TclError:
                pass

        self._apply_window_icon()
        self._build_shell()
        self._build_account_tab()
        self._build_schedule_tab()
        self._build_log_tab()
        self._build_bottom_bar()
        self._setup_log_handler()
        self._show_page("account")

    def _setup_styles(self):
        self.root.configure(bg=self.COLORS["bg"])
        self._configure_fonts()
        self.root.bind_all("<MouseWheel>", self._on_global_mousewheel, add="+")
        self.root.bind_all("<Button-4>", self._on_global_mousewheel, add="+")
        self.root.bind_all("<Button-5>", self._on_global_mousewheel, add="+")

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

        default_font.configure(family=UI_FONT, size=11)
        text_font.configure(family=UI_FONT, size=11)
        heading_font.configure(family=UI_FONT_BOLD, size=11, weight="bold")

    def _build_shell(self):
        shell = tk.Frame(self.root, bg=self.COLORS["bg"])
        shell.pack(fill="both", expand=True, padx=24, pady=(18, 18))

        header = tk.Frame(
            shell,
            bg=self.COLORS["surface"],
            highlightbackground=self.COLORS["border"],
            highlightthickness=1,
            bd=0,
        )
        header.pack(fill="x")

        header_inner = tk.Frame(header, bg=self.COLORS["surface"])
        header_inner.pack(fill="x", padx=28, pady=(24, 20))

        brand_row = tk.Frame(header_inner, bg=self.COLORS["surface"])
        brand_row.pack(fill="x")

        brand_left = tk.Frame(brand_row, bg=self.COLORS["surface"])
        brand_left.pack(side="left", fill="x", expand=True)

        icon_path = _resource_path("assets/app_icon.png")
        if icon_path.exists():
            try:
                self.header_icon_photo = tk.PhotoImage(file=str(icon_path)).subsample(26, 26)
                tk.Label(brand_left, image=self.header_icon_photo, bg=self.COLORS["surface"]).pack(side="left", padx=(0, 14))
            except Exception:
                self.header_icon_photo = None

        title_block = tk.Frame(brand_left, bg=self.COLORS["surface"])
        title_block.pack(side="left", fill="x", expand=True)
        tk.Label(
            title_block,
            text=self.APP_TITLE,
            bg=self.COLORS["surface"],
            fg=self.COLORS["text"],
            font=(UI_FONT_BOLD, 26, "bold"),
        ).pack(anchor="w")
        tk.Label(
            title_block,
            text="登录一次后可重复使用，抓取结果支持一键导出。",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 12),
        ).pack(anchor="w", pady=(6, 0))

        brand_right = tk.Frame(brand_row, bg=self.COLORS["surface"])
        brand_right.pack(side="right")
        self.header_status_hint = tk.Label(
            brand_right,
            text="本地运行",
            padx=12,
            pady=6,
            bg=self.COLORS["accent_soft"],
            fg=self.COLORS["accent"],
            font=(UI_FONT_BOLD, 10, "bold"),
            bd=0,
        )
        self.header_status_hint.pack(anchor="e")
        tk.Label(
            brand_right,
            text="按场景组织抓取任务，先检查页面，再执行导出",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
        ).pack(anchor="e", pady=(8, 0))

        nav_row = tk.Frame(header_inner, bg=self.COLORS["surface"])
        nav_row.pack(fill="x", pady=(20, 0))
        self.nav_bar = tk.Frame(
            nav_row,
            bg=self.COLORS["nav_bg"],
            highlightbackground=self.COLORS["border"],
            highlightthickness=1,
            bd=0,
        )
        self.nav_bar.pack(side="left", anchor="w")

        self._create_nav_button("account", "执行任务")
        self._create_nav_button("schedule", "调度设置")
        self._create_nav_button("log", "运行日志")

        self.action_strip = tk.Frame(
            header_inner,
            bg=self.COLORS["surface"],
            highlightbackground=self.COLORS["border"],
            highlightthickness=1,
            bd=0,
        )
        self.action_strip.pack(fill="x", pady=(18, 0))

        content_wrap = tk.Frame(shell, bg=self.COLORS["bg"])
        content_wrap.pack(fill="both", expand=True, pady=(18, 0))

        self.page_host = tk.Frame(content_wrap, bg=self.COLORS["bg"])
        self.page_host.pack(fill="both", expand=True)
        self.page_host.grid_rowconfigure(0, weight=1)
        self.page_host.grid_columnconfigure(0, weight=1)

        self.tab_account = tk.Frame(self.page_host, bg=self.COLORS["bg"])
        self.tab_schedule = tk.Frame(self.page_host, bg=self.COLORS["bg"])
        self.tab_log = tk.Frame(self.page_host, bg=self.COLORS["bg"])
        self.pages = {
            "account": self.tab_account,
            "schedule": self.tab_schedule,
            "log": self.tab_log,
        }
        for frame in self.pages.values():
            frame.grid(row=0, column=0, sticky="nsew")

        self.tab_account_content = self._make_scrollable_page(self.tab_account, "account")
        self.tab_schedule_content = self._make_scrollable_page(self.tab_schedule, "schedule")


    def _make_scrollable_page(self, parent: tk.Frame, page_key: str) -> tk.Frame:
        container = tk.Frame(parent, bg=self.COLORS["bg"])
        container.pack(fill="both", expand=True)

        canvas = tk.Canvas(
            container,
            bg=self.COLORS["bg"],
            highlightthickness=0,
            bd=0,
            relief="flat",
        )
        scrollbar = tk.Scrollbar(container, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = tk.Frame(canvas, bg=self.COLORS["bg"])
        window_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        def on_inner_configure(_event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def on_canvas_configure(event):
            canvas.itemconfigure(window_id, width=event.width)

        inner.bind("<Configure>", on_inner_configure)
        canvas.bind("<Configure>", on_canvas_configure)

        self.page_scroll_canvases[page_key] = canvas
        return inner

    def _on_global_mousewheel(self, event):
        canvas = self.page_scroll_canvases.get(self.current_page_key)
        if not canvas:
            return None

        units = compute_scroll_units(
            delta=getattr(event, "delta", 0),
            num=getattr(event, "num", None),
            platform="windows" if IS_WINDOWS else "other",
        )
        if units == 0:
            return None

        canvas.yview_scroll(units, "units")
        return "break"

    def _create_nav_button(self, key: str, text: str):
        btn = AppButton(
            self.nav_bar,
            text=text,
            command=lambda page=key: self._show_page(page),
            font=(UI_FONT_BOLD, 11, "bold"),
            padx=18,
            pady=11,
            anchor="center",
            normal_palette={
                "bg": self.COLORS["nav_bg"],
                "fg": self.COLORS["nav_text"],
                "border": self.COLORS["nav_bg"],
                "font": (UI_FONT_BOLD, 11, "bold"),
            },
            hover_palette={
                "bg": self.COLORS["accent_soft"],
                "fg": self.COLORS["accent"],
                "border": self.COLORS["accent_soft"],
                "font": (UI_FONT_BOLD, 11, "bold"),
            },
            disabled_palette={
                "bg": self.COLORS["nav_bg"],
                "fg": "#98a2b3",
                "border": self.COLORS["nav_bg"],
                "font": (UI_FONT_BOLD, 11, "bold"),
            },
        )
        btn.pack(side="left", padx=6, pady=6)
        self.nav_buttons[key] = btn


    def _show_page(self, key: str):
        self.current_page_key = key
        frame = self.pages[key]
        frame.tkraise()
        for page_key, btn in self.nav_buttons.items():
            active = page_key == key
            if active:
                btn.set_palettes(
                    {
                        "bg": self.COLORS["nav_active"],
                        "fg": "#ffffff",
                        "border": self.COLORS["nav_active"],
                        "font": (UI_FONT_BOLD, 11, "bold"),
                    },
                    hover={
                        "bg": self.COLORS["nav_active_hover"],
                        "fg": "#ffffff",
                        "border": self.COLORS["nav_active_hover"],
                        "font": (UI_FONT_BOLD, 11, "bold"),
                    },
                )
            else:
                btn.set_palettes(
                    {
                        "bg": self.COLORS["nav_bg"],
                        "fg": self.COLORS["nav_text"],
                        "border": self.COLORS["nav_bg"],
                        "font": (UI_FONT_BOLD, 11, "bold"),
                    },
                    hover={
                        "bg": self.COLORS["accent_soft"],
                        "fg": self.COLORS["accent"],
                        "border": self.COLORS["accent_soft"],
                        "font": (UI_FONT_BOLD, 11, "bold"),
                    },
                )


    def _make_segmented_group(self, parent: tk.Widget, variable: tk.StringVar, options: list[tuple[str, str]], command=None):
        frame = tk.Frame(parent, bg=self.COLORS["surface"])
        buttons: dict[str, AppButton] = {}

        def on_select(value: str):
            variable.set(value)
            if command:
                command()

        def refresh(*_):
            current = variable.get()
            for value, btn in buttons.items():
                active = value == current
                if active:
                    btn.set_palettes(
                        {
                            "bg": self.COLORS["accent"],
                            "fg": "#ffffff",
                            "border": self.COLORS["accent"],
                            "font": (UI_FONT_BOLD, 11, "bold"),
                        },
                        hover={
                            "bg": self.COLORS["accent_hover"],
                            "fg": "#ffffff",
                            "border": self.COLORS["accent_hover"],
                            "font": (UI_FONT_BOLD, 11, "bold"),
                        },
                    )
                else:
                    btn.set_palettes(
                        {
                            "bg": self.COLORS["surface_alt"],
                            "fg": self.COLORS["text"],
                            "border": self.COLORS["border"],
                            "font": (UI_FONT, 11),
                        },
                        hover={
                            "bg": self.COLORS["accent_soft"],
                            "fg": self.COLORS["accent"],
                            "border": self.COLORS["accent_soft"],
                            "font": (UI_FONT, 11),
                        },
                    )

        for index, (label, value) in enumerate(options):
            btn = AppButton(
                frame,
                text=label,
                command=lambda selected=value: on_select(selected),
                padx=18,
                pady=10,
                font=(UI_FONT, 11),
            )
            btn.pack(side="left", padx=(0 if index == 0 else 10, 0))
            buttons[value] = btn

        variable.trace_add("write", refresh)
        refresh()
        return frame


    def _make_toggle_row(self, parent: tk.Widget, variable: tk.BooleanVar, title: str, description: str):
        frame = tk.Frame(
            parent,
            bg=self.COLORS["surface_alt"],
            highlightbackground=self.COLORS["border"],
            highlightthickness=1,
            bd=0,
        )
        frame.grid_columnconfigure(0, weight=1)

        info = tk.Frame(frame, bg=self.COLORS["surface_alt"])
        info.grid(row=0, column=0, sticky="w", padx=16, pady=14)
        tk.Label(
            info,
            text=title,
            bg=self.COLORS["surface_alt"],
            fg=self.COLORS["text"],
            font=(UI_FONT_BOLD, 11, "bold"),
        ).pack(anchor="w")
        tk.Label(
            info,
            text=description,
            bg=self.COLORS["surface_alt"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
            justify="left",
            wraplength=560,
        ).pack(anchor="w", pady=(4, 0))

        button = AppButton(
            frame,
            text="",
            command=lambda: variable.set(not variable.get()),
            padx=14,
            pady=8,
            width=8,
            font=(UI_FONT_BOLD, 10, "bold"),
            anchor="center",
        )
        button.grid(row=0, column=1, sticky="e", padx=16, pady=14)

        def refresh(*_):
            enabled = bool(variable.get())
            if enabled:
                button.configure(text="已开启")
                button.set_palettes(
                    {
                        "bg": self.COLORS["success_bg"],
                        "fg": self.COLORS["success_fg"],
                        "border": self.COLORS["success_bg"],
                        "font": (UI_FONT_BOLD, 10, "bold"),
                    },
                    hover={
                        "bg": self.COLORS["success_bg"],
                        "fg": self.COLORS["success_fg"],
                        "border": self.COLORS["success_bg"],
                        "font": (UI_FONT_BOLD, 10, "bold"),
                    },
                )
            else:
                button.configure(text="已关闭")
                button.set_palettes(
                    {
                        "bg": self.COLORS["surface"],
                        "fg": self.COLORS["muted"],
                        "border": self.COLORS["border"],
                        "font": (UI_FONT_BOLD, 10, "bold"),
                    },
                    hover={
                        "bg": self.COLORS["surface_alt"],
                        "fg": self.COLORS["text"],
                        "border": self.COLORS["border"],
                        "font": (UI_FONT_BOLD, 10, "bold"),
                    },
                )

        variable.trace_add("write", refresh)
        refresh()
        return frame


    def _make_card(self, parent: tk.Widget, title: str, subtitle: str | None = None, expand: bool = False):
        card = tk.Frame(
            parent,
            bg=self.COLORS["surface"],
            highlightbackground=self.COLORS["border"],
            highlightthickness=1,
            bd=0,
        )
        card.pack(fill="both" if expand else "x", expand=expand, pady=(0, 16))

        inner = tk.Frame(card, bg=self.COLORS["surface"])
        inner.pack(fill="both", expand=True, padx=22, pady=20)

        tk.Label(
            inner,
            text=title,
            bg=self.COLORS["surface"],
            fg=self.COLORS["text"],
            font=(UI_FONT_BOLD, 16, "bold"),
        ).pack(anchor="w")

        if subtitle:
            tk.Label(
                inner,
                text=subtitle,
                bg=self.COLORS["surface"],
                fg=self.COLORS["muted"],
                font=(UI_FONT, 11),
                justify="left",
                wraplength=760,
            ).pack(anchor="w", pady=(6, 0))

        body = tk.Frame(inner, bg=self.COLORS["surface"])
        body.pack(fill="both", expand=True, pady=(16, 0))
        return card, body

    def _make_button(self, parent: tk.Widget, text: str, command, variant: str = "secondary", width: int | None = None):
        styles = {
            "primary": {
                "normal": {
                    "bg": self.COLORS["accent"],
                    "fg": "#ffffff",
                    "border": self.COLORS["accent"],
                    "font": (UI_FONT_BOLD, 11, "bold"),
                },
                "hover": {
                    "bg": self.COLORS["accent_hover"],
                    "fg": "#ffffff",
                    "border": self.COLORS["accent_hover"],
                    "font": (UI_FONT_BOLD, 11, "bold"),
                },
            },
            "secondary": {
                "normal": {
                    "bg": self.COLORS["surface"],
                    "fg": self.COLORS["text"],
                    "border": self.COLORS["border"],
                    "font": (UI_FONT, 11),
                },
                "hover": {
                    "bg": self.COLORS["accent_soft"],
                    "fg": self.COLORS["accent"],
                    "border": self.COLORS["accent_soft"],
                    "font": (UI_FONT, 11),
                },
            },
            "success": {
                "normal": {
                    "bg": self.COLORS["success_bg"],
                    "fg": self.COLORS["success_fg"],
                    "border": self.COLORS["success_bg"],
                    "font": (UI_FONT_BOLD, 11, "bold"),
                },
                "hover": {
                    "bg": self.COLORS["success_bg"],
                    "fg": self.COLORS["success_fg"],
                    "border": self.COLORS["success_bg"],
                    "font": (UI_FONT_BOLD, 11, "bold"),
                },
            },
        }
        style = styles[variant]
        btn = AppButton(
            parent,
            text=text,
            command=command,
            padx=16,
            pady=10,
            font=style["normal"]["font"],
            normal_palette=style["normal"],
            hover_palette=style["hover"],
            disabled_palette={
                "bg": self.COLORS["surface_alt"],
                "fg": "#98a2b3",
                "border": self.COLORS["border"],
                "font": style["normal"]["font"],
            },
        )
        if width is not None:
            btn.configure(width=width)
        return btn


    def _make_entry(self, parent: tk.Widget, width: int | None = None):
        entry = tk.Entry(
            parent,
            relief="flat",
            bd=0,
            bg="#ffffff",
            fg=self.COLORS["text"],
            insertbackground=self.COLORS["text"],
            highlightthickness=1,
            highlightbackground=self.COLORS["border"],
            highlightcolor=self.COLORS["accent"],
            font=(UI_FONT, 11),
        )
        if width is not None:
            entry.configure(width=width)
        return entry

    def _make_separator(self, parent: tk.Widget):
        tk.Frame(parent, bg=self.COLORS["border"], height=1).pack(fill="x", pady=18)

    def _set_badge(self, label: tk.Label, text: str, fg: str, bg: str):
        label.configure(text=text, fg=fg, bg=bg)

    def _status_palette(self, tone: str) -> tuple[str, str]:
        mapping = {
            "success": (self.COLORS["success_fg"], self.COLORS["success_bg"]),
            "warning": (self.COLORS["warning_fg"], self.COLORS["warning_bg"]),
            "danger": (self.COLORS["danger_fg"], self.COLORS["danger_bg"]),
            "muted": (self.COLORS["muted"], self.COLORS["surface_alt"]),
        }
        return mapping.get(tone, mapping["muted"])

    def _make_info_tile(self, parent: tk.Widget, title: str):
        tile = tk.Frame(
            parent,
            bg=self.COLORS["surface_alt"],
            highlightbackground=self.COLORS["border"],
            highlightthickness=1,
            bd=0,
        )
        tk.Label(
            tile,
            text=title,
            bg=self.COLORS["surface_alt"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
        ).pack(anchor="w", padx=14, pady=(12, 4))
        value = tk.Label(
            tile,
            text="--",
            bg=self.COLORS["surface_alt"],
            fg=self.COLORS["text"],
            font=(UI_FONT_BOLD, 12, "bold"),
            justify="left",
            wraplength=260,
        )
        value.pack(anchor="w", padx=14, pady=(0, 12))
        return tile, value

    def _build_account_tab(self):
        page = self.tab_account_content

        _, overview_body = self._make_card(
            page,
            "任务概览",
            "先确认目标场景和日期，再做页面检查。检查通过后即可执行抓取。",
        )
        overview_grid = tk.Frame(overview_body, bg=self.COLORS["surface"])
        overview_grid.pack(fill="x")
        for column in range(3):
            overview_grid.grid_columnconfigure(column, weight=1)

        tile, self.label_target_scene = self._make_info_tile(overview_grid, "目标场景")
        tile.grid(row=0, column=0, sticky="nsew", padx=(0, 10), pady=(0, 10))
        tile, self.label_target_portal = self._make_info_tile(overview_grid, "入口类型")
        tile.grid(row=0, column=1, sticky="nsew", padx=(0, 10), pady=(0, 10))
        tile, self.label_target_date = self._make_info_tile(overview_grid, "目标日期")
        tile.grid(row=0, column=2, sticky="nsew", pady=(0, 10))
        tile, self.label_task_status_card = self._make_info_tile(overview_grid, "任务状态")
        tile.grid(row=1, column=0, sticky="nsew", padx=(0, 10))
        tile, self.label_detected_scene = self._make_info_tile(overview_grid, "页面识别")
        tile.grid(row=1, column=1, sticky="nsew", padx=(0, 10))
        tile, self.label_detected_account = self._make_info_tile(overview_grid, "当前账号")
        tile.grid(row=1, column=2, sticky="nsew")

        self.label_precheck_note = tk.Label(
            overview_body,
            text='还没有执行页面检查。建议先点击顶部的"检查页面"。',
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
            justify="left",
            wraplength=860,
        )
        self.label_precheck_note.pack(anchor="w", pady=(14, 0))

        _, login_body = self._make_card(
            page,
            "页面与登录",
            "首次执行时会自动拉起浏览器。完成一次登录后，后续就能复用本地浏览器缓存。",
        )
        login_body.grid_columnconfigure(0, weight=1)
        login_body.grid_columnconfigure(1, weight=1)

        left = tk.Frame(login_body, bg=self.COLORS["surface"])
        left.grid(row=0, column=0, sticky="nw")
        tk.Label(
            left,
            text='如果要更换账号，先点击"切换账号"。\n如果登录状态失效，再清除浏览器数据重新登录。',
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            justify="left",
            wraplength=430,
            font=(UI_FONT, 11),
        ).pack(anchor="w")

        right = tk.Frame(login_body, bg=self.COLORS["surface"])
        right.grid(row=0, column=1, sticky="ne", padx=(18, 0))

        self.label_cookie_status = tk.Label(
            right,
            text="检查中...",
            padx=12,
            pady=6,
            font=(UI_FONT_BOLD, 11, "bold"),
            bd=0,
        )
        self.label_cookie_status.pack(anchor="e")

        self.label_cookie_note = tk.Label(
            right,
            text="",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            justify="left",
            wraplength=320,
            font=(UI_FONT, 10),
        )
        self.label_cookie_note.pack(anchor="e", pady=(10, 0))

        button_row = tk.Frame(login_body, bg=self.COLORS["surface"])
        button_row.grid(row=1, column=0, columnspan=2, sticky="w", pady=(18, 0))
        self._make_button(button_row, "切换账号", self._switch_account, variant="primary").pack(side="left")
        self._make_button(button_row, "清除浏览器数据（重新登录）", self._clear_cookies).pack(side="left", padx=(12, 0))

        _, settings_body = self._make_card(
            page,
            "任务参数",
            "先选抓取场景，再选日期策略。不同场景会走不同的页面导航和日期设置逻辑。",
        )
        settings_body.grid_columnconfigure(1, weight=1)

        row = 0
        tk.Label(settings_body, text="入口类型", bg=self.COLORS["surface"], fg=self.COLORS["muted"], font=(UI_FONT, 11)).grid(row=row, column=0, sticky="ne", pady=8)
        portal_frame = tk.Frame(settings_body, bg=self.COLORS["surface"])
        portal_frame.grid(row=row, column=1, sticky="w", pady=8)
        self.var_portal = tk.StringVar(value=self.config.get("portal_type", "creator"))
        self.portal_selector = self._make_segmented_group(
            portal_frame,
            self.var_portal,
            [("达人入口", "creator"), ("店铺入口", "shop")],
            command=self._on_portal_changed,
        )
        self.portal_selector.pack(anchor="w")
        tk.Label(
            settings_body,
            text="达人入口支持：渠道数据、直播数据、短视频数据。店铺入口默认走实时直播数据。",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
            justify="left",
        ).grid(row=row + 1, column=1, sticky="w")

        row += 2
        tk.Label(settings_body, text="抓取模式", bg=self.COLORS["surface"], fg=self.COLORS["muted"], font=(UI_FONT, 11)).grid(row=row, column=0, sticky="ne", pady=8)
        self.var_scene = tk.StringVar(value="unified")
        self.scene_selector_host = tk.Frame(settings_body, bg=self.COLORS["surface"])
        self.scene_selector_host.grid(row=row, column=1, sticky="w", pady=8)
        self._render_scene_selector()
        tk.Label(
            settings_body,
            text="一次抓取渠道明细、直播整体、短视频引流三类数据，输出到同一张 Excel 表的三个 Sheet",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
            justify="left",
        ).grid(row=row + 1, column=1, sticky="w")

        row += 2
        tk.Label(settings_body, text="抓取日期", bg=self.COLORS["surface"], fg=self.COLORS["muted"], font=(UI_FONT, 11)).grid(row=row, column=0, sticky="ne", pady=12)
        date_frame = tk.Frame(settings_body, bg=self.COLORS["surface"])
        date_frame.grid(row=row, column=1, sticky="w", pady=12)
        self.var_date_mode = tk.StringVar(value=self.config.get("date_mode", "last_7_days"))
        self.date_selector = self._make_segmented_group(
            date_frame,
            self.var_date_mode,
            build_date_mode_options(),
            command=self._on_date_mode_changed,
        )
        self.date_selector.pack(anchor="w")
        tk.Label(
            settings_body,
            text="当前仅保留两个稳定模式：近期七天、近一天。自定义日期已移除，避免抓错数据。",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
            justify="left",
        ).grid(row=row + 1, column=1, sticky="w")

        row += 2
        tk.Label(settings_body, text="罗盘地址", bg=self.COLORS["surface"], fg=self.COLORS["muted"], font=(UI_FONT, 11)).grid(row=row, column=0, sticky="e", pady=12)
        self.entry_url = self._make_entry(settings_body)
        self.entry_url.grid(row=row, column=1, sticky="ew", pady=12)
        self.entry_url.insert(0, self.config.get("compass_url", ""))

        row += 1
        self.var_headless = tk.BooleanVar(value=self.config.get("headless", False))
        self.headless_toggle = self._make_toggle_row(
            settings_body,
            self.var_headless,
            "无头模式",
            "Cookie 有效时可用，登录时会自动关闭无头模式，适合稳定抓取时使用。",
        )
        self.headless_toggle.grid(row=row, column=1, sticky="ew", pady=(2, 8))

        row += 1
        save_row = tk.Frame(settings_body, bg=self.COLORS["surface"])
        save_row.grid(row=row, column=1, sticky="e", pady=(18, 0))
        self._make_button(save_row, "保存配置", self._save_account, variant="primary").pack(side="right")

        _, output_body = self._make_card(
            page,
            "最近结果",
            "抓取完成后，最近一次结果会显示在这里。你不需要再去日志里找路径。",
        )
        self.label_latest_result_name = tk.Label(
            output_body,
            text="还没有执行成功的抓取任务",
            bg=self.COLORS["surface"],
            fg=self.COLORS["text"],
            font=(UI_FONT_BOLD, 12, "bold"),
            justify="left",
        )
        self.label_latest_result_name.pack(anchor="w")
        self.label_latest_result_meta = tk.Label(
            output_body,
            text="",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
            justify="left",
            wraplength=860,
        )
        self.label_latest_result_meta.pack(anchor="w", pady=(8, 0))
        self.label_latest_result_path = tk.Label(
            output_body,
            text="",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
            justify="left",
            wraplength=860,
        )
        self.label_latest_result_path.pack(anchor="w", pady=(6, 0))

        result_actions = tk.Frame(output_body, bg=self.COLORS["surface"])
        result_actions.pack(anchor="w", pady=(14, 0))
        self._make_button(result_actions, "导出最近一次数据", self._export_latest_data, variant="primary").pack(side="left")

        self._update_cookie_status()
        self._refresh_task_overview()
        self._update_precheck_summary(None)
        self._refresh_latest_result_summary()

    def _build_schedule_tab(self):
        page = self.tab_schedule_content

        _, schedule_body = self._make_card(
            page,
            "定时任务",
            "应用保持打开时，会按照你设置的时间自动执行任务。",
        )
        self.var_schedule_on = tk.BooleanVar(value=self.config.get("schedule_enabled", True))
        self.schedule_toggle = self._make_toggle_row(
            schedule_body,
            self.var_schedule_on,
            "启用每日定时抓取",
            "开启后会把当前时间配置保存为默认定时方案，关闭后不会自动参与定时执行。",
        )
        self.schedule_toggle.pack(fill="x")

        time_row = tk.Frame(schedule_body, bg=self.COLORS["surface"])
        time_row.pack(anchor="w", pady=(16, 0))
        tk.Label(time_row, text="执行时间", bg=self.COLORS["surface"], fg=self.COLORS["muted"], font=(UI_FONT, 11)).pack(side="left")
        self.spin_hour = tk.Spinbox(
            time_row,
            from_=0,
            to=23,
            width=4,
            format="%02.0f",
            font=(UI_FONT, 11),
            relief="flat",
            highlightthickness=1,
            highlightbackground=self.COLORS["border"],
            highlightcolor=self.COLORS["accent"],
            bd=0,
        )
        self.spin_hour.delete(0, tk.END)
        self.spin_hour.insert(0, f"{self.config.get('schedule_hour', 8):02d}")
        self.spin_hour.pack(side="left", padx=(16, 6))
        tk.Label(time_row, text="时", bg=self.COLORS["surface"], fg=self.COLORS["muted"], font=(UI_FONT, 11)).pack(side="left")
        self.spin_minute = tk.Spinbox(
            time_row,
            from_=0,
            to=59,
            width=4,
            format="%02.0f",
            font=(UI_FONT, 11),
            relief="flat",
            highlightthickness=1,
            highlightbackground=self.COLORS["border"],
            highlightcolor=self.COLORS["accent"],
            bd=0,
        )
        self.spin_minute.delete(0, tk.END)
        self.spin_minute.insert(0, f"{self.config.get('schedule_minute', 0):02d}")
        self.spin_minute.pack(side="left", padx=(16, 6))
        tk.Label(time_row, text="分", bg=self.COLORS["surface"], fg=self.COLORS["muted"], font=(UI_FONT, 11)).pack(side="left")

        output_dir_row = tk.Frame(schedule_body, bg=self.COLORS["surface"])
        output_dir_row.pack(anchor="w", pady=(16, 0))
        tk.Label(output_dir_row, text="自动导出目录", bg=self.COLORS["surface"], fg=self.COLORS["muted"], font=(UI_FONT, 11)).pack(side="left")
        self.entry_output_dir = tk.Entry(
            output_dir_row,
            textvariable=tk.StringVar(value=self.config.get("output_dir", "")),
            width=32,
            font=(UI_FONT, 11),
            relief="flat",
            highlightthickness=1,
            highlightbackground=self.COLORS["border"],
            highlightcolor=self.COLORS["accent"],
            bd=0,
        )
        self.entry_output_dir.pack(side="left", padx=(16, 6))
        self._make_button(output_dir_row, "选择...", self._browse_output_dir).pack(side="left")
        tk.Label(
            output_dir_row,
            text="（留空则不自动导出）",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
        ).pack(side="left", padx=(8, 0))

        save_row = tk.Frame(schedule_body, bg=self.COLORS["surface"])
        save_row.pack(fill="x", pady=(18, 0))
        self._make_button(save_row, "保存设置", self._save_schedule, variant="primary").pack(side="right")

        _, control_body = self._make_card(
            page,
            "调度控制",
            "调度启动后会定期检查时间，命中后自动执行抓取。",
        )
        self.label_scheduler_status = tk.Label(
            control_body,
            text="未启动",
            padx=12,
            pady=6,
            font=(UI_FONT_BOLD, 11, "bold"),
            bd=0,
        )
        self._set_badge(self.label_scheduler_status, "未启动", self.COLORS["muted"], self.COLORS["surface_alt"])
        self.label_scheduler_status.pack(anchor="w")

        control_row = tk.Frame(control_body, bg=self.COLORS["surface"])
        control_row.pack(anchor="w", pady=(16, 0))
        self.btn_start_scheduler = self._make_button(control_row, "启动调度", self._start_scheduler, variant="primary")
        self.btn_start_scheduler.pack(side="left")
        self.btn_stop_scheduler = self._make_button(control_row, "停止调度", self._stop_scheduler)
        self.btn_stop_scheduler.configure(state="disabled")
        self.btn_stop_scheduler.pack(side="left", padx=(12, 0))

        tk.Label(
            control_body,
            text="建议在确认登录状态有效后再启用调度。若任务执行中关闭程序，调度会随之停止。",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            justify="left",
            wraplength=760,
            font=(UI_FONT, 11),
        ).pack(anchor="w", pady=(16, 0))

    def _build_log_tab(self):
        page = self.tab_log
        card = tk.Frame(
            page,
            bg=self.COLORS["surface"],
            highlightbackground=self.COLORS["border"],
            highlightthickness=1,
            bd=0,
        )
        card.pack(fill="both", expand=True)

        inner = tk.Frame(card, bg=self.COLORS["surface"])
        inner.pack(fill="both", expand=True, padx=22, pady=20)

        top = tk.Frame(inner, bg=self.COLORS["surface"])
        top.pack(fill="x")
        tk.Label(
            top,
            text="运行日志",
            bg=self.COLORS["surface"],
            fg=self.COLORS["text"],
            font=(UI_FONT_BOLD, 16, "bold"),
        ).pack(side="left")
        tk.Label(
            top,
            text="这里会显示登录、导航、导出和错误信息。",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 11),
        ).pack(side="left", padx=(12, 0))
        self._make_button(top, "清空日志", self._clear_log).pack(side="right")

        self.log_text = scrolledtext.ScrolledText(
            inner,
            state="disabled",
            wrap="word",
            font=MONO_FONT,
            bg="#0f172a",
            fg="#e5edf7",
            insertbackground="#ffffff",
            relief="flat",
            bd=0,
            padx=16,
            pady=16,
        )
        self.log_text.pack(fill="both", expand=True, pady=(16, 0))

    def _build_bottom_bar(self):
        left = tk.Frame(self.action_strip, bg=self.COLORS["surface"])
        left.pack(side="left", fill="x", expand=True, padx=18, pady=14)
        tk.Label(
            left,
            text="任务操作",
            bg=self.COLORS["surface"],
            fg=self.COLORS["text"],
            font=(UI_FONT_BOLD, 12, "bold"),
        ).pack(anchor="w")
        tk.Label(
            left,
            text="推荐顺序：保存配置 → 检查页面 → 立即执行一次。最近结果可直接导出。",
            bg=self.COLORS["surface"],
            fg=self.COLORS["muted"],
            font=(UI_FONT, 10),
        ).pack(anchor="w", pady=(4, 0))

        right = tk.Frame(self.action_strip, bg=self.COLORS["surface"])
        right.pack(side="right", padx=18, pady=14)

        self.label_status = tk.Label(
            right,
            text="就绪",
            padx=12,
            pady=6,
            font=(UI_FONT_BOLD, 11, "bold"),
            bd=0,
        )
        self.label_status.pack(side="right", padx=(12, 0))
        self._set_badge(self.label_status, "就绪", self.COLORS["success_fg"], self.COLORS["success_bg"])

        self.btn_minimize = self._make_button(right, "最小化到托盘", self._minimize_to_tray)
        self.btn_minimize.pack(side="right", padx=(12, 0))
        self.btn_export = self._make_button(right, "导出最近一次数据", self._export_latest_data)
        self.btn_export.pack(side="right", padx=(12, 0))
        self.btn_cancel = self._make_button(right, "取消任务", self._cancel_task)
        self.btn_cancel.configure(state="disabled")
        self.btn_cancel.pack(side="right", padx=(12, 0))
        self.btn_check_page = self._make_button(right, "检查页面", self._check_page)
        self.btn_check_page.pack(side="right", padx=(12, 0))
        self.btn_run_once = self._make_button(right, "立即执行一次", self._run_once, variant="primary")
        self.btn_run_once.pack(side="right")
        self.btn_confirm_switch = self._make_button(right, "确认已切换", self._confirm_switch, variant="success")

        self._refresh_export_button()

    def _apply_window_icon(self):
        icon_path = _resource_path("assets/app_icon.png")
        if not icon_path.exists():
            return

        try:
            self.app_icon_photo = tk.PhotoImage(file=str(icon_path))
            self.root.iconphoto(True, self.app_icon_photo)
        except Exception as e:
            logger.warning(f"窗口图标加载失败: {e}")

    def _setup_log_handler(self):
        handler = TextHandler(self.log_text)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)

    def _render_scene_selector(self):
        for child in self.scene_selector_host.winfo_children():
            child.destroy()

        options = build_scene_options(self.var_portal.get())
        self.scene_selector = self._make_segmented_group(
            self.scene_selector_host,
            self.var_scene,
            options,
            command=self._refresh_task_overview,
        )
        self.scene_selector.pack(anchor="w")

    def _collect_form_config(self, *, show_errors: bool) -> dict | None:
        compass_url = self.entry_url.get().strip()
        headless = self.var_headless.get()
        portal_type = self.var_portal.get()
        date_mode = self.var_date_mode.get()

        return {
            **self.config,
            "compass_url": compass_url,
            "headless": headless,
            "portal_type": portal_type,
            "date_mode": date_mode,
        }

    def _refresh_task_overview(self):
        if not hasattr(self, "label_target_scene"):
            return

        config = self._collect_form_config(show_errors=False) or {
            **self.config,
            "portal_type": getattr(self, "var_portal", tk.StringVar(value=self.config.get("portal_type", "creator"))).get(),
            "date_mode": getattr(self, "var_date_mode", tk.StringVar(value=self.config.get("date_mode", DATE_MODE_LAST_7_DAYS))).get(),
        }

        scene_text = "全部数据（渠道 + 直播 + 短视频）"
        self.label_target_scene.config(text=scene_text)

        portal_text = "达人入口" if config.get("portal_type") == "creator" else "店铺入口"
        self.label_target_portal.config(text=portal_text)

        selection = resolve_target_date_range(config)
        if selection.is_single_day:
            date_text = f"{selection.label} · {selection.start:%Y-%m-%d}"
        else:
            date_text = f"{selection.label} · {selection.start:%Y-%m-%d} ~ {selection.end:%Y-%m-%d}"
        self.label_target_date.config(text=date_text)

    def _refresh_latest_result_summary(self):
        if not hasattr(self, "label_latest_result_name"):
            return

        source = self.latest_export_path
        if not source or not source.exists():
            self.label_latest_result_name.config(text="还没有执行成功的抓取任务")
            self.label_latest_result_meta.config(text="执行成功后，这里会显示最新结果的场景、日期和文件信息。")
            self.label_latest_result_path.config(text="")
            return

        stat = source.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        meta_parts = [f"文件类型: {source.suffix or '未知'}", f"更新时间: {mtime}"]

        if self.last_task_result and self.last_task_result.get("success"):
            scene_name = self.last_task_result.get("scene_name")
            if scene_name:
                meta_parts.insert(0, f"场景: {scene_name}")
            target = self.last_task_result.get("target_date_range") or {}
            if target.get("start"):
                if target.get("start") == target.get("end"):
                    meta_parts.append(f"目标日期: {target['start']}")
                else:
                    meta_parts.append(f"目标范围: {target['start']} ~ {target['end']}")
            if self.last_task_result.get("account_name"):
                meta_parts.append(f"账号: {self.last_task_result['account_name']}")

        self.label_latest_result_name.config(text=f"最近结果：{source.name}")
        self.label_latest_result_meta.config(text="    ".join(meta_parts))
        self.label_latest_result_path.config(text=f"文件位置：{source}")

    def _apply_task_status(self, task_status: str, detail: str | None = None):
        text, tone = build_task_status_badge(task_status)
        fg, bg = self._status_palette(tone)
        if hasattr(self, "label_status"):
            self._set_badge(self.label_status, text, fg, bg)
        if hasattr(self, "label_task_status_card"):
            self._set_badge(self.label_task_status_card, text, fg, bg)
        if detail and hasattr(self, "label_precheck_note"):
            self.label_precheck_note.config(text=detail)

    def _update_precheck_summary(self, result: dict | None):
        if not hasattr(self, "label_detected_scene"):
            return

        if not result:
            self.label_detected_scene.config(text="未检查")
            self.label_detected_account.config(text="未识别")
            self.label_precheck_note.config(text='还没有执行页面检查。建议先点击顶部的"检查页面"。')
            self._apply_task_status(TASK_STATUS_PENDING)
            return

        detected = result.get("detected_scene") or {}
        scene_name = detected.get("scene_name") or result.get("scene_name") or "未知场景"
        confidence = detected.get("confidence")
        if confidence and confidence != "unknown":
            scene_text = f"{scene_name} · {confidence}"
        else:
            scene_text = scene_name
        self.label_detected_scene.config(text=scene_text)

        account_name = result.get("account_name") or "未识别"
        self.label_detected_account.config(text=account_name)
        self._apply_task_status(result.get("task_status", TASK_STATUS_PENDING), result.get("message"))

    def _on_portal_changed(self):
        self._render_scene_selector()
        self._refresh_task_overview()

    def _on_date_mode_changed(self):
        self._refresh_task_overview()

    def _save_account(self):
        new_config = self._collect_form_config(show_errors=True)
        if not new_config:
            return

        save_config(new_config)
        self.config = new_config
        self._refresh_task_overview()

        portal_name = "达人入口" if new_config["portal_type"] == "creator" else "店铺入口"
        mode_map = {
            DATE_MODE_LAST_7_DAYS: "近期七天",
            DATE_MODE_LAST_1_DAY: "近一天",
        }
        messagebox.showinfo(
            "提示",
            "配置已保存\n"
            f"入口类型: {portal_name}\n"
            f"抓取模式: 全部数据（渠道 + 直播 + 短视频）\n"
            f"抓取日期: {mode_map[new_config['date_mode']]}",
        )

    def _update_cookie_status(self):
        profile_dir = APP_DIR / "browser_profile"
        if profile_dir.exists() and any(profile_dir.iterdir()):
            self._set_badge(self.label_cookie_status, "登录状态已保存", self.COLORS["success_fg"], self.COLORS["success_bg"])
            self.label_cookie_note.config(text="后续运行会直接沿用当前浏览器缓存，通常不需要重复登录。")
        else:
            self._set_badge(self.label_cookie_status, "首次运行需登录", self.COLORS["warning_fg"], self.COLORS["warning_bg"])
            self.label_cookie_note.config(text="执行抓取时会自动打开浏览器，请在浏览器中完成登录后返回程序。")

    def _clear_cookies(self):
        profile_dir = APP_DIR / "browser_profile"
        if profile_dir.exists():
            try:
                shutil.rmtree(profile_dir)
                logger.info("浏览器数据已清除")
            except Exception as e:
                logger.warning(f"清除浏览器数据失败: {e}")
                messagebox.showwarning("提示", f"清除失败: {e}\n请先关闭正在运行的任务")
                return
        if COOKIE_FILE.exists():
            COOKIE_FILE.unlink()
        self._update_cookie_status()
        messagebox.showinfo("提示", "浏览器数据已清除，下次执行时需要重新登录")

    def _switch_account(self):
        if self.task_thread and self.task_thread.is_alive():
            messagebox.showwarning("提示", "任务正在执行中，请等待完成后再切换账号")
            return

        self.btn_run_once.config(state="disabled")
        self.btn_check_page.config(state="disabled")
        self.btn_cancel.config(state="normal")
        self._apply_task_status(TASK_STATUS_PRECHECKING, '浏览器已打开，请在浏览器中完成账号切换后点击"确认已切换"。')
        self._show_page("log")

        self.btn_confirm_switch.pack(side="right", padx=(12, 0))

        self.scraper = DouyinCompassScraper(self.config)
        self.task_thread = threading.Thread(target=self._switch_account_worker, daemon=True)
        self.task_thread.start()

    def _switch_account_worker(self):
        result = self.scraper.run_switch_account()
        self.root.after(0, self._on_switch_done, result)

    def _confirm_switch(self):
        if self.scraper:
            self.scraper.confirm_switch()
            self._apply_task_status(TASK_STATUS_PRECHECKING, "正在保存新的登录状态。")

    def _on_switch_done(self, result: dict):
        self.btn_run_once.config(state="normal")
        self.btn_check_page.config(state="normal")
        self.btn_cancel.config(state="disabled")
        self.btn_confirm_switch.pack_forget()
        self._update_cookie_status()
        if result["success"]:
            self._apply_task_status(TASK_STATUS_SUCCESS, "账号切换成功。你现在可以检查页面或直接执行抓取。")
            messagebox.showinfo("提示", '账号切换成功！现在可以点击"立即执行一次"抓取新账号的数据。')
        else:
            self._apply_task_status(TASK_STATUS_FAILED, result.get("message", "账号切换失败"))

    def _browse_output_dir(self):
        path = filedialog.askdirectory(title="选择自动导出目录")
        if path:
            self.entry_output_dir.delete(0, tk.END)
            self.entry_output_dir.insert(0, path)

    def _save_schedule(self):
        new_config = {
            **self.config,
            "schedule_enabled": self.var_schedule_on.get(),
            "schedule_hour": int(self.spin_hour.get()),
            "schedule_minute": int(self.spin_minute.get()),
            "output_dir": self.entry_output_dir.get().strip(),
        }
        save_config(new_config)
        self.config = new_config
        messagebox.showinfo("提示", "调度设置已保存\n定时任务会按当前已保存的任务配置执行。")

    def _run_once(self, *, use_saved_task: bool = False):
        if self.task_thread and self.task_thread.is_alive():
            messagebox.showwarning("提示", "任务正在执行中")
            return

        form_config = None
        if not use_saved_task:
            form_config = self._collect_form_config(show_errors=True)
            if not form_config:
                return

        runtime_config = resolve_runtime_config(self.config, form_config, use_saved_task=use_saved_task)
        self.btn_run_once.config(state="disabled")
        self.btn_check_page.config(state="disabled")
        self.btn_cancel.config(state="normal")
        if use_saved_task:
            self._apply_task_status(TASK_STATUS_PRECHECKING, "正在按已保存的定时方案打开浏览器并进入目标场景。")
        else:
            self._apply_task_status(TASK_STATUS_PRECHECKING, "正在打开浏览器并进入目标场景。")
        self._show_page("log")

        self.scraper = DouyinCompassScraper(runtime_config)
        self.task_thread = threading.Thread(target=self._task_worker, daemon=True)
        self.task_thread.start()

    def _check_page(self):
        if self.task_thread and self.task_thread.is_alive():
            messagebox.showwarning("提示", "任务正在执行中")
            return

        runtime_config = self._collect_form_config(show_errors=True)
        if not runtime_config:
            return

        self.btn_run_once.config(state="disabled")
        self.btn_check_page.config(state="disabled")
        self.btn_cancel.config(state="normal")
        self._apply_task_status(TASK_STATUS_PRECHECKING, "正在检查登录状态、账号和目标页面。")
        self._show_page("log")

        self.scraper = DouyinCompassScraper(runtime_config)
        self.task_thread = threading.Thread(target=self._precheck_worker, daemon=True)
        self.task_thread.start()

    def _precheck_worker(self):
        result = self.scraper.run_precheck()
        self.root.after(0, self._on_precheck_done, result)

    def _task_worker(self):
        result = self.scraper.run()
        self.root.after(0, self._on_task_done, result)

    def _on_precheck_done(self, result: dict):
        self.btn_run_once.config(state="normal")
        self.btn_check_page.config(state="normal")
        self.btn_cancel.config(state="disabled")
        self._update_cookie_status()
        self.last_precheck_result = result
        self._update_precheck_summary(result)

        if result["success"]:
            messagebox.showinfo(
                "页面检查通过",
                f"{result['message']}\n\n识别场景：{result['detected_scene']['scene_name']}\n"
                f"当前账号：{result.get('account_name') or '未识别'}",
            )
        else:
            messagebox.showwarning("页面检查失败", result["message"])

    def _on_task_done(self, result: dict):
        self.btn_run_once.config(state="normal")
        self.btn_check_page.config(state="normal")
        self.btn_cancel.config(state="disabled")
        self._update_cookie_status()
        self.last_task_result = result
        self.last_precheck_result = {
            "success": result.get("success", False),
            "message": result.get("message", ""),
            "task_status": result.get("task_status", TASK_STATUS_PENDING),
            "scene_name": result.get("scene_name", ""),
            "detected_scene": {
                "scene_name": result.get("scene_name", ""),
                "confidence": "high" if result.get("success") else "unknown",
            },
            "account_name": result.get("account_name", ""),
        }
        self._update_precheck_summary(self.last_precheck_result)
        if result["success"]:
            self.latest_export_path = self._resolve_export_source(result)
            self._refresh_export_button()
            self._refresh_latest_result_summary()
            account = result.get("account_name", "")
            detail = result["message"]
            if account:
                detail += f"\n当前账号：{account}"
            self._apply_task_status(TASK_STATUS_SUCCESS, detail)

            # 自动复制到指定目录（定时/自动导出场景）
            output_dir = self.config.get("output_dir", "")
            if output_dir and self.latest_export_path:
                auto_copied = self._auto_copy_to_output_dir(self.latest_export_path, output_dir)
                if auto_copied:
                    detail += f"\n已自动导出至：{auto_copied}"
                    self._apply_task_status(TASK_STATUS_SUCCESS, detail)

            if self.latest_export_path:
                messagebox.showinfo(
                    "抓取完成",
                    f'{result["message"]}\n\n现在可以点击"导出最近一次数据"直接导出文件。'
                )
        else:
            self._apply_task_status(result.get("task_status", TASK_STATUS_FAILED), result.get("message"))

    def _cancel_task(self):
        if self.scraper:
            self.scraper.cancel()
            self._apply_task_status(TASK_STATUS_CANCELLED, "正在取消当前任务。")

    def _start_scheduler(self):
        self._save_schedule()
        if self.scheduler_running:
            return

        self.scheduler_running = True
        self.btn_start_scheduler.config(state="disabled")
        self.btn_stop_scheduler.config(state="normal")

        hour = self.config["schedule_hour"]
        minute = self.config["schedule_minute"]
        self._set_badge(
            self.label_scheduler_status,
            f"运行中 · 每天 {hour:02d}:{minute:02d}",
            self.COLORS["success_fg"],
            self.COLORS["success_bg"],
        )
        logger.info(f"调度已启动: 每天 {hour:02d}:{minute:02d}")

        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()

    def _scheduler_loop(self):
        import time
        last_run_date = None

        while self.scheduler_running:
            now = datetime.now()
            target_hour = self.config["schedule_hour"]
            target_minute = self.config["schedule_minute"]

            if (now.hour == target_hour and now.minute == target_minute and last_run_date != now.date()):
                last_run_date = now.date()
                logger.info("定时触发任务...")
                self.root.after(0, lambda: self._run_once(use_saved_task=True))

            time.sleep(30)

    def _stop_scheduler(self):
        self.scheduler_running = False
        self.btn_start_scheduler.config(state="normal")
        self.btn_stop_scheduler.config(state="disabled")
        self._set_badge(self.label_scheduler_status, "已停止", self.COLORS["muted"], self.COLORS["surface_alt"])
        logger.info("调度已停止")

    def _minimize_to_tray(self):
        try:
            import pystray
            from PIL import Image
        except ImportError:
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

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")

    def _find_latest_export_path(self) -> Path | None:
        patterns = ["罗盘数据抓取_*.xlsx", "live_*.csv", "*.xlsx"]
        candidates = []

        candidates.extend(DOWNLOAD_DIR.glob(patterns[0]))
        if not candidates:
            candidates.extend(DATA_DIR.glob(patterns[1]))
        if not candidates:
            for pattern in patterns:
                candidates.extend(DOWNLOAD_DIR.glob(pattern))
                candidates.extend(DATA_DIR.glob(pattern))

        files = [path for path in candidates if path.is_file()]
        if not files:
            return None
        return max(files, key=lambda path: path.stat().st_mtime)

    def _resolve_export_source(self, result: dict) -> Path | None:
        for key in ("xlsx_path", "excel_path", "filepath", "csv_path"):
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
        self._refresh_latest_result_summary()

    def _auto_copy_to_output_dir(self, source: Path, output_dir: str) -> str | None:
        """自动将抓取结果复制到指定目录。"""
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            target = Path(output_dir) / source.name
            shutil.copy2(source, target)
            logger.info(f"数据已自动导出到: {target}")
            return str(target)
        except Exception as e:
            logger.exception(f"自动导出失败: {e}")
            return None

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
        logger.info(f"{self.APP_TITLE} 已启动")
        self.root.mainloop()
