# -*- coding: utf-8 -*-
"""
浣熊 Raccoon —— MoviePilot V3 插件

架构（feng 定版 2026-11-23）：
- 下载与做种留在 Download（原始文件名/结构不动，种子校验永远可过）
- MP 核心以硬链接方式把文件整理进 MediaLib（飞牛索引 MediaLib，看片/删片都在那边）
- 本插件补 MP 缺的四件事：
  1) 转种：qB 下载完成且已整理 -> 把种子加入 TR（指 Download 原路径）校验做种 -> 移除 qB 任务
  2) 双向删除同步：任一侧文件被删 -> 延迟后同步删除另一侧孪生文件；
     库侧删除且整种文件已清 -> 连带删除 TR/qB 种子记录；删到订阅中的剧 -> 自动停订
  3) 死种清道夫：每日清理 TR 中“数据不存在”的错误种子记录
  4) 存量处理：把 Download 中尚未整理的存量条目按批硬链接整理进 MediaLib 并登记映射
- 全部破坏性动作受 dry_run 总闸控制：dry_run=True 时只记录与通知，不真删、不真转。
"""
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from apscheduler.triggers.cron import CronTrigger

from app.db.oper.transferhistory import TransferHistoryOper
from app.schemas.types import MessageType
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase
from app.sdk.services import DownloaderHelper

STATE_KEY = "raccoon_state_v2"
MEDIA_EXTS = {".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mov", ".wmv", ".iso", ".rmvb", ".flv", ".webm"}


def _is_media(path: str) -> bool:
    return Path(path).suffix.lower() in MEDIA_EXTS


class Raccoon(_PluginBase):
    plugin_name = "浣熊"
    plugin_desc = "你只管下和看：转种、删片腾空间、清死种，浣熊都替你收拾好。"
    plugin_icon = "raccoon.png"
    plugin_version = "0.4.5"
    plugin_author = "feng"
    author_url = ""
    plugin_config_prefix = "raccoon_"
    plugin_order = 50
    auth_level = 1

    # ---------------- 配置 ----------------
    _enabled = False
    _dry_run = True
    _auto_delete = True
    _notify_enabled = True
    _transfer_enabled = True
    _sync_delete_enabled = True
    _sweep_enabled = True
    _delete_delay_minutes = 60
    _stock_batch = 5
    _stock_run_once = False
    _transfer_mode = "now"
    _transfer_run_once = False
    _download_root = "/vol4/1000/Download"
    _library_root = "/vol4/1000/MediaLib"
    _music_download_root = "/vol4/1000/Download"
    _music_library_root = "/vol3/1000/MusicLib"
    _bt_backup = "/BT_backup"

    # ---------------- 运行时 ----------------
    _lock = threading.Lock()
    _state: Dict[str, Any] = {}
    _tick_busy = False

    # ============================================================
    # 生命周期
    # ============================================================
    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._dry_run = bool(config.get("dry_run", True))
        self._auto_delete = bool(config.get("auto_delete", True))
        self._notify_enabled = bool(config.get("notify", True))
        self._transfer_enabled = bool(config.get("transfer_enabled", True))
        self._sync_delete_enabled = bool(config.get("sync_delete_enabled", True))
        self._sweep_enabled = bool(config.get("sweep_enabled", True))
        try:
            self._delete_delay_minutes = int(config.get("delete_delay_minutes", 60))
        except Exception:
            self._delete_delay_minutes = 60
        try:
            self._stock_batch = int(config.get("stock_batch", 5))
        except Exception:
            self._stock_batch = 5
        self._stock_run_once = bool(config.get("stock_run_once", False))
        _tm = str(config.get("transfer_mode") or "now")
        self._transfer_mode = _tm if _tm in ("now", "daily", "weekly") else "now"
        self._transfer_run_once = bool(config.get("transfer_run_once", False))
        self._approve_once = bool(config.get("approve_deletes_once", False))
        self._reject_once = bool(config.get("reject_deletes_once", False))
        self._download_root = str(config.get("download_root") or self._download_root).rstrip("/")
        self._library_root = str(config.get("library_root") or self._library_root).rstrip("/")
        self._music_download_root = str(config.get("music_download_root") or self._music_download_root).rstrip("/")
        self._music_library_root = str(config.get("music_library_root") or self._music_library_root).rstrip("/")
        self._music_base_root = str(config.get("music_base_root") or "").rstrip("/")
        self._state = self.get_data(STATE_KEY) or {}
        for k, v in (("links", {}), ("pending_deletes", []), ("pending_transfers", {}),
                     ("done_transfers", {}), ("stock_done", []), ("stock_failed", {}),
                     ("sub_paused", []), ("last_sweep", ""), ("hist_seen", []),
                     ("pending_verify", {}), ("verified_ok", {}),
                     ("pending_verify_qb", {}), ("last_sweep_qb", ""),
                     ("transfer_blocked", {}), ("pending_approval", []),
                     ("audit", []), ("stock_uncovered", {}),
                     ("last_approval_notify", 0), ("sweep_rejected", {}),
                     ("last_transfer_day", "")):
            self._state.setdefault(k, v)
        _onoff = lambda b: "开" if b else "关"
        logger.info(
            f"浣熊 已加载：{'已停用' if not self._enabled else ('试运行' if self._dry_run else '正式运行')}，"
            f"转种 {_onoff(self._transfer_enabled)}，删片联动 {_onoff(self._sync_delete_enabled)}，"
            f"清死种 {_onoff(self._sweep_enabled)}，自动删除 {_onoff(self._auto_delete)}，"
            f"删除延迟 {self._delete_delay_minutes} 分钟，映射 {len(self._state.get('links', {}))} 条"
        )
        if self._enabled and self._stock_run_once:
            threading.Thread(target=self._stock_job_entry, daemon=True).start()
        if self._enabled and self._transfer_enabled and self._transfer_run_once:
            threading.Thread(target=self._transfer_job_entry, daemon=True).start()
        if self._enabled and self._approve_once:
            threading.Thread(target=self._approval_job_entry, args=(True,), daemon=True).start()
        if self._enabled and self._reject_once:
            threading.Thread(target=self._approval_job_entry, args=(False,), daemon=True).start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/relay_stock", "endpoint": self.api_stock, "methods": ["POST"],
             "summary": "触发一批存量整理"},
            {"path": "/relay_status", "endpoint": self.api_status, "methods": ["GET"],
             "summary": "浣熊状态"},
            {"path": "/relay_pending", "endpoint": self.api_pending, "methods": ["GET"],
             "summary": "待删清单（自动删除关闭时）"},
            {"path": "/relay_approve_deletes", "endpoint": self.api_approve_deletes, "methods": ["POST"],
             "summary": "批准执行待删清单"},
            {"path": "/relay_reject_deletes", "endpoint": self.api_reject_deletes, "methods": ["POST"],
             "summary": "驳回待删清单（不删除）"},
            {"path": "/relay_audit", "endpoint": self.api_audit, "methods": ["GET"],
             "summary": "审计流水（每个动作的依据与结果）"},
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        return [{
            "id": "RaccoonTick",
            "name": "浣熊巡检",
            "trigger": CronTrigger.from_crontab("*/2 * * * *"),
            "func": self.tick,
            "kwargs": {},
        }]

    def get_form(self):
        """配置页（v0.2.0 起按蜂巢签到插件的设计语言）：
        英雄卡 + 分区卡片（彩色图标块标题 + 分隔线）+ 彩色开关 + 说明卡收尾。
        说明文字仍按 feng 的规矩集中在最后的「使用说明」卡里。"""

        def rgba(hex_color, alpha):
            h = hex_color.lstrip("#")
            r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
            return f"rgba({r}, {g}, {b}, {alpha})"

        def icon_tile(icon, color, size=36):
            return {"component": "div",
                    "props": {"class": "d-flex align-center justify-center",
                              "style": f"width: {size}px; height: {size}px; border-radius: 10px; background: {rgba(color, 0.12)}; flex-shrink: 0;"},
                    "content": [{"component": "VIcon",
                                 "props": {"size": int(size * 0.56), "color": color},
                                 "text": icon}]}

        def section(title, icon, color, rows):
            return {"component": "VCard",
                    "props": {"variant": "flat", "rounded": "xl",
                              "class": "mb-3 overflow-hidden",
                              "style": "border: 1px solid rgba(128, 128, 128, 0.18);"},
                    "content": [
                        {"component": "div",
                         "props": {"class": "d-flex align-center ga-3 px-4 pt-4 pb-3"},
                         "content": [icon_tile(icon, color),
                                     {"component": "div",
                                      "props": {"class": "text-subtitle-1 font-weight-bold"},
                                      "text": title}]},
                        {"component": "VDivider"},
                        {"component": "VCardText",
                         "props": {"class": "px-4 pt-3 pb-4"}, "content": rows},
                    ]}

        def col(content, md=None):
            props = {"cols": 12}
            if md:
                props["md"] = md
            return {"component": "VCol", "props": props, "content": content}

        def row(*cols):
            return {"component": "VRow", "content": list(cols)}

        def sw(model, label, color, disabled=None):
            props = {"model": model, "label": label, "color": color,
                     "hide-details": True}
            if disabled:
                props["disabled"] = disabled
            return {"component": "VSwitch", "props": props}

        def tf(model, label, icon, type_=None):
            props = {"model": model, "label": label, "variant": "outlined",
                     "density": "comfortable", "prepend-inner-icon": icon}
            if type_:
                props["type"] = type_
            return {"component": "VTextField", "props": props}

        st = self._state or {}
        header_chips = [
            {"component": "VChip",
             "props": {"size": "small", "variant": "tonal",
                       "color": "warning" if self._dry_run else "success"},
             "text": "试运行（只报告）" if self._dry_run else "实况运行"},
            {"component": "VChip",
             "props": {"size": "small", "variant": "tonal",
                       "color": "info" if self._auto_delete else "warning"},
             "text": "自动删除：开" if self._auto_delete else "自动删除：关"},
        ]
        if st.get("pending_approval"):
            header_chips.append(
                {"component": "VChip",
                 "props": {"size": "small", "variant": "tonal", "color": "warning"},
                 "text": f"待删清单 {len(st['pending_approval'])} 项"})
        if st.get("transfer_blocked"):
            header_chips.append(
                {"component": "VChip",
                 "props": {"size": "small", "variant": "tonal", "color": "error"},
                 "text": f"转种没成 {len(st['transfer_blocked'])} 个"})
        hero = {
            "component": "VCard",
            "props": {"variant": "flat", "rounded": "xl",
                      "class": "mb-3 overflow-hidden",
                      "style": "border: 1px solid rgba(128, 128, 128, 0.18);"},
            "content": [
                {"component": "div",
                 "props": {"class": "d-flex align-center ga-3 px-4 pt-3 pb-2"},
                 "content": [
                     icon_tile("mdi-movie-open", "#6366f1", 40),
                     {"component": "div", "content": [
                         {"component": "div",
                          "props": {"class": "text-subtitle-1 font-weight-bold"},
                          "text": "浣熊"},
                         {"component": "div",
                          "props": {"class": "text-caption text-medium-emphasis"},
                          "text": "下载整理 · QB转种TR · 删片联动 · 清死种记录"},
                     ]},
                 ]},
                {"component": "div",
                 "props": {"class": "d-flex flex-wrap ga-2 px-4 pb-3"},
                 "content": header_chips},
            ],
        }

        remarks = [
            "1. 试运行：打开时只发报告，不真删文件、不真转种、不真清记录；拿不准时先开着观察一轮，再关掉正式运行。",
            "2. 删片联动等待：删片后不马上删另一份，先等这里设的分钟数；等待期间把删掉的文件恢复回来，这次联动删除自动取消。",
            "3. 自动删除与待删清单：自动删除开着时，删除到时间自动执行，「执行待删清单」「放弃待删清单」两个开关会变灰；关掉自动删除后，删除先进待删清单，打开「执行待删清单」保存后才删，打开「放弃待删清单」保存则不删。两个开关都是打开并保存后执行一次，执行完自动关闭。",
            "4. 清死种记录：每天自动跑一轮，不用手动触发；校验后确认完好的种子会自动恢复做种，确认文件不在了的才清记录。",
            "5. 存量整理：打开「马上整理一批存量」并保存，按「存量每批整理数量」处理一批还没整理的片子，执行完开关自动关闭。",
            "6. 音乐基础库（可选）：把一套现成音乐库的目录填进「音乐基础库」，它就成为整理时的基准；在音乐整理目录里删基础库的歌只删镜像，基础库的原件永远不动，不填则没有基础库。从做种目录整理进来的专辑不受此限。",
            "7. QB转种TR：两个条件都满足就转——qB 已下载完成、种子文件逐个核对完整；跟是什么资源、整没整理进整理目录无关。暂停在 qB 里的种子，qB 显示的进度可能不准，按磁盘文件核对，文件完整就照样转。文件不全的会被拦下并通知，补齐后自动放行。「转种时机」选立即就是随时转，选每天/每周一就攒到那个时间点统一转；打开「立即转种一次」保存，可无视时机马上转一批，执行完开关自动关闭，并发一条结果通知（转了几个、没转成的原因）。已经开始的转种（TR 校验、做种确认）不受时机影响，会一直跟进到底。",
        ]

        return [
            {"component": "VForm", "content": [
                hero,
                section("运行设置", "mdi-tune-variant", "#6366f1", [
                    row(col([sw("enabled", "启用插件", "#6366f1")], md=4),
                        col([sw("dry_run", "试运行", "#f59e0b")], md=4),
                        col([sw("notify", "消息通知", "#6366f1")], md=4)),
                ]),
                section("功能开关", "mdi-puzzle-outline", "#6366f1", [
                    row(col([sw("sync_delete_enabled", "删片联动", "#6366f1")], md=4),
                        col([sw("sweep_enabled", "清死种记录", "#6366f1")], md=4)),
                ]),
                section("QB转种TR", "mdi-swap-horizontal", "#6366f1", [
                    row(col([sw("transfer_enabled", "自动转种", "#6366f1")], md=4),
                        col([sw("transfer_run_once", "立即转种一次", "#6366f1")], md=4),
                        col([{"component": "VSelect", "props": {
                            "model": "transfer_mode", "label": "转种时机",
                            "variant": "outlined", "density": "comfortable",
                            "prepend-inner-icon": "mdi-calendar-clock-outline",
                            "hide-details": True,
                            "items": [
                                {"title": "立即转移（随时）", "value": "now"},
                                {"title": "每天一次", "value": "daily"},
                                {"title": "每周一", "value": "weekly"},
                            ]}}], md=4)),
                ]),
                section("删除策略", "mdi-shield-check-outline", "#f59e0b", [
                    row(col([sw("auto_delete", "自动删除", "#f59e0b")], md=4),
                        col([sw("approve_deletes_once", "执行待删清单", "#ef4444",
                                disabled="{{ auto_delete }}")], md=4),
                        col([sw("reject_deletes_once", "放弃待删清单", "#64748b",
                                disabled="{{ auto_delete }}")], md=4)),
                ]),
                section("参数与存量", "mdi-database-cog-outline", "#6366f1", [
                    row(col([tf("delete_delay_minutes", "删片联动等待（分钟）",
                                "mdi-timer-outline", "number")], md=4),
                        col([tf("stock_batch", "存量每批整理数量",
                                "mdi-numeric", "number")], md=4),
                        col([sw("stock_run_once", "马上整理一批存量", "#6366f1")], md=4)),
                ]),
                section("视频目录", "mdi-movie-open", "#6366f1", [
                    row(col([tf("download_root", "视频下载目录",
                                "mdi-folder-download-outline")], md=6),
                        col([tf("library_root", "视频整理目录",
                                "mdi-folder-star-outline")], md=6)),
                ]),
                section("音乐目录", "mdi-music", "#6366f1", [
                    row(col([tf("music_download_root", "音乐下载目录",
                                "mdi-folder-download-outline")], md=6),
                        col([tf("music_library_root", "音乐整理目录",
                                "mdi-folder-music-outline")], md=6)),
                    row(col([tf("music_base_root", "音乐基础库",
                                "mdi-database-outline")], md=6)),
                ]),
                section("使用说明", "mdi-information-outline", "#64748b", [
                    row(col([{"component": "div",
                              "props": {"class": "text-body-2 text-medium-emphasis mb-1"},
                              "text": ln} for ln in remarks])),
                ]),
            ]},
        ], {
            "enabled": False, "dry_run": True, "auto_delete": True, "notify": True,
            "transfer_enabled": True, "sync_delete_enabled": True, "sweep_enabled": True,
            "delete_delay_minutes": 60, "stock_batch": 5, "stock_run_once": False,
            "approve_deletes_once": False, "reject_deletes_once": False,
            "transfer_mode": "now", "transfer_run_once": False,
            "download_root": "/vol4/1000/Download", "library_root": "/vol4/1000/MediaLib",
            "music_download_root": "/vol4/1000/Download", "music_library_root": "/vol3/1000/MusicLib",
            "music_base_root": "",
        }

    def get_page(self) -> Optional[List[dict]]:
        """介绍页（v0.2.0 按蜂巢签到插件的设计语言重做）：
        功能简介（图标块 + 标题 + 一句说明）+ 数据看板（玻璃拟态数字格 + 状态筹码）。
        看板数字渲染时取实时状态；待删清单明细与每条动作的依据仍走通知。"""
        st = self._state or {}

        def rgba(hex_color, alpha):
            h = hex_color.lstrip("#")
            r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
            return f"rgba({r}, {g}, {b}, {alpha})"

        def icon_tile(icon, color, size=40):
            return {"component": "div",
                    "props": {"class": "d-flex align-center justify-center",
                              "style": f"width: {size}px; height: {size}px; border-radius: 12px; background: {rgba(color, 0.12)}; flex-shrink: 0;"},
                    "content": [{"component": "VIcon",
                                 "props": {"size": int(size * 0.55), "color": color},
                                 "text": icon}]}

        def section(title, icon, color, rows):
            return {"component": "VCard",
                    "props": {"variant": "flat", "rounded": "xl",
                              "class": "mb-3 overflow-hidden",
                              "style": "border: 1px solid rgba(128, 128, 128, 0.18);"},
                    "content": [
                        {"component": "div",
                         "props": {"class": "d-flex align-center ga-3 px-4 pt-4 pb-3"},
                         "content": [icon_tile(icon, color, 36),
                                     {"component": "div",
                                      "props": {"class": "text-subtitle-1 font-weight-bold"},
                                      "text": title}]},
                        {"component": "VDivider"},
                        {"component": "VCardText",
                         "props": {"class": "px-4 pt-3 pb-4"}, "content": rows},
                    ]}

        def feature(icon, color, title, text):
            return {"component": "div",
                    "props": {"class": "d-flex align-center ga-3 mb-3"},
                    "content": [
                        icon_tile(icon, color),
                        {"component": "div", "content": [
                            {"component": "div",
                             "props": {"class": "text-body-2 font-weight-medium"},
                             "text": title},
                            {"component": "div",
                             "props": {"class": "text-caption text-medium-emphasis mt-1"},
                             "text": text},
                        ]},
                    ]}

        glass = ("background-color: rgba(var(--v-theme-surface), 0.72); "
                 "color: rgb(var(--v-theme-on-surface)); "
                 "border: 1px solid rgba(var(--v-theme-on-surface), 0.10); "
                 "border-radius: 10px;")

        def stat(icon, color, value, label):
            return {"component": "VCol", "props": {"cols": 6, "md": 3, "class": "pa-1"},
                    "content": [
                        {"component": "div",
                         "props": {"class": "d-flex flex-column align-center justify-center pa-3",
                                   "style": glass},
                         "content": [
                             {"component": "div",
                              "props": {"class": "d-flex align-center ga-2"},
                              "content": [
                                  {"component": "VIcon",
                                   "props": {"size": 20, "color": color},
                                   "text": icon},
                                  {"component": "span",
                                   "props": {"class": "text-h5 font-weight-bold"},
                                   "text": f"{value:,}"},
                              ]},
                             {"component": "div",
                              "props": {"class": "text-caption text-medium-emphasis mt-1"},
                              "text": label},
                         ]},
                    ]}

        def chip(text, color):
            return {"component": "VChip",
                    "props": {"size": "small", "variant": "tonal", "color": color},
                    "text": text}

        intro_rows = [
            feature("mdi-download", "#0ea5e9", "下载整理",
                    "下完不用管：片子自动整理进媒体库，下载目录留原文件做种——看片、做种、文件规整一次到位。"),
            feature("mdi-swap-horizontal", "#14b8a6", "QB转种TR",
                    "告别手动搬种：qB 下完自动交棒 TR 做种，交接前逐个文件核对，缺文件当场拦下——不怕转过去才发现种是坏的。"),
            feature("mdi-link-variant", "#a855f7", "删片联动",
                    "删了就真腾空间：媒体库里删掉，下载目录那份和种子记录跟着清；在订的剧删了自动停订阅，绝不偷偷补回来。"),
            feature("mdi-archive-outline", "#f59e0b", "存量整理",
                    "老库也不用重下：下载目录里的存量老片分批整理入库，硬链接成对登记，和新片一个待遇。"),
            feature("mdi-music", "#ec4899", "专辑整理（音乐）",
                    "专辑自动归位：做种目录里的专辑按语种、歌手、专辑整理成库，重复的歌只留一份；原文件不动，做种不中断。"),
            feature("mdi-broom", "#6366f1", "清死种记录",
                    "告别一排红条：qB 和 TR 的死种每天先校验再清，完好的当场救活继续做种——不误杀，不用你手动挑。"),
        ]

        board_rows = [
            {"component": "VRow", "content": [
                stat("mdi-file-check-outline", "#0ea5e9",
                     len(st.get("links", {})), "已整理文件（份）"),
                stat("mdi-clipboard-list-outline", "#f59e0b",
                     len(st.get("pending_approval", [])), "待删清单（项）"),
                stat("mdi-swap-horizontal", "#ef4444",
                     len(st.get("transfer_blocked", {})), "转种没成（个）"),
                stat("mdi-archive-check-outline", "#14b8a6",
                     len(st.get("stock_done", [])), "存量已整理（个）"),
            ]},
            {"component": "div",
             "props": {"class": "d-flex align-center flex-wrap ga-2 mt-3"},
             "content": [
                 chip("试运行（只报告）" if self._dry_run else "实况运行",
                      "warning" if self._dry_run else "success"),
                 chip("自动删除：开" if self._auto_delete else "自动删除：关（删前先出待删清单）",
                      "info" if self._auto_delete else "warning"),
                 {"component": "span",
                  "props": {"class": "text-body-2 text-medium-emphasis"},
                  "text": (f"转种完成 {len(st.get('done_transfers', {}))} 个 · "
                           f"进行中 {len(st.get('pending_transfers', {}))} 个 · "
                           f"等联动删除 {len(st.get('pending_deletes', []))} 项")},
             ]},
        ]
        for v in list(st.get("transfer_blocked", {}).values())[:5]:
            board_rows.append(
                {"component": "div",
                 "props": {"class": "d-flex align-center ga-3 w-100 mt-3",
                           "style": f"background: {rgba('#f59e0b', 0.08)}; border: 1px solid {rgba('#f59e0b', 0.22)}; border-radius: 12px; padding: 12px 16px;"},
                 "content": [
                     {"component": "VIcon", "props": {"size": 22, "color": "#f59e0b"},
                      "text": "mdi-alert-outline"},
                     {"component": "div",
                      "props": {"class": "text-body-2 text-medium-emphasis"},
                      "text": (f"没成的转种：《{v.get('title')}》——文件只有 "
                               f"{v.get('present')}/{v.get('total')} 个完整，已停下，"
                               f"TR 和 qB 的记录都没动")},
                 ]})

        for it in (st.get("pending_approval", []) or [])[:5]:
            nm = it.get("title") or it.get("name") or "（未命名）"
            if it.get("kind") == "record":
                what = f"{it.get('source', '')} 种子记录"
            else:
                side = "视频整理目录" if it.get("origin") == "lib" else "视频下载目录"
                what = f"文件（{side}里那份）"
            board_rows.append(
                {"component": "div",
                 "props": {"class": "d-flex align-center ga-3 w-100 mt-3",
                           "style": f"background: {rgba('#f59e0b', 0.08)}; border: 1px solid {rgba('#f59e0b', 0.22)}; border-radius: 12px; padding: 12px 16px;"},
                 "content": [
                     {"component": "VIcon", "props": {"size": 22, "color": "#f59e0b"},
                      "text": "mdi-clipboard-alert-outline"},
                     {"component": "div",
                      "props": {"class": "text-body-2 text-medium-emphasis"},
                      "text": f"待删清单：《{nm}》{what}——{it.get('evidence', '')}"},
                 ]})

        return [
            section("数据看板", "mdi-view-dashboard-outline", "#6366f1", board_rows),
            section("功能简介", "mdi-information-outline", "#6366f1", intro_rows),
        ]

    def stop_service(self):
        pass

    # ============================================================
    # 工具
    # ============================================================
    def _save_state(self):
        try:
            self.save_data(STATE_KEY, self._state)
        except Exception as e:
            logger.error(f"浣熊 保存状态失败: {e}")

    def _notify(self, title: str, text: str):
        logger.info(f"浣熊通知 [{title}] {text}")
        if not self._notify_enabled:
            return
        try:
            self.post_message(mtype=MessageType.Plugin, title=title, text=text)
        except Exception as e:
            logger.error(f"浣熊 发送通知失败: {e}")

    def _audit(self, action: str, target, evidence: str):
        """审计流水：每个关键动作记下「做了什么 / 对谁 / 依据什么」，滚动保留最近 300 条。
        所有交接点有据可查——任何删除、转种、停订都能事后按这条流水对账。"""
        try:
            log = self._state.setdefault("audit", [])
            log.append({"ts": time.time(), "action": action,
                        "target": str(target)[:120], "evidence": str(evidence)[:300]})
            del log[:-300]
        except Exception:
            pass

    def _downloaders(self):
        """返回 (qb_name, qb_instance, tr_name, tr_instance)，找不到的为 None"""
        from app.sdk.services import DownloaderHelper
        qb_name = tr_name = None
        qb_inst = tr_inst = None
        try:
            helper = DownloaderHelper()
            for name, cfg in (helper.get_configs() or {}).items():
                ctype = str(getattr(cfg, "type", "") or "").lower()
                svc = helper.get_service(name=name)
                if not svc:
                    continue
                if "transmission" in ctype and tr_inst is None:
                    tr_name, tr_inst = name, svc.instance
                elif "qbittorrent" in ctype and qb_inst is None:
                    qb_name, qb_inst = name, svc.instance
        except Exception as e:
            logger.error(f"浣熊 获取下载器失败: {e}")
        return qb_name, qb_inst, tr_name, tr_inst

    @staticmethod
    def _torrents(instance) -> List[Any]:
        if instance is None:
            return []
        try:
            res = instance.get_torrents()
            torrents = res[0] if isinstance(res, tuple) else res
            return list(torrents or [])
        except Exception as e:
            logger.error(f"浣熊 读取种子列表失败: {e}")
            return []

    @staticmethod
    def _thash(t):
        """种子哈希：DownloaderTorrent 用 hash，TR 原生 Torrent 对象用 hashString"""
        return Raccoon._tget(t, "hash", "hashString", "hash_string", "info_hash")

    @staticmethod
    def _progress100(t) -> float:
        """进度口径不一（0-1 或 0-100），统一成 0-100；TR 原生对象用 percent_done"""
        p = Raccoon._tget(t, "progress", "percent_done", default=0) or 0
        try:
            p = float(p)
        except (TypeError, ValueError):
            return 0.0
        return p * 100 if p <= 1.5 else p

    @staticmethod
    def _tget(t, *names, default=None):
        for n in names:
            v = getattr(t, n, None)
            if v is not None:
                return v
        return default

    # ============================================================
    # 巡检主循环（每 2 分钟）
    # ============================================================
    def tick(self):
        if not self._enabled or self._tick_busy:
            return
        self._tick_busy = True
        try:
            self._refresh_links_from_history()
            if self._sync_delete_enabled:
                self._detect_deletions()
                self._execute_pending_deletes()
            if self._transfer_enabled:
                if self._transfer_due():
                    self._state["last_transfer_day"] = time.strftime("%Y-%m-%d")
                    self._save_state()
                    self._scan_transfers()
                self._check_pending_transfers()
                self._recheck_blocked_transfers()
            if self._sweep_enabled:
                self._check_pending_verify()
                self._sweep_dead()
                self._check_pending_verify_qb()
                self._sweep_dead_qb()
        except Exception as e:
            logger.error(f"浣熊 巡检异常: {e}", exc_info=True)
        finally:
            self._tick_busy = False

    # ============================================================
    # 映射登记：转移记录 -> (dev,inode) 双侧路径
    # ============================================================
    def _refresh_links_from_history(self):
        oper = TransferHistoryOper()
        rows = []
        for d in (time.strftime("%Y-%m-%d"), time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400))):
            try:
                rows.extend(oper.list_by_date(d) or [])
            except Exception as e:
                logger.error(f"浣熊 读取转移记录失败: {e}")
                return
        seen = set(self._state.get("hist_seen", []))
        links = self._state["links"]
        changed = False
        for row in rows:
            rid = getattr(row, "id", None)
            if rid is None or rid in seen:
                continue
            seen.add(rid)
            changed = True
            dest = getattr(row, "dest", None)
            src = getattr(row, "src", None)
            mode = str(getattr(row, "mode", "") or "")
            if not dest or not src or mode != "link":
                continue
            if not dest.startswith(self._library_root) or not src.startswith(self._download_root):
                continue
            n_before = len(links)
            self._register_tree_links(src, dest, getattr(row, "download_hash", None),
                                      getattr(row, "title", None))
            if len(links) != n_before:
                logger.info(f"浣熊 登记映射: {getattr(row, 'title', None)} -> {len(links)} 条")
        self._state["hist_seen"] = sorted(seen)[-2000:]
        if changed:
            self._save_state()

    def _register_tree_links(self, src_root: str, dest_root: str, thash, title):
        """把 src/dest 两棵树按 (dev,inode) 配对登记"""
        links = self._state["links"]
        src_index = {}
        for base, _dirs, files in os.walk(src_root):
            for fn in files:
                p = os.path.join(base, fn)
                try:
                    st = os.stat(p)
                except OSError:
                    continue
                src_index[(st.st_dev, st.st_ino)] = p
        if not src_index:
            # 单文件转移
            try:
                st = os.stat(src_root)
                if os.path.isfile(src_root):
                    src_index[(st.st_dev, st.st_ino)] = src_root
            except OSError:
                pass
        dest_files = []
        if os.path.isfile(dest_root):
            dest_files = [dest_root]
        else:
            for base, _dirs, files in os.walk(dest_root):
                for fn in files:
                    dest_files.append(os.path.join(base, fn))
        for p in dest_files:
            try:
                st = os.stat(p)
            except OSError:
                continue
            key = f"{st.st_dev}:{st.st_ino}"
            src_path = src_index.get((st.st_dev, st.st_ino))
            if not src_path:
                continue
            old = links.get(key)
            if not old or old.get("lib") != p or old.get("dl") != src_path:
                links[key] = {"lib": p, "dl": src_path, "hash": thash, "title": title}

    # ============================================================
    # 删除同步
    # ============================================================
    def _detect_deletions(self):
        links = self._state["links"]
        pendings = self._state["pending_deletes"]
        changed = False
        now = time.time()
        for key, info in list(links.items()):
            lib, dl = info.get("lib"), info.get("dl")
            lib_ok = bool(lib) and os.path.exists(lib)
            dl_ok = bool(dl) and os.path.exists(dl)
            if lib_ok and dl_ok:
                if info.pop("dry_notified", None):
                    changed = True
                continue
            if not lib_ok and not dl_ok:
                links.pop(key, None)
                changed = True
                continue
            # 一侧消失
            origin = "lib" if not lib_ok else "dl"
            twin = dl if origin == "lib" else lib
            # 登记过期防护：幸存侧文件的硬链接数 >=2，说明同一数据在别处还有一份
            # （多半是被重新整理到了新路径、登记没跟上），不是真删除，不调度
            try:
                if twin and os.path.exists(twin) and os.stat(twin).st_nlink >= 2:
                    if not info.get("nlink_blocked"):
                        info["nlink_blocked"] = True
                        changed = True
                        logger.info(f"浣熊 跳过疑似过期登记（仍有其他硬链接）: {twin}")
                    continue
            except OSError:
                pass
            if any(p.get("twin") == twin for p in pendings):
                continue
            if info.get("delete_rejected"):
                continue  # 用户已驳回过这份的删除，不再重复调度
            if any(p.get("twin") == twin for p in self._state.get("pending_approval", [])):
                continue  # 已在待删清单里等批准
            if self._dry_run and info.get("dry_notified"):
                continue
            info["dry_notified"] = True
            pendings.append({
                "twin": twin, "origin": origin, "hash": info.get("hash"),
                "title": info.get("title"), "key": key,
                "due": now + self._delete_delay_minutes * 60,
                "detected": now,
            })
            changed = True
            gone_side = "媒体库" if origin == "lib" else "下载目录"
            twin_side = "下载目录" if origin == "lib" else "媒体库"
            act = "（试运行，仅报告）" if self._dry_run else ""
            self._notify(
                "浣熊 · 检测到删除",
                f"{gone_side}里的这个文件被删了：{lib if origin == 'lib' else dl}\n"
                f"{act}{self._delete_delay_minutes} 分钟后，{twin_side}里对应的那份也会删：{twin}"
                + (f"\n所属：{info.get('title')}" if info.get("title") else ""),
            )
            if origin == "lib":
                self._pause_subscription_if_needed(info.get("title"), evidence=lib)
        if changed:
            self._save_state()

    def _pause_subscription_if_needed(self, title, evidence=None):
        if not title or title in self._state["sub_paused"]:
            return
        from app.db.oper.subscribe import SubscribeOper
        try:
            subs = SubscribeOper().list() or []
        except Exception as e:
            logger.error(f"浣熊 读取订阅失败: {e}")
            return
        for s in subs:
            name = getattr(s, "name", None)
            if not name or (name != title and title not in name and name not in title):
                continue
            if getattr(s, "state", None) != "R":
                continue
            sid = getattr(s, "id", None)
            if self._dry_run:
                self._notify("浣熊 · 在订剧集被删",
                             f"《{name}》的文件被删除。（试运行）本应自动停掉该订阅以防 MP 补下。")
            else:
                try:
                    SubscribeOper().update(sid, {"state": "S"})
                    self._audit("subscription_paused", name,
                                f"订阅 id={sid}，触发它的被删文件：{evidence}")
                    self._notify("浣熊 · 已停订阅",
                                 f"检测到《{name}》的文件被删除，已自动停掉订阅，防止 MP 把删掉的集补回来。\n"
                                 f"触发文件：{evidence}")
                except Exception as e:
                    logger.error(f"浣熊 停订阅失败 {name}: {e}")
                    return
            self._state["sub_paused"].append(title)
            self._save_state()
            return

    def _delete_gates_pass(self, p):
        """删除执行前的全部安全闸（自动执行与人工批准共用一套）：
        复活校验（源侧又出现了）/ inode 一致性（要删的正是登记那一份）/ 硬链接数兜底。
        返回 (ok, reason)。"""
        _link = self._state["links"].get(p.get("key") or "", {})
        _origin_path = _link.get("lib") if p.get("origin") == "lib" else _link.get("dl")
        if _origin_path and os.path.exists(_origin_path):
            return False, f"先删的那边文件又出现了（可能被重新整理了）：{_origin_path}"
        twin = p.get("twin")
        if not twin or not os.path.exists(twin):
            return False, "要删的这份文件已经没了"
        try:
            st = os.stat(twin)
        except OSError:
            return False, "要删的这份文件读不到状态"
        key = p.get("key") or ""
        if ":" in key:
            try:
                kdev, kino = key.split(":", 1)
                if (int(kdev), int(kino)) != (st.st_dev, st.st_ino):
                    return False, "这份文件和当初登记的不是同一份了（登记已过期）"
            except (ValueError, TypeError):
                pass
        if st.st_nlink >= 2:
            return False, "这份文件还有别的硬链接在用"
        return True, ""

    def _do_delete_twin(self, p):
        """真正执行一份孪生文件删除 + 空目录清理 + 尾巴清记录。调用前必须过 _delete_gates_pass。"""
        twin = p.get("twin")
        try:
            size = os.path.getsize(twin)
            os.remove(twin)
        except OSError as e:
            logger.error(f"浣熊 删除失败 {twin}: {e}")
            return False
        logger.info(f"浣熊 已同步删除: {twin}")
        self._audit("sync_delete_file", twin,
                    f"{'媒体库' if p.get('origin') == 'lib' else '下载目录'}那边删了，"
                    f"{'下载目录' if p.get('origin') == 'lib' else '媒体库'}这份跟着删了"
                    f"（{size} 字节），所属《{p.get('title')}》，hash={str(p.get('hash'))[:12]}")
        self._cleanup_empty_dirs(twin)
        thash = p.get("hash")
        if thash:
            alive = [v for v in self._state["links"].values()
                     if v.get("hash") == thash and os.path.exists(v.get("dl") or "")]
            if not alive:
                self._remove_torrent_records(thash, p.get("title"))
        self._state["links"].pop(p.get("key"), None)
        return True

    def _queue_approval(self, item):
        """自动删除关闭时，删除动作先进待删清单，等用户批准后才执行。"""
        item = dict(item)
        item["queued"] = time.time()
        self._state["pending_approval"].append(item)
        self._audit("delete_queued", item.get("twin") or item.get("name") or item.get("hash"),
                    item.get("evidence", ""))
        self._notify_approval_digest()

    def _notify_approval_digest(self, force=False):
        items = self._state.get("pending_approval", [])
        if not items:
            return
        now = time.time()
        if not force and now - self._state.get("last_approval_notify", 0) < 1800:
            return
        self._state["last_approval_notify"] = now
        files = [i for i in items if i.get("kind") == "file"]
        records = [i for i in items if i.get("kind") == "record"]
        total_size = sum(i.get("size", 0) for i in files)
        lines = []
        for i in items[:8]:
            if i.get("kind") == "file":
                lines.append(f"文件：{i.get('twin')}")
            else:
                lines.append(f"种子记录：{i.get('name')}")
        more = f"\n……另有 {len(items) - 8} 项" if len(items) > 8 else ""
        self._notify("浣熊 · 待删清单（等你确认）",
                     f"自动删除已关闭，以下 {len(items)} 项删除在等你批准"
                     f"（文件 {len(files)} 个共 {total_size / 1e9:.2f} GB，种子记录 {len(records)} 条）：\n"
                     + "\n".join(lines) + more
                     + "\n确认后才会执行删除。")
        self._save_state()

    def _execute_pending_deletes(self):
        pendings = self._state["pending_deletes"]
        if not pendings:
            return
        now = time.time()
        remain = []
        changed = False
        for p in pendings:
            if now < p.get("due", 0):
                remain.append(p)
                continue
            changed = True
            ok, reason = self._delete_gates_pass(p)
            if not ok:
                logger.info(f"浣熊 取消联动删除（{reason}）: {p.get('twin')}")
                self._audit("sync_delete_cancelled", p.get("twin"), reason)
                continue
            if self._dry_run:
                logger.info(f"浣熊（试运行）到期未执行删除: {p.get('twin')}")
                continue
            if not self._auto_delete:
                try:
                    size = os.path.getsize(p.get("twin"))
                except OSError:
                    size = 0
                det = time.strftime("%m-%d %H:%M", time.localtime(p.get("detected", now)))
                gone_side = "媒体库" if p.get("origin") == "lib" else "下载目录"
                twin_side = "下载目录" if p.get("origin") == "lib" else "媒体库"
                self._queue_approval({
                    "kind": "file", "twin": p.get("twin"), "origin": p.get("origin"),
                    "hash": p.get("hash"), "title": p.get("title"), "key": p.get("key"),
                    "size": size, "detected": p.get("detected"),
                    "evidence": f"{gone_side}那边已经删了（{det} 发现），这是{twin_side}里对应的那份"
                                f"（{size} 字节）；删前检查都过了"})
                continue
            self._do_delete_twin(p)
        self._state["pending_deletes"] = remain
        if changed:
            self._save_state()

    def _execute_approved_deletes(self):
        """执行待删清单（用户批准后）：仍逐项过全部安全闸，闸不过就跳过并留痕。"""
        items = self._state.get("pending_approval", [])
        if not items:
            return
        self._state["pending_approval"] = []
        n_files = n_records = n_skip = 0
        for item in items:
            if item.get("kind") == "record":
                if self._remove_one_record(item.get("hash"), item.get("name"),
                                            source=item.get("source", "TR")):
                    n_records += 1
                else:
                    n_skip += 1
                continue
            ok, reason = self._delete_gates_pass(item)
            if not ok:
                n_skip += 1
                self._audit("approved_delete_skipped", item.get("twin"), reason)
                continue
            if self._do_delete_twin(item):
                n_files += 1
            else:
                n_skip += 1
        self._save_state()
        self._audit("approval_executed", "pending_approval",
                    f"files={n_files} records={n_records} skipped={n_skip}")
        self._notify("浣熊 · 待删清单已执行",
                     f"按你的批准执行完毕：删除文件 {n_files} 个，清除种子记录 {n_records} 条，"
                     f"跳过 {n_skip} 项（临删前检查没过，或文件已经没了）。")

    def _remove_one_record(self, thash, name, source="TR"):
        """清除一条种子记录（TR 或 qB）并复核确实消失。"""
        _qn, qb_inst, _tn, tr_inst = self._downloaders()
        inst = qb_inst if source == "qB" else tr_inst
        if inst is None or not thash:
            return False
        if not self._call_remove(inst, thash):
            return False
        try:
            if thash in {self._thash(t) for t in self._torrents(inst)}:
                logger.error(f"浣熊 清记录后复核未消失: {name}")
                return False
        except Exception:
            pass
        self._audit("record_removed", name,
                    f"{source} 记录，hash={str(thash)[:12]}，清除后复核已消失")
        return True

    def _cleanup_empty_dirs(self, file_path: str):
        parent = os.path.dirname(file_path)
        for root in (self._library_root, self._download_root):
            if parent == root or not parent.startswith(root):
                continue
            cur = parent
            while cur.startswith(root) and cur != root:
                try:
                    os.rmdir(cur)
                except OSError:
                    break
                cur = os.path.dirname(cur)

    def _remove_torrent_records(self, thash: str, title):
        # 终核（有据可查）：不只信映射表——按种子清单核对磁盘，
        # 只要还有一个媒体文件在，清记录就拦下（.nfo 等附属文件不拦）。
        tfile = self._find_torrent_file(thash)
        _qn, qb_inst, _tn, tr_inst = self._downloaders()
        if tfile:
            save_path = None
            for inst in (qb_inst, tr_inst):
                if inst is None:
                    continue
                for t in self._torrents(inst):
                    if self._thash(t) == thash:
                        save_path = self._tget(t, "save_path", "download_dir")
                        break
                if save_path:
                    break
            if save_path:
                try:
                    _st, present, total, _m = self._verify_content(
                        save_path, Path(tfile).read_bytes(),
                        strict_size=False, media_only=True)
                    if present > 0:
                        self._audit("records_kept", title,
                                    f"清之前核对：这个种子的文件还有 {present}/{total} 个在磁盘上，"
                                    f"没清记录 hash={thash[:12]}")
                        logger.warning(f"浣熊 终核拦下清记录 {title}：仍有 {present}/{total} 个媒体文件")
                        return
                except Exception as e:
                    logger.error(f"浣熊 清记录终核异常 {title}: {e}")
        removed = []
        for label, inst in (("TR", tr_inst), ("qB", qb_inst)):
            if inst is None:
                continue
            try:
                hashes = [self._thash(t) for t in self._torrents(inst)]
                if thash in hashes and self._call_remove(inst, thash):
                    removed.append(label)
            except Exception as e:
                logger.error(f"浣熊 删除{label}种子记录失败 {thash}: {e}")
        if removed:
            # 复核：记录确实从列表消失才算清掉
            for label, inst in (("TR", tr_inst), ("qB", qb_inst)):
                if inst is None or label not in removed:
                    continue
                try:
                    if thash in [self._thash(t) for t in self._torrents(inst)]:
                        logger.error(f"浣熊 清记录后复核未消失 {label}: {thash}")
                except Exception:
                    pass
            self._audit("records_removed", title,
                        f"已清 {'/'.join(removed)} 记录 hash={thash[:12]}；清之前核对过：这个种子的文件一个都不在磁盘上了")
            self._notify("浣熊 · 种子记录已清",
                         f"《{title or thash}》文件已删净，已连带删除 {'/'.join(removed)} 中的种子记录。")

    # ============================================================
    # 转种 qB -> TR
    # ============================================================
    def _organized_info(self, thash: str):
        try:
            rows = TransferHistoryOper().list_by_hash(thash) or []
        except Exception as e:
            logger.warning(f"浣熊 查整理记录异常 {thash[:8]}: {type(e).__name__} {e}")
            return None
        for row in rows:
            if str(getattr(row, "mode", "") or "") == "link":
                return {"src": getattr(row, "src", None), "title": getattr(row, "title", None)}
        if rows:
            logger.info(f"浣熊 整理记录无 link 模式 {thash[:8]}: "
                        f"{[(getattr(r, 'mode', None), getattr(r, 'status', None)) for r in rows[:3]]}")
        return None

    def _find_torrent_file(self, thash: str) -> Optional[str]:
        try:
            for fn in os.listdir(self._bt_backup):
                if fn.lower() == f"{thash}.torrent".lower():
                    return os.path.join(self._bt_backup, fn)
        except OSError:
            pass
        return None

    # ---------- 转种前数据完整性校验（v0.1.18） ----------
    # 教训（2026-11-24《仙逆》）：qB 的进度是缓存值、从不复检，文件被移走仍报 100%；
    # 转种前必须按种子清单逐个核对磁盘文件（存在且大小一致），不过关绝不往 TR 里加。
    @staticmethod
    def _bdecode(data: bytes, i: int = 0):
        c = data[i:i + 1]
        if c == b"i":
            j = data.index(b"e", i)
            return int(data[i + 1:j]), j + 1
        if c == b"l":
            out, i = [], i + 1
            while data[i:i + 1] != b"e":
                v, i = Raccoon._bdecode(data, i)
                out.append(v)
            return out, i + 1
        if c == b"d":
            out, i = {}, i + 1
            while data[i:i + 1] != b"e":
                k, i = Raccoon._bdecode(data, i)
                v, i = Raccoon._bdecode(data, i)
                out[k] = v
            return out, i + 1
        j = data.index(b":", i)
        n = int(data[i:j])
        return data[j + 1:j + 1 + n], j + 1 + n

    @staticmethod
    def _torrent_files(content: bytes):
        """解析 .torrent -> [(相对路径(含种子根名), 长度)]，失败返回 None"""
        try:
            meta, _ = Raccoon._bdecode(content)
            info = meta[b"info"]
            name = info[b"name"].decode("utf-8", "surrogateescape")
            if b"files" in info:
                out = []
                for f in info[b"files"]:
                    parts = [p.decode("utf-8", "surrogateescape") for p in f[b"path"]]
                    out.append((name + "/" + "/".join(parts), int(f[b"length"])))
                return out
            return [(name, int(info[b"length"]))]
        except Exception:
            return None

    def _verify_content(self, save_path, content, org_src=None, strict_size=True, media_only=False):
        """逐个核对种子文件在磁盘上的实况（strict_size=True 要求大小一致，
        False 只看文件是否存在——清种子记录前的终核用存在性口径）。
        返回 (status, present, total, missing[:5])，status = ok / incomplete / unknown。
        org_src（整理记录里的源文件路径）可反推真实保存目录，应对 qB 改名/无子目录布局。"""
        files = self._torrent_files(content)
        if files is None:
            return "unknown", 0, 0, []
        if media_only:
            files = [f for f in files if _is_media(f[0])]
            if not files:
                return "ok", 0, 0, []
        candidates = [save_path]
        if org_src:
            for rel, _ln in files:
                if org_src.endswith("/" + rel):
                    candidates.append(org_src[: -len(rel)].rstrip("/"))
                    break
        best = None
        for base in candidates:
            if not base:
                continue
            present, missing = 0, []
            for rel, ln in files:
                p = os.path.join(base, rel)
                try:
                    if os.path.isfile(p) and (not strict_size or os.path.getsize(p) == ln):
                        present += 1
                        continue
                except OSError:
                    pass
                missing.append(rel)
            if best is None or present > best[0]:
                best = (present, missing)
            if present == len(files):
                break
        present, missing = best if best else (0, [r for r, _ in files])
        return ("ok" if present == len(files) else "incomplete"), present, len(files), missing[:5]

    def _block_transfer(self, thash, title, present, total, missing, reason):
        self._state["transfer_blocked"][thash] = {
            "ts": time.time(), "last_check": time.time(), "title": title,
            "present": present, "total": total, "missing": missing, "reason": reason}
        self._audit("transfer_blocked", title,
                    f"种子文件实况 {present}/{total} 完整，reason={reason}，hash={thash[:12]}")

    def _scan_transfers(self):
        _qn, qb_inst, _tn, tr_inst = self._downloaders()
        if qb_inst is None or tr_inst is None:
            return
        tr_hashes = {h for h in (self._thash(t) for t in self._torrents(tr_inst)) if h}
        pendings = self._state["pending_transfers"]
        done = self._state["done_transfers"]
        blocked = self._state["transfer_blocked"]
        changed = False
        qb_list = self._torrents(qb_inst)
        skip = {"in_tr": 0, "pending": 0, "low_prog": 0, "no_file": 0, "blocked": 0}
        started = 0
        blocked_now = 0
        started_titles = []
        for t in qb_list:
            thash = self._tget(t, "hash")
            # 是否已转只认 TR 实况与在途清单这两份实据；done 只是历史账，
            # 尤其试运行期记的 done(dry=True) 从没真转过，绝不能当跳过依据。
            if not thash or thash in tr_hashes:
                skip["in_tr"] += 1
                continue
            if thash in pendings:
                skip["pending"] += 1
                continue
            if thash in blocked:
                skip["blocked"] += 1
                continue
            # 转种是下载器之间的交接，只看两件事：下载完成、文件完整。
            # 跟是什么资源、整没整理进整理目录无关（整理是另一条线）。
            org = self._organized_info(thash) or {}
            progress = self._progress100(t)
            via_disk = False
            if progress < 99.9:
                state = str(self._tget(t, "state") or "").lower()
                if "paus" in state or "stop" in state:
                    # 暂停中的种子 qB 进度可能过期（暂停态校验会冻结），
                    # 是否下完改由后面的磁盘文件核对定夺。
                    via_disk = True
                else:
                    skip["low_prog"] += 1
                    continue
            save_path = self._tget(t, "save_path", "download_dir")
            if not save_path:
                continue
            tfile = self._find_torrent_file(thash)
            if not tfile:
                skip["no_file"] += 1
                logger.warning(f"浣熊 转种跳过（找不到 .torrent）: {thash}")
                continue
            title = org.get("title") or self._tget(t, "name", "title")
            if self._dry_run:
                self._notify("浣熊 · 转种（试运行）",
                             f"本应把《{title}》从 qB 转种到 TR（路径 {save_path}）。")
                done[thash] = {"dry": True, "ts": time.time()}
                changed = True
                started += 1
                continue
            try:
                content = Path(tfile).read_bytes()
                status, present, total, missing = self._verify_content(
                    save_path, content, org.get("src"))
                if status == "incomplete":
                    if via_disk:
                        # 暂停+文件也不全 = 还没下完的种子，安静跳过，不当受阻处理
                        skip["low_prog"] += 1
                        continue
                    self._block_transfer(thash, title, present, total, missing, "precheck")
                    changed = True
                    blocked_now += 1
                    logger.warning(f"浣熊 转种前校验不过 {title}: {present}/{total}")
                    self._notify("浣熊 · 转种跳过（数据不完整）",
                                 f"《{title}》qB 显示 100%，但磁盘文件只有 {present}/{total} 个完整，"
                                 f"转到 TR 必然校验失败，已跳过。缺失示例："
                                 f"{(missing[0].split('/')[-1] if missing else '')}")
                    continue
                if status == "unknown":
                    logger.warning(f"浣熊 转种前校验无法解析种子 {title}，按旧流程继续")
                result = self._tr_add(tr_inst, content, save_path)
                logger.info(f"浣熊 转种已加入 TR: {title} -> {result}")
                self._audit("transfer_started", title,
                            f"转种前文件核对 {present}/{total} 完整，路径 {save_path}，hash={thash[:12]}")
                pendings[thash] = {"ts": time.time(), "title": title, "save_path": save_path}
                changed = True
                started += 1
                started_titles.append(title)
            except Exception as e:
                logger.error(f"浣熊 转种失败 {title}: {e}")
        if started_titles:
            if len(started_titles) <= 3:
                for title in started_titles:
                    self._notify("浣熊 · 转种开始",
                                 f"《{title}》已加入 TR 校验做种，校验通过后将自动移除 qB 任务。")
            else:
                self._notify("浣熊 · 转种开始",
                             f"本轮 {len(started_titles)} 个种子已加入 TR 校验做种"
                             f"（如 {self._fmt_titles(started_titles)} 等），校验通过后将自动移除 qB 任务。")
        logger.info(f"浣熊 转种扫描：qB {len(qb_list)} 个，TR {len(tr_hashes)} 个，"
                    f"跳过：{self._fmt_skip(skip)}")
        if changed:
            self._save_state()
        return {"started": started, "blocked_now": blocked_now,
                "skip": skip, "qb": len(qb_list)}

    def _check_pending_transfers(self):
        pendings = self._state["pending_transfers"]
        if not pendings:
            return
        _qn, qb_inst, _tn, tr_inst = self._downloaders()
        if tr_inst is None:
            return
        tr_map = {self._thash(t): t for t in self._torrents(tr_inst)}
        changed = False
        done_titles = []
        for thash, info in list(pendings.items()):
            t = tr_map.get(thash)
            if t is None:
                if time.time() - info.get("ts", 0) > 3600:
                    logger.warning(f"浣熊 转种超时未在 TR 出现: {thash}")
                    pendings.pop(thash, None)
                    changed = True
                continue
            progress = self._progress100(t)
            if progress >= 99.9:
                if qb_inst is not None and not self._call_remove(qb_inst, thash):
                    logger.error(f"浣熊 移除 qB 任务失败 {thash}")
                self._state["done_transfers"][thash] = {"ts": time.time(), "title": info.get("title")}
                pendings.pop(thash, None)
                changed = True
                self._audit("transfer_done", info.get("title"),
                            f"TR 校验进度 {progress:.1f}%（实况），已移除 qB 任务，hash={thash[:12]}")
                done_titles.append(info.get("title"))
                continue
            age = time.time() - info.get("ts", 0)
            if info.get("last_progress") != progress:
                info["last_progress"] = progress
                info["last_progress_ts"] = time.time()
                changed = True
            if age > 1800 and not info.get("warned"):
                info["warned"] = True
                changed = True
                self._notify("浣熊 · 转种校验中",
                             f"《{info.get('title')}》在 TR 校验进度 {progress}%，超过 30 分钟仍未完成，已保留 qB 任务。")
            if age > 7200 and not self._is_checking(t):
                # 超过 2 小时仍未过校验：先按磁盘实况定性。数据本身不全则停止等待——
                # 只改插件自己的记账，TR 记录与 qB 任务一律不动。
                status, present, total, missing = "unknown", 0, 0, []
                tfile = self._find_torrent_file(thash)
                if tfile and info.get("save_path"):
                    try:
                        status, present, total, missing = self._verify_content(
                            info["save_path"], Path(tfile).read_bytes())
                    except Exception:
                        pass
                stagnant = time.time() - info.get("last_progress_ts", info.get("ts", 0)) > 7200
                if status == "incomplete" or stagnant:
                    title = info.get("title")
                    self._block_transfer(thash, title, present, total, missing,
                                         "tr_check_failed" if status == "incomplete" else "stalled")
                    pendings.pop(thash, None)
                    changed = True
                    detail = (f"磁盘文件只有 {present}/{total} 个完整，数据本身不全"
                              if status == "incomplete" else "TR 校验进度长时间停滞")
                    self._notify("浣熊 · 转种受阻",
                                 f"《{title}》在 TR 校验停在 {progress:.1f}%，{detail}，"
                                 f"转种无法完成，已停止等待。TR 记录与 qB 任务均未改动。")
        if done_titles:
            if len(done_titles) == 1:
                self._notify("浣熊 · 转种完成",
                             f"《{done_titles[0]}》已在 TR 做种（校验 100%），qB 任务已移除。")
            else:
                self._notify("浣熊 · 转种完成",
                             f"本轮 {len(done_titles)} 个种子已在 TR 做种（校验 100%），"
                             f"qB 任务已移除（如 {self._fmt_titles(done_titles)} 等）。")
        if changed:
            self._save_state()

    def _recheck_blocked_transfers(self):
        """被数据不完整拦下的转种：每天复查一次，补齐了自动放行；qB 任务没了就销账。"""
        blocked = self._state["transfer_blocked"]
        if not blocked:
            return
        _qn, qb_inst, _tn, _ti = self._downloaders()
        if qb_inst is None:
            return
        qb_map = {self._thash(t): t for t in self._torrents(qb_inst)}
        changed = False
        for thash, info in list(blocked.items()):
            if time.time() - info.get("last_check", 0) < 86400:
                continue
            info["last_check"] = time.time()
            changed = True
            t = qb_map.get(thash)
            if t is None:
                blocked.pop(thash, None)
                continue
            save_path = self._tget(t, "save_path", "download_dir")
            tfile = self._find_torrent_file(thash)
            if not save_path or not tfile:
                continue
            try:
                status, present, total, _m = self._verify_content(
                    save_path, Path(tfile).read_bytes())
            except Exception:
                continue
            info["present"], info["total"] = present, total
            if status == "ok":
                blocked.pop(thash, None)
                self._notify("浣熊 · 转种恢复",
                             f"《{info.get('title')}》数据已补齐（{present}/{total}），将重新转种到 TR。")
        if changed:
            self._save_state()

    # ============================================================
    # 死种清道夫
    # ============================================================
    @staticmethod
    def _call_remove(inst, thash) -> bool:
        """删除种子记录（不删文件），兼容 module 与 server 两种对象形态"""
        for fn in (lambda: inst.remove_torrents(hashs=thash, delete_file=False),
                   lambda: inst.delete_torrents(delete_file=False, ids=[thash]),
                   lambda: inst.delete_torrents(False, [thash])):
            try:
                fn()
                return True
            except Exception:
                continue
        return False

    @staticmethod
    def _call_start(inst, thash) -> bool:
        """启动种子，兼容两种签名"""
        for fn in (lambda: inst.start_torrents(ids=[thash]),
                   lambda: inst.start_torrents(hashs=thash),
                   lambda: inst.start_torrents([thash])):
            try:
                fn()
                return True
            except Exception:
                continue
        return False

    @staticmethod
    def _tr_add(inst, content: bytes, save_path: str):
        """向 TR 添加种子，兼容 module.download 与 server.add_torrent"""
        try:
            return inst.download(content=content, download_dir=Path(save_path), cookie="")
        except Exception:
            pass
        return inst.add_torrent(content=content, download_dir=save_path)

    def _tr_server(self, tr_name, tr_inst):
        """拿到带 recheck_torrents 的 TR server 对象，失败返回 None。
        实测 DownloaderHelper 给的 TR instance 本身就是 Transmission server 对象。"""
        if tr_inst is None:
            return None
        if hasattr(tr_inst, "recheck_torrents"):
            return tr_inst
        try:
            srv = tr_inst.get_instance(tr_name)
            return srv if srv is not None and hasattr(srv, "recheck_torrents") else None
        except Exception as e:
            logger.error(f"浣熊 获取 TR server 失败: {e}")
            return None

    def _recheck(self, tr_name, tr_inst, hashes) -> bool:
        """对 TR 种子触发文件校验（torrent-verify）。校验是处理死种的前置条件：
        触发不了校验就绝不处理（feng 2026-11-23 规则）。"""
        if not hashes:
            return True
        server = self._tr_server(tr_name, tr_inst)
        if server is None:
            return False
        try:
            return bool(server.recheck_torrents(ids=list(hashes)))
        except Exception as e:
            logger.error(f"浣熊 触发校验失败: {e}")
            return False

    @staticmethod
    def _is_checking(t) -> bool:
        if Raccoon._tget(t, "check_pending") or Raccoon._tget(t, "checking"):
            return True
        st = Raccoon._tget(t, "status", "state", default="")
        return st in (1, 2) or (isinstance(st, str) and "check" in st.lower())

    def _check_pending_verify(self):
        """跟进已触发校验的疑似死种：校验完好 -> 排除（并在实况模式下恢复做种）；
        校验确认缺失 -> 才进入清除（试运行只报告）。"""
        pend = self._state.get("pending_verify", {})
        if not pend:
            return
        _qn, _qi, tr_name, tr_inst = self._downloaders()
        if tr_inst is None:
            return
        tr_map = {self._thash(t): t for t in self._torrents(tr_inst)}
        good, dead, changed = [], [], False
        for thash, info in list(pend.items()):
            t = tr_map.get(thash)
            if t is None:
                pend.pop(thash, None)
                changed = True
                continue
            if self._is_checking(t):
                if time.time() - info.get("ts", 0) > 24 * 3600:
                    logger.warning(f"浣熊 校验超时，放弃本轮: {info.get('name')}")
                    pend.pop(thash, None)
                    changed = True
                continue
            pend.pop(thash, None)
            changed = True
            if self._progress100(t) >= 99.9:
                self._state["verified_ok"][thash] = time.time()
                good.append((thash, info.get("name") or self._tget(t, "name", "title", default="?")))
            else:
                dead.append((thash, info.get("name") or self._tget(t, "name", "title", default="?")))
        if good:
            names = [n for _h, n in good]
            self._audit("sweep_verified_ok", f"{len(good)} 条",
                        "TR 文件校验后完整度 100%，判定误报不清除：" + "、".join(str(n)[:30] for n in names[:5]))
            extra = ""
            if not self._dry_run:
                started = 0
                for h, _n in good:
                    if self._call_start(tr_inst, h):
                        started += 1
                    else:
                        logger.error(f"浣熊 校验后恢复做种失败 {_n}")
                extra = f"已恢复其中 {started} 条做种。" if started else ""
            self._notify("浣熊 · 死种校验结果",
                         f"{len(good)} 条经文件校验数据完好，不是死种，不会清除。{extra}\n"
                         + "\n".join(str(n)[:40] for n in names[:10]))
        if dead:
            names = [n for _h, n in dead]
            if self._dry_run:
                self._notify("浣熊 · 死种清道夫（试运行）",
                             f"{len(dead)} 条经文件校验确认数据缺失，本应清除记录：\n"
                             + "\n".join(str(n)[:40] for n in names[:10]))
            elif not self._auto_delete:
                for h, n in dead:
                    self._queue_approval({
                        "kind": "record", "hash": h, "name": n,
                        "evidence": "TR 说这个种子的文件没了，重新校验了一遍，确认文件确实不在了"})
                self._save_state()
            else:
                ok = 0
                for h, _n in dead:
                    if self._remove_one_record(h, _n):
                        ok += 1
                    else:
                        logger.error(f"浣熊 清死种失败 {_n}")
                self._notify("浣熊 · 死种清道夫",
                             f"经文件校验确认后，已清除 {ok}/{len(dead)} 条死种记录（逐条复核已消失）。")
        if changed:
            self._save_state()

    def _sweep_dead(self):
        today = time.strftime("%Y-%m-%d")
        if self._state.get("last_sweep") == today:
            return
        _qn, _qi, tr_name, tr_inst = self._downloaders()
        if tr_inst is None:
            return
        now = time.time()
        vok = self._state.get("verified_ok", {})
        pend = self._state.get("pending_verify", {})
        cand = []
        for t in self._torrents(tr_inst):
            err = str(self._tget(t, "error_string", "errorString", "errmsg", default="") or "")
            code = self._tget(t, "error", default=0)
            if not ("No data found" in err or "数据不存在" in err
                    or (code == 3 and "data" in err.lower())):
                continue
            thash = self._thash(t)
            if not thash or thash in pend:
                continue
            if now - vok.get(thash, 0) < 7 * 24 * 3600:
                continue  # 7 天内已校验完好，不重复校验
            if now - self._state.get("sweep_rejected", {}).get(thash, 0) < 30 * 24 * 3600:
                continue  # 用户驳回过这条的清除，30 天内不再提
            cand.append(t)
        # 先把今日标记占上（无论后续成败，每天只发起一轮，避免反复触发校验）
        self._state["last_sweep"] = today
        self._save_state()
        if not cand:
            logger.info("浣熊 死种清道夫: 无新的疑似死种")
            return
        hashes = [self._thash(t) for t in cand]
        names = [str(self._tget(t, "name", "title", default="?"))[:40] for t in cand]
        if not self._recheck(tr_name, tr_inst, hashes):
            self._notify("浣熊 · 死种清道夫",
                         f"发现 {len(cand)} 条疑似死种，但文件校验触发失败，本轮不做任何处理。")
            return
        for h, n in zip(hashes, names):
            pend[h] = {"ts": now, "name": n}
        self._save_state()
        self._notify("浣熊 · 死种清道夫",
                     f"发现 {len(cand)} 条疑似死种，已触发文件校验；校验确认数据缺失后才会清除，完好则排除。\n"
                     + "\n".join(names[:10]))

    # ============================================================
    # 死种清道夫（qB 侧，与 TR 同一套校验前置纪律）
    # ============================================================
    def _qb_server(self, qb_name, qb_inst):
        """拿到带 recheck_torrents 的 qB server 对象，失败返回 None。"""
        if qb_inst is None:
            return None
        if hasattr(qb_inst, "recheck_torrents"):
            return qb_inst
        try:
            srv = qb_inst.get_instance(qb_name)
            return srv if srv is not None and hasattr(srv, "recheck_torrents") else None
        except Exception as e:
            logger.error(f"浣熊 获取 qB server 失败: {e}")
            return None

    def _recheck_qb(self, qb_name, qb_inst, hashes) -> bool:
        """对 qB 种子触发文件校验。触发不了校验就绝不处理（与 TR 同规则）。"""
        if not hashes:
            return True
        server = self._qb_server(qb_name, qb_inst)
        if server is None:
            return False
        try:
            server.recheck_torrents(ids=list(hashes))
            return True
        except Exception as e:
            logger.error(f"浣熊 qB 触发校验失败: {e}")
            return False

    def _sweep_dead_qb(self):
        """qB 疑似死种：qB 自认为已下完（进度 >=99.9%），但内容路径在磁盘上不存在，
        或任务状态是 error/missing。先触发校验，确认后才清（判定在 _check_pending_verify_qb）。"""
        today = time.strftime("%Y-%m-%d")
        if self._state.get("last_sweep_qb") == today:
            return
        qb_name, qb_inst, _tn, _ti = self._downloaders()
        if qb_inst is None:
            return
        now = time.time()
        vok = self._state.get("verified_ok", {})
        pend = self._state.get("pending_verify_qb", {})
        cand = []
        for t in self._torrents(qb_inst):
            if self._progress100(t) < 99.9:
                continue  # 还在下载的任务不是死种
            thash = self._thash(t)
            if not thash or thash in pend:
                continue
            if now - vok.get(thash, 0) < 7 * 24 * 3600:
                continue
            if now - self._state.get("sweep_rejected", {}).get(thash, 0) < 30 * 24 * 3600:
                continue
            cp = self._tget(t, "content_path", "save_path", default=None)
            missing = bool(cp) and not os.path.exists(str(cp))
            state = str(self._tget(t, "state", default="") or "").lower()
            if not missing and "error" not in state and "missing" not in state:
                continue
            cand.append((thash, str(self._tget(t, "title", "name", default="?"))[:40]))
        self._state["last_sweep_qb"] = today
        self._save_state()
        if not cand:
            logger.info("浣熊 死种清道夫(qB): 无新的疑似死种")
            return
        hashes = [h for h, _n in cand]
        if not self._recheck_qb(qb_name, qb_inst, hashes):
            self._notify("浣熊 · 死种清道夫（qB）",
                         f"发现 {len(cand)} 条疑似死种，但文件校验触发失败，本轮不做任何处理。")
            return
        for h, n in cand:
            pend[h] = {"ts": now, "name": n}
        self._save_state()
        self._notify("浣熊 · 死种清道夫（qB）",
                     f"发现 {len(cand)} 条疑似死种，已触发文件校验；校验确认数据缺失后才会清除，完好则排除。\n"
                     + "\n".join(n for _h, n in cand[:10]))

    def _check_pending_verify_qb(self):
        """跟进 qB 已触发校验的疑似死种：校验后文件在且进度满 -> 误报排除；
        校验结束（至少等 10 分钟）文件仍不在或进度不满 -> 确认死种。"""
        pend = self._state.get("pending_verify_qb", {})
        if not pend:
            return
        _qn, qb_inst, _tn, _ti = self._downloaders()
        if qb_inst is None:
            return
        qb_map = {self._thash(t): t for t in self._torrents(qb_inst)}
        good, dead, changed = [], [], False
        for thash, info in list(pend.items()):
            t = qb_map.get(thash)
            if t is None:
                pend.pop(thash, None)
                changed = True
                continue
            if self._is_checking(t):
                if time.time() - info.get("ts", 0) > 24 * 3600:
                    logger.warning(f"浣熊 qB 校验超时，放弃本轮: {info.get('name')}")
                    pend.pop(thash, None)
                    changed = True
                continue
            if time.time() - info.get("ts", 0) < 600:
                continue  # 校验刚触发，给它至少 10 分钟再判定
            pend.pop(thash, None)
            changed = True
            cp = self._tget(t, "content_path", "save_path", default=None)
            if self._progress100(t) >= 99.9 and cp and os.path.exists(str(cp)):
                self._state["verified_ok"][thash] = time.time()
                good.append((thash, info.get("name") or self._tget(t, "title", "name", default="?")))
            else:
                dead.append((thash, info.get("name") or self._tget(t, "title", "name", default="?")))
        if good:
            names = [n for _h, n in good]
            self._audit("sweep_verified_ok", f"qB {len(good)} 条",
                        "qB 文件校验后完好，判定误报不清除：" + "、".join(str(n)[:30] for n in names[:5]))
            self._notify("浣熊 · 死种校验结果（qB）",
                         f"{len(good)} 条经文件校验数据完好，不是死种，不会清除。\n"
                         + "\n".join(str(n)[:40] for n in names[:10]))
        if dead:
            names = [n for _h, n in dead]
            if self._dry_run:
                self._notify("浣熊 · 死种清道夫（qB，试运行）",
                             f"{len(dead)} 条经文件校验确认数据缺失，本应清除 qB 记录：\n"
                             + "\n".join(str(n)[:40] for n in names[:10]))
            elif not self._auto_delete:
                for h, n in dead:
                    self._queue_approval({
                        "kind": "record", "source": "qB", "hash": h, "name": n,
                        "evidence": "qB 说这个种子的文件没了，重新校验了一遍，确认文件确实不在了"})
                self._save_state()
            else:
                ok = 0
                for h, _n in dead:
                    if self._remove_one_record(h, _n, source="qB"):
                        ok += 1
                    else:
                        logger.error(f"浣熊 清 qB 死种失败 {_n}")
                self._notify("浣熊 · 死种清道夫（qB）",
                             f"经文件校验确认后，已清除 {ok}/{len(dead)} 条 qB 死种记录（逐条复核已消失）。")
        if changed:
            self._save_state()

    # ============================================================
    # 存量处理
    # ============================================================
    def api_stock(self, **kwargs):
        threading.Thread(target=self._stock_job_entry, daemon=True).start()
        return {"success": True, "message": "存量处理已启动"}

    def api_status(self, **kwargs):
        return {"success": True, "data": {
            "links": len(self._state.get("links", {})),
            "pending_deletes": len(self._state.get("pending_deletes", [])),
            "pending_approval": len(self._state.get("pending_approval", [])),
            "pending_transfers": len(self._state.get("pending_transfers", {})),
            "done_transfers": len(self._state.get("done_transfers", {})),
            "transfer_blocked": len(self._state.get("transfer_blocked", {})),
            "stock_done": len(self._state.get("stock_done", [])),
            "dry_run": self._dry_run,
            "auto_delete": self._auto_delete,
            "audit_recent": self._state.get("audit", [])[-10:],
        }}

    def api_pending(self, **kwargs):
        return {"success": True, "data": self._state.get("pending_approval", [])}

    def api_approve_deletes(self, **kwargs):
        threading.Thread(target=self._execute_approved_deletes, daemon=True).start()
        return {"success": True, "message": "待删清单已批准，正在执行"}

    def _do_reject_deletes(self):
        items = self._state.get("pending_approval", [])
        self._state["pending_approval"] = []
        now = time.time()
        for item in items:
            if item.get("kind") == "file" and item.get("key"):
                link = self._state["links"].get(item["key"])
                if link is not None:
                    link["delete_rejected"] = True
            if item.get("kind") == "record" and item.get("hash"):
                self._state.setdefault("sweep_rejected", {})[item["hash"]] = now
        self._audit("approval_rejected", "pending_approval", f"用户驳回 {len(items)} 项待删，不执行")
        self._save_state()
        return len(items)

    def api_reject_deletes(self, **kwargs):
        n = self._do_reject_deletes()
        return {"success": True, "message": f"已驳回 {n} 项，不执行删除"}

    def _approval_job_entry(self, approve: bool):
        """待删清单批准/驳回的一次性触发（配置开关，和 stock_run_once 同一套路，
        不依赖插件 HTTP 接口）。执行后自动复位开关。"""
        try:
            if approve:
                self._execute_approved_deletes()
            else:
                self._do_reject_deletes()
        finally:
            key = "approve_deletes_once" if approve else "reject_deletes_once"
            if approve:
                self._approve_once = False
            else:
                self._reject_once = False
            try:
                cfg = self.get_config() or {}
                cfg[key] = False
                self.update_config(cfg)
            except Exception as e:
                logger.error(f"浣熊 复位 {key} 失败: {e}")

    def api_audit(self, **kwargs):
        return {"success": True, "data": self._state.get("audit", [])[-100:]}

    @staticmethod
    def _fmt_skip(skip) -> str:
        """跳过计数的人话版：只列非零项，全零显示「无」。"""
        labels = {"in_tr": "已在 TR", "pending": "正在转种", "low_prog": "还没下完",
                  "no_file": "找不到种子文件", "blocked": "文件不全拦下"}
        parts = [f"{labels.get(k, k)} {v} 个" for k, v in skip.items() if v]
        return "、".join(parts) if parts else "无"

    @staticmethod
    def _fmt_titles(titles, limit=5) -> str:
        """标题列表的人话版：同名合并计数（《成年人》×2），保序，最多列 limit 个。"""
        counts, order = {}, []
        for t in titles:
            if t not in counts:
                counts[t] = 0
                order.append(t)
            counts[t] += 1
        parts = [f"《{t}》" + (f"×{counts[t]}" if counts[t] > 1 else "")
                 for t in order[:limit]]
        return "、".join(parts)

    def _transfer_due(self) -> bool:
        """转种扫描的周期闸门：now=每轮都扫；daily=每天第一轮扫一次；
        weekly=每周一扫一次。已开始的转种监控不受闸门影响（在 tick 里独立跑）。"""
        if self._transfer_mode == "now":
            return True
        today = time.strftime("%Y-%m-%d")
        if self._state.get("last_transfer_day") == today:
            return False
        if self._transfer_mode == "daily":
            return True
        return time.localtime().tm_wday == 0  # weekly: 周一

    def _transfer_job_entry(self):
        """「立即转种一次」一次性触发：无视周期闸门扫一批，并记下今日已扫。
        执行结果必须有回音：转了几个有逐个通知，一无所获也要发一条结果通知，
        否则用户无从分辨「跑了但没活」和「根本没跑」。"""
        try:
            logger.info("浣熊 立即转种一次：开始执行")
            self._state["last_transfer_day"] = time.strftime("%Y-%m-%d")
            self._save_state()
            res = self._scan_transfers()
            if res is None:
                self._notify("浣熊 · 立即转种结果",
                             "手动转种没能执行：暂时连不上 qB 或 TR，请稍后再试。")
            elif not res["started"]:
                detail = self._fmt_skip(res["skip"])
                if res.get("blocked_now"):
                    detail += f"；这次新拦下 {res['blocked_now']} 个（文件不全）"
                self._notify("浣熊 · 立即转种结果",
                             f"手动转种已执行：qB 里共 {res['qb']} 个种子，"
                             f"这次没有符合条件的新种子。跳过分布：{detail}。")
            else:
                logger.info(f"浣熊 立即转种一次：已启动 {res['started']} 个转种")
        finally:
            if self._transfer_run_once:
                self._transfer_run_once = False
                try:
                    cfg = self.get_config() or {}
                    cfg["transfer_run_once"] = False
                    self.update_config(cfg)
                except Exception as e:
                    logger.error(f"浣熊 复位 transfer_run_once 失败: {e}")

    def _stock_job_entry(self):
        try:
            self.process_stock()
        finally:
            if self._stock_run_once:
                self._stock_run_once = False
                try:
                    cfg = self.get_config() or {}
                    cfg["stock_run_once"] = False
                    self.update_config(cfg)
                except Exception as e:
                    logger.error(f"浣熊 复位 stock_run_once 失败: {e}")

    def _stock_uncovered_files(self, entry: Path):
        """文件级覆盖审计：条目内每个媒体文件按 inode 查链接映射，
        返回未被覆盖的相对路径——杜绝「记录说整理过、其实有文件漏网」的盲区。"""
        links = self._state.get("links", {})
        uncovered = []
        try:
            files = [entry] if entry.is_file() else [p for p in entry.rglob("*") if p.is_file()]
        except OSError:
            return uncovered
        for p in files:
            if not _is_media(str(p)):
                continue
            try:
                st = p.stat()
            except OSError:
                continue
            if f"{st.st_dev}:{st.st_ino}" not in links:
                uncovered.append(str(p.relative_to(entry)) if entry.is_dir() else p.name)
        return uncovered

    def _record_stock_uncovered(self, entry: Path):
        uncovered = self._stock_uncovered_files(entry)
        if uncovered:
            self._state["stock_uncovered"][entry.name] = uncovered[:50]
            self._audit("stock_uncovered", entry.name,
                        f"{len(uncovered)} 个媒体文件未被链接映射覆盖：" + "、".join(uncovered[:3]))
        else:
            self._state["stock_uncovered"].pop(entry.name, None)
        return uncovered

    def process_stock(self):
        from app.chain.transfer import TransferChain
        from app.schemas.file import FileItem
        root = Path(self._download_root)
        if not root.exists():
            logger.error(f"浣熊 存量: 下载目录不存在 {root}")
            return
        oper = TransferHistoryOper()
        done = set(self._state["stock_done"])
        failed = self._state["stock_failed"]
        candidates = []
        media_map = {}
        scan = sorted(root.iterdir(), key=lambda p: p.name)
        for entry in scan:
            if entry.name in done:
                continue
            if failed.get(entry.name, 0) >= 2:
                continue
            # 覆盖检查（递归）：已有成功记录且目标文件还在 -> 已整理，登记映射后跳过
            try:
                rows = oper.list_success_by_src(str(entry), recursive=True) or []
            except Exception:
                rows = []
            alive = [r for r in rows if getattr(r, "dest", None) and os.path.exists(r.dest)]
            if alive:
                for r in alive:
                    if str(getattr(r, "mode", "") or "") == "link":
                        self._register_tree_links(getattr(r, "src"), getattr(r, "dest"),
                                                  getattr(r, "download_hash", None),
                                                  getattr(r, "title", None))
                done.add(entry.name)
                self._record_stock_uncovered(entry)
                logger.info(f"浣熊 存量跳过（已整理）: {entry.name}")
                continue
            if rows:
                # 成功记录的目标已不存在（旧整理被回滚）-> 删除陈旧记录，允许重新整理
                for r in rows:
                    try:
                        oper.delete(getattr(r, "id"))
                    except Exception as e:
                        logger.error(f"浣熊 删除陈旧整理记录失败 {entry.name}: {e}")
                logger.info(f"浣熊 已清除 {entry.name} 的 {len(rows)} 条陈旧整理记录")
            candidates.append(entry)
        self._state["stock_done"] = sorted(done)
        self._save_state()
        batch = candidates[: self._stock_batch]
        if not batch:
            logger.info("浣熊 存量: 本批无候选")
            return
        self._notify("浣熊 · 存量处理开始",
                     f"本批 {len(batch)} 个：\n" + "\n".join(e.name for e in batch))
        ok_names, fail_names = [], []
        for entry in batch:
            fi = FileItem(storage="local", path=str(entry),
                          type="dir" if entry.is_dir() else "file", name=entry.name)
            extra = {}
            if entry.name in media_map:
                ms, mid = media_map[entry.name]
                ms_norm = {"tmdb": "themoviedb"}.get(ms.strip().lower(), ms.strip().lower())
                try:
                    from app.schemas.types import MediaSource
                    ms_val = MediaSource(ms_norm)
                except Exception:
                    ms_val = ms_norm
                extra = {"media_source": ms_val, "media_id": mid}
                # 按 ID 查详情必须同时给媒体类型，否则 TMDB 侧无法定位 tv/movie
                try:
                    import re as _re
                    from app.schemas.types import MediaType
                    extra["mtype"] = (MediaType.TV if _re.search(
                        r"S\d{1,2}E\d{1,3}|S\d{1,2}\b|Season|第.季|全.集|全\d{1,3}集|E\d{1,2}\s*[-~]\s*E?\d{1,2}|第\d{1,3}集", entry.name, _re.I)
                        else MediaType.MOVIE)
                except Exception as e:
                    logger.error(f"浣熊 推断媒体类型失败: {e}")
                logger.info(f"浣熊 存量指定媒体: {entry.name} -> {ms_norm}:{mid} type={extra.get('mtype')}")
            try:
                success, msg = TransferChain().manual_transfer(
                    fileitem=fi, target_storage="local",
                    target_path=Path(self._library_root), transfer_type="link",
                    library_type_folder=True, library_category_folder=True,
                    background=False, **extra,
                )
            except Exception as e:
                success, msg = False, str(e)
            # 复核：必须有目标存在的成功记录才算成功
            verified = False
            if success:
                try:
                    rows = oper.list_success_by_src(str(entry), recursive=True) or []
                    for r in rows:
                        if getattr(r, "dest", None) and os.path.exists(r.dest):
                            verified = True
                            if str(getattr(r, "mode", "") or "") == "link":
                                self._register_tree_links(getattr(r, "src"), getattr(r, "dest"),
                                                          getattr(r, "download_hash", None),
                                                          getattr(r, "title", None))
                except Exception as e:
                    logger.error(f"浣熊 存量复核失败 {entry.name}: {e}")
            if verified:
                done.add(entry.name)
                self._record_stock_uncovered(entry)
                ok_names.append(entry.name)
            else:
                failed[entry.name] = failed.get(entry.name, 0) + 1
                fail_names.append(f"{entry.name}（{str(msg)[:60]}）")
                logger.warning(f"浣熊 存量失败 {entry.name}: {str(msg)[:200]}")
        self._state["stock_done"] = sorted(done)
        self._save_state()
        text = f"成功 {len(ok_names)} 个"
        if ok_names:
            text += "：\n" + "\n".join(ok_names)
        if fail_names:
            text += f"\n失败 {len(fail_names)} 个：\n" + "\n".join(fail_names)
        unc = [(n, self._state["stock_uncovered"][n]) for n in ok_names
               if self._state["stock_uncovered"].get(n)]
        if unc:
            text += (f"\n文件级审计：{len(unc)} 个条目有未覆盖文件（已留痕）：\n"
                     + "\n".join(f"{n}：{len(u)} 个" for n, u in unc))
        self._notify("浣熊 · 存量处理完成", text)
