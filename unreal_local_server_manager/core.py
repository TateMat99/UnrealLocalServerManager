from __future__ import annotations

import json
import os
import re
import shlex
import socket
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from PySide6 import QtCore, QtGui, QtWidgets
import requests 


# Data Models
@dataclass
class ServerConfig:
    name: str
    engine_path: str
    project_path: str
    port: int
    custom_params: str = ""
    id: str = field(default_factory=lambda: __import__("uuid").uuid4().hex)


@dataclass
class ServerRuntime:
    #runtime state for a server process and live metrics.
    config: ServerConfig
    process: Optional[object] = None
    ps: Optional[object] = None
    reader: Optional[object] = None     # LogReaderThread
    state: str = "Offline"              # Offline, Starting , Running, Stopped
    private_ip: str = "Unknown"
    public_ip: str = "Unknown"
    cpu_percent: float = 0.0
    mem_mb: float = 0.0
    log_lines: List[str] = field(default_factory=list)
    last_started_ts: float = 0.0

    def append_log(self, text: str, max_lines: int = 8000) -> None:
        self.log_lines.append(text)
        if len(self.log_lines) > max_lines:
            del self.log_lines[0 : len(self.log_lines) - max_lines]


# Settings/Theme
def program_saved_dir() -> str:
    app_name = "UnrealLocalServerManager"
    
    if sys.platform.startswith("win"):
        base_dir = os.environ.get("APPDATA")
        if not base_dir:
            base_dir = os.path.expanduser("~\\AppData\\Roaming")
    
    app_data_dir = os.path.join(base_dir, app_name)
    os.makedirs(app_data_dir, exist_ok=True)
    return app_data_dir


def settings_path() -> str:
    return os.path.join(program_saved_dir(), "settings.json")


def load_settings() -> Dict:
    try:
        with open(settings_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"theme": "dark"}


def save_settings(d: Dict) -> None:
    try:
        with open(settings_path(), "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
    except Exception:
        pass


def apply_theme(theme: str) -> None:
    app = QtWidgets.QApplication.instance()
    if not app:
        return
    app.setStyle("Fusion")
    if theme == "dark":
        palette = QtGui.QPalette()
        palette.setColor(QtGui.QPalette.Window, QtGui.QColor(53, 53, 53))
        palette.setColor(QtGui.QPalette.WindowText, QtCore.Qt.white)
        palette.setColor(QtGui.QPalette.Base, QtGui.QColor(35, 35, 35))
        palette.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor(53, 53, 53))
        palette.setColor(QtGui.QPalette.ToolTipBase, QtCore.Qt.white)
        palette.setColor(QtGui.QPalette.ToolTipText, QtCore.Qt.white)
        palette.setColor(QtGui.QPalette.Text, QtCore.Qt.white)
        palette.setColor(QtGui.QPalette.Button, QtGui.QColor(53, 53, 53))
        palette.setColor(QtGui.QPalette.ButtonText, QtCore.Qt.white)
        palette.setColor(QtGui.QPalette.BrightText, QtCore.Qt.red)
        palette.setColor(QtGui.QPalette.Link, QtGui.QColor(42, 130, 218))
        palette.setColor(QtGui.QPalette.Highlight, QtGui.QColor(42, 130, 218))
        palette.setColor(QtGui.QPalette.HighlightedText, QtCore.Qt.black)
    else:
        palette = app.style().standardPalette()
    app.setPalette(palette)


# Engine/Command 
def resolve_engine_executable(path: str) -> Optional[str]:
    if not path:
        return None
    if os.path.isfile(path):
        return path

    p = os.path.abspath(path)
    plat = sys.platform

    candidates: List[str] = []
    if plat.startswith("win"):
        candidates += [
            os.path.join(p, "Engine", "Binaries", "Win64", "UnrealEditor.exe"),
            os.path.join(p, "Engine", "Binaries", "Win64", "UE4Editor.exe"),
        ]

    for c in candidates:
        if os.path.exists(c):
            return c

    names = {"UnrealEditor.exe", "UnrealEditor", "UE4Editor.exe", "UE4Editor"}
    for root, dirs, files in os.walk(p):
        depth = os.path.relpath(root, p).count(os.sep)
        if depth > 5:
            del dirs[:]
            continue
        for n in files:
            if n in names:
                return os.path.join(root, n)
    return None

#Build the Unreal server command line from config.
def build_command(cfg: ServerConfig) -> List[str]:
    exe = resolve_engine_executable(cfg.engine_path) or cfg.engine_path
    cmd = [exe]
    if cfg.project_path:
        cmd.append(cfg.project_path)
    cmd += ["-server", "-unattended", "-stdout", "-FullStdOutLogOutput"]

    params_lower = cfg.custom_params.lower()
    if "-port=" not in params_lower and "-netport=" not in params_lower:
        cmd.append(f"-Port={cfg.port}")

    if cfg.custom_params.strip():
        cmd.extend(shlex.split(cfg.custom_params))
    return cmd

#Return the port that will be used
def effective_port(cfg: ServerConfig) -> int:
    m = re.search(r"-(?:Port|NetPort)=(\d+)", cfg.custom_params, flags=re.I)
    return int(m.group(1)) if m else cfg.port


# Networking utils
def get_private_ip() -> str:
    try:
        hostname = socket.gethostname()
        addrs = socket.getaddrinfo(hostname, None, socket.AF_INET)
        for _, _, _, _, sockaddr in addrs:
            if sockaddr and not sockaddr[0].startswith("127."):
                return sockaddr[0]
    except Exception:
        pass
    return "127.0.0.1"


def get_public_ip(timeout: float = 2.5) -> str:
    try:
        resp = requests.get("https://api.ipify.org", timeout=timeout)
        if resp.ok:
            return resp.text.strip()
    except Exception:
        pass
    return "Unknown"

#Check if port is busy
def port_in_use(port: int, host: str = "0.0.0.0") -> bool:
    busy_tcp = False
    busy_udp = False
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.listen(1)
        s.close()
    except OSError:
        busy_tcp = True
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((host, port))
        s.close()
    except OSError:
        busy_udp = True
    return busy_tcp or busy_udp


# Persistence
class Store:
    def __init__(self) -> None:
        base = program_saved_dir()
        self.file = os.path.join(base, "servers.json")

    def load(self) -> List[ServerConfig]:
        try:
            with open(self.file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return [ServerConfig(**d) for d in data]
        except Exception:
            return []

    def save(self, servers: List[ServerConfig]) -> None:
        try:
            data = [s.__dict__ for s in servers]
            with open(self.file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass


# Log helper
def classify_log_line(line: str) -> str:
    low = line.lower()
    if ": error:" in low or " error: " in low or low.startswith("error:"):
        return "error"
    if ": warning:" in low or " warning: " in low or low.startswith("warning:"):
        return "warning"
    return "info"
