from __future__ import annotations

import os
import subprocess as sp
import sys
import threading
import time
from typing import Callable, Dict, List, Optional

import psutil
from PySide6 import QtCore, QtGui, QtWidgets

from .core import (
    ServerConfig,
    ServerRuntime,
    Store,
    apply_theme,
    classify_log_line,
    effective_port,
    get_private_ip,
    get_public_ip,
    load_settings,
    port_in_use,
    program_saved_dir,
    resolve_engine_executable,
    save_settings,
    build_command,
)


class LogReaderThread(QtCore.QThread):
    line_received = QtCore.Signal(str, str)
    process_finished = QtCore.Signal(str, int)

    def __init__(self, server_id: str, popen: psutil.Popen):
        super().__init__()
        self.server_id = server_id
        self.popen = popen
        self._stop = threading.Event()

    def run(self) -> None:
        try:
            if getattr(self.popen, "stdout", None) is None:
                return
            for line in self.popen.stdout:
                if self._stop.is_set():
                    break
                self.line_received.emit(self.server_id, line)
        finally:
            try:
                rc = self.popen.wait()
            except Exception:
                rc = -1
            self.process_finished.emit(self.server_id, int(rc))

    def stop(self) -> None:
        self._stop.set()


class StopWorker(QtCore.QThread):
    done = QtCore.Signal(str)

    def __init__(self, server_id: str, process: psutil.Popen):
        super().__init__()
        self.server_id = server_id
        self.process = process

    def run(self) -> None:
        import signal
        try:
            if sys.platform.startswith("win"):
                try:
                    self.process.send_signal(signal.CTRL_BREAK_EVENT)
                    self.process.wait(timeout=7)
                except Exception:
                    pass
            if self.process.is_running():
                try:
                    self.process.terminate()
                    self.process.wait(timeout=5)
                except Exception:
                    try:
                        self.process.kill()
                    except Exception:
                        pass
            try:
                if self.process.stdout:
                    self.process.stdout.close()
            except Exception:
                pass
        finally:
            self.done.emit(self.server_id)


# UI Widgets
class StatusBadge(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        h = QtWidgets.QHBoxLayout(self)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(6)
        self.dot = QtWidgets.QLabel()
        self.dot.setFixedSize(12, 12)
        self.dot.setStyleSheet("border-radius:6px;background:#9ca3af;") 
        self.text = QtWidgets.QLabel("Offline")
        h.addWidget(self.dot)
        h.addWidget(self.text)
        h.addStretch(1)

    def set_state(self, state: str) -> None:
        colors = {
            "Running": "#22c55e", # green
            "Starting": "#f59e0b", # orange
            "Stopping": "#f59e0b", # orange
            "Stopped": "#ef4444", # red
            "Offline": "#9ca3af", # gray
        }
        self.dot.setStyleSheet(f"border-radius:6px;background:{colors.get(state, '#9ca3af')};")
        self.text.setText(state)


class ServersTable(QtWidgets.QTableWidget):
    """Table listing all servers."""
    ip_cell_clicked = QtCore.Signal(int, int, str)  # row, column, text

    columns = [
        "Server Name",
        "Public (IP:Port)",
        "Private (IP:Port)",
        "State",
        "CPU %",
        "Memory MB",
        "Actions",
    ]

    def __init__(self, parent=None):
        super().__init__(0, len(self.columns), parent)
        self.setHorizontalHeaderLabels(self.columns)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.verticalHeader().setVisible(False)
        self.horizontalHeader().setStretchLastSection(False)
        self.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        self.setAlternatingRowColors(True)
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

    def make_copyable_item(self, text: str) -> QtWidgets.QTableWidgetItem:
        item = QtWidgets.QTableWidgetItem(text)
        item.setToolTip("Click to copy")
        return item

    def add_server_row(self, server_id: str, name: str) -> int:
        row = self.rowCount()
        self.insertRow(row)

        name_item = QtWidgets.QTableWidgetItem(name)
        name_item.setData(QtCore.Qt.UserRole, server_id)
        self.setItem(row, 0, name_item)

        self.setItem(row, 1, self.make_copyable_item("--"))
        self.setItem(row, 2, self.make_copyable_item("--"))

        state_w = StatusBadge()
        state_w.set_state("Offline")
        self.setCellWidget(row, 3, state_w)

        self.setItem(row, 4, QtWidgets.QTableWidgetItem("0.0"))
        self.setItem(row, 5, QtWidgets.QTableWidgetItem("0.0"))

        container = QtWidgets.QWidget()
        h = QtWidgets.QHBoxLayout(container)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(6)
        start_stop = QtWidgets.QPushButton("Start")
        start_stop.setProperty("server_id", server_id)
        delete_btn = QtWidgets.QPushButton("Delete")
        delete_btn.setProperty("server_id", server_id)
        h.addWidget(start_stop)
        h.addWidget(delete_btn)
        h.addStretch(1)
        self.setCellWidget(row, 6, container)

        return row

    def find_row_by_id(self, server_id: str) -> int | None:
        for row in range(self.rowCount()):
            item = self.item(row, 0)
            if item and item.data(QtCore.Qt.UserRole) == server_id:
                return row
        return None

    def mousePressEvent(self, event: QtGui.QMouseEvent) -> None:
        index = self.indexAt(event.pos())
        if index.isValid() and index.column() in (1, 2):
            item = self.item(index.row(), index.column())
            if item:
                self.ip_cell_clicked.emit(index.row(), index.column(), item.text())
            return
        super().mousePressEvent(event)


# Main Window
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, initial_theme: str = "dark") -> None:
        super().__init__()
        self.setWindowTitle("Unreal Local Server Manager")
        self.resize(1160, 800)

        self.servers: Dict[str, ServerRuntime] = {}
        self.active_server_id: Optional[str] = None
        self.stop_workers: Dict[str, StopWorker] = {}
        self.store = Store()
        self.current_theme = initial_theme
        self.is_closing = False  # check if we are about to close

        root = QtWidgets.QWidget()
        self.setCentralWidget(root)
        self.vbox = QtWidgets.QVBoxLayout(root)
        self.vbox.setContentsMargins(10, 10, 10, 10)
        self.vbox.setSpacing(10)

        self.vbox.addLayout(self._build_topbar())
        self.vbox.addWidget(self._build_add_section())
        self.vbox.addWidget(self._build_servers_section(), 1)
        self.vbox.addWidget(self._build_log_section(), 2)

        self._restore_saved_servers()

        self.metrics_timer = QtCore.QTimer(self)
        self.metrics_timer.setInterval(1000)
        self.metrics_timer.timeout.connect(self._refresh_metrics)
        self.metrics_timer.start()

    # Top Bar 
    def _build_topbar(self) -> QtWidgets.QHBoxLayout:
        h = QtWidgets.QHBoxLayout()
        h.addStretch(1)
        self.theme_toggle = QtWidgets.QCheckBox("Dark mode")
        self.theme_toggle.setChecked(self.current_theme == "dark")
        self.theme_toggle.toggled.connect(self._on_toggle_theme)
        h.addWidget(self.theme_toggle)
        return h
    
    # Toggle Theme
    def _on_toggle_theme(self, checked: bool) -> None:
        self.current_theme = "dark" if checked else "light"
        apply_theme(self.current_theme)
        save_settings({"theme": self.current_theme})

    # Add Server Section
    def _build_add_section(self) -> QtWidgets.QGroupBox:
        box = QtWidgets.QGroupBox("Add Server")
        form = QtWidgets.QGridLayout(box)
        row = 0

        form.addWidget(QtWidgets.QLabel("Server Name:"), row, 0)
        self.name_edit = QtWidgets.QLineEdit()
        self.name_edit.setPlaceholderText("e.g., Test Server")
        form.addWidget(self.name_edit, row, 1, 1, 3)
        row += 1

        form.addWidget(QtWidgets.QLabel("Unreal Engine Path (exe OR folder):"), row, 0)
        self.engine_edit = QtWidgets.QLineEdit()
        self.engine_edit.setPlaceholderText("Select UnrealEditor or an engine folder…")
        btn_engine = QtWidgets.QPushButton("Browse…")
        btn_engine.clicked.connect(self._browse_engine)
        form.addWidget(self.engine_edit, row, 1, 1, 2)
        form.addWidget(btn_engine, row, 3)
        row += 1

        form.addWidget(QtWidgets.QLabel("Project (.uproject):"), row, 0)
        self.project_edit = QtWidgets.QLineEdit()
        self.project_edit.setPlaceholderText("Select your .uproject file…")
        btn_proj = QtWidgets.QPushButton("Browse…")
        btn_proj.clicked.connect(self._browse_project)
        form.addWidget(self.project_edit, row, 1, 1, 2)
        form.addWidget(btn_proj, row, 3)
        row += 1

        form.addWidget(QtWidgets.QLabel("Port:"), row, 0)
        self.port_edit = QtWidgets.QLineEdit()
        self.port_edit.setPlaceholderText("7777")
        self.port_edit.setValidator(QtGui.QIntValidator(1, 65535, self))
        form.addWidget(self.port_edit, row, 1)

        form.addWidget(QtWidgets.QLabel("Custom Parameters:"), row, 2)
        self.custom_edit = QtWidgets.QLineEdit()
        self.custom_edit.setPlaceholderText("")
        form.addWidget(self.custom_edit, row, 3)
        row += 1

        self.add_btn = QtWidgets.QPushButton("Add Server")
        self.add_btn.setFixedWidth(160)
        self.add_btn.clicked.connect(self._add_server)
        form.addWidget(self.add_btn, row, 3, alignment=QtCore.Qt.AlignRight)

        return box

    def _browse_engine(self) -> None:
        menu = QtWidgets.QMenu(self)
        act_file = menu.addAction("Choose Executable…")
        act_dir = menu.addAction("Choose Engine Folder…")
        action = menu.exec(QtGui.QCursor.pos())
        if action == act_file:
            path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Select Unreal Engine Executable", "", "Executables (*.exe);;All Files (*)")
            if path:
                self.engine_edit.setText(path)
        elif action == act_dir:
            path = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Unreal Engine Folder", "")
            if path:
                self.engine_edit.setText(path)

    def _browse_project(self) -> None:
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Select .uproject File", "", "Unreal Project (*.uproject)")
        if path:
            self.project_edit.setText(path)

    def _add_server(self) -> None:
        name = self.name_edit.text().strip() or f"Server {len(self.servers) + 1}"
        engine = self.engine_edit.text().strip()
        proj = self.project_edit.text().strip()
        port_text = self.port_edit.text().strip() or "7777"
        try:
            port = int(port_text)
            assert 1 <= port <= 65535
        except Exception:
            QtWidgets.QMessageBox.warning(self, "Invalid Port", "Port must be an integer between 1 and 65535.")
            return
        custom = self.custom_edit.text().strip()

        if not engine or not os.path.exists(engine):
            QtWidgets.QMessageBox.warning(self, "Invalid Engine Path", "Please select an Unreal executable or engine folder that exists.")
            return
        if not proj or not os.path.exists(proj):
            QtWidgets.QMessageBox.warning(self, "Missing Project", "Please select a valid .uproject file.")
            return

        cfg = ServerConfig(name=name, engine_path=engine, project_path=proj, port=port, custom_params=custom)
        runtime = ServerRuntime(config=cfg)
        runtime.private_ip = get_private_ip()
        threading.Thread(target=self._resolve_public_ip, args=(cfg.id,), daemon=True).start()

        self.servers[cfg.id] = runtime
        row = self.table.add_server_row(cfg.id, cfg.name)

        container = self.table.cellWidget(row, 6)
        start_stop: QtWidgets.QPushButton = container.layout().itemAt(0).widget()  
        delete_btn: QtWidgets.QPushButton = container.layout().itemAt(1).widget()
        start_stop.clicked.connect(self._toggle_server_from_button)
        delete_btn.clicked.connect(self._delete_server_from_button)

        self._save_all()
        self._set_active_server(cfg.id)
        self.name_edit.clear()

    def _resolve_public_ip(self, server_id: str) -> None:
        ip = get_public_ip()
        srv = self.servers.get(server_id)
        if srv:
            srv.public_ip = ip
            self._update_row(server_id)

    # Servers Section
    def _build_servers_section(self) -> QtWidgets.QGroupBox:
        box = QtWidgets.QGroupBox("Servers")
        v = QtWidgets.QVBoxLayout(box)
        self.table = ServersTable()
        self.table.itemSelectionChanged.connect(self._on_row_selected)
        self.table.ip_cell_clicked.connect(self._copy_ip_cell)
        v.addWidget(self.table)
        return box

    def _on_row_selected(self) -> None:
        sel = self.table.selectedItems()
        if not sel:
            return
        row = sel[0].row()
        item = self.table.item(row, 0)
        if not item:
            return
        sid = item.data(QtCore.Qt.UserRole)
        self._set_active_server(sid)

    def _copy_ip_cell(self, row: int, column: int, text: str) -> None:
        QtWidgets.QApplication.clipboard().setText(text)
        QtWidgets.QToolTip.showText(QtGui.QCursor.pos(), f"Copied: {text}", self.table, msecShowTime=1200)

    def _set_active_server(self, server_id: Optional[str]) -> None:
        self.active_server_id = server_id
        if server_id:
            row = self.table.find_row_by_id(server_id)
            if row is not None:
                self.table.setCurrentCell(row, 0)
                self.table.selectRow(row)
        self._refresh_log_view()

    def _toggle_server_from_button(self) -> None:
        btn = self.sender()
        if not isinstance(btn, QtWidgets.QPushButton):
            return
        sid = btn.property("server_id")
        if not sid:
            return
        runtime = self.servers.get(sid)
        if not runtime:
            return
        
        # Don't allow toggling if we're in the middle of stopping
        if runtime.state == "Stopping":
            return
            
        if runtime.state != "Running":
            self._start_server(sid)
        else:
            self._stop_server(sid)

    def _delete_server_from_button(self) -> None:
        btn = self.sender()
        if not isinstance(btn, QtWidgets.QPushButton):
            return
        sid = btn.property("server_id")
        if not sid:
            return
        self._delete_server(sid)

    def _delete_server(self, server_id: str) -> None:
        srv = self.servers.get(server_id)
        if not srv:
            return

        def really_delete():
            row = self.table.find_row_by_id(server_id)
            if row is not None:
                self.table.removeRow(row)
            self.servers.pop(server_id, None)
            if self.active_server_id == server_id:
                self.active_server_id = None
                self.log_view.clear()
            self._save_all()

        if srv.state in ("Running", "Starting"):
            self._stop_server(server_id, on_done=really_delete)
        else:
            really_delete()

    # Log Section
    def _build_log_section(self) -> QtWidgets.QGroupBox:
        box = QtWidgets.QGroupBox("Server Log (Active Server)")
        v = QtWidgets.QVBoxLayout(box)

        h = QtWidgets.QHBoxLayout()
        self.search_edit = QtWidgets.QLineEdit()
        self.search_edit.setPlaceholderText("Search in log…")
        self.search_edit.textChanged.connect(self._apply_log_search_highlight)
        export_btn = QtWidgets.QPushButton("Export Log")
        export_btn.clicked.connect(self._export_log)
        h.addWidget(self.search_edit)
        h.addStretch(1)
        h.addWidget(export_btn)
        v.addLayout(h)

        self.log_view = QtWidgets.QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setLineWrapMode(QtWidgets.QTextEdit.NoWrap)
        v.addWidget(self.log_view)
        return box

    def _export_log(self) -> None:
        sid = self.active_server_id
        if not sid:
            QtWidgets.QMessageBox.information(self, "Export Log", "No active server selected.")
            return
        srv = self.servers.get(sid)
        if not srv:
            return
        logs_dir = os.path.join(program_saved_dir(), "Logs")
        os.makedirs(logs_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        default_name = f"{srv.config.name.replace(' ', '_')}_{timestamp}.log"
        default_path = os.path.join(logs_dir, default_name)
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Export Log", default_path, "Log Files (*.log);;All Files (*)")
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(srv.log_lines)
            QtWidgets.QMessageBox.information(self, "Export Log", f"Saved to:\n{path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Export Log Failed", str(e))

    def _refresh_log_view(self) -> None:
        self.log_view.clear()
        sid = self.active_server_id
        if not sid:
            return
        srv = self.servers.get(sid)
        if not srv:
            return
        for line in srv.log_lines:
            self._insert_colored_line(line)
        self._apply_log_search_highlight()

    def _apply_log_search_highlight(self) -> None:
        term = self.search_edit.text()
        if not term:
            self.log_view.moveCursor(QtGui.QTextCursor.End)
            self.log_view.setExtraSelections([])
            return
        self.log_view.setExtraSelections([])
        extra = []
        cursor = self.log_view.textCursor()
        cursor.movePosition(QtGui.QTextCursor.Start)
        fmt = QtGui.QTextCharFormat()
        fmt.setBackground(QtGui.QBrush(QtGui.QColor(255, 255, 0, 120)))
        doc = self.log_view.document()
        while True:
            cursor = doc.find(term, cursor)
            if cursor.isNull():
                break
            sel = QtWidgets.QTextEdit.ExtraSelection()
            sel.cursor = cursor
            sel.format = fmt
            extra.append(sel)
        self.log_view.setExtraSelections(extra)

    def _save_all(self) -> None:
        self.store.save([s.config for s in self.servers.values()])

    def _restore_saved_servers(self) -> None:
        for cfg in self.store.load():
            runtime = ServerRuntime(config=cfg)
            runtime.private_ip = get_private_ip()
            threading.Thread(target=self._resolve_public_ip, args=(cfg.id,), daemon=True).start()
            self.servers[cfg.id] = runtime
            row = self.table.add_server_row(cfg.id, cfg.name)
            container = self.table.cellWidget(row, 6)
            start_stop: QtWidgets.QPushButton = container.layout().itemAt(0).widget()
            delete_btn: QtWidgets.QPushButton = container.layout().itemAt(1).widget()
            start_stop.clicked.connect(self._toggle_server_from_button)
            delete_btn.clicked.connect(self._delete_server_from_button)
            self._update_row(cfg.id)

    def _shutdown_all_servers(self) -> None:
        #Shutdown all running servers and wait for them to stop.
        self.is_closing = True
        
        # Stop the metrics timer
        if hasattr(self, 'metrics_timer'):
            self.metrics_timer.stop()
        
        # Identify servers that need to be stopped
        servers_to_stop = []
        for sid, srv in list(self.servers.items()):
            if srv.state in ("Running", "Starting"):
                servers_to_stop.append(sid)
            elif srv.reader:
                try:
                    srv.reader.stop()
                except Exception:
                    pass

        if not servers_to_stop:
            return

        for sid in servers_to_stop:
            self._stop_server(sid)

        # Wait for all servers to stop with a timeout
        deadline = time.time() + 10.0
        app = QtWidgets.QApplication.instance()
        
        while time.time() < deadline:
            all_stopped = True
            for sid in servers_to_stop:
                srv = self.servers.get(sid)
                if srv and srv.state not in ("Stopped", "Offline"):
                    all_stopped = False
                    break
            
            if all_stopped:
                break
                
            if app:
                app.processEvents(QtCore.QEventLoop.AllEvents, 50)
            time.sleep(0.05)

        # Force kill any remaining processes
        for sid, srv in list(self.servers.items()):
            if srv.process and srv.process.is_running():
                try:
                    srv.process.kill()
                    srv.process.wait(timeout=2)
                except Exception:
                    pass
            
            # Clean up log readers
            if srv.reader:
                try:
                    srv.reader.stop()
                    srv.reader.wait(1000)
                except Exception:
                    pass
                srv.reader = None

        # Clean up any remaining stop workers
        for worker in list(self.stop_workers.values()):
            try:
                worker.wait(1000)
            except Exception:
                pass
        self.stop_workers.clear()

    # Close Event
    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        progress = QtWidgets.QProgressDialog("Shutting down servers...", "Force Quit", 0, 100, self)
        progress.setWindowTitle("Unreal Server Manager - Shutting Down")
        progress.setWindowModality(QtCore.Qt.WindowModal)
        progress.setAutoClose(True)
        progress.setAutoReset(False)
        progress.setValue(10)
        progress.show()
        
        app = QtWidgets.QApplication.instance()
        if app:
            app.processEvents()

        try:
            progress.setValue(30)
            if app:
                app.processEvents()
            
            self._shutdown_all_servers()
            
            progress.setValue(80)
            if app:
                app.processEvents()
            
            self._save_all()
            
            progress.setValue(100)
            if app:
                app.processEvents()
                
        except Exception as e:
            print(f"Error during shutdown: {e}")
        finally:
            progress.close()
            
        super().closeEvent(event)

    # Server Operations
    def _start_server(self, server_id: str) -> None:
        srv = self.servers.get(server_id)
        if not srv or (srv.process and srv.process.is_running()):
            return

        exe_path = resolve_engine_executable(srv.config.engine_path)
        if not exe_path or not os.path.exists(exe_path):
            QtWidgets.QMessageBox.critical(self, "Engine Executable Not Found", "Could not locate UnrealEditor in the provided engine path.")
            return

        # Refresh IPs on start
        srv.private_ip = get_private_ip()
        threading.Thread(target=self._resolve_public_ip, args=(server_id,), daemon=True).start()

        # Check port availability
        eport = effective_port(srv.config)
        if port_in_use(eport):
            ans = QtWidgets.QMessageBox.question(
                self,
                "Port In Use",
                f"Port {eport} appears to be in use.\nStart the server anyway?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.No,
            )
            if ans != QtWidgets.QMessageBox.Yes:
                return

        self._update_row(server_id)

        cmd = build_command(srv.config)
        try:
            creationflags = 0
            if sys.platform.startswith("win"):
                creationflags = getattr(sp, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(sp, "CREATE_NO_WINDOW", 0)

            popen = psutil.Popen(
                cmd,
                stdout=psutil.subprocess.PIPE,
                stderr=psutil.subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace",
                bufsize=1,
                creationflags=creationflags,
            )
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Failed to Start", f"Error: {e}\nCommand: {' '.join(cmd)}")
            return

        srv.process = popen
        srv.ps = psutil.Process(popen.pid)
        srv.state = "Starting"
        srv.append_log(f"\n[Manager] Started: {' '.join(cmd)}\n")
        srv.last_started_ts = time.time()
        self._set_active_server(server_id)

        reader = LogReaderThread(server_id, popen)
        reader.line_received.connect(self._on_log_line)
        reader.process_finished.connect(self._on_process_finished)
        reader.finished.connect(lambda sid=server_id: self._reader_gone(sid))
        reader.start()
        srv.reader = reader

        self._update_row(server_id)

    def _stop_server(self, server_id: str, on_done: Optional[Callable[[], None]] = None) -> None:
        srv = self.servers.get(server_id)
        if not srv:
            return

        srv.state = "Stopping"
        self._update_row(server_id)
        row = self.table.find_row_by_id(server_id)
        if row is not None:
            container = self.table.cellWidget(row, 6)
            if container:
                start_stop: QtWidgets.QPushButton = container.layout().itemAt(0).widget()
                start_stop.setEnabled(False)
                start_stop.setText("Stopping…")

        if srv.reader:
            try:
                srv.reader.stop()
                srv.reader.finished.connect(lambda sid=server_id: self._reader_gone(sid))
            except Exception:
                pass

        if not srv.process:
            self._stop_finalize(server_id, on_done)
            return

        worker = StopWorker(server_id, srv.process)
        self.stop_workers[server_id] = worker
        worker.done.connect(lambda sid=server_id: self._stop_finalize(sid, on_done))
        worker.start()

    def _reader_gone(self, server_id: str) -> None:
        srv = self.servers.get(server_id)
        if srv and srv.reader:
            srv.reader = None

    def _stop_finalize(self, server_id: str, on_done: Optional[Callable[[], None]]) -> None:
        self.stop_workers.pop(server_id, None)
        srv = self.servers.get(server_id)
        if not srv:
            if on_done:
                on_done()
            return

        srv.state = "Stopped"
        srv.process = None
        srv.ps = None
        srv.cpu_percent = 0.0
        srv.mem_mb = 0.0

        row = self.table.find_row_by_id(server_id)
        if row is not None:
            container = self.table.cellWidget(row, 6)
            if container:
                start_stop: QtWidgets.QPushButton = container.layout().itemAt(0).widget()
                start_stop.setEnabled(True)
                start_stop.setText("Start")

        self._update_row(server_id)
        if on_done:
            on_done()

    def _on_log_line(self, server_id: str, line: str) -> None:
        srv = self.servers.get(server_id)
        if not srv:
            return
        if srv.state == "Starting":
            srv.state = "Running"
            self._update_row(server_id)
        srv.append_log(line)
        if self.active_server_id == server_id:
            self._insert_colored_line(line)
            self._apply_log_search_highlight()

    def _on_process_finished(self, server_id: str, rc: int) -> None:
        srv = self.servers.get(server_id)
        if not srv:
            return
        srv.state = "Stopped"
        srv.append_log(f"\n[Manager] Process exited with code {rc}\n")
        self._update_row(server_id)

    @staticmethod
    def _classify_line(line: str) -> str:
        return classify_log_line(line)

    def _insert_colored_line(self, line: str) -> None:
        severity = self._classify_line(line)
        cursor = self.log_view.textCursor()
        cursor.movePosition(QtGui.QTextCursor.End)
        if severity == "warning":
            self.log_view.setTextColor(QtGui.QColor("#f59e0b"))
        elif severity == "error":
            self.log_view.setTextColor(QtGui.QColor("#ef4444"))
        else:
            self.log_view.setTextColor(self.palette().color(QtGui.QPalette.Text))
        cursor.insertText(line)
        self.log_view.setTextCursor(cursor)

    def _update_row(self, server_id: str) -> None:
        row = self.table.find_row_by_id(server_id)
        if row is None:
            return
        srv = self.servers.get(server_id)
        if not srv:
            return

        port_display = effective_port(srv.config)

        self.table.item(row, 1).setText(f"{srv.public_ip}:{port_display}" if srv.public_ip else f"--:{port_display}")
        self.table.item(row, 2).setText(f"{srv.private_ip}:{port_display}" if srv.private_ip else f"--:{port_display}")

        state_w: StatusBadge = self.table.cellWidget(row, 3)
        if state_w:
            state_w.set_state(srv.state)

        if srv.state == "Running" and srv.ps and srv.ps.is_running():
            self.table.item(row, 4).setText(f"{srv.cpu_percent:.1f}")
            self.table.item(row, 5).setText(f"{srv.mem_mb:.1f}")
        else:
            self.table.item(row, 4).setText("0.0")
            self.table.item(row, 5).setText("0.0")

        if srv.state != "Stopping":
            container = self.table.cellWidget(row, 6)
            if container:
                start_stop: QtWidgets.QPushButton = container.layout().itemAt(0).widget()
                start_stop.setText("Stop" if srv.state in ("Running", "Starting") else "Start")
                start_stop.setEnabled(True)

    def _refresh_metrics(self) -> None:
        # Don't refresh metrics if we're closing
        if self.is_closing:
            return
            
        updated_ids: List[str] = []
        for sid, srv in self.servers.items():
            if srv.ps and srv.ps.is_running() and srv.state == "Running":
                try:
                    srv.cpu_percent = srv.ps.cpu_percent(interval=None) / max(1, psutil.cpu_count(logical=True))
                    srv.mem_mb = float(srv.ps.memory_info().rss / (1024 * 1024))
                except Exception:
                    pass
                updated_ids.append(sid)
            else:
                if srv.state != "Running":
                    srv.cpu_percent = 0.0
                    srv.mem_mb = 0.0
                    updated_ids.append(sid)
        for sid in updated_ids:
            self._update_row(sid)
