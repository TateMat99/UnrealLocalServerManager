from __future__ import annotations
import sys
import os
from PySide6.QtGui import QIcon
from PySide6 import QtWidgets
from .core import apply_theme, load_settings
from .main_window import MainWindow

def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'app.ico')
    app.setWindowIcon(QIcon(icon_path))  # Set window and taskbar icon
    
    prefs = load_settings()
    apply_theme(prefs.get("theme", "dark"))

    font = app.font()
    font.setPointSize(font.pointSize() + 1)
    app.setFont(font)

    w = MainWindow(initial_theme=prefs.get("theme", "dark"))
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
