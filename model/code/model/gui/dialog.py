
from distutils.log import error
import imp
from PyQt5.QtWidgets import QDialog, QApplication,QMainWindow
from .ProgressBarThread import Ui_Dialog
from PyQt5 import QtCore
import sys

class MyForm(QDialog):
    def __init__(self,popup=None):
        super().__init__()
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)
        self.ui.pushButtonStart.clicked.connect(self.stop)
        self.runwithin = "TC9"
        # self.stop_threads = False
        # self._want_to_close = True         # when you want to destroy the dialog set this to True
        self.setWindowFlags(
            QtCore.Qt.Window |
            QtCore.Qt.CustomizeWindowHint |
            QtCore.Qt.WindowTitleHint |
            # QtCore.Qt.FramelessWindowHint
            QtCore.Qt.WindowMinimizeButtonHint
            )
        self.show()

        if popup == None:
            self.runwithin = "others"
            # return None

    def stop(self):
        raise KeyboardInterrupt()
        # self.active_thrd.kill()

        # self.active_thrd.join()
    def progressbar_slot(self,tuple):
        value,txt = tuple
        self.ui.progressBar.setValue(value)
        self.ui.label.setText(txt)
        if value == 100:
            self.done(1)
            self.close()