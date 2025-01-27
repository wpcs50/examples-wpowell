# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'demoProgressBarThread.ui'
#
# Created by: PyQt5 UI code generator 5.13.0
#
# WARNING! All changes made in this file will be lost!


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("StatusBar")
        Dialog.resize(600, 100)
        self.label = QtWidgets.QLabel(Dialog)
        # Four arguments are required: x, y, width and height.
        self.label.setGeometry(QtCore.QRect(200, 10, 300, 20))
        self.label.setObjectName("label")
        self.progressBar = QtWidgets.QProgressBar(Dialog)
        self.progressBar.setGeometry(QtCore.QRect(150, 40, 300, 20))
        self.progressBar.setProperty("value", 0)
        self.progressBar.setObjectName("progressBar")
        ## button
        self.pushButtonStart = QtWidgets.QPushButton(Dialog)
        self.pushButtonStart.setGeometry(QtCore.QRect(270, 70, 60, 20))
        font = QtGui.QFont()
        font.setPointSize(8)
        self.pushButtonStart.setFont(font)
        self.pushButtonStart.setObjectName("pushButtonStart")
        
        ## window button

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("StatusBar", "Status for python component"))
        # self.label.setText(_translate("StatusBar", "running the component"))
        self.label.setText("Initializing the component")
        self.pushButtonStart.setText(_translate("Dialog", "Cancel"))