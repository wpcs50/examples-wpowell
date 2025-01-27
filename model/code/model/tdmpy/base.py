
import pandas as pd 
from pathlib import Path
import json
import os
from dbfread import DBF
from ..sqlite import SQLiteDB
import logging
# import sys
from PyQt5.QtCore import QThread,pyqtSignal


# rtlevel = "J:\\Shared drives\\TMD_TSA\\Model\\platform\\inputs"

class disagg_model(QThread):
    pct_signal = pyqtSignal(object)
    def __init__(self, popup=None, init=False
                     ,json_config_file="config.json"):
        """
         Args: 
             init  - a QT dialog object,  if object is None then hide the dialog
             init (Boolean) - initialize DB flag [appears unnecesssary]
             json_config_file - full path to JSON config file
         Returns:
            None
         Summary:
            Reads JSON config file into self.args as a dict
            Initializes connection to model database, saves it in self.db_conn
        """
        super(disagg_model,self).__init__()
        self.popup = popup
        self.name = "thread-%s"%int(self.currentThreadId())
        

        dict_log = { "LEAN":40 ,"STANDARD":30,"FULL":20,"DEBUG":10}

        with open(json_config_file,"r") as file:
            self.args = json.load(file)
        #
        
        print("Running TDM23 base.py: Logging level is: " + self.args["loglevel"])
        self.log_level = dict_log[self.args["loglevel"]]
        self.logfile = self.args["OutputFolder"] + "\\_logs\\all_run.log"
        
        db_path = self.args['OutputFolder'] + '\\tdm23.db'
        print('Establishing connection to database: ' + db_path)
        self.db = SQLiteDB(database_path = db_path,initialize=init)
        self.db_conn = self.db.conn
        

        ## initialiaze a logger
        if init:
            self.init_logger()
        logger = self.add_logger(name=__name__)
        self.logger = logger
        logger.debug("logger initialized")

        ## initialize a status bar 
        # self.appui = QApplication(sys.argv)
        # self.stbar = status_bar(title="loading...")




        return None
    # end_def __init__()

    def init_logger(self):
        LOG_FILE = self.logfile
        if os.path.exists(LOG_FILE):
            os.remove(LOG_FILE)
        else:
            pass

        

    def add_logger(self,name = "__name__"):
        LOG_FILE = self.logfile
        logger = logging.getLogger(name)
        FORMATTER = logging.Formatter("%(asctime)s * %(levelname)8s * %(module)15s * %(funcName)20s * %(message)s")
        file_handler = logging.FileHandler(LOG_FILE,mode='a')
        file_handler.setFormatter(FORMATTER)
        logger.addHandler(file_handler)
        logger.setLevel(self.log_level)
        return logger


    def status_updater(self, value = 0, txt="starting"):
        if self.popup == None:
            self.logger.info(txt)  
        elif self.popup.runwithin == "TC9":
            self.pct_signal.emit((value,txt))
            # self.popup.ui.label.setText(txt)
            # self.popup.update()
            self.logger.info("%4s_%s"%(value,txt))
        elif self.popup.runwithin == "others":
            txt = "Only TC9 can run with dialogbox, click cancle to exit"
            self.popup.ui.label.setText(txt)
            raise KeyError("only TC9 can run with dialogbox")


if __name__ == "__main__":
    mod = disagg_model()
