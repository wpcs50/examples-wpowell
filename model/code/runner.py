from unittest import runner
from numpy import byte
from model.tdmpy import *
from model.tdmpy import disagg_model
from model.tdmpy import db_loader
from model.tdmpy import access_density
from model.tdmpy import export_transit_activity_summary
from model.tdmpy import employment_access
from model.tdmpy import trip_generation
from model.tdmpy import aggregate_metrics
import json
import sys
import logging
from PyQt5.QtWidgets import QApplication
from model.gui.dialog import MyForm

# json_file = "C:\\Users\\ZJin\\Desktop\\demo\\debug\\tdm23_args.json"
json_file = 'C:\\Users\\Bkrepp\\tdm23_platform\\inputs\\tdm23_args.json'

def Init_InputFileCheck(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC9"
    dbl = db_loader(init = True, json_config_file=json_file,popup=popup)
    dbl.pct_signal.connect(popup.progressbar_slot)
    dbl.start()
    sys.exit(app.exec_())

def TransitAccessDensity(json_file):
    #
    # Run the pre-processors
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC9"
    pp = access_density(init=False, json_config_file=json_file,popup=popup)
    pp.pct_signal.connect(popup.progressbar_slot)
    pp.start()
    pp.logger.info('Step 1.2 runner end ')
    # popup.exec()
    sys.exit(popup.exec_())

def ExportTransitActivitySummary(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC9"
    pp = export_transit_activity_summary(init=False, json_config_file=json_file,popup=popup)
    pp.pct_signal.connect(popup.progressbar_slot)
    pp.start()
    pp.logger.info('Step 1.2 runner end ')
    # popup.exec()
    sys.exit(popup.exec_())
    
def EmploymentAccess(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC9"
    pp = employment_access(init=False, json_config_file=json_file,popup=popup)
    pp.pct_signal.connect(popup.progressbar_slot)
    pp.start()
    sys.exit(popup.exec_())

def VehicleAvailability(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    va = veh_avail(json_config_file=json_file,popup=popup)
    va.pct_signal.connect(popup.progressbar_slot)
    va.start()
    sys.exit(app.exec_())

def WorkFromHome(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    wfh = work_from_home(json_config_file=json_file,
                          popup=popup)
    wfh.pct_signal.connect(popup.progressbar_slot)
    wfh.start()
    sys.exit(app.exec_())

def TripGeneration(json_file,hbo=0):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    tg = trip_generation(json_config_file=json_file
                        ,hbo=hbo, popup=popup)
    tg.logger.info("Step 4 runner call: Trip Generation ")
    tg.pct_signal.connect(popup.progressbar_slot)
    tg.start()
    sys.exit(app.exec_())

def PeakNonpeak(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    pn = peak_nonpeak(json_config_file=json_file
                        , popup=popup)
    pn.pct_signal.connect(popup.progressbar_slot)
    pn.start()
    sys.exit(app.exec_())

def Aggregate(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    ag = aggregate_and_balance(json_config_file=json_file
                        , popup=popup)
    ag.pct_signal.connect(popup.progressbar_slot)
    ag.start()
    sys.exit(app.exec_())

def TruckTripGeneration(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    tr = truck_tripgeneration(json_config_file=json_file
                        ,popup=popup)
    tr.logger.info("runner call: Truck Trip Generation ")
    tr.pct_signal.connect(popup.progressbar_slot)
    tr.start()
    sys.exit(app.exec_())    

def ExternalTripGeneration(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    tr = ext_tripgeneration(json_config_file=json_file
                        ,popup=popup)
    tr.logger.info("runner call: External Trip Generation ")
    tr.pct_signal.connect(popup.progressbar_slot)
    tr.start()
    sys.exit(app.exec_())      

def SpecialGeneratorTripGeneration(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    tr = spcgen_tripgeneration(json_config_file=json_file
                        ,popup=popup)
    tr.logger.info("runner call: Special Generator Trip Generation ")
    tr.pct_signal.connect(popup.progressbar_slot)
    tr.start()
    sys.exit(app.exec_())       

def AirportTripGeneration(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    tr = airport_tripgeneration(json_config_file=json_file
                        ,popup=popup)
    tr.logger.info("runner call: Airport Trip Generation ")
    tr.pct_signal.connect(popup.progressbar_slot)
    tr.start()
    sys.exit(app.exec_())   

def HBUTripGeneration(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    tr = hbu_tripgeneration(json_config_file=json_file
                        ,popup=popup)
    tr.logger.info("runner call: HBU Trip Generation ")
    tr.pct_signal.connect(popup.progressbar_slot)
    tr.start()
    sys.exit(app.exec_())   

def AggregateMetricValues(json_file):
    # -- call with progressbar in TC9
    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    tr = aggregate_metrics(json_config_file=json_file
                        ,popup=popup)
    tr.logger.info("runner call: Aggregate Metric Values ")
    tr.pct_signal.connect(popup.progressbar_slot)
    tr.start()
    sys.exit(app.exec_())   

def AirQuality(json_file):
    # -- call with progressbar in TC9

    app = QApplication(sys.argv)
    popup = MyForm(popup="TC9") #None "TC"
    tr = air_quality(json_config_file=json_file
                        ,popup=popup)
    tr.logger.info("runner call: air quality emission")
    tr.pct_signal.connect(popup.progressbar_slot)
    tr.start()
    sys.exit(app.exec_())   

if __name__ == "__main__":
    # pass
    # Check_Environment_Variables()
    # Add_Environment_Variable()
    # Set_Environment_Variables()
    json_file=r"C:\Users\phoebe.AD\Documents\GitHub\tdm23\outputs\Base\config.json"
    # json_file=r"D:\Projects\tdm23\outputs\Base\config.json"
    # Init_InputFileCheck(json_file=json_file)
    # Init_InputFileCheck(json_file=json_file)
    # TransitAccessDensity(json_file)
    # EmploymentAccess(json_file)
    # VehicleAvailability(json_file=json_file)
    # WorkFromHome(json_file=json_file)
    # TripGeneration(json_file,hbo=0)
    # PeakNonpeak(json_file)
    AirQuality(json_file)