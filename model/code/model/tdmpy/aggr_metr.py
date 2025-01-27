
from asyncio.log import logger
from pickle import TRUE
import pandas as pd # dh
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np 

class aggregate_metrics(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        
    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:    
            self.run_model()
            self.status_updater(100, "Closing component" )
            print("Exiting " + self.name)
            if self.popup == None:
                raise SystemExit()
            elif self.popup.runwithin == "others" :
                raise SystemExit()

        except Exception as e:
            import traceback
            errfile = self.args["OutputFolder"] +'\\_logs\\' + "py.err"
            with open(errfile,"a") as file:
                traceback.print_exc(file=file)

            self.status_updater(-1, "**Error**: Click cancel to check the error message %s"%str(e) )
        

    def run_model(self):
        """[load parameters and call appropriate model run method]"""

        # inputs
        demogr_file = self.args["TAZ Demographic Data"]
        metric_file = self.args["Equity Metrics by TAZ"]
        emission_file = self.args["Air Quality Metrics by TAZ"]
        emis_metr = self.args["Selected Air Quality Fields"].lower().split(",")
        pop_share_array = self.args["Equity Pop Share Array"]
        # output
        output_file = self.args["Aggregated Metric Values"]

        # read data
        demogr_df = pd.read_csv(demogr_file)
        # selected TAZs: BRMPO
        # accepted filter: 'town', 'state', 'mpo', 'subregion', 'corridor', 'ring', 'district'
        demogr_df = demogr_df[demogr_df['mpo']=='BRMPO'].reset_index(drop=True)

        pop_grps = []
        for i in range(len(pop_share_array["Source Field"])):
            pop_share_df = pd.read_csv(pop_share_array["Source File"][i])
            pop_share_df[pop_share_array["Target Group Name"][i]] = pop_share_df[pop_share_array["Source Field"][i]]
            pop_share_df[pop_share_array["Control Group Name"][i]] = 1-pop_share_df[pop_share_array["Target Group Name"][i]]
            demogr_df = demogr_df.merge(pop_share_df[["taz_id",pop_share_array["Target Group Name"][i],
                                                      pop_share_array["Control Group Name"][i]]], how='left', on='taz_id')
            pop_grps.append(pop_share_array["Target Group Name"][i])
            pop_grps.append(pop_share_array["Control Group Name"][i])
        
        # calculate equity population from share
        for p in pop_grps:
            demogr_df[p] = demogr_df[p]*demogr_df['population']

        metric_df = pd.read_csv(metric_file)
        # metric names
        acc_mob_metr = metric_df.columns.tolist()
        acc_mob_metr.remove("taz_id")

        emission_df = pd.read_csv(emission_file)
        emission_df.columns= emission_df.columns.str.lower()

        # join tables
        calc_df = demogr_df.merge(metric_df, on='taz_id', how='left') 
        calc_df = calc_df.merge(emission_df, on='taz_id', how='left') 

        # calculate emissions per square mile
        calc_df = calc_df.fillna(0) 
        # metric names
        emis_metr_sqmi = [m+"_sqmi" for m in emis_metr]
        for col in emis_metr:
            calc_df[col+'_sqmi'] = calc_df[col] / calc_df['land_area']

        # aggregate and export
        aggr_df = pd.DataFrame(columns=['pop_grp','metric','value'])
        metr_list = acc_mob_metr + emis_metr_sqmi
        for m in (metr_list): 
            for p in pop_grps:
                v = (calc_df[m]*calc_df[p]).sum()/calc_df[p].sum() 
                aggr_df=aggr_df.append({'pop_grp':p,'metric':m,'value':v},ignore_index=True) 
        # Reshape from long to wide
        df_wide=pd.pivot(aggr_df, index='pop_grp', columns = ['metric'],values = 'value') 

        df_wide.to_csv(output_file)