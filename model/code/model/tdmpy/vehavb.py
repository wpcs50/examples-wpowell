from asyncio.log import logger
from os import system
# from typing import Literal
import pandas as pd 
import numpy as np
from pathlib import Path
import math
from .base import disagg_model
import yaml
import logging
import sys

import threading
import time
from ..gui.ProgressBarThread import *
class MyDescriptiveError(Exception):
    pass
class veh_avail(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        # logger = self.add_logger(name=__name__)
        # self.logger = logger
        
        self.logger.debug("check the location of JSON file %s" %kwargs)
        self.logger.debug("Arguments passed in JSON config file:")
        self.logger.debug(self.args)

        # Parse the model specification file
        try:       
            with open(self.args['va_spec'], 'r') as stream:
                self.specs = yaml.load(stream, Loader = yaml.FullLoader)
        except Exception as err:
            msg = "Error reading model specification file.\n " + str(err)
            raise RuntimeError(msg) from err
       
        # initialize the status
        self.status_pct = [0, 8, 44, 45, 45, 45, 73, 85, 94, 100]


    # Overrriding of run() method in the subclass
    def run(self):
        """
         The standard run() method
        invokes the callable object passed to the object's constructor as the
        target argument, if any, with sequential and keyword arguments taken
        from the args and kwargs arguments, respectively.

        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:
            if self.specs['model_type']== 'poisson':
                self.poisson_regression_model()
            elif self.specs['model_type']== 'mnl':
                self.mnl_model()
            self.run_summaries()
            
            self.status_updater(100, "Closing component" )
            print("Exiting " + self.name)
            if self.popup == None:
                raise SystemExit()
                # self.popup.close_hide()
            elif self.popup.runwithin == "others" :
                raise SystemExit()
            # else: ## "TC"
            #     self.popup.close()
        except Exception as e:
            import traceback
            errfile = self.args["OutputFolder"] +'\\_logs\\' + "py.err"
            with open(errfile,"a") as file:
                traceback.print_exc(file=file)

            self.status_updater(-1, "**Error**: Click cancel to check the error message %s"%str(e) )


    def mnl_model(self):
        """ Multi-Nomial Logit implementation of Vehicle Availability"""

        # load data
        self.status_updater(self.status_pct[0], "VA component: loading table: hh")
        query_string = """SELECT * FROM hh 
                            JOIN access_density USING(taz_id)
                            JOIN emp_access USING(taz_id)
                            JOIN walkbike USING (taz_id)"""
        var_df = self.db._raw_query(qry=query_string)

        # derive needed variables
        self.status_updater(self.status_pct[1], "VA component: calculating model attributes")
        rows = list(var_df.columns)
        hhsi_idx = rows.index("hh_inc_cat_by_size")
        wrk_idx = rows.index("workers")
        drv_idx = rows.index("drivers")
        chld_idx = rows.index("children")
        sen_idx = rows.index("nwseniors")
        at_idx = rows.index("access_density")

        var_df["chld"] =  var_df.apply(lambda row : 1 if row[chld_idx] > 0 else 0, axis=1,raw=True)
        var_df["wrk"] =  var_df.apply(lambda row : 1 if row[wrk_idx] > 0 else 0, axis=1,raw=True)
        var_df["sen"] =  var_df.apply(lambda row : 1 if row[sen_idx] > 0 else 0, axis=1,raw=True)
        var_df["all_wrk"] =  var_df.apply(lambda row : 1 if row[wrk_idx] == row[drv_idx] else 0, axis=1,raw=True)
        var_df["low_inc"] =  var_df.apply(lambda row : 1 if row[hhsi_idx] == 1 else 0, axis=1,raw=True)
        var_df["high_inc"] =  var_df.apply(lambda row : 1 if row[hhsi_idx] == 3 else 0, axis=1,raw=True)
        var_df["drv_3p"] =  var_df.apply(lambda row : 1 if row[drv_idx] > 2 else 0, axis=1,raw=True)
        var_df["per_drv_3p"] =  var_df.apply(lambda row : row[drv_idx] - 2 if row[drv_idx] > 2 else 0, axis=1,raw=True)

        var_df["dense"] =  var_df.apply(lambda row : 1 if row[at_idx] < 3 else 0, axis=1,raw=True)
        var_df["sub"] =  var_df.apply(lambda row : 1 if row[at_idx] > 4 else 0, axis=1,raw=True)

        var_df['intden'] = var_df.walkability / 1000
        var_df['accrat'] = var_df.pctemp30t / (var_df.pctemp30a + 0.0001)

        var_df['asc'] = 1

        # calculate utilities
        self.status_updater(self.status_pct[5], "VA component: calculating utilities")
        for alt in ['zv','iv','sv']:
            coeffs = self.specs["%s_coeffs"%alt]
            var_df[alt + "_util"] = 0
            for term in coeffs:
                var = term.replace(alt + "_","")
                var_df[alt + "_util"] += coeffs[term] * var_df[var]

        # disable iv for households with only 1 driver (zero or sufficient)
        var_df.loc[var_df.drivers==1,'iv_util'] = -999

        # exponentiate and calculate shares
        var_df['sum_x'] = 0

        for alt in ['zv','iv','sv']:
            var_df[alt + "_x"] = np.exp(var_df[alt + "_util"])
            var_df['sum_x'] += var_df[alt + "_x"]

        for alt in ['zv','iv','sv']:
            var_df[alt + "_p"] = var_df[alt + "_x"] / var_df['sum_x']

        # choose
        self.status_updater(self.status_pct[6], "VA component: simulate va choice")
        rnd = np.random.default_rng(self.specs['random_seed'])

        rows = list(var_df.columns)
        zv_idx = rows.index("zv_p")
        iv_idx = rows.index("iv_p")
        sv_idx = rows.index("sv_p")
        var_df['veh_suff'] = var_df.apply(lambda row : rnd.choice(['zv','iv','sv'],
                            p=[row[zv_idx],row[iv_idx],row[sv_idx]]),axis=1,raw=True)        

        # convert from sufficiency to number of vehicles
        def num_vehs(veh_suff, drivers):
            nvehs = 0
            if veh_suff == 'sv':
                nvehs = drivers
            elif veh_suff == 'iv':
                nvehs = np.round(drivers / 2.001,0)
            return nvehs

        rows = list(var_df.columns)
        vs_idx = rows.index("veh_suff")
        drv_idx = rows.index("drivers")
        var_df['num_vehs'] = var_df.apply(lambda row: num_vehs(row[vs_idx],row[drv_idx]),axis=1,raw=True)

        # export variables, utilities, shares and choice with debug
        if self.args["loglevel"] == "DEBUG":
            dump_csv_fn = self.args['OutputFolder'] + '\\_logs\\' + 'va_df_dump.csv'
            var_df.to_csv(dump_csv_fn, index=False)       

        # write out to db
        self.status_updater(self.status_pct[7], "VA component: writing to table: veh")
        va_exp_df = var_df[['hid','block_id','veh_suff','num_vehs']]
        va_exp_df.to_sql(name="veh",con=self.db.conn,if_exists="replace",index=False)

        return None

        
    def poisson_regression_model(self):
        """
        [run_model: runs vehicle availability model]
        inputs:     model parameters
                    database "hh" table
                    database "walkbike" table
                    database "emp_access" table
                    database "access_density" table
        outputs:    database "veh" table, also stored in self.va_df
        returns:    None
        """
        # Save the model coefficient names in a list
        self.coeffs = self.specs['intercept_coeff']
        self.hh_coeffs = self.specs['hh_coeffs']
        self.zonal_coeffs = self.specs['zonal_coeffs']
        self.coeffs.update(self.hh_coeffs)
        self.coeffs.update(self.zonal_coeffs)        
        coeff_names=list(self.coeffs.keys())
        
        # Create a dataframe, based on data in the "hh" DB table, for running the VA calculations.
        # We will calculate new columns in this dataframe.
        # self.popup.ui.label.setText("VA component: loading table: hh")
        self.status_updater(self.status_pct[0], "VA component: loading table: hh")
        query_string = "SELECT * from hh;"
        self.va_df = self.db._raw_query(qry=query_string)


        self.status_updater(self.status_pct[1], "VA component: calculating model attributes")
        
        # New columns, part 1: household attributes
        cols = list(self.va_df.columns)
        ix1 = cols.index("persons")
        ix2 = cols.index("seniors")
        ix3 = cols.index("hh_inc")
        ix4 = cols.index("drivers")
        
        # New column: senior_only
        self.va_df['senior_only'] = self.va_df.apply(lambda row: 1 if row[ix1] == row[ix2] else 0, axis=1,raw=True)
        
        #New column: drivers
        self.va_df['drivers_1']=self.va_df.apply(lambda row: min(row[ix4], 1), axis=1,raw=True)
        self.va_df['drivers_2']=self.va_df.apply(lambda row: max(0,min((row[ix4]-1), 1)), axis=1,raw=True)
        self.va_df['drivers_3_4']=self.va_df.apply(lambda row: max(0,min((row[ix4]-2), 2)), axis=1,raw=True)
        self.va_df['drivers_5_above']=self.va_df.apply(lambda row: max((row[ix4]-4), 0), axis=1,raw=True)
        
        # New columns: income levels
        self.va_df['income_lt_15k'] = self.va_df.apply(lambda row: 1 if (row[ix3] < 15000) else 0, axis=1)
        self.va_df['income_ge_15k_lt_25k'] = self.va_df.apply(lambda row: 1 if (row[ix3] >= 15000 and row[ix3] <= 24999) else 0, axis=1,raw=True)
        self.va_df['income_ge_25k_lt_35k'] = self.va_df.apply(lambda row: 1 if (row[ix3] >= 25000 and row[ix3] <= 34999) else 0, axis=1,raw=True)
        self.va_df['income_ge_35k_lt_50k'] = self.va_df.apply(lambda row: 1 if (row[ix3] >= 35000 and row[ix3] <= 49999) else 0, axis=1,raw=True)
        self.va_df['income_ge_50k_lt_75k'] = self.va_df.apply(lambda row: 1 if (row[ix3] >= 50000 and row[ix3] <= 74999) else 0, axis=1,raw=True)
        self.va_df['income_ge_75k_lt_100k'] = self.va_df.apply(lambda row: 1 if (row[ix3] >= 75000 and row[ix3] <= 99999) else 0, axis=1 ,raw=True)
        self.va_df['income_ge_100k_lt_150k'] = self.va_df.apply(lambda row: 1 if (row[ix3] >= 100000 and row[ix3] <= 149999) else 0, axis=1 ,raw=True)
        
        # New columns, part 2: zonal attributes
        #
        # New column: walkability, from walkbike DB table
        # Read walkbike DB table into a data frame and join DFs
        self.status_updater(self.status_pct[2], "VA component: loading table: walkbike")
        query_string = "SELECT * from walkbike;"
        temp_walkbike_df = self.db._raw_query(qry=query_string)
        self.va_df = pd.merge(left=self.va_df, right=temp_walkbike_df, how="left", left_on='taz_id',right_on='taz_id')
        self.va_df['walkability_per_1000'] = self.va_df['walkability'] / 1000
        
        self.status_updater(self.status_pct[3], "VA component: loading table: emp_access")
        # New column: pctemp30t, from emp_access DB table
        query_string = "SELECT * from emp_access;"
        temp_emp_access_df = self.db._raw_query(qry=query_string)
        
        self.va_df = pd.merge(left=self.va_df, right=temp_emp_access_df, how="left", left_on='taz_id',right_on='taz_id')
        
        self.status_updater(self.status_pct[4], "VA component: loading table: access_density")
        # New columns: 
		# Add 'dense', 'urban', 'fringe', and 'rural' to a local DF created from the "access_density" DB table.
        query_string = "SELECT * from access_density;"
        temp_df = self.db._raw_query(qry=query_string)

        self.status_updater(self.status_pct[5], "VA component: working on regression model")
        cols = list(temp_df.columns)
        ix7 = cols.index("access_density")

        # Set up a temp dataframe, with explict columns for each class of access density
        temp_df['dense'] = temp_df.apply(lambda row: 1 if (row[ix7] == 1 or row[ix7] == 2) else 0, axis=1,raw=True)
        temp_df['urban'] = temp_df.apply(lambda row: 1 if row[ix7] == 3 else 0, axis=1,raw=True)
        temp_df['fringe'] = temp_df.apply(lambda row: 1 if row[ix7] == 4 else 0, axis=1,raw=True)
        temp_df['rural'] = temp_df.apply(lambda row: 1 if row[ix7] == 6 else 0, axis=1,raw=True)
        temp_df = temp_df.drop(columns=['access_density'])
        self.va_df = pd.merge(left=self.va_df, right=temp_df, how="left", left_on='taz_id',right_on='taz_id')
        
        #
        # Paul Reim's implementation continues here:
        
        # Calculate the log of the vehicle count
        # Intercept is the first coefficient
        self.va_df['log_veh']=self.coeffs[coeff_names[0]]

        # Iterate over the remaining coefficients
        for i in range(1,len(coeff_names)):
            coeff_name = coeff_names[i]
            self.va_df['log_veh'] = self.va_df['log_veh'] + self.va_df[coeff_name] * self.coeffs[coeff_name]
        
        # Calculate the predicted household vehicle count (note: estimates veh + 1).
        self.va_df['num_vehs'] = self.va_df.apply(lambda row: int(max(round(math.exp(row.log_veh),0)-1,0)), axis=1)
        
        # DEBUG
        if self.args["loglevel"] == "DEBUG":
            dump_csv_fn = self.args['OutputFolder'] + '\\_logs\\' + 'va_df_dump.csv'
            self.va_df.to_csv(dump_csv_fn, index=False)
            max_log_veh = self.va_df['log_veh'].max()
            min_log_veh = self.va_df['log_veh'].min()
            print("max log_veh = " + str(max_log_veh) + " min log_veh = " + str(min_log_veh))


        self.status_updater(self.status_pct[6], "VA component: create fields for digest")
        # Set some flags
        cols = list(self.va_df.columns)
        veh_idx = cols.index("num_vehs")
        drv_idx = cols.index("drivers")

        def vsuff(num_vehs, drivers):
            if num_vehs == 0:
                veh_suff = 'zv'
            elif num_vehs == drivers:
                veh_suff = 'sv'
            elif num_vehs < drivers:
                veh_suff = 'iv'
            return veh_suff

        self.va_df['veh_suff'] = self.va_df.apply(lambda row: vsuff(row[veh_idx],row[drv_idx]),axis=1)        
        self.va_df = self.va_df[['hid','block_id','veh_suff','num_vehs']]

        self.status_updater(self.status_pct[7], "VA component: writing to table: veh")
        self.va_df.to_sql(name="veh",con=self.db.conn,if_exists="replace",index=False)
        return None


    def run_summaries(self):
        """
        [generate summaries of model estimates]
        inputs:     database "veh" table, also stored in self.va_df
        outputs:    va_summary.csv file
        returns:    None
        """
        self.status_updater(self.status_pct[8], "VA component: preparing summary")

        query_string = "SELECT * from veh;"
        veh_df = self.db._raw_query(qry=query_string)
        num_hhs = len(veh_df)
        tot_veh = veh_df['num_vehs'].sum()
        tot_zero_veh_hhs = veh_df.loc[veh_df.veh_suff=='zv','veh_suff'].count()
        tot_veh_lt_drv_hhs = veh_df.loc[veh_df.veh_suff=='iv','veh_suff'].count()
        tot_veh_ge_drv_hhs = veh_df.loc[veh_df.veh_suff=='sv','veh_suff'].count()
        
        data = { 'data' : ['total vehicles', 
                            'vehicles per household',
                            'zero vehicle households', 
                            'insufficient vehicle households', 
                            'sufficient vehicle households'],
                 'share' : [ '-','-',tot_zero_veh_hhs/num_hhs, tot_veh_lt_drv_hhs/num_hhs, tot_veh_ge_drv_hhs/num_hhs],
                 'value' : [tot_veh, tot_veh/num_hhs, tot_zero_veh_hhs, tot_veh_lt_drv_hhs, tot_veh_ge_drv_hhs] }
        summary_df = pd.DataFrame(data)
        
        self.logger.debug("Contents of vehicle availability summary DF:\n")
        self.logger.debug(summary_df.head(10))
        
       
        self.status_updater(self.status_pct[9], "VA component: dumping table to datebase")
        output_csv_fn = self.args['OutputFolder']  + '\\_summary\\zonal\\' + 'vehicle_availability_summary.csv'
        summary_df.to_csv(output_csv_fn, index=False)

        return None
    # end_def run_summaries()






if __name__ == "__main__":

    va = veh_avail()
    #hh_df = va.load_hh()
    #print(hh_df.head())
    #emp_df = va.load_emp()
    #print(emp_df.head())
    #va.load_zonal_data()
    #va.run_model()
