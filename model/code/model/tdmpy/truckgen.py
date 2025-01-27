
from asyncio.log import logger
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np

class truck_tripgeneration(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        
        ymf = self.args["Truck Generation"] 
        with open(ymf, 'r') as file:
            self.trk_rate = yaml.safe_load(file)

        # initialize the status
        self.status_pct = [0, 25, 50, 75, 100]
        
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
            # print(traceback.format_exc())

            self.status_updater(-1, "**Error**: Click cancel to check the error message %s"%str(e) )
            # self.popup.accept() //reject done close
        

    def run_model(self):
        """[load parameters and call appropriate model run method]"""

        trk_df = self.truck_generation()   
        self.run_summaries(trk_df)

        
    def truck_generation(self):
        """	[estimate truck trips by TAZ]
        """
        trk_ls = self.args["Purpose Segments"]["trk"]["Modes"].split(",")

        # load land use data, convert from block to taz level
        self.status_updater(self.status_pct[1],"Truck Trips: preparing data")

        qry_txt = """SELECT * FROM taz_block_allocation"""
        taz_blk_df = self.db._raw_query(qry_txt)

        qry_txt = """SELECT * FROM block_sed"""
        blk_emp_df = self.db._raw_query(qry_txt).drop(columns=['taz_id'])
        data_cols = list(blk_emp_df.columns)
        data_cols.remove('block_id')

        taz_emp_df = taz_blk_df.merge(blk_emp_df, 'left', on='block_id').fillna(0)
        taz_emp_df = taz_emp_df.set_index(['taz_id'])
        trk_df = taz_emp_df[data_cols].multiply(taz_emp_df['area_fct'], axis="index").groupby(by="taz_id").agg('sum')

        # create additional data vectors as needed
        qry_txt = """SELECT * FROM access_density"""
        accd_df = self.db._raw_query(qry_txt)
        trk_df = trk_df.merge(accd_df, on='taz_id')
        trk_df['1_constr_subrur'] = trk_df.loc[trk_df.access_density>4,'1_constr']
        trk_df['8_other_subrur'] = trk_df.loc[trk_df.access_density>4,'8_other']
        trk_df.fillna(0,inplace=True)
        trk_df.set_index(['taz_id'],inplace=True)

        # apply coefficients
        self.status_updater(self.status_pct[2],"Truck Trips: calculating trips")

        for trk in trk_ls: 
            trk_df[trk] = 0
            trk_coeffs = self.trk_rate["%s_coeffs"%trk]
            for term in trk_coeffs: 
                trk_df[trk] = trk_df[trk] + trk_df[term] * trk_coeffs[term]
        
        if self.args['loglevel'] in {"DEBUG"}: 
            trk_df.to_csv(self.args["OutputFolder"] + '\\_logs\\' + 'truck_trip_generation.csv')

        trk_export_df = trk_df[trk_ls]

        # copy generated values to outputs for both productions and attractions
        for trk in trk_ls:
            trk_export_df[trk+"_p"] = trk_export_df[trk]
            trk_export_df[trk+"_a"] = trk_export_df[trk]
            trk_export_df.drop(columns={trk}, inplace=True)


        trk_export_df.reset_index().to_sql(name="trk_trip",con=self.db.conn,if_exists="replace",index=False)

        return trk_df

    def run_summaries(self, trk_df):	
        """[generate summaries of model estimates]"""
        self.status_updater(self.status_pct[3],"Truck Trips: summaries")

        trk_ls = self.args["Purpose Segments"]["trk"]["Modes"].split(",")    
        trk_df['total'] = trk_df[trk_ls].sum(axis=1)

        tot_trk_df = trk_df[trk_ls + ['total']].sum()

        summ_df = pd.concat([tot_trk_df,
                  tot_trk_df / trk_df['total_jobs'].sum(),
                  tot_trk_df / trk_df['total_households'].sum()],axis=1).transpose().round(3)
        summ_df.rename(index={0:"Total",
                        1:"Trips per Total Employment", 
                        2:"Trips per Household"},inplace=True)
        summ_df.index.name = 'Truck Trips'
        
        output_csv_fn = self.args['OutputFolder']  + '\\_summary\\trips\\' + 'truck_trip_summary.csv'
        summ_df.to_csv(output_csv_fn)
