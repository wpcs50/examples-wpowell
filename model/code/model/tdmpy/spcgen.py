
from asyncio.log import logger
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np

class spcgen_tripgeneration(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        
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

        self.status_updater(self.status_pct[3],"Loading Special Generators")

        #Read in external station volume file
        

        self.load_and_balance()   
        self.run_summaries()

        
    def load_and_balance(self):
        """	[load and balance special generator volumes]
        """
        spc_ls = self.args["Purpose Segments"]["spcgen"]["Segments"].split(",")
        raw_df = pd.read_csv(self.args["Special Generators"])

        # read in desired segments from special generator inputs
        spc_df = raw_df[['taz_id'] +
                        [sub + "_pk" for sub in spc_ls] + 
                        [sub + "_np" for sub in spc_ls]]       

        #Read household trip attractions by zone
        qry_txt = """SELECT * FROM taz_block_allocation"""
        taz_blk_df = self.db._raw_query(qry_txt)

        qry_txt = "SELECT block_id, " +\
                    ','.join([sub + "_p" for sub in spc_ls]) +\
                    " FROM trip_prod"
        blk_attr_df = self.db._raw_query(qry_txt)
        data_cols = list(blk_attr_df.columns)
        data_cols.remove('block_id')

        taz_attr_df = taz_blk_df.merge(blk_attr_df, 'left', on='block_id').fillna(0)
        taz_attr_df = taz_attr_df.set_index(['taz_id'])
        taz_attr_df = taz_attr_df[data_cols].multiply(taz_attr_df['area_fct'], axis="index").groupby(by="taz_id").agg('sum')   

        # Merge productions and attractions and balance
        spc_df = spc_df.merge(taz_attr_df, how='right', on='taz_id').fillna(0)
        for seg in spc_ls:
            for per in ["pk","np"]:
                factor = spc_df[seg + "_" + per].sum() / spc_df[seg + "_p"].sum()
                spc_df[seg + "_p" + "_" + per] = spc_df[seg + "_p"] * factor
        
        # Write out to database
        def spc_export_per(per):
            x_df = spc_df[['taz_id']]
            peak = 1 if per=="pk" else 0
            x_df["peak"] = peak
            for seg in spc_ls:
                x_df[seg + "_a"] = spc_df[seg + "_" + per]
                x_df[seg + "_p"] = spc_df[seg + "_p_" + per]
            return x_df

        spc_export_df = pd.concat([spc_export_per("pk"),spc_export_per("np")])
          
        spc_export_df.to_sql(name="spcgen_trip",con=self.db.conn,if_exists="replace",index=False)
        return spc_df

    def run_summaries(self):	
        """[generate summaries of model estimates]"""
        self.status_updater(self.status_pct[3],"Special Generator Trips: summaries")

        qry_txt = """SELECT * FROM spcgen_trip"""
        raw_df = self.db._raw_query(qry_txt)

        spc_ls = self.args["Purpose Segments"]["spcgen"]["Segments"].split(",")
        spc_df = raw_df[['taz_id'] +
                        [sub + "_a" for sub in spc_ls]] 

        summ_df = spc_df.set_index('taz_id').agg(['sum','max','mean'])
        
        output_csv_fn = self.args['OutputFolder']  + '\\_summary\\trips\\' + 'spcgen_summary.csv'
        summ_df.to_csv(output_csv_fn, index=True)
