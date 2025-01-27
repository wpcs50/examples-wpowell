
from asyncio.log import logger
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np

class airport_tripgeneration(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        
        ymf = self.args["Airport Generation"] 
        with open(ymf, 'r') as file:
            self.air_rate = yaml.safe_load(file)

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
        """[estimate airport trips by TAZ balanced to input enplanements]"""

        air_ls = self.args["Purpose Segments"]["air"]["Segments"].split(",")

        air_attr_df = self.load_attractions(air_ls)   
        air_prod_df = self.calc_productions(air_ls)   
        air_df = self.balance_prod_to_attr(air_ls, air_attr_df, air_prod_df)   
        self.pknp(air_ls, air_df)   

        self.run_summaries(air_ls)
        
    def load_attractions(self, air_ls):
        """	[Load internal airport attractions by segment]
        """

        # initialize dataframe
        query_string = "SELECT taz_id from MA_taz_geography;"
        taz_df = self.db._raw_query(qry=query_string)
        air_df = taz_df.reindex(columns=list(["taz_id"]+
                                            [seg + "_a" for seg in air_ls]), fill_value=0)        

        ### calculate attraction end trips
        air_taz = self.args["Airport TAZ"]

        # total trips, less externals 
        air_tot = self.args["Daily air trips"]
        air_ext = self.args["Airport Externals"]
        air_int = air_tot * (1-air_ext)
        air_int

        # load and calculate trips attracted by segment
        for seg in air_ls: 
            shr = self.args["Airport Segments"][seg]["Share"]
            air_df.loc[air_df.taz_id==air_taz, seg + "_a"] =  shr * air_int
        
        return air_df.set_index('taz_id')

    def calc_productions(self, air_ls):
        """	Calculate unbalanced productions by segment
        """
        self.status_updater(self.status_pct[1],"Airport Trip Production")

        # get employment and households by block
        qry_txt = """SELECT total_households, j.* 
                    FROM block_sed b JOIN jobs j USING(block_id)"""
        emphh_df = self.db._raw_query(qry_txt).drop(columns=['taz_id'])

        # enrollment
        enr_df = self.db._raw_query(qry="SELECT taz_id, college_total FROM enrollment").set_index('taz_id')

        # aggregate from blocks to taz
        qry_txt = """SELECT * FROM taz_block_allocation"""
        taz_blk_df = self.db._raw_query(qry_txt)

        data_cols = list(emphh_df.columns)
        data_cols.remove('block_id')

        blk_emphh_df = taz_blk_df.merge(emphh_df, 'left', on='block_id').fillna(0)
        blk_emphh_df.set_index(['taz_id'], inplace=True)
        taz_emphh_df = blk_emphh_df[data_cols].multiply(blk_emphh_df['area_fct'], axis="index").groupby(by="taz_id").agg('sum')

        taz_all_df = taz_emphh_df.merge(enr_df[['college_total']], how='left',left_index=True,right_index=True)    

        # calculate ring segments
        qry_txt = """SELECT taz_id, 
                        ring in (0,1) as ring01, 
                        ring in (0,1,2) as ring012, 
                        ring in (0,1,2,3) as ring0123,
						ring in (0,1,2,3,4,5,6,7) as ring_total
                        FROM MA_taz_geography;"""
        ring_df = self.db._raw_query(qry=qry_txt).set_index('taz_id')

        # create segmented and combined variables
        air_df = taz_all_df.merge(ring_df, how='left',left_index=True,right_index=True)    
        air_df['rl_ring01'] = air_df['6_ret_leis'] * air_df['ring01']   
        air_df['rl_ring_total'] = air_df['6_ret_leis']  * air_df['ring_total']
        air_df['hh_ring012'] = air_df['total_households'] * air_df['ring012']   
        air_df['hh_ring0123'] = air_df['total_households'] * air_df['ring0123']   
        air_df['total_jobs_ring0123'] = air_df['total_jobs'] * air_df['ring0123']   
        air_df['office_service'] = air_df['2_eduhlth'] + air_df['3_finance'] + air_df['4_public'] + air_df['9_profbus']

        # apply coefficients
        for seg in air_ls: 
            col = seg + "_p"
            air_df[col] = 0
            air_coeffs = self.air_rate["%s_coeffs"%seg]
            for term in air_coeffs: 
                air_df[col] += air_df[term] * air_coeffs[term]      

        if self.args['loglevel'] in {"DEBUG"}: 
            air_df.to_csv(self.args["OutputFolder"] + '\\_summary\\trips\\' + 'air_trip_productions.csv')

        air_export_df = air_df[[seg + "_p" for seg in air_ls]]        
        return air_export_df
          

    def balance_prod_to_attr(self, air_ls, air_attr_df, air_prod_df):
        """	Combine and balance to attractions
        """
        air_df = air_attr_df.merge(air_prod_df, left_index=True,right_index=True)

        for seg in air_ls: 
            factor = air_df[seg+'_a'].sum() / air_df[seg+'_p'].sum()
            air_df[seg+'_p'] *= factor

        return air_df

    def pknp(self,air_ls, air_df):
        """ Apply peak / non-peak factors and export to db """

        pknp_facts = self.args["AirPeakNonpeak"]

        def airport_per(per):
            x_df = air_df.copy().reset_index()
            peak = 1 if per=="Peak" else 0
            x_df["peak"] = peak
            for seg in air_ls:
                seg_pknp = pknp_facts[per][seg]
                for trip_end in ['_a','_p']:
                    x_df[seg + trip_end] *= seg_pknp
            return x_df

        air_exp_df = pd.concat([airport_per("Peak"),airport_per("Non-Peak")])
        air_exp_df.to_sql(name="air_trip",con=self.db.conn,if_exists="replace",index=False)

    def run_summaries(self, air_ls):	
        """[generate summaries of model estimates]"""
        self.status_updater(self.status_pct[3],"Airport Trips: summaries")

        qry_txt = "SELECT * FROM air_trip"
        air_df = self.db._raw_query(qry=qry_txt).set_index('taz_id')
        air_df['total'] = air_df[[seg + "_a" for seg in air_ls]].sum(axis=1)
        
        tot_df = air_df.drop(columns={"peak"}).sum().to_frame().transpose()
        tot_df.rename(index={0:"Total"}, inplace=True)

        summ_df = pd.concat([air_df.groupby(by='peak').sum(),tot_df])

        summ_df.rename(index={0:"Non-Peak Trips",
                        1:"Peak Trips"},inplace=True)
        summ_df.index.name = 'Internal Airport Ground Access Trips'
        
        output_csv_fn = self.args['OutputFolder']  + '\\_summary\\trips\\' + 'airport_trip_summary.csv'
        summ_df.to_csv(output_csv_fn)
