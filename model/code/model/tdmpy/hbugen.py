
from asyncio.log import logger
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np

class hbu_tripgeneration(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        
        ymf = self.args["hbu_trip_gen"] 
        with open(ymf, 'r') as file:
            self.hbu_rate = yaml.safe_load(file)

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
        """[estimate hbu trips by TAZ balanced to attractions at enrollment]"""

        hbu_ls = self.args["Purpose Segments"]["hbu"]["Segments"].split(",")

        hbu_attr_df = self.calc_attractions(hbu_ls)   
        hbu_prod_df = self.calc_productions(hbu_ls)   
        hbu_df = self.balance_prod_to_attr(hbu_ls, hbu_attr_df, hbu_prod_df)   
        self.pknp(hbu_ls, hbu_df)   

        self.run_summaries(hbu_ls)
        
    def calc_attractions(self, hbu_ls):
        """	[Load internal hbu attractions by segment]
        """
        self.status_updater(self.status_pct[1],"HBU Trip Attractions")        
        query_string = "SELECT * from enrollment;"
        hbu_df = self.db._raw_query(qry=query_string)
        hbu_df.set_index('taz_id',inplace=True)

        # apply coefficients
        hbu_coeffs = self.hbu_rate["attr_coeffs"]
        for seg in hbu_ls:
            seg_coeffs = hbu_coeffs[seg]
            for term in seg_coeffs:
                col = seg + "_a"
                hbu_df[col] = hbu_df[term] * seg_coeffs[term]
            
        if self.args['loglevel'] in {"DEBUG"}: 
            hbu_df.to_csv(self.args["OutputFolder"] + '\\_logs\\' + 'hbu_trip_attraction.csv')

        hbu_export_df = hbu_df[[seg + "_a" for seg in hbu_ls]]
        return hbu_export_df

    def calc_productions(self, hbu_ls):
        """	Calculate unbalanced productions by segment
        """
        self.status_updater(self.status_pct[2],"HBU Trip Production")

        # load SE data and derived needed variables
        qry_txt = """SELECT hh.block_id, 
                        hh.taz_id,
                        1 as const,
                        sum(workers) as workers, 
                        sum(nwadult) as nwadult, 
                        sum(seniors) as seniors,
                        (veh_suff == "sv") as sv_hh,
                        (hh_inc_cat_by_size == 3) as highinc_hh,
                        (access_density < 3) as cbd_dense
                    FROM hh JOIN veh USING(hid) JOIN access_density USING(taz_id)
                    GROUP BY hh.block_id, hh.taz_id,
                    (veh_suff == "sv"), (hh_inc_cat_by_size == 3),
                    (access_density < 3)"""
        hh_df = self.db._raw_query(qry_txt)        

        # attach ring definition to limit trip production
        qry_txt = "SELECT taz_id, ring in " + \
                    self.hbu_rate['ring_area'] + \
                    " as trip_area FROM MA_taz_geography;"
        taz_df = self.db._raw_query(qry=qry_txt).set_index('taz_id')   

        hbu_df = hh_df.merge(taz_df, on='taz_id')

        # apply coefficients
        prod_coeffs = self.hbu_rate['prod_coeffs']

        for seg in hbu_ls: 
            col = seg + "_p"
            hbu_df[col] = 0
            hbu_coeffs = prod_coeffs[seg]
            for term in hbu_coeffs: 
                hbu_df.loc[hbu_df.trip_area==1,col] += \
                    hbu_df.loc[hbu_df.trip_area==1,term] * hbu_coeffs[term]                  

        # aggregate from blocks to taz
        qry_txt = """SELECT * FROM taz_block_allocation"""
        taz_blk_df = self.db._raw_query(qry_txt)

        hbu_cln_df = hbu_df.set_index('block_id').drop(columns={'taz_id'})
        blk_hh_df = taz_blk_df.merge(hbu_cln_df, 'left', on='block_id').fillna(0)
        blk_hh_df.set_index(['taz_id'], inplace=True)

        # apply block to taz factor and aggregate to taz level
        tazfct_hh_df = blk_hh_df[[seg + "_p" for seg in hbu_ls]].multiply(
                    blk_hh_df['area_fct'], axis="index")
        taz_hh_df = tazfct_hh_df.groupby(by="taz_id").agg('sum')

        if self.args['loglevel'] in {"DEBUG"}: 
            hbu_df.to_csv(self.args["OutputFolder"] + '\\_logs\\' + 'hbu_trip_productions.csv')

        hbu_export_df = taz_hh_df[[seg + "_p" for seg in hbu_ls]]   
        return hbu_export_df        
          

    def balance_prod_to_attr(self, hbu_ls, hbu_attr_df, hbu_prod_df):
        """	Combine and balance to attractions
        """
        hbu_df = hbu_attr_df.merge(hbu_prod_df, left_index=True,right_index=True)

        for seg in hbu_ls: 
            factor = hbu_df[seg+'_a'].sum() / hbu_df[seg+'_p'].sum()
            hbu_df[seg+'_p'] *= factor

        return hbu_df

    def pknp(self,hbu_ls, hbu_df):
        """ Apply peak / non-peak factors and export to db """

        pknp_facts = self.args["hbu_pknp"]

        def hbu_per(per):
            x_df = hbu_df.copy().reset_index()
            peak = 1 if per=="Peak" else 0
            x_df["peak"] = peak
            for seg in hbu_ls:
                seg_pknp = pknp_facts[per][seg]
                for trip_end in ['_a','_p']:
                    x_df[seg + trip_end] *= seg_pknp
            return x_df

        hbu_exp_df = pd.concat([hbu_per("Peak"),hbu_per("Non-Peak")])
        hbu_exp_df.to_sql(name="hbu_trip",con=self.db.conn,if_exists="replace",index=False)

    def run_summaries(self, hbu_ls):	
        """[generate summaries of model estimates]"""
        self.status_updater(self.status_pct[3],"HBU Trips: summaries")

        qry_txt = "SELECT * FROM hbu_trip"
        hbu_df = self.db._raw_query(qry=qry_txt).set_index('taz_id')
        hbu_df['total'] = hbu_df[[seg + "_a" for seg in hbu_ls]].sum(axis=1)
        
        tot_df = hbu_df.drop(columns={"peak"}).sum().to_frame().transpose()
        tot_df.rename(index={0:"Total"}, inplace=True)

        summ_df = pd.concat([hbu_df.groupby(by='peak').sum(),tot_df])

        summ_df.rename(index={0:"Non-Peak Trips",
                        1:"Peak Trips"},inplace=True)
        summ_df.index.name = 'Off-Campus HBU Trips Trips'
        
        output_csv_fn = self.args['OutputFolder']  + '\\_summary\\trips\\' + 'hbu_trip_summary.csv'
        summ_df.to_csv(output_csv_fn)
