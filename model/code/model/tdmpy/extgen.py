
from asyncio.log import logger
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np

class ext_tripgeneration(disagg_model):
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

        self.status_updater(self.status_pct[3],"Loading Externals")

        #Read in external station volume file
        ext_df = pd.read_csv(self.args["ext_station_volumes"])

        # apply growth factors for future years
        ext_df['total'] = ext_df['total'] * (1 + self.args["ext_vol_growth"]/100)

        ee_df = self.load_ee_volumes(ext_df)   
        eair_df = self.load_eair_volumes(ext_df)   
        ext_seg_df = self.load_ei_volumes(ext_df, ee_df, eair_df)   
        self.run_summaries(ext_seg_df)

        
    def load_ee_volumes(self, ext_df):
        """	[load and balance external station volumes for ei, ee, and eair]
        """
        ee_ls = self.args["Purpose Segments"]["ext_ee"]["Modes"].split(",")

        # external to externals only expand to external stations
        query_string = "SELECT taz_id from MA_taz_geography WHERE type = 'E';"
        exttaz_df = self.db._raw_query(qry=query_string)

        #Calculate external to external share of trips proportion and save to ext_ee_df
        ee_df = exttaz_df.reindex(columns=list(["taz_id","total"]+ee_ls), fill_value=0)
        ee_df = ee_df.merge(ext_df, on="taz_id", how="left", suffixes=(None,"_input")).fillna(0)
        ee_df["total"] = ee_df["total_input"] * ee_df["ee"]
        ee_df["mtrk"] = ee_df["total"] * ee_df["truck"] * ee_df["mtrk_input"]
        ee_df["htrk"] = ee_df["total"] * ee_df["truck"] * ee_df["htrk_input"]
        ee_df["auto"] = ee_df["total"] * ee_df["auto_input"]

        # assert balanced external to external flows
        ee_export_df = ee_df[["taz_id"] + ee_ls]
        for mode in ee_ls:
            ee_export_df[mode+"_p"] = ee_export_df[mode] * 0.5 # inputs are 2-way traffic
            ee_export_df[mode+"_a"] = ee_export_df[mode] * 0.5 # inputs are 2-way traffic
            ee_export_df.drop(columns={mode}, inplace=True)                

        ee_export_df.to_sql(name="ext_ee_trip",con=self.db.conn,if_exists="replace",index=False)
        return ee_df[["taz_id"] + ee_ls]

    def load_eair_volumes(self, ext_df):
        """	[load and balance external station volumes for ei, ee, and eair]
        """
        eair_ls = self.args["Purpose Segments"]["ext_eair"]["Modes"].split(",")
        air_trips = self.args["Daily air trips"]
        ext_share = self.args["Airport Externals"]
        ext_occ = self.args["Airport Externals Occupancy"]
        airport_taz = self.args["Airport TAZ"]        

        # get full list of tazs to expand external inputs
        query_string = "SELECT taz_id from MA_taz_geography;"
        taz_df = self.db._raw_query(qry=query_string)

        #Calculate airport share of trips proportion and save to ext_eair_df
        eair_total = air_trips * ext_share / ext_occ    
        eair_shr = eair_total / ext_df["total"].sum()    

        eair_df = taz_df.reindex(columns=list(["taz_id"]+eair_ls), fill_value=0)
        eair_df = eair_df.merge(ext_df, on="taz_id", how="left", suffixes=(None,"_input")).fillna(0)
        eair_df["auto"] = eair_df["total"] * eair_shr

        # set externals to production, airport to attraction
        eair_df["auto_p"] = eair_df["auto"]
        eair_df["auto_a"] = 0
        eair_df.loc[eair_df["taz_id"]==airport_taz, "auto_a"] = eair_total

        eair_export_df = eair_df[["taz_id","auto_p","auto_a"]]

        eair_export_df.to_sql(name="ext_eair_trip",con=self.db.conn,if_exists="replace",index=False)
        return eair_df[["taz_id"] + eair_ls]


    def load_ei_volumes(self, ext_df, ee_df, eair_df):
        """	[load and balance external station volumes for ei, ee, and eair]
        """
        ei_ls = self.args["Purpose Segments"]["ext_ei"]["Modes"].split(",")

        # get full list of tazs to expand external inputs
        query_string = "SELECT taz_id from MA_taz_geography;"
        taz_df = self.db._raw_query(qry=query_string)

        #Calculate total external trips for input external stations only        
        ei_df = ext_df[["taz_id"]].reindex(columns=list(["taz_id"]+ei_ls), fill_value=0)
        ei_df = ei_df.merge(ext_df, on="taz_id", how="left", suffixes=(None,"_input")).fillna(0)
        ei_df["mtrk"] = ei_df["total"] * ei_df["truck"] * ei_df["mtrk_input"]
        ei_df["htrk"] = ei_df["total"] * ei_df["truck"] * ei_df["htrk_input"]
        ei_df["auto"] = ei_df["total"] * ei_df["auto_input"]
        ei_df = ei_df[["taz_id"]+ei_ls]

        # remove ee and eair
        ei_df = ei_df.merge(ee_df, on="taz_id", how="left", suffixes=(None,"_ee"))
        ei_df = ei_df.merge(eair_df, on="taz_id", how="left", suffixes=(None,"_eair"))
        ei_df["mtrk"] = ei_df["mtrk"] - ei_df["mtrk_ee"]
        ei_df["htrk"] = ei_df["htrk"] - ei_df["htrk_ee"]
        ei_df["auto"] = ei_df["auto"] - ei_df["auto_ee"] - ei_df["auto_eair"]
        #ei_df = ei_df[["taz_id"]+ei_ls]

        #expand to all tazs
        ei_df = taz_df.merge(ei_df, on="taz_id", how="left").fillna(0)

        #Read household trip attractions by zone
        qry_txt = """SELECT * FROM taz_block_allocation"""
        taz_blk_df = self.db._raw_query(qry_txt)

        qry_txt = """SELECT * FROM trip_attr"""
        blk_attr_df = self.db._raw_query(qry_txt).drop(columns=['taz_id'])
        data_cols = list(blk_attr_df.columns)
        data_cols.remove('block_id')

        taz_attr_df = taz_blk_df.merge(blk_attr_df, 'left', on='block_id').fillna(0)
        taz_attr_df = taz_attr_df.set_index(['taz_id'])
        taz_attr_df = taz_attr_df[data_cols].multiply(taz_attr_df['area_fct'], axis="index").groupby(by="taz_id").agg('sum')
        ext_autoa_df = pd.DataFrame(taz_attr_df.sum(axis=1), columns={"auto_a"})
        if(ext_autoa_df.size == 0):
            raise Exception("No auto attractions - trip gen model not yet run")

        #Read truck trip attractions by zone
        qry_txt = """SELECT taz_id, mtrk_a, htrk_a FROM trk_trip"""
        ext_trka_df = self.db._raw_query(qry_txt)
        if(ext_trka_df.size == 0):
            raise Exception("No truck attractions - truck model not yet run")

        #Balance attractions to productions
        auto_fact = ei_df['auto'].sum() / ext_autoa_df['auto_a'].sum()
        mtrk_fact = ei_df['mtrk'].sum() / ext_trka_df['mtrk_a'].sum()
        htrk_fact = ei_df['htrk'].sum() / ext_trka_df['htrk_a'].sum()

        ext_autoa_df['auto_a'] = ext_autoa_df['auto_a'] * auto_fact 
        ext_trka_df['mtrk_a'] = ext_trka_df['mtrk_a'] * mtrk_fact 
        ext_trka_df['htrk_a'] = ext_trka_df['htrk_a'] * htrk_fact 

        # Combine and export
        ei_df = ei_df.merge(ext_autoa_df, on="taz_id")
        ei_df = ei_df.merge(ext_trka_df, on="taz_id")

        for mode in ei_ls:
            ei_df[mode+"_p"] = ei_df[mode]
            ei_df.drop(columns={mode}, inplace=True)                

        ei_export_df = ei_df[["taz_id"] + [mode + "_p" for mode in ei_ls] + [mode + "_a" for mode in ei_ls]]            
        ei_export_df.to_sql(name="ext_ei_trip",con=self.db.conn,if_exists="replace",index=False)
        return ei_df

    def run_summaries(self, ext_seg_df):	
        """[generate summaries of model estimates]"""
        self.status_updater(self.status_pct[3],"External Trips: summaries")

        # only summarize at external station
        query_string = "SELECT taz_id from MA_taz_geography WHERE type = 'E';"
        exttaz_df = self.db._raw_query(qry=query_string)

        summ_df = exttaz_df.merge(ext_seg_df, on="taz_id", how="left")
        summ_df['total_ee'] = summ_df['auto_ee'] + summ_df['mtrk_ee'] + summ_df['htrk_ee'] 
        summ_df['total_ei'] = summ_df['auto_p'] + summ_df['mtrk_p'] + summ_df['htrk_p'] 
        summ_df['total_ext'] = summ_df['total_ee'] + summ_df['total_ei'] + summ_df['auto_eair']
        summ_df['total_auto'] = summ_df['auto_ee'] + summ_df['auto_p'] + summ_df['auto_eair']
        summ_df['total_mtrk'] = summ_df['mtrk_p'] + summ_df['mtrk_ee']
        summ_df['total_htrk'] = summ_df['htrk_p'] + summ_df['htrk_ee']

        tot_df = pd.DataFrame({'total':[summ_df['total_ext'].sum(),summ_df['total_ee'].sum(),
                                summ_df['auto_eair'].sum(),summ_df['total_ei'].sum()],
                      'auto':[summ_df['total_auto'] .sum(),summ_df['auto_ee'].sum(),
                                summ_df['auto_eair'].sum(),summ_df['auto_p'].sum()],
                      'mtrk':[summ_df['total_mtrk'] .sum(),summ_df['mtrk_ee'].sum(),
                                0,summ_df['mtrk_p'].sum()],        
                      'htrk':[summ_df['total_htrk'] .sum(),summ_df['htrk_ee'].sum(),
                                0,summ_df['htrk_p'].sum()]})

        tot_df.rename(index={0:"Total",
                        1:"To External", 
                        2:"To Airport",
                        3:"To Internal"},inplace=True)
        tot_df.index.name = 'External Trips'
        
        output_csv_fn = self.args['OutputFolder']  + '\\_summary\\trips\\' + 'external_generation_summary.csv'
        tot_df.to_csv(output_csv_fn, index=True)
