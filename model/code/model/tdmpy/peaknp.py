
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np

class peak_nonpeak(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger

        self.pnpfct = self.args["PeakNonpeak"]

        # clear records from output tables
        self.db._raw_query("delete from trip_prod_pknp;") 
        self.db._raw_query("delete from trip_prod_nhb_pknp;") 
        # initialize the status
        self.status_pct = [0, 12, 67, 97, 100]
        
    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:    
            self.hh_split()
            self.taz_split()
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
            # print(traceback.format_exc())

            self.status_updater(-1, "**Error**: Click cancel to check the error message %s"%str(e) )
            # self.popup.accept() //reject done close


    def hh_split(self):
        """
        Trips produced by workers / household 
        (if hh, person_num = null) for peak and non-peak periods

        """
        df_prod = self.db._raw_query("""SELECT hid,block_id,person_num, 
                                        hbw_p,hbsr_p,hbsc_p,hbpb_p,nhbw_p,nhbnw_p
                                        FROM trip_prod """)

        # self.logger.info("hh_split ")
        self.status_updater(self.status_pct[1],"Trip Gen component: peak-np hh_split  ")

        df_prod_pk = df_prod.copy()
        # if peak == 1:
        df_prod_pk["peak"] 		 = 1
        df_prod_pk["hbw_p"]   *= self.pnpfct["Peak"]["hbw"]
        df_prod_pk["hbsr_p"]  *= self.pnpfct["Peak"]["hbsr"]
        df_prod_pk["hbpb_p"]  *= self.pnpfct["Peak"]["hbpb"]
        df_prod_pk["hbsc_p"]  *= self.pnpfct["Peak"]["hbsc"]
        df_prod_pk["nhbw_p"]  *= self.pnpfct["Peak"]["nhbw"]
        df_prod_pk["nhbnw_p"] *= self.pnpfct["Peak"]["nhbnw"]
        # else peak == 0
        df_prod_np = df_prod.copy()
        df_prod_np["peak"] 		 = 0
        df_prod_np["hbw_p"]   *= self.pnpfct["Non-Peak"]["hbw"]
        df_prod_np["hbsr_p"]  *= self.pnpfct["Non-Peak"]["hbsr"]
        df_prod_np["hbpb_p"]  *= self.pnpfct["Non-Peak"]["hbpb"]
        df_prod_np["hbsc_p"]  *= self.pnpfct["Non-Peak"]["hbsc"]
        df_prod_np["nhbw_p"]  *= self.pnpfct["Non-Peak"]["nhbw"]
        df_prod_np["nhbnw_p"] *= self.pnpfct["Non-Peak"]["nhbnw"]
        
        df_pknp = pd.concat([df_prod_pk,df_prod_np],axis=0)
        df_pknp = df_pknp.round(4)

        df_pknp.to_sql(name="trip_prod_pknp",con=self.db.conn,if_exists="append",index=False,
							schema={"block_id":"text"}) 


    def taz_split(self):
        """
        Non-home based trips produced by block for peak and non-peak periods

        """

        df_pdnhb = self.db._raw_query("""SELECT block_id,taz_id,
                                                nhbw_p,nhbnw_p FROM prod_nhb """)
        # self.logger.info("taz_split ")
        self.status_updater(self.status_pct[2],"Trip Gen component: peak-np taz_split  ")
        df_pdnhb_pk = df_pdnhb.copy()
        # if peak == 1:
        df_pdnhb_pk["peak"] 		 = 1
        df_pdnhb_pk["nhbw_p"]   *= self.pnpfct["Peak"]["nhbw"]
        df_pdnhb_pk["nhbnw_p"]  *= self.pnpfct["Peak"]["nhbnw"]
        # else:
        df_pdnhb_np = df_pdnhb.copy()
        df_pdnhb_np["peak"] 		 = 0
        df_pdnhb_np["nhbw_p"]   *= self.pnpfct["Non-Peak"]["nhbw"]
        df_pdnhb_np["nhbnw_p"]  *= self.pnpfct["Non-Peak"]["nhbnw"]

        df_pknp = pd.concat([df_pdnhb_pk,df_pdnhb_np],axis=0)
        df_pknp = df_pknp.round(4)

        df_pknp.to_sql(name="trip_prod_nhb_pknp",con=self.db.conn,if_exists="append",index=False,
                                    schema={"block_id":"text"}) 

    def run_summaries(self):

        trips_df = self.db._raw_query("SELECT * from trip_prod_pknp;")
        hh_tot_df = self.db._raw_query("""
                        SELECT sum(persons) as tot_persons, 
                                sum(workers) as tot_workers, 
                                count(hid) as tot_hh FROM hh""")

        # self.logger.info("run_summaries ")
        self.status_updater(self.status_pct[3],"Trip Gen component: peak-np run_summaries  ")

        trips_df['total'] = trips_df[['hbw_p','hbpb_p','hbsr_p','hbsc_p','nhbw_p','nhbnw_p']].sum(axis=1)

        tot_trips_p = trips_df[['hbw_p','hbpb_p','hbsr_p','hbsc_p','nhbw_p','nhbnw_p','total']].sum()
        pk_trips_p = trips_df[['hbw_p','hbpb_p','hbsr_p','hbsc_p','nhbw_p','nhbnw_p','total']][trips_df['peak']==1].sum()

        summ_df = pd.concat([tot_trips_p,
                  tot_trips_p / hh_tot_df['tot_persons'][0],
                  tot_trips_p / hh_tot_df['tot_workers'][0],
                  tot_trips_p / hh_tot_df['tot_hh'][0],
                  pk_trips_p / tot_trips_p,
                  1 - pk_trips_p / tot_trips_p],axis=1).transpose().round(3)
        summ_df.rename(index={0:"Total",
                        1:"Trips per Person", 
                        2:"Trips per Worker",
                        3:"Trips per Household",
                        4:"Peak Share",
                        5:"Non-Peak Share"},inplace=True)
        summ_df.index.name = 'Household Trips'
        summ_df = summ_df.rename(columns={'hbw_p':"hbw",'hbsr_p':'hbsr','hbpb_p':'hbpb','hbsc_p':'hbsc','nhbw_p':'nhbw','nhbnw_p':'nhbnw'})

        self.logger.debug("Contents of trip generation summary DF:\n")
        self.logger.debug(summ_df.head(10))
        
        output_csv_fn = self.args['OutputFolder']  + '\\_summary\\trips\\' + 'pknp_summary.csv'
        summ_df.to_csv(output_csv_fn)
