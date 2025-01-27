
from asyncio.log import logger
import pandas as pd 
from pathlib import Path
from decimal import Decimal

# from .base import disagg_model
from . import disagg_model
from ipfn import ipfn
import csv
import numpy as np
class work_from_home(disagg_model):
    def __init__(self,**kwargs):
        """
        Args:
            reg_rmw (float): reginal remote work level
        """
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        
        logger.debug("check the location of JSON file %s" %kwargs)

        # initialize the status
        self.status_pct = [0, 5, 40, 50, 90, 100]
        # clear records from output tables
        self.db._raw_query("delete from jobs;")         
        self.db._raw_query("delete from wfh;")         

        
    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:    
            
            self.worker_geographic_wfh()
            self.status_updater(self.status_pct[2],"summarizing wfh rate for workers")
            self.report_wfh_worker()
            self.logger.info("Work from Home: worker estimate complete ")
            
            self.job_by_sector_wfh()
            self.status_updater(self.status_pct[4],"summarizing wfh rate for jobs")
            self.report_wfh_job()
            self.logger.info("Work from Home: job estimate complete ")

            self.status_updater(100, "Closing component" )
            print("Exiting " + self.name)
            
            if self.popup == None:
                raise SystemExit()
                # self.popup.close_hide()
            elif self.popup.runwithin == "others" :
                raise SystemExit()
            # else: ## "TC"
            #     self.popup.done(1)
            #     self.popup.close()

        except Exception as e:
            import traceback
            errfile = self.args["OutputFolder"] +'\\_logs\\' + "py.err"
            with open(errfile,"a") as file:
                traceback.print_exc(file=file)
            # print(traceback.format_exc())

            self.status_updater(-1, "**Error**: Click cancel to check the error message %s"%str(e) )
            # self.popup.accept() //reject done close

    def worker_geographic_wfh(self):

        # inputs 1/2
        wfh_mode = self.args["WFH Mode"]

        # read Database tables 1/2
        ## worker info
        wkr_df = self.db._raw_query("select block_id, taz_id, hid, person_num from per where is_worker = 1")   
     
        if (wfh_mode == "WFH for workers only" or wfh_mode == "WFH for both workers and employment"):
            # inputs 2/2
            wkr_reg_rate =  self.args["Regional Default WFH Rate"]

            state_list = self.args["WFH Rate by State"]["State"] 
            state_bool_list = self.args["WFH Rate by State"]["Different from Regional Default"]
            state_rate_list = self.args["WFH Rate by State"]["WFH Rate"]
            
            mpo_list = self.args["WFH Rate by MPO"]["MPO"] 
            mpo_bool_list = self.args["WFH Rate by MPO"]["Different from Regional and State Defaults"]
            mpo_rate_list = self.args["WFH Rate by MPO"]["WFH Rate"]

            town_bool = self.args["WFH Rate by Other Town_Bool"] 
            town_rate_file = self.args["WFH Rate by Other Town"] 

            # format inputs
            state_rate_df = pd.DataFrame(list(zip(state_list,state_bool_list, state_rate_list)),
                        columns =['state', 'specified', 'wfh_rate'])
            state_rate_df=state_rate_df[state_rate_df['specified']==1][['state','wfh_rate']]
            
            mpo_rate_df = pd.DataFrame(list(zip(mpo_list,mpo_bool_list, mpo_rate_list)),
                        columns =['mpo', 'specified', 'wfh_rate'])
            mpo_rate_df=mpo_rate_df[mpo_rate_df['specified']==1][['mpo','wfh_rate']]

            if town_bool:
                town_rate_df = pd.read_csv(town_rate_file)
            
            self.status_updater(self.status_pct[1],"calculating wfh rate for workers")

            # read Database tables 2/2
            ## taz geographic info
            taz_geo_df = self.db._raw_query("select * from MA_taz_geography")

            # calculate rates
            geo_cols = ['taz_id','state','mpo','town']

            wkr_merged_df = taz_geo_df[geo_cols].copy()
            wkr_merged_df['temp_wfh_rate'] = wkr_reg_rate 

            wkr_merged_df = wkr_merged_df.merge(state_rate_df, left_on='state', right_on='state', how='left')
            wkr_merged_df.loc[wkr_merged_df['wfh_rate'].notna(), 'temp_wfh_rate'] = wkr_merged_df.loc[wkr_merged_df['wfh_rate'].notna(), 'wfh_rate'] 
            wkr_merged_df = wkr_merged_df[geo_cols+['temp_wfh_rate']]

            wkr_merged_df = wkr_merged_df.merge(mpo_rate_df, left_on='mpo', right_on='mpo', how='left')
            wkr_merged_df.loc[wkr_merged_df['wfh_rate'].notna(), 'temp_wfh_rate'] = wkr_merged_df.loc[wkr_merged_df['wfh_rate'].notna(), 'wfh_rate'] 
            wkr_merged_df = wkr_merged_df[geo_cols+['temp_wfh_rate']]

            if town_bool:
                wkr_merged_df = wkr_merged_df.merge(town_rate_df, left_on='town', right_on='town', how='left')
                wkr_merged_df.loc[wkr_merged_df['wfh_rate'].notna(), 'temp_wfh_rate'] = wkr_merged_df.loc[wkr_merged_df['wfh_rate'].notna(), 'wfh_rate'] 
                wkr_merged_df = wkr_merged_df[geo_cols+['temp_wfh_rate']]

            wkr_wfh_rate_df = wkr_merged_df[['taz_id','temp_wfh_rate']].rename(columns={'temp_wfh_rate':'wfh_rate'})

            # calculate eqs
            wkr_eqs_df = wkr_df.merge(wkr_wfh_rate_df, on='taz_id', how='left')
            wkr_eqs_df["wfh_eqs"] = wkr_eqs_df["wfh_rate"] 
            wkr_eqs_df["commute_eqs"] = 1 - wkr_eqs_df["wfh_eqs"]
            wkr_eqs_df = wkr_eqs_df[['block_id', 'taz_id', 'hid', 'person_num','wfh_eqs','commute_eqs']]
        else: 
            wkr_eqs_df = wkr_df.copy()
            wkr_eqs_df["wfh_eqs"] = 0 
            wkr_eqs_df["commute_eqs"] = 1            

        # export to database
        wkr_eqs_df.to_sql(name="wfh",con=self.db_conn,if_exists='replace',index=False) # if_exists="append"
        
        return wkr_eqs_df     

    def job_by_sector_wfh(self):

        # inputs 1/2
        wfh_mode = self.args["WFH Mode"]
        job_sec_list = self.args['Remote Level by Job Segment']['Code']

        # read Database table: job info
        job_by_sec_df = self.db._raw_query("select * from block_sed")

        # calculate eqs
        if (wfh_mode == "WFH for employment only" or wfh_mode == "WFH for both workers and employment"):
            # inputs 2/2
            job_rate_list= self.args['Remote Level by Job Segment']['WFH Rate'] 
            
            self.status_updater(self.status_pct[3],"calculating wfh rate for jobs")
            
            job_eqs_df = job_by_sec_df[['block_id','taz_id'] + job_sec_list].copy()

            job_rate_dict = dict(zip(job_sec_list, job_rate_list))
            for job_code in job_sec_list:
                if job_code in job_rate_dict:
                    job_eqs_df[job_code] = job_eqs_df[job_code] * (1 - job_rate_dict[job_code])
            job_eqs_df['total_jobs'] = job_eqs_df[job_sec_list].sum(axis=1)
        else:
            job_eqs_df = job_by_sec_df[['block_id','taz_id'] + job_sec_list + ['total_jobs']].copy()

        # export to database
        job_eqs_df.to_sql(name="jobs",con=self.db_conn,if_exists='replace',index=False) # if_exists="append"
        
        return job_eqs_df     
    
    def report_wfh_worker(self):

        # output files
        log_folder = self.args["OutputFolder"] + "\\_summary\\zonal\\"
        file_worker_state = log_folder + "wfh_summary_worker_by_state.csv"
        file_worker_mpo = log_folder + "wfh_summary_worker_by_mpo.csv"

        query_string = '''select g.state, 
        g.mpo, 
        count(w.person_num) as workers, 
        sum(w.commute_eqs) as commute_eqs, 
        sum(w.wfh_eqs) as remote_eqs 

        from wfh as w 
		
		left join MA_taz_geography as g 

        on w.taz_id = g.taz_id  

        group by g.state, g.mpo
        -- [where per.is_worker = 1] is implied when using wfh 
        '''
        wkr_sql_df = self.db._raw_query(query_string)

        wkr_sql_df = wkr_sql_df[(wkr_sql_df['commute_eqs'].notna()) | (wkr_sql_df['remote_eqs'].notna())] 
        
        wkr_state_df = wkr_sql_df.groupby(by='state').sum().reset_index() 
        wkr_state_df['wfh_rate'] = wkr_state_df['remote_eqs']/wkr_state_df['workers']
        wkr_state_df.to_csv(file_worker_state, index=None)   

        wkr_mpo_df = wkr_sql_df.groupby(by='mpo').sum().reset_index() 
        wkr_mpo_df['wfh_rate'] = wkr_mpo_df['remote_eqs']/wkr_mpo_df['workers'] 
        wkr_mpo_df.to_csv(file_worker_mpo, index=None)   

        return 1 
    
    def report_wfh_job(self):
        # output files
        log_folder = self.args["OutputFolder"] + "\\_summary\\zonal\\"
        file_job_state = log_folder + "wfh_summary_job_by_state.csv"
        file_job_mpo = log_folder + "wfh_summary_job_by_mpo.csv"

        query_string = '''select g.state, 
        g.mpo, 
        sum(s.total_jobs) as jobs, 
        sum(j.total_jobs) as commute_eqs 

        from MA_taz_geography as g  

        left join block_sed as s 
        on g.taz_id = s.taz_id 

        left join jobs as j 
        on g.taz_id = j.taz_id 

        group by g.state, g.mpo   
        '''

        job_sql_df = self.db._raw_query(query_string)
        job_sql_df = job_sql_df[job_sql_df['jobs'].notna()] 

        job_state_df = job_sql_df.groupby(by='state').sum().reset_index() 
        job_state_df['remote_eqs'] = job_state_df['jobs'] - job_state_df['commute_eqs'] 
        job_state_df['wfh_rate'] = job_state_df['remote_eqs'] / job_state_df['jobs'] 
        job_state_df.to_csv(file_job_state, index=None)  

        job_mpo_df = job_sql_df.groupby(by='mpo').sum().reset_index() 
        job_mpo_df['remote_eqs'] = job_mpo_df['jobs'] - job_mpo_df['commute_eqs'] 
        job_mpo_df['wfh_rate'] = job_mpo_df['remote_eqs'] / job_mpo_df['jobs'] 
        job_mpo_df.to_csv(file_job_mpo, index=None)  

        return 1
    

if __name__ == "__main__":


    wfh = work_from_home()
    wfh.worker_eqs()
