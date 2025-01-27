
from asyncio.log import logger
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np

class trip_generation(disagg_model):
    def __init__(self,hbo,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        
        ymf = self.args["Production Coefficient"] 
        with open(ymf, 'r') as file:
            self.coprod = yaml.safe_load(file)

        ymf_att = self.args["Attraction Coefficient"] 
        with open(ymf_att, 'r') as file:
            self.coatt = yaml.safe_load(file)    

        self.df_incsc = pd.read_csv(self.args["Income Level across Job Sectors"] )
        self.hbo = hbo
        # initialize the status
        self.status_pct = [0, 5, 9, 20, 25, 31, 52, 62, 79, 84, 85, 88,100]

    def _clear_tables(self):
        
        # clear records from trip gen tables
        self.db._raw_query("delete from trip_prod;") 
        self.db._raw_query("delete from prod_nhb;") 
        self.db._raw_query("delete from trip_attr;")      
        

        
    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:    
            self.run_model(hbo=self.hbo)
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
        

    def run_model(self,hbo=0):
        """[load parameters and call appropriate model run method]"""

        self._clear_tables()
        
        # trip productions at the home end
        self.wrk_trip_production(hbo)
        self.logger.debug("wrk_trip_production completed")
        self.hh_trip_production()
        self.logger.debug("hh_trip_production completed")

        # trip attractions
        self.trip_attraction()
        self.logger.debug("trip_attraction completed")

        # allocate trips from home to non-home
        self.nhb_allocation()
        self.logger.debug("nhb_allocation completed")        

        # summary
        self.run_summaries()
        self.logger.debug("run_summaries completed")

        
    def hh_trip_production(self):
        """	[estimate household non-work standard trip productions]
        """
        tpurp_ls = ["nhbnw","hbsc","hbsr","hbpb"] 
        exprt_ls = ['hid', 'block_id','nhbnw_p','hbsc_p', 'hbsr_p', 'hbpb_p']

        self.status_updater(self.status_pct[4],"Trip Productions: Loading Households")
        query_string = """SELECT * FROM hh 
                            JOIN emp_access USING(taz_id)
                            JOIN veh USING (hid)"""
        hh_df = self.db._raw_query(qry=query_string)

        # create other variables as needed
        rows = list(hh_df.columns)
        hhsi_idx = rows.index("hh_inc_cat_by_size")
        veh_idx = rows.index("veh_suff")

        hh_df["zv_hh"] =  hh_df.apply(lambda row : 1 if row[veh_idx] == 'zv' else 0, axis=1,raw=True)
        hh_df["midinc_hh"] =  hh_df.apply(lambda row : 1 if row[hhsi_idx] == 2 else 0, axis=1,raw=True)
        hh_df["highinc_hh"] =  hh_df.apply(lambda row : 1 if row[hhsi_idx] == 3 else 0, axis=1,raw=True)
        hh_df['const'] = 1

        self.status_updater(self.status_pct[5],"Estimating Household Trips")

        # estimate trips        
        for tpurp in tpurp_ls: 
            hh_df["res_%s_p"%tpurp] = 0
            rows = list(hh_df.columns)
            tp_idx = rows.index("res_%s_p"%tpurp)

            tg_coeffs = self.coprod["%s_coeffs"%tpurp]
            for term in tg_coeffs: 
                hh_df["res_%s_p"%tpurp] += hh_df[term] * tg_coeffs[term]

            # set minimum trips to 0
            hh_df["res_%s_p"%tpurp] = hh_df.apply(lambda row: max(row[tp_idx],0), axis=1, raw=True)

            ## take exponential
            if self.coprod["%s_model"%tpurp] in ["Negative Binomial"]:
                hh_df["%s_p"%tpurp] = hh_df.apply(lambda row: round(math.exp(row[tp_idx]),2), axis=1, raw=True)

            elif self.coprod["%s_model"%tpurp] in ["Linear"]:
                hh_df["%s_p"%tpurp] = hh_df.apply(lambda row: round(row[tp_idx],2), axis=1, raw=True)

        # hard fix for hbsc trips in 0 child household
        rows = list(hh_df.columns)
        chld_idx = rows.index("children")
        sc_idx = rows.index("hbsc_p")
        hh_df["hbsc_p"] = hh_df.apply(lambda row: 0.0 if row[chld_idx] == 0 else row[sc_idx], axis=1,raw=True) 

        self.status_updater(self.status_pct[6],"Trip Productions: Writing Household Trips to DB")

        # export variables, utilities, shares and choice with debug
        if self.args["loglevel"] == "DEBUG":
            dump_csv_fn = self.args['OutputFolder'] + '\\_logs\\' + 'tripgen_hhprod_df_dump.csv'
            hh_df.to_csv(dump_csv_fn, index=False)               

        hh_df = hh_df[exprt_ls]
        hh_df["block_id"] = hh_df["block_id"].astype(str)
        # hh trip records have person = 0
        hh_df["person_num"] = 0
        
        hh_df.to_sql(name="trip_prod",con=self.db.conn,if_exists="append",index=False
                     ,schema={"block_id":"text","hid":"text"})

        return hh_df

    def wrk_trip_production(self, hbo=0):
        """[estimate worker standard trips productions]"""

        self.status_updater(self.status_pct[1],"Trip Productions: Loading Workers")

        if hbo > 0: #TODO: estimate hbo trips for workers if enabled
            tpurp_ls = ["hbw","nhbw","hbsr","hbpb"] 
            exprt_ls = ['hid', 'block_id', 'person_num', 'hbw_p','nhbw_p','hbsr_p', 'hbpb_p']
        else:
            tpurp_ls = ["hbw","nhbw"] 
            exprt_ls = ['hid', 'block_id', 'person_num', 'hbw_p','nhbw_p']

        wrk_df = self.db._raw_query("""SELECT wfh.hid, wfh.block_id, wfh.person_num, 
                                    hh.workers, hh_inc_cat_by_size, veh_suff, age, commute_eqs 
                                    FROM wfh 
                                    LEFT JOIN hh USING(hid) 
                                    LEFT JOIN veh USING(hid)
                                    LEFT JOIN per USING(hid,person_num)"""  )

        # self.logger.info("timestamp 1")
        rows = list(wrk_df.columns)
        hhsi_idx = rows.index("hh_inc_cat_by_size")
        veh_idx = rows.index("veh_suff")
        age_idx = rows.index("age")

        wrk_df["zv_hh"] =  wrk_df.apply(lambda row : 1 if row[veh_idx] == 'zv' else 0, axis=1,raw=True)
        wrk_df["sv_hh"] =  wrk_df.apply(lambda row : 1 if row[veh_idx] == 'sv' else 0, axis=1,raw=True)
        wrk_df["midinc_hh"] =  wrk_df.apply(lambda row : 1 if row[hhsi_idx] == 2 else 0, axis=1,raw=True)
        wrk_df["highinc_hh"] =  wrk_df.apply(lambda row : 1 if row[hhsi_idx] == 3 else 0, axis=1,raw=True)
        wrk_df["age_65p"] =  wrk_df.apply(lambda row : 1 if row[age_idx] > 64 else 0, axis=1,raw=True)
        wrk_df['const'] = 1   

        self.status_updater(self.status_pct[2],"Trip Productions: Worker Trip Estimation")

        # estimate trips        
        for tpurp in tpurp_ls: 
            wrk_df["res_%s_p"%tpurp] = 0
            rows = list(wrk_df.columns)
            tp_idx = rows.index("res_%s_p"%tpurp)
            tg_coeffs = self.coprod["%s_coeffs"%tpurp]
            for term in tg_coeffs: 
                wrk_df["res_%s_p"%tpurp] += wrk_df[term] * tg_coeffs[term]

            # set minimum trips to 0
            wrk_df["res_%s_p"%tpurp] = wrk_df.apply(lambda row: max(row[tp_idx],0), axis=1, raw=True)

            ## take exponential
            if self.coprod["%s_model"%tpurp] in ["Negative Binomial","Poisson"]:
                wrk_df["%s_p"%tpurp] = wrk_df.apply(lambda row: round(math.exp(row[tp_idx]),2), axis=1, raw=True)

            elif self.coprod["%s_model"%tpurp] in ["Linear"]:
                wrk_df["%s_p"%tpurp] = wrk_df.apply(lambda row: round(row[tp_idx],2), axis=1, raw=True)

            ## consider the impact of remote work level
            if tpurp =="hbw" or tpurp == "nhbw" :   
                wrk_df["%s_p"%tpurp] = wrk_df["%s_p"%tpurp] * wrk_df["commute_eqs"]

            # elif tpurp =="HBSR" or tpurp =="HBPB"  : ## TODO: estimate worker non-work trips if not commuting
            #     wrk_df["%s_p"%tpurp.lower()] = wrk_df["%s_p"%tpurp.lower()] * wrk_df["wfh_eqs"]
        
        # self.logger.info("timestamp 4")
        self.status_updater(self.status_pct[3],"Trip Productions: Write Worker Trips to DB")

        # export variables, utilities, shares and choice with debug
        if self.args["loglevel"] == "DEBUG":
            dump_csv_fn = self.args['OutputFolder'] + '\\_logs\\' + 'tripgen_wrkprod_df_dump.csv'
            wrk_df.to_csv(dump_csv_fn, index=False)     

        ## store a full copy for summary report processing 
        wrk_df = wrk_df[exprt_ls]
        wrk_df["block_id"] = wrk_df["block_id"].astype(str)
        
        # self.logger.info("timestamp 15")
        wrk_df.to_sql(name="trip_prod",con=self.db.conn,if_exists="append",index=False
                     ,schema={"block_id":"text","hid":"text"})

        return wrk_df


    def trip_attraction(self):	
        """[estimate trip attractions]"""
        tpurp_ls = ['hbw','hbsc','hbpb','hbsr','nhbw','nhbnw']
        exprt_ls = ['block_id', 'taz_id',  'hbw_inc1_a','hbw_inc2_a','hbw_inc3_a','hbw_inc4_a',
                                                    'hbsc_a','hbsr_a','hbpb_a','nhbw_a','nhbnw_a']  

        wfh_job_df = self.db._raw_query("""
                            SELECT jobs.*, block_sed.total_households,
                            access_density.access_density
                            FROM jobs LEFT OUTER JOIN block_sed
                            USING(block_id)
                            LEFT OUTER JOIN access_density 
                            USING(taz_id)
                            """)

        # associate enrollment with largest block in taz
        enr_df = self.db._raw_query("""SELECT block_id, k12 FROM enrollment e LEFT JOIN 
                        (SELECT taz_id, block_id, max(area_fct) as fct 
                         FROM taz_block_allocation GROUP BY taz_id) USING(taz_id)""")

        attr_df = wfh_job_df.merge(enr_df, on='block_id', how='left').fillna(0)          

        # extend hbw factors for access desntiy segments
        rows = list(attr_df.columns)
        rl_idx = rows.index("6_ret_leis")
        hh_idx = rows.index("total_households")
        at_idx = rows.index("access_density")          
        attr_df["rl_cbddens"] =  attr_df.apply(lambda row : row[rl_idx] 
                                                if row[at_idx] <= 2 else 0, axis=1,raw=True)
        attr_df["rl_notsubrur"] =  attr_df.apply(lambda row : row[rl_idx] 
                                                if row[at_idx] <= 4 else 0, axis=1,raw=True)    
        attr_df["hh_subrur"] =  attr_df.apply(lambda row : row[hh_idx] 
                                                if row[at_idx] >= 5 else 0, axis=1,raw=True)

        # Prepare variables for hbw income segmented attractions
        # calculate total_jobs by income sector
        ei_df = self.df_incsc.set_index('sector')
        for inc in ei_df.columns:
            attr_df['total_jobs_' + inc] = 0
            for sector, inc_fct in ei_df.iterrows():
                attr_df['total_jobs_' + inc] += attr_df[sector] * inc_fct[inc]    

        # extend factors with access density segments
        rl_cbddens = self.df_incsc.loc[self.df_incsc.sector=='6_ret_leis',]
        rl_cbddens.sector='rl_cbddens'
        rl_notsubrur = self.df_incsc.loc[self.df_incsc.sector=='6_ret_leis',]
        rl_notsubrur.sector='rl_notsubrur'
        emp_inc_exp = pd.concat([self.df_incsc,rl_cbddens,rl_notsubrur])
        emp_inc_df = emp_inc_exp.set_index('sector')
        
        # calculate attractions - hbw and hbsc are handled differently
        self.status_updater(self.status_pct[8],"Trip Attractions: calculate attractions")
        for tpurp in tpurp_ls:
            coeffs = self.coatt["%s_coeffs"%tpurp]

            # HBW attractions are segmented by income
            if tpurp == "hbw": 
                for inc in emp_inc_df.columns:
                    att_fld = 'hbw_' + inc + "_a"
                    attr_df[att_fld] = 0

                    # segmented employment
                    for sector, inc_fct in emp_inc_df.iterrows():
                        if coeffs[sector] !=0:
                            attr_df[att_fld] += attr_df[sector] * coeffs[sector] * inc_fct[inc]

                    # total jobs
                    attr_df[att_fld] += attr_df['total_jobs_' + inc] * coeffs['total_jobs']

                    # non-segmented terms
                    for term in ['total_households','hh_subrur']:
                        attr_df[att_fld] += attr_df[term] * coeffs[term]

            # HBSC set to enrollment
            elif tpurp == "hbsc":
                attr_df["hbsc_a"]  = attr_df["k12"] 

            # all others are from full set
            else:
                att_fld = "%s_a"%tpurp.lower()
                attr_df[att_fld] = 0                
                for term in coeffs:
                    if term != 0:
                        attr_df[att_fld] += attr_df[term] * coeffs[term] 
        
        # self.logger.info("timestamp 5")
        self.status_updater(self.status_pct[9],"Trip Attractions: write to DB")
        attr_df = attr_df[exprt_ls] 
    
        attr_df.to_sql(name="trip_attr",con=self.db.conn,if_exists="append",index=False,schema={"block_id":"text","taz_id":"integer"})
        return attr_df

    def nhb_allocation(self):
        """allocate estimated nhb productions from home location to trip production block
        """
        ## part 2: allocation from prod trip to nhb trips
        self.status_updater(self.status_pct[10],"Trip Gen component: allocating estimated nhb productions")
        job_df = self.db._raw_query("SELECT * FROM trip_attr")
        prod_df = self.db._raw_query("SELECT * FROM trip_prod")

        ## allocatie the non-home based trips
        df_job_blk = self._nhb_purpose_allocation(job_df,prod_df,prd_purp="nhbw")
        df_job_blk["nhbnw_p"] = self._nhb_purpose_allocation(job_df,prod_df,prd_purp="nhbnw")["nhbnw_p"]
        ## export the non-home based trips to table

        df_nhb = pd.merge(left =df_job_blk[["block_id","nhbw_p","nhbnw_p"]],
                          right=job_df[["block_id","taz_id"]], 
                          on="block_id" , how="left" )    
        df_nhb.to_sql(name="prod_nhb",con=self.db.conn,if_exists="append",index=False,
                      schema={"block_id":"text","taz_id":"integer"})         

        
    def _nhb_purpose_allocation(self,job_df,prod_df,prd_purp="nhbnw"):
        """allocate estimated nhb purpose productions to blocks

        Args:
            job_df (dataframe): trip attraction records by block   
            prod_df (dataframe): worker level trip records  
            prd_purp (str, optional): . Defaults to "nhbnw".

        Returns:
            _type_: dataframe
        """
        
        job_df.fillna(value=0,inplace=True)
        job_df["hbw_a"] = job_df[["hbw_inc1_a","hbw_inc2_a","hbw_inc3_a","hbw_inc4_a"]].sum(axis=1)
        if prd_purp == "nhbw":
            att_purp = "hbw"
            purp_ls = ["%s_a"%att_purp]
        elif prd_purp == "nhbnw":
            att_purp = "nhb"
            purp_ls = ["nhbw_a","nhbnw_a"]
        ## create a succint copy for futher column-wise calculation; to avoid 
        df_att2prd = job_df[["block_id","taz_id"]+purp_ls].copy()
    
        if att_purp == "nhb" or len(purp_ls) > 1:
            df_att2prd["%s_a"%att_purp] = df_att2prd[purp_ls].sum(axis=1)

        ## total trip produced
        prod_df_p = prod_df[ ["%s_p"%prd_purp ]].sum()
        ## proportion for allocation based on block_level
        vsum = df_att2prd["%s_a"%att_purp].sum()
        ## calculate the attraction rate 
        df_att2prd["%s_ar"%att_purp] = df_att2prd["%s_a"%att_purp] / vsum 
        ## use rate * total production
        df_att2prd["%s_p"%prd_purp] = df_att2prd["%s_ar"%att_purp] * prod_df_p["%s_p"%prd_purp]

        return df_att2prd


    def run_summaries(self):	
        """[generate summaries of model estimates]"""
        self.status_updater(self.status_pct[11],"Trip Gen component: writing summaries")

        # helper functions
        def summ_trips(df):
            out_df = pd.concat([df.max().rename("max"),
                    df.min().rename("min"),
                    df.mean().rename("mean")], axis=1)
            return(out_df)

        # trip productions - by block and household
        prod_df = self.db._raw_query("SELECT * FROM trip_prod")
        hh_df = self.db._raw_query("SELECT hid, persons, workers FROM hh")

        prod_hh_df = prod_df.groupby(by=['hid','block_id']).sum().drop(columns={'person_num'}).reset_index()
        prod_hh_df = prod_hh_df.merge(hh_df, on='hid')
        prod_hh_df['all_trips'] = prod_hh_df.drop(columns={'hid','block_id','persons','workers'}).sum(axis=1)

        prod_blk_df = prod_hh_df.groupby(by=['block_id']).sum().drop(columns={'persons','workers'}).reset_index()

        prod_hh_df = prod_hh_df.drop(columns={"hid","block_id"})

        # clean up and calculate rates
        hh_cols = {'persons', 'workers'}
        phh_blk_df = prod_hh_df.drop(columns=hh_cols)
        pper_blk_df = prod_hh_df.div(prod_hh_df.persons,axis=0).drop(columns=hh_cols)
        pwrk_blk_df = prod_hh_df.loc[prod_hh_df.workers>0,].div(prod_hh_df.workers,axis=0).drop(columns=hh_cols)
        pblk_df = prod_blk_df.drop(columns={"block_id"})

        # record in summary df
        summ_df = pd.concat([summ_trips(phh_blk_df).add_prefix("hh_"),
                            summ_trips(pper_blk_df).add_prefix("per_"),
                            summ_trips(pwrk_blk_df).add_prefix("wrk_"),
                            summ_trips(pblk_df).add_prefix("blk_")],axis=1).round(3)
        summ_df.index.name = "productions per"
        summ_df.to_csv(self.args["OutputFolder"] + '\\_summary\\trips\\' + 'trip_production_summary.csv')


        # trip attractions - by block, job, and hh
        attr_df = self.db._raw_query("SELECT * FROM trip_attr")
        job_df = self.db._raw_query("""SELECT block_id, total_jobs, total_households 
                                        FROM block_sed """).set_index('block_id')

        attr_df['all_trips'] = attr_df.drop(columns={'block_id','taz_id'}).sum(axis=1)
        attr_df.insert(0,"hbw_a",attr_df[["hbw_inc1_a","hbw_inc2_a","hbw_inc3_a","hbw_inc4_a"]].sum(axis=1))
        attr_blk_df = attr_df.groupby(by='block_id').sum().drop(columns={'taz_id'})
        attr_blk_df = attr_blk_df.merge(job_df,how='left', left_index=True, right_index=True).fillna(0)
        attr_blk_df['total_emphh'] = attr_blk_df['total_jobs'] + attr_blk_df['total_households']

        # clean up and calculate rates
        tot_cols = {'total_jobs', 'total_households','total_emphh'}
        aemp_blk_df = attr_blk_df.loc[attr_blk_df.total_jobs>0,].div(
                        attr_blk_df.total_jobs,axis=0).drop(columns=tot_cols)
        ahh_blk_df = attr_blk_df.loc[attr_blk_df.total_households>0,].div(
                        attr_blk_df.total_households,axis=0).drop(columns=tot_cols)
        ahhemp_blk_df = attr_blk_df.loc[attr_blk_df.total_emphh>0,].div(
                        attr_blk_df.total_emphh,axis=0).drop(columns=tot_cols)
        blk_df = attr_blk_df.drop(columns=tot_cols)

        # record in summary df
        summ_df = pd.concat([summ_trips(aemp_blk_df).add_prefix("emp_"),
                            summ_trips(ahh_blk_df).add_prefix("hh_"),
                            summ_trips(ahhemp_blk_df).add_prefix("hh+emp_"),
                            summ_trips(blk_df).add_prefix("blk_")],axis=1).round(3)
        summ_df.index.name = "attractions per"
        summ_df.to_csv(self.args["OutputFolder"] + '\\_summary\\trips\\' + 'trip_attraction_summary.csv')

        # productions and attractions
        def pa_totals(seg):
            prod = prod_df[seg+"_p"].sum()
            attr = attr_df[seg+"_a"].sum()
            ratio = prod / attr
            return [prod, attr, ratio]

        purps = self.args["Trip Purp"]
        vals = [pa_totals(item) for item in purps]
        seg_df = pd.DataFrame({purps[i]: vals[i] for i in range(len(purps))})
        seg_df = seg_df.rename(index={0:'Productions',1:'Attractions',2:'P/A Ratio'})
        seg_df['total']= seg_df.sum(axis=1)
        seg_df.loc['P/A Ratio','total'] = seg_df.loc['Productions','total'] / seg_df.loc['Attractions','total']
        seg_df.index.name='trips'
        seg_df.to_csv(self.args["OutputFolder"] + '\\_summary\\trips\\' + 'trip_generation_summary.csv')


