from asyncio.log import logger
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import math
import numpy as np

class aggregate_and_balance(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        # initialize the status
        self.status_pct = [0, 0, 59, 94, 96, 99, 100]
        
    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:    
            self.aggregate_hh_trips()
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
   

    def aggregate_hh_trips(self):
        """[Aggregate household productions and attractions to TAZ and write to CSV]"""

        # get production block totals
        self.status_updater(self.status_pct[1],"get production block totals")
        prod_df = self.aggregate_hbw_productions()
        prod_df = prod_df.merge(self.aggregate_hbnw_productions(), on = "block_id", how='outer').fillna(0)
        prod_df = prod_df.merge(self.aggregate_nhb_productions(), on = "block_id", how='outer').fillna(0)

        # aggregate to taz and write out
        self.status_updater(self.status_pct[4],"aggregate to taz and write out")
        prod_taz_df = self.aggregate_productions_taz(prod_df)
        self.logger.info("Production purposes: ")
        self.logger.debug(prod_taz_df.columns)        
        prod_taz_df.to_csv(self.args["hh_prod"])

        attr_taz_df = self.aggregate_attractions_taz()
        self.logger.info("Attraction purposes: ")
        self.logger.debug(attr_taz_df.columns)      
        attr_taz_df.to_csv(self.args["hh_attr"])

        # summaries for sanity checking
        self.status_updater(self.status_pct[5]," summaries for sanity checking")
        self._summarize_hh_agg(prod_taz_df, 'prod_agg_summary.csv')
        self._summarize_hh_agg(attr_taz_df, 'attr_agg_summary.csv')


    def aggregate_hbw_productions(self):
        """[Aggregate home-based work trip purposes by income, segment and time period]"""

        qry_txt = """SELECT t.block_id, 
                            sum(hbw_p) as hbw,
                            wage_inc,
                            veh_suff,
                            peak
                FROM trip_prod_pknp t 
                JOIN per p USING(hid, person_num)
                JOIN veh v USING(hid)
                WHERE person_num > 0
                GROUP BY t.block_id, veh_suff, peak"""

        trip_df = self.db._raw_query(qry_txt)

        #period
        # self.logger.info("Aggregate home-based work trip purposes")
        self.status_updater(self.status_pct[2],"aggregating home-based work trip purpose")
        trip_df['pknp'] = 'np'
        trip_df.loc[trip_df['peak'] == 1, 'pknp'] = 'pk'              

        # calculate wage income group
        # TODO: pull thresholds and number of categories from parameters
        trip_df['inc_cat'] = 4
        trip_df.loc[trip_df['wage_inc'] < 100000, 'inc_cat'] = 3
        trip_df.loc[trip_df['wage_inc'] < 60000, 'inc_cat'] = 2
        trip_df.loc[trip_df['wage_inc'] < 30000, 'inc_cat'] = 1

        return_list = ['block_id']        

        # run against scenario houshold home-based-work purposes (if hbw is in scenario purposes)
        for purp in list(set(['hbw']) & set(self.args["Trip Purp"])):

            purp_seg = self.args["Purpose Segments"][purp]['Segments'].split(",")

            for inc in range(1,5):
                for seg in purp_seg: 
                    for per in ['pk','np']:
                        seg_name = self._segment_name(purp + "_inc" + str(inc), seg, per)
                        trip_df[seg_name] = trip_df[purp] * \
                                            (trip_df['pknp'] == per) * \
                                            (trip_df['veh_suff'] == seg) * \
                                            (trip_df['inc_cat'] == inc) 
                        return_list = return_list + [seg_name]                            

        return trip_df[return_list].groupby('block_id').agg('sum')      
    
    def aggregate_hbnw_productions(self):
        """[Aggregate home-based non-work trip purposes by segment and time period]"""

        qry_txt = """SELECT t.block_id, 
                            sum(hbsr_p) as hbsr,
                            sum(hbpb_p) as hbpb,
                            sum(hbsc_p) as hbsc,
                            veh_suff,
                            peak
                FROM trip_prod_pknp t 
                JOIN hh h USING(hid)
                JOIN veh v USING(hid)
                WHERE person_num = 0
                GROUP BY t.block_id, veh_suff, peak"""

        trip_df = self.db._raw_query(qry_txt)

        #period
        # self.logger.info("Aggregate home-based non-work trip purposes")
        self.status_updater(self.status_pct[3],"aggregating home-based non-work trip purpose")
        trip_df['pknp'] = 'np'
        trip_df.loc[trip_df['peak'] == 1, 'pknp'] = 'pk'              

        return_list = ['block_id']

        # run against scenario houshold home-based purposes
        for purp in list(set(['hbpb','hbsr','hbsc']) & set(self.args["Trip Purp"])):

            purp_seg = self.args["Purpose Segments"][purp]['Segments'].split(",")
            
            for seg in purp_seg: 
                for per in ['pk','np']:
                    seg_name = self._segment_name(purp, seg, per)
                    trip_df[seg_name] = trip_df[purp] * \
                                         (trip_df['pknp'] == per) * \
                                         (trip_df['veh_suff'] == seg)

                    return_list = return_list + [seg_name]

        return trip_df[return_list].groupby('block_id').agg('sum')             

    def aggregate_nhb_productions(self):
        """[Aggregate non-home-based trip purposes by segment and time period]"""

        qry_txt = """SELECT t.block_id, 
                            sum(nhbw_p) as nhbw,
                            sum(nhbnw_p) as nhbnw,
                            peak
                FROM trip_prod_nhb_pknp t
                GROUP BY t.block_id, peak"""

        trip_df = self.db._raw_query(qry_txt)

        #period
        trip_df['pknp'] = 'np'
        trip_df.loc[trip_df['peak'] == 1, 'pknp'] = 'pk'                

        return_list = ['block_id']

        # run against scenario houshold home-based purposes
        for purp in list(set(['nhbw','nhbnw']) & set(self.args["Trip Purp"])):
 
            purp_seg = self.args["Purpose Segments"][purp]['Segments'].split(",")
            
            for seg in purp_seg: 
                for per in ['pk','np']:
                    seg_name = self._segment_name(purp, seg, per)
                    trip_df[seg_name] = trip_df[purp] * \
                                            (trip_df['pknp'] == per)

                    return_list = return_list + [seg_name]

        return trip_df[return_list].groupby('block_id').agg('sum')       

    def aggregate_productions_taz(self, prod_df):
        """[Aggregate productions to taz]"""
                
        qry_txt = """SELECT * FROM taz_block_allocation"""
        taz_blk_df = self.db._raw_query(qry_txt)

        purp_l = self._prod_purp_list()

        prod_blk_df = taz_blk_df.merge(prod_df, 'left', on='block_id').fillna(0)
        prod_blk_df = prod_blk_df.set_index(['taz_id'])
        prod_taz_df = prod_blk_df[purp_l].multiply(
                    prod_blk_df['area_fct'], axis="index").groupby(by="taz_id").agg('sum')

        return(prod_taz_df)

    def aggregate_attractions_taz(self):
        """[Aggregate attractions by purpose]"""

        qry_txt = """SELECT * FROM trip_attr t"""
        attr_df = self.db._raw_query(qry_txt)
        attr_df = attr_df.drop(columns=['taz_id'])

        purp_a_l = self._attr_purp_list("_a")
        purp_l = self._attr_purp_list("")

        qry_txt = """SELECT * FROM taz_block_allocation"""
        taz_blk_df = self.db._raw_query(qry_txt)

        attr_blk_df = taz_blk_df.merge(attr_df, 'left', on='block_id').fillna(0)
        attr_blk_df = attr_blk_df.set_index(['taz_id'])
        attr_taz_df = attr_blk_df[purp_a_l].multiply(
                    attr_blk_df['area_fct'], axis="index").groupby(by="taz_id").agg('sum')

        return(attr_taz_df.rename(columns=dict(zip(purp_a_l, purp_l))))
  
  
    def _segment_name(self, purp, seg, per):
        """utility to specify purpose segment name (must match naming convention in utilities.rsc)"""
        return(purp + "_" + seg + "_" + per)        

    def _prod_purp_list(self):
        """utility to generate list of production segments"""
        purp_l = []
        for purp in self.args["Trip Purp"]: 
            purp_seg = self.args["Purpose Segments"][purp]['Segments'].split(",")

            for seg in purp_seg: 
                for per in ['pk','np']:

                    if (purp == 'hbw'):
                        for inc in range(1,5): # TODO: pull categories from params
                            pname = purp + "_inc" + str(inc)
                            purp_l = purp_l + [self._segment_name(pname, seg, per)]
                        continue

                    purp_l = purp_l + [self._segment_name(purp, seg, per)]

        return purp_l

    def _attr_purp_list(self, suffix):
        """utility to generate list of attraction segments"""
        purp_l = []
        for purp in self.args["Trip Purp"]: 
        
            if (purp == 'hbw'):
                for inc in range(1,5): # TODO: pull categories from params
                    pname = purp + "_inc" + str(inc) + suffix
                    purp_l = purp_l + [pname]
                continue

            pname = purp + suffix
            purp_l = purp_l + [pname]

        return purp_l


    def _summarize_hh_agg(self, agg_taz_df, out_file):
        """utility to output summary files for checking"""

        nz_df = pd.DataFrame(agg_taz_df.astype(bool).sum(axis=0), columns=["NonZero"])
        mx_df = pd.DataFrame(agg_taz_df.max(axis=0), columns=["Max"])
        temp_df = agg_taz_df.replace(0,np.NaN)
        av_df = pd.DataFrame(temp_df.mean(axis=0), columns=["Mean"])

        out1_df = nz_df.merge(mx_df, left_index=True, right_index=True)
        out2_df = out1_df.merge(av_df, left_index=True, right_index=True)
        out2_df.index.name = 'purpose_segment'

        csv_fn = self.args["OutputFolder"] + '\\_summary\\trips\\' + out_file
        out2_df.to_csv(csv_fn)

if __name__ == "__main__":

    anb = aggregate_and_balance()
    anb.aggregate_hh_trips()
