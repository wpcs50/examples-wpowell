from asyncio.log import logger
import imp
import pandas as pd 
import numpy as np
from pathlib import Path
from dbfread import DBF
from .base import disagg_model

class db_loader(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)    

        # initialize the status
        self.status_pct = [0, 0, 0, 0, 0, 0, 0, 0, [7, 7, 20, 21, 55, 58, 58, 70, 72, 93], 99, 100]
        
    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:
            self.load_taz_table()
            self.load_tazpuma_table()
            self.load_block_sed_table()
            self.load_parking()
            self.load_walkbike()
            self.load_enrollment()
            self.load_per_and_hh_tables()
            self.load_block_allocation_table()
            self.load_block_assignment_table()
            self.status_updater(100, "Closing component" )
            print("Exiting " + self.name)
            if self.popup == None:
                raise SystemExit()

            elif self.popup.runwithin == "others" :
                raise SystemExit()

        except Exception as e:
            import traceback
            errfile = self.args["OutputFolder"] +'\\_logs\\' + "py.err"
            with open(errfile,"a") as file:
                traceback.print_exc(file=file)

            self.status_updater(-1, "**Error**: Click cancel to check the error message %s"%str(e) )
    
    def load_taz_table(self):
        """
        [Populate the MA_taz_geography DB table from the TAZ shapefile's DBF file.]
        """
        # The TAZ (non-) geography table: the '.dbf' (i.e., attribute table) component of the TAZ 'shapefile'.
        path_geo =  self.args["geo"]
        table = DBF(path_geo)
        taz_df = pd.DataFrame(iter(table))
        self.logger.debug('Number of records in TAZ dataframe = ' + str(len(taz_df)))

        taz_df = taz_df.drop(['OBJECTID', 'Shape_Leng', 'Shape_Area'], axis=1)
        taz_df.to_sql(name="MA_taz_geography",con=self.db_conn,if_exists="append",index=False)

        # generate index for municipalities (matrix aggregation requires integers)
        ts_idx = pd.DataFrame(taz_df.town_state.unique()).reset_index().rename(columns={'index':'ts_index', 0 :'town_state'})
        ts_idx.to_csv(self.args["OutputFolder"] + "\\_networks\\ts_index.csv",index=False)
        taz_df = taz_df.merge(ts_idx, on='town_state')

        # export index of taz's to aggregation levels for summaries
        aggr_df = taz_df[['taz_id','state','type','mpo','subregion','ring','corridor','district','urban','ts_index']]
        aggr_df = aggr_df.rename(columns={'ts_index':'town_state'})

        map_mpo = {'BRMPO':1,
                'MVPC':2,
                'NMCOG':3,
                'OCPC':4,
                'MRPC':5,
                'CMRPC':6,
                'SRPEDD':7,
                'CCC':8,
                'MVC':9,
                'NPEDC':10,
                'PVPC':11,
                'FRCOG':12,
                'BRPC':13,
                '':99}
        map_subr = {"ICC":1,
                    "NSTF":2,
                    "NSPC":3,
                    "MAGIC":4,
                    "TRIC":5,
                    "ICC/TRIC":6,
                    "METROWEST":7,
                    "TRIC/SWAP":8,
                    "SWAP":9,
                    "SSC":10,
                    "":99}
        map_state = {"MA":25,
                     "NH":33,
                     "RI":44}

        aggr_df.mpo = aggr_df.mpo.map(map_mpo).fillna(99)
        aggr_df.subregion = aggr_df.subregion.map(map_subr).fillna(99)
        aggr_df.state = aggr_df.state.map(map_state).fillna(99)

        aggr_df.loc[aggr_df.type=='E','ring'] = 99
        aggr_df.loc[aggr_df.type=='E','corridor'] = 99
        aggr_df.loc[aggr_df.type=='E','district'] = 99

        aggr_df.drop(columns={"type"},inplace=True)
        aggr_df.sort_values(by='taz_id',inplace=True)
        aggr_df.to_csv(self.args["OutputFolder"] + "\\_networks\\taz_index.csv",index=False)
        
        # Generate summary report
        tot_taz = len(taz_df)
        tot_external = len(taz_df[taz_df['type'] == 'E'])
        tot_internal = len(taz_df[taz_df['type'] == 'I'])
        tot_ma = len(taz_df[taz_df['state'] == 'MA'])
        tot_brmpo = len(taz_df[taz_df['in_brmpo'] == 1])
        data = { 'category' : ['total TAZes', 'external TAZes', 'internal TAZes', 'TAZes in Massachusetts', 'TAZes in Boston Region MPO' ],
                 'value'    : [tot_taz, tot_external, tot_internal, tot_ma, tot_brmpo] }
        summary_df = pd.DataFrame(data)
        csv_fn = self.args["OutputFolder"] + '\\_summary\\zonal\\' + 'taz_summary.csv'
        summary_df.to_csv(csv_fn, index=False)
        return None

    def load_tazpuma_table(self):
        """
        [Populate the tazpuma DB table from the tazpuma csv]
        """
        path_tp =  self.args["tazpuma"]
        self.logger.debug('Path to geocrosswalk = ' + path_tp)
        self.tazpuma = pd.read_csv(path_tp)
        self.logger.debug('Number of records in geocrosswalk dataframe = ' + str(len(self.tazpuma)))

        self.logger.debug("Populating 'tazpuma' table.")
        self.tazpuma.to_sql(name="tazpuma",con=self.db_conn,if_exists="append",index=False)

        return None
        
                    
    def load_parking(self):
        """
        [Populate the parking DB table from the parking CSV file.]
        """
        path_parking = self.args['Parking']
        self.parking_df = pd.read_csv(path_parking)
        self.logger.debug("Populating 'parking' table.")
        self.parking_df.to_sql(name="parking",con=self.db_conn,if_exists="append",index=False)
        
        # Generate summary report
        nz_park = self.parking_df.iloc[self.parking_df['cost_dr'].to_numpy().nonzero()]

        max_hourly = nz_park['cost_hr'].max()
        avg_hourly = nz_park['cost_hr'].mean()
        max_daily  = nz_park['cost_dr'].max()
        avg_daily  = nz_park['cost_dr'].mean()
        max_monthly  = nz_park['cost_mr'].max()
        avg_monthly  = nz_park['cost_mr'].mean()        
        data = { 'category' : ['zones with parking cost', 
                               'max hourly cost', 'avg hourly cost',
                               'max daily cost', 'avg daily cost',
                               'max monthly cost', 'avg monthly cost'  ],
                 'value'    : [len(nz_park), 
                               max_hourly, avg_hourly, 
                               max_daily, avg_daily,
                               max_monthly, avg_monthly] }
        summary_df = pd.DataFrame(data)
        csv_fn = self.args["OutputFolder"] + '\\_summary\\zonal\\' + 'parking_summary.csv'
        summary_df.to_csv(csv_fn, index=False)
        return None
        
    def load_walkbike(self):
        """
        [Populate the walk_bike DB table from the walkbike CSV file.]
        """

        path_walk_bike = self.args['Walk Bike Conditions']
        self.walkbike_df = pd.read_csv(path_walk_bike)
        self.logger.debug("Populating 'walkbike' table.")
        self.walkbike_df.to_sql(name="walkbike",con=self.db_conn,if_exists="append",index=False)
        
        # Generate summary report
        min_walk = self.walkbike_df['walkability'].min()
        max_walk = self.walkbike_df['walkability'].max()
        avg_walk = self.walkbike_df['walkability'].mean()
        min_bike = self.walkbike_df['bikeability'].min()
        max_bike = self.walkbike_df['bikeability'].max()
        avg_bike = self.walkbike_df['bikeability'].mean()
        data = { 'category' : ['walkability', 'bikeability' ],
                 'min'      : [min_walk, min_bike],
                 'max'      : [max_walk, max_bike],
                 'avg'      : [avg_walk, avg_bike] }
        summary_df = pd.DataFrame(data)
        csv_fn = self.args["OutputFolder"] + '\\_summary\\zonal\\' + 'walk_bike_summary.csv'
        summary_df.to_csv(csv_fn, index=False)
        return None
    
    def load_enrollment(self):
        """
        [Populate the enrollment DB table from the enrollment CSV file.]
        """
        enrollment_df = pd.read_csv(self.args['School Enrollment'])
        enrollment_df.to_sql(name="enrollment",con=self.db_conn,if_exists="append",index=False)
        
        data = { 'category'         : ['K-12', 'University Total', 'University Commuter'],
                 'total enrollment' : [enrollment_df['k12'].sum(),
                                        enrollment_df['college_total'].sum(),
                                        enrollment_df['college_commuter'].sum()] }
        summary_df = pd.DataFrame(data)
        csv_fn = self.args["OutputFolder"] + '\\_summary\\zonal\\' + 'enrollment_summary.csv'
        summary_df.to_csv(csv_fn, index=False)
        return None

    def pick_sim_type(self,tabtype):
        path_tab_Mass = self.args[tabtype+' MA']
        path_tab_NHRI = self.args[tabtype+' NHRI']
        
        tab_Mass = pd.read_csv(path_tab_Mass,dtype={'block_id' : str, 'hid' : str}).query("block_id.str.startswith('25')", engine='python')
        tab_NHRI = pd.read_csv(path_tab_NHRI,dtype={'block_id' : str, 'hid' : str}).query("~block_id.str.startswith('25')", engine='python')
        
        vartype = pd.concat([tab_Mass, tab_NHRI])

        if self.args['loglevel'] == "DEBUG":
            csv_fn = self.args["OutputFolder"] + '\\_logs\\' +tabtype+'_full.csv'
            vartype.to_csv(csv_fn, index=False)

        return vartype
    
    def process_aggregation(self, temp_df, processing_function):
        """
        Processes geographic data aggregation based on args filepath.

        This function correlates 'taz_id' from the input DataFrame with MPO and State information 
        from cached geographic data. It then applies a specified aggregation function to data 
        grouped by MPO.

        Parameters:
        - temp_df (pd.DataFrame): A DataFrame containing the data to be processed. This DataFrame
        must have 'taz_id' as one of its columns. It should contain only one non-index column 
        which will be processed by the aggregation function.
        - processing_function (function): A function that defines the aggregation operation to be 
        applied to the data grouped by MPO. This function will be applied to the non-index column 
        of temp_df.

        Note:
        - Ensure that 'taz_id' exists in temp_df and it aligns with the geographic data used for 
        aggregation.
        - The structure of temp_df should be validated before using this function to avoid unexpected 
        behavior.
        """

        # Bring in csv file with the data to associate row entries with block_id and taz_id to be associated with an MPO
        path_geo =  self.args["geo"]
        table = DBF(path_geo)
        taz_df = pd.DataFrame(iter(table))
        self.logger.debug('Number of records in TAZ dataframe = ' + str(len(taz_df)))
        taz_df = taz_df.drop(['OBJECTID', 'Shape_Leng', 'Shape_Area'], axis=1)

        # if the selected geogrpahic_area is not 'ALL', join the data and aggregate data - based on MA vs MPO
        df = pd.merge(temp_df, taz_df, left_on='taz_id', right_on='taz_id', how='inner')
        
        # Initialize a list to hold all aggregation results
        aggregation_results = []

        # Handle aggregation for all regions
        all_results = processing_function(df, 'all_')
        aggregation_results.append(all_results)

        # Handle aggregation for MA
        ma_results = processing_function(df[df['state'] == 'MA'], 'ma_')
        aggregation_results.append(ma_results)

        # Handle aggregation for each unique MPO
        unique_mpos = df['mpo'].unique()
        for mpo in unique_mpos:
            mpo_filtered_df = df[df['mpo'] == mpo]
            mpo_results = processing_function(mpo_filtered_df, f'mpo_{mpo}_'.lower())
            aggregation_results.append(mpo_results)

        # Concatenate all the results dataframes horizontally
        final_results_df = pd.concat(aggregation_results, axis=1)

        # Identify duplicate column headers
        duplicated_columns = final_results_df.columns.duplicated()

        # Select columns that are not duplicates
        final_results_df = final_results_df.loc[:, ~duplicated_columns]
        
        return final_results_df

    def load_per_and_hh_tables(self):
        """
        [Populate the per DB table from hh_per and taz_2010block_assignment CSV files.]
        [Populate the hh DB table from hh_per and taz_2010block_assignment CSV files.]
        """
    
        # Step 1: Populate the "per" table.
        #
        # Read hh_per (UrbanSim output file) into a DF
        #
        self.status_updater(self.status_pct[8][0], "DB loader: loading per_hh table")
        self.logger.debug("Preparing to populate the 'per' table.")
        hh_per_df = self.pick_sim_type("Population")
        
        # Read taz_2010block_assignment into a DF
        self.logger.debug("Reading 'block_assignment' table.")
        path_blk_assign = self.args['blk_assign']
        blk_assign_df = pd.read_csv(path_blk_assign, dtype={'block_id' : str})
        self.status_updater(self.status_pct[8][2], "DB loader: loading per table")
        # Populate the "per" table
        join_df = pd.merge(left=hh_per_df, right=blk_assign_df, how="left", left_on='block_id', right_on='block_id')
        per_df = join_df[['block_id', 'taz_id', 'hid', 'person_num', 'age', 'wage_inc', 
                                      'is_worker', 'persons', 'children', 'workers']]
        self.logger.debug("Populating 'per' table.")
        per_df.to_sql(name="per",con=self.db_conn,if_exists="append",index=False)
        
        def per_processing_function(per_df, column_prefix):
            tot_persons = len(per_df)
            temp_df = per_df[per_df['age'] < 18]
            tot_children = len(temp_df)
            temp_df = per_df[per_df['is_worker']  != 0]
            tot_workers = len(temp_df)
            temp_df = per_df[per_df['age'] >= 16]
            tot_drivers = len(temp_df)
                
            data = { 'type' : ['children', 'workers', 'drivers', 'total'  ],
                    f'{column_prefix}_count'    : [tot_children, tot_workers, tot_drivers, tot_persons] }
            
            return pd.DataFrame(data)
        
        summary_df = self.process_aggregation(per_df, per_processing_function)
        csv_fn = self.args["OutputFolder"] + '\\_summary\\zonal\\' + 'person_table_summary.csv'
        summary_df.to_csv(csv_fn, index=False)
        
        # Step 2: populate "hh" table.
        #
        self.logger.debug("Preparing to populate the 'hh' table.")
        
        # Get unique records per household
        hh_base_df = join_df.groupby(by="hid").first().copy()

        # drivers per household
        drivers_df = pd.DataFrame(
                join_df[join_df['age']>=16].groupby('hid')['hid'].count()
                ).rename(columns={"hid":"drivers"})
        temp_hh_df = hh_base_df.merge(drivers_df, how="left", on='hid').fillna(0)

        # seniors per household
        seniors_df = pd.DataFrame(
                join_df[join_df['age']>=65].groupby('hid')['hid'].count()
                ).rename(columns={"hid":"seniors"})
        temp_hh_df = temp_hh_df.merge(seniors_df, how="left", on='hid').fillna(0)

         # non-working seniors per household
        nwseniors_df = pd.DataFrame(
                join_df[(join_df['age']>=65)&(join_df['is_worker']==0)].groupby('hid')['hid'].count()
                ).rename(columns={"hid":"nwseniors"})
        temp_hh_df = temp_hh_df.merge(nwseniors_df, how="left", on='hid').fillna(0)

        # non-worker adult per household
        non_wrkr_df = pd.DataFrame(
                join_df[(join_df['age']>=18) & (join_df['age']<65) & (join_df['is_worker']==0)].groupby('hid')['hid'].count()
                ).rename(columns={"hid":"nwadult"})

        temp_hh_df = temp_hh_df.merge(non_wrkr_df, how="left", on='hid').fillna(0)

        # Indicate if the household is 'low-income', 'medium-income', or 'high-income'
        # according to a household size-based measure of income
        self.status_updater(self.status_pct[8][6], "DB loader: Loading hh income classification table")
        # Load CSV file with hh size-based low/medium/high income classification factors:
        self.logger.debug("Loading hh income classification table:")
        path_hh_inc_classifier = self.args['HH Income Segments']
        inc_classification_df = pd.read_csv(path_hh_inc_classifier)
        self.logger.debug(inc_classification_df.head(8))

        # Utility 'household income given household size classifer' function: 
        #     returns 3 if household income is high, given its size
        #             2 if household income is medium, given its size
        #             1 if household income is low, given its size
        def income_category_by_hh_size(row):
            if row['hh_inc'] < row['low_income_threshold']:
                retval = 1 # low-income
            elif row['hh_inc'] > row['high_income_threshold']:
                retval = 3 # high-income
            else:
                retval = 2 # medium-income, i.e. NOT low AND NOT high
            return retval
        
        # The hh income classifier caps hh size at 8: account for this
        temp_hh_df['persons_capped'] = temp_hh_df.apply(lambda row: 8 if row['persons'] > 8 else row['persons'], axis=1)
        temp_hh_df = temp_hh_df.reset_index().merge(inc_classification_df, how="left", left_on='persons_capped', right_on='hh_size')
        temp_hh_df['hh_inc_cat_by_size'] = temp_hh_df.apply(lambda row: income_category_by_hh_size(row), axis=1)
        
        hh_df = temp_hh_df[['block_id', 'taz_id', 'hid', 'persons', 'hh_inc', 'hh_inc_cat_by_size', 
                                        'children', 'seniors', 'nwseniors', 'workers', 'drivers','nwadult']]
        self.logger.debug("Populating 'hh' table.")
        hh_df.to_sql(name="hh",con=self.db_conn,if_exists="append",index=False)

        def hh_dem_proc_function(hh_df, column_prefix):
            '''
            Processes household demographic data
            '''

            tot_hh = len(hh_df)
            tot_persons = hh_df['persons'].sum()
            tot_children = hh_df['children'].sum()
            tot_seniors = hh_df['seniors'].sum()
            tot_nwseniors = hh_df['nwseniors'].sum()
            tot_nwadult = hh_df['nwadult'].sum()
            tot_workers = hh_df['workers'].sum()
            tot_drivers = hh_df['drivers'].sum()
            data = { 'attribute' : ['total', 'avg size', 'avg children', 'avg seniors',
                                'avg nw seniors','avg nw adults', 'avg workers', 'avg drivers'],
                    f'{column_prefix}_values'    : [tot_hh, 
                                tot_persons/tot_hh, 
                                tot_children/tot_hh, 
                                tot_seniors/tot_hh, 
                                tot_nwseniors/tot_hh, 
                                tot_nwadult/tot_hh, 
                                tot_workers/tot_hh, 
                                tot_drivers/tot_hh] }
            return pd.DataFrame(data)
        
        summary_df = self.process_aggregation(hh_df, hh_dem_proc_function)
        csv_fn = self.args["OutputFolder"] + '\\_summary\\zonal\\' + 'household_table_summary.csv'
        summary_df.to_csv(csv_fn, index=False)
        
        # Total Households by Size Income
        def hh_inc_proc_function(hh_df, column_prefix):
            '''
            Processes household incomes
            '''

            # Count the number of households in each income category
            tot_high = len(hh_df[hh_df['hh_inc_cat_by_size'] == 3])
            tot_medium = len(hh_df[hh_df['hh_inc_cat_by_size'] == 2])
            tot_low = len(hh_df[hh_df['hh_inc_cat_by_size'] == 1])
            tot_all = len(hh_df)
            data = { 'attribute' : ['high', 'medium', 'low', 'total'],
                    f'{column_prefix}_values'    : [
                                tot_high, 
                                tot_medium, 
                                tot_low,
                                tot_all] }
            return pd.DataFrame(data)
        
        summary_df = self.process_aggregation(hh_df, hh_inc_proc_function)
        csv_fn = self.args["OutputFolder"] + '\\_summary\\zonal\\' + 'household_income_table_summary.csv'
        summary_df.to_csv(csv_fn, index=False)
        
        return None
    # end_def load_per_and_hh_tables()
    
    def load_block_sed_table(self):
        """
        [Populate the block_sed DB table from the "emp" and "blk_assign" CSV files.]
        """

        self.logger.debug("Entering load_block_sed_table.")
        # Read the "Employment" CSV into a DF
        # OK

        self.block_sed_df = self.pick_sim_type("Employment")
        
        # Read the "taz_block_assignment" CSV into a DF
        self.logger.debug("Reading 'block_assignment' table.")
        path_blk_assign = self.args['blk_assign']
        blk_assign_df = pd.read_csv(path_blk_assign, dtype={'block_id' : str, 'taz_id' : int})
        
        self.logger.debug("Head of blk_assign_df:")
        self.logger.debug(blk_assign_df.head(20))
        
        # Join the 2 DFs on 'block_id'
        self.block_sed_df = pd.merge(left=self.block_sed_df, right=blk_assign_df, how="left", left_on='block_id', right_on='block_id')
        
        # Debug
        self.logger.debug("Head of merged block_sed_df:")
        self.logger.debug(self.block_sed_df.head(20))
        
        self.logger.debug("Populating 'block_sed' table.")
        self.block_sed_df.to_sql(name="block_sed",con=self.db_conn,if_exists="append",dtype={'taz_id':'int'},index=False)
        

        def block_sed_processing_function(block_sed_df, column_prefix):
            # Generate summary report
            totals_by_sector = [ 0 for i in range(1,11) ] # list[0] -> sector 1, etc.
            job_sec_lst = ['1_constr' ,	'2_eduhlth'	,	'3_finance'	,	'4_public',	'5_info',
                        '6_ret_leis',	'7_manu',	'8_other',	'9_profbus',  '10_ttu']	
            for sector_num in range(1,11):
                col_name = job_sec_lst[sector_num-1] 
                totals_by_sector[sector_num-1] = block_sed_df[col_name].sum()
            #
            data = { 'sector'     : [ 'Construction', 'Education and Health Services',
                                    'Financial Activities', 'Public Administration',
                                    'Information', 'Retail, Leisure, and Hospitality',
                                    'Manufacturing', 'Other Services',
                                    'Professional and Business Services', 'Trade, Transportation, and Utilities' ],
                    f'{column_prefix}_total_jobs' : totals_by_sector }
            
            return pd.DataFrame(data)
        
        summary_df = self.process_aggregation(self.block_sed_df, block_sed_processing_function)
        csv_fn = self.args["OutputFolder"] + '\\_summary\\zonal\\' + 'block_sed_summary.csv'
        summary_df.to_csv(csv_fn, index=False)

        return None

    def load_block_allocation_table(self):
        """
        [Populate the taz_block_allocation DB table from the taz_2010block_allocation CSV file.]
        """
        path_blk_alloc = self.args['blk_alloc']
        self.blk_alloc_df = pd.read_csv(path_blk_alloc)
        self.logger.debug("Populating 'block_alloc' table.")
        self.blk_alloc_df.to_sql(name="taz_block_allocation",con=self.db_conn,if_exists="append",index=False)
        return None

    def load_block_assignment_table(self):
        """
        [Populate the taz_block_assignment DB table from the taz_2010block_assignment CSV file.]
        """
        path_blk_assign = self.args['blk_assign']
        self.blk_assign_df = pd.read_csv(path_blk_assign)
        self.logger.debug("Populating 'block_assign' table.")
        self.blk_assign_df.to_sql(name="taz_block_assignment",con=self.db_conn,if_exists="append",index=False)
        return None
        
    
    def hh_preprocessor(self):
        """generate helper attributes for household data
        """

if __name__ == "__main__":
    loader = db_loader()
    loader.load_taz_table()
    loader.load_parking()
    loader.load_walkbike()
    loader.load_enrollment()
    loader.load_per_and_hh_tables()
    loader.load_block_sed_table()
    loader.load_block_allocation_table()