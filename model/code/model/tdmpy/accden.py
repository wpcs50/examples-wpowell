# Import required packages.
from asyncio.log import logger
import pandas as pd 
import geopandas as gpd
import copy
from .base import disagg_model

# Symbolic constants
#
METERS_PER_MILE = 1609.34
SQUARE_METERS_PER_SQUARE_MILE = 2.59e+6
#
ONE_MILE_BUFFER = METERS_PER_MILE
HALF_MILE_BUFFER = (0.5*METERS_PER_MILE)
QUARTER_MILE_BUFFER = (0.25*METERS_PER_MILE)
#
# Enumeration constants for TDM23 transit 'MODES'
#
MODE_LOCAL_BUS = 1
MODE_EXPRESS_BUS = 2
MODE_BUS_RAPID_TRANSIT = 3
MODE_LIGHT_RAIL = 4
MODE_HEAVY_RAIL = 5
MODE_COMMUTER_RAIL = 6
MODE_FERRY = 7
MODE_SHUTTLE = 8
MODE_RTA_LOCAL_BUS = 9
MODE_REGIONAL_BUS = 10
#
# Symbolic constants for Population+Employment Density threshold values
#
POP_EMP_DENSITY_THRESHOLD_FOR_DENSE_URBAN  = 10000
POP_EMP_DENSITY_THRESHOLD_FOR_URBAN        = 7500
POP_EMP_DENSITY_THRESHOLD_FOR_FRINGE_URBAN = 5000
#
# Enumeration constants for Population+Employment Density classification
#
POP_EMP_DENSITY_UNDEFINED   = 0      # 'No data' indicator
POP_EMP_DENSITY_HIGH        = 1      # pop-emp density > 10,000 per sqmi
POP_EMP_DENSITY_HIGH_MEDIUM = 2      # pop-emp density > 7,500 per sqmi and < 10,000 per sqmi
POP_EMP_DENSITY_LOW_MEDIUM  = 3      # pop-emp density > 5,000 per sqmi and < 5,000 per sqmi
POP_EMP_DENSITY_LOW         = 4      # pop-emp density < 5,000 per sqmi
POP_EMP_DENSITY_NO_DATA     = 0      # No pop-emp data for TAZ
#
# Enumeration constants for Transit Access Density classifications
#
TAD_CLASS_CBD = 1
TAD_CLASS_DENSE_URBAN = 2
TAD_CLASS_URBAN = 3
TAD_CLASS_FRINGE_URBAN = 4
TAD_CLASS_SUBURBAN = 5
TAD_CLASS_RURAL = 6


class access_density(disagg_model):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)    
        if "init" in kwargs:
            # print (kwargs)
            if kwargs["init"]:
                self.init_logger()
        else:
            pass

        logger.debug("arguments passed in %s" %kwargs)

        # initialize the status
        self.status_pct = [0, 5, 5, 8, 8, 9, 14, 14, 14, 88, 99, 99, 100]

        # clear records from output table
        self.db._raw_query("delete from access_density;") 
        self.db._raw_query("delete from terminal_times;") 
    
       
    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:
            if self.args['acc_load']==1:
                tr_acc_df = self.load_transit_access_density(self.args['tr_acc_den_load'])
            else:
                tr_acc_df = self.calc_transit_access_density()
            
            summary_df = self.print_summary_info(tr_acc_df)
            term_times = self.calc_terminal_times(tr_acc_df,
                                                       self.args['tt_lk_up'])
            
            # Export the DataFrame to SQL and CSV
            self.export_data(tr_acc_df,
                             self.args['OutputFolder'] + '\\_networks\\' + 'access_density.csv',
                             "access_density")
            self.export_data(summary_df,
                             self.args['OutputFolder'] + '\\_summary\\zonal\\' + 'access_density_summary.csv')
            self.export_data(term_times,
                             self.args['OutputFolder'] + '\\_networks\\' + 'terminal_times.csv',
                             "terminal_times")

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
        
    # Utility function: dump geodataframe to shapefile
    def dump_gdf_to_shapefile(self, gdf, shapefile_fq_path):
        gdf.to_file(shapefile_fq_path)
    # dump_gdf_to_shapefile()
    
    # Utility function to dump one row of a 'stops' table
    def dump_stop(row):
        s = 'Stop = ' + row['stop_name'] + ' Route = ' + row['route_name'] + ' Mode = ' + str(row['mode'])
        print(s)
    # dump_stop()

    def calc_transit_access_density(self):
        """
        calc_transit_access_density: This is the 'new' driver for transit access density calculation.
        
        It calls 'helper' functions to perform initialization, and calculate each of the 6 levels of transit access density.
        The 'helper' functions modify self.taz_df, updating it with per-TAZ access density classification.
        The 'helper' functions also return a list of the TAZes to which each classification was assigned;
        this was useful during development and debug, and has been reatined in case it might be helpful in future also.
        
        NOTE: The 'helper' functions MUST be called in the order specified in this 'driver' routine.
              Each 'helper' function relies upon its predecessor(s) to have culled previously-classified TAZes 
              from the 'universe of discourse' being worked on at the time.
              
        inputs:         TAZ shapefile
                        database 'hh' table
                        transit 'routes' shapefile, generated by TransCAD
                        transit 'stops' shapefile, generated by TransCAD
                        transit 'nodes' shapefile, generated by TransCAD
                        Bus headway CSV file                        
        outputs:        database 'access_density' table
                        optional CSV file with per-TAZ access_density classification;
                        optional shapefile with per-TAZ access_density classification
       
        Side Effects:
        - Initializes various dataframes for TAZes, transit routes, stops, and other 
        transit related features.
        - Updates TAZ dataframe with population, employment data and classifies them based 
        on population and employment density.
        - Modifies CRS of geospatial data to match CTPS standard.
        
        Returns:
        tuple of pandas.DataFrame
            Returns multiple dataframes related to TAZ, transit routes, stops, headway data,
            and other classifications.
        """
        self.logger.debug('Calling initialize_for_tad_calc.')
        taz_df, routes_df, stops_df, all_stops_df, \
                hr_stops_df, cr_stops_df, lr_stops_df, all4_lrStop_df, \
                nodes_df, nodesf5_df, nodesf15_df, sub_routes_df = self.initialize_for_tad_calc(self.args["geo"])

        self.logger.debug('Calling calc_cbd_tazes.')
        cbd_taz_list, taz_df = self.calc_cbd_tazes(taz_df,
                                                   hr_stops_df,
                                                   all4_lrStop_df)
        self.logger.debug('Calling calc_dense_urban_tazes.')
        dense_urban_taz_list, taz_df = self.calc_dense_urban_tazes(taz_df,
                                                                   hr_stops_df,
                                                                   lr_stops_df)
        self.logger.debug('Calling calc_urban_tazes.')
        urban_taz_list, taz_df = self.calc_urban_tazes(taz_df,
                                                       cr_stops_df,
                                                       nodesf5_df)
        self.logger.debug('Calling calc_fringe_urban_tazes.')
        fringe_urban_taz_list, taz_df = self.calc_fringe_urban_tazes(taz_df,
                                                                     cr_stops_df,
                                                                     hr_stops_df,
                                                                     lr_stops_df,
                                                                     nodesf15_df)
        self.logger.debug('Calling calc_suburban_tazes.')
        suburban_taz_list, taz_df = self.calc_suburban_tazes(taz_df, sub_routes_df) 
        self.logger.debug('Calling calc_rural_tazes.')
        rural_taz_list, taz_df = self.calc_rural_tazes(taz_df)
        
        # Export results to DB table;
        # also export CSV file to _networks directory
        tr_acc_df = taz_df[["taz_id","access_density"]]
        self.logger.debug('Returning from calc_transit_access_density.')

        return tr_acc_df
    # end_def calc_transit_access_density()

    def initialize_for_tad_calc(self, path_taz):
        """
        initialize_for_tad_calc: Initialize data structures used to calculate transit access density.
        inputs:
        outputs:
        side effects:   Creates taz_df - this is the 'universe of discourse: all TAZes in the model'
                                routes_df - transit routes
                                stops_df - transit stops
                                all_stops_df - stops_df joined to routes_df
                                hr_stops_df - heavy rail rapid transit stops
                                cr_stops_df - commuter rail stops
                                lr_stops_df - light rail rapid transit stops
                                all4_lrStop_df - light rail rapid transit stops carrying all 4 green line routes
                                nodes_df - TransCAD 'nodes'
                                nodesf5_df - 'nodes' with < 5 minute headway
                                nodesf15_df - 'nodes' with < 15 minute headway
                                sub_routes_df - stops for bus routes identifying 'suburban' TAZes
        returns:        None
        """   
        # Path to the TAZ 'shapefile'.
        self.logger.debug('Using TAZ shapefile: ' + path_taz)
                
        # All transit routes shapefile
        path_routes = self.args['OutputFolder'] + '\\_networks\\routes.shp'
        self.logger.debug('Using transit routes shapefile: ' + path_routes)
        
        # All stops shapefile (must have line column)
        path_stops = self.args['OutputFolder'] + '\\_networks\\stops.shp'
        self.logger.debug('Using stops shapefile: ' + path_stops)
        
        # headway CSV file
        path_hdwy = self.args['OutputFolder'] + "\\_networks\\stop_hdwy.csv"
        self.logger.debug('Using headway file: ' + path_hdwy)
        
        # Read input data: TAZ shapefile
        taz_df = gpd.read_file(path_taz)
        self.logger.debug('Number of records in TAZ shapefile = ' + str(len(taz_df)))
        
        taz_df = taz_df.rename(columns=str.lower)
        # Filter out all fields except taz_id, land_area, and geometry
        taz_df = taz_df[['taz_id','geometry', 'land_area']]
        
        # Load the "hh" database table into a dataframe
        query_string = "SELECT * from hh;"
        hh_df = self.db._raw_query(qry=query_string)
  
        # Remove un-needed columns
        hh_df = hh_df.drop(columns=['block_id', 'hid', 'hh_inc', 'children', 'seniors', 'drivers'])
    
        # Get per-TAZ total population, total employment, and number of households
        hh_df['num_hh'] = 1
        aggregate_se_data = hh_df.groupby(['taz_id']).agg(tot_pop = ( 'persons', 'sum' ),
                                                          tot_hh  = ( 'num_hh', 'sum' ),
                                                          tot_emp = ( 'workers', 'sum' ))
        self.logger.debug('Number of records (TAZes) in aggregate SE data: ' + str(len(aggregate_se_data)))
        
        taz_df = pd.merge(left=taz_df, right=aggregate_se_data, how='left', left_on='taz_id', right_on='taz_id')
        self.logger.debug('Number of records in TAZ-joined-to-SE-data: ' + str(len(taz_df)))
        
        # Read input data: All transit routes
        routes_df = gpd.read_file(path_routes)
        routes_df = routes_df.rename(columns=str.lower)
        # Transform routes layer's CRS to CTPS Standard SRS: 'EPSG:26986'" (Massachusetts State Plane, NAD 83, meters)
        routes_df = routes_df.to_crs("EPSG:26986")
        
        # Read input data: All transit stops (Note: Input records must have a 'line' column)
        stops_df = gpd.read_file(path_stops) 
        stops_df = stops_df.rename(columns=str.lower)
        stops_df = stops_df[stops_df['route_id'].isin(routes_df['route_id'])]

        # Transform stops layer's CRS to CTPS Standard SRS: 'EPSG:26986'" (Massachusetts State Plane, NAD 83, meters)
        stops_df = stops_df.to_crs("EPSG:26986")
         
        # Join stops to routes in order to get 'mode' and 'route_name' of service at each stop
        all_stops_df = pd.merge(left=stops_df, right=routes_df, how="left", left_on='route_id', right_on='route_id')

        # Prune un-needed fields from all_stops_df, and rename 'geometry_x' column to 'geometry'
        all_stops_df = all_stops_df.drop(columns=['pass_count', 'milepost', 'distanceto', 
                                                  'fare_zone', 'available_y', 
                                                  'time_next', 'geometry_y'])                                                 
        all_stops_df = all_stops_df.rename(columns={'geometry_x' : 'geometry',
                                                    'available_x' : 'available'})  
        
        # Read input data  headway file
        hdwy_df = pd.read_csv(path_hdwy)
        
        # Having loaded the raw data, continue with initialization:
        #
        # Create individual dataframes from the 'stops' dataframe for the
        #     (1) heavy rail rapid transit mode
        #     (2) commuter rail mode
        #     (3) light rail rapid transit mode
        #     (4) light rail rapid transit mode stops carrying all 4 green line branches, a subset of (3)
        
        # Heavy rail rapid transit 
        hr_stops_df = copy.deepcopy(all_stops_df.loc[all_stops_df['mode'].isin([MODE_HEAVY_RAIL])]) 
        hr_stops_df = hr_stops_df.rename(columns=str.lower)
        # Remove un-needed fields
        hr_stops_df = hr_stops_df.loc[:,['id', 'mode', 'route_name', 'stop_name', 'near_node', 'geometry']]

        # Commuter Rail 
        cr_stops_df = copy.deepcopy(all_stops_df.loc[all_stops_df['mode'].isin([MODE_COMMUTER_RAIL])]) 
        cr_stops_df = cr_stops_df.rename(columns=str.lower)
        # Remove un-needed fields
        cr_stops_df = cr_stops_df.loc[:,['id', 'mode', 'route_name', 'stop_name', 'near_node', 'geometry']] 
         
        # Light Rail (Green Line and Mattapan Line
        lr_stops_df = copy.deepcopy(all_stops_df.loc[all_stops_df['mode'].isin([MODE_LIGHT_RAIL])]) 
        lr_stops_df = lr_stops_df.rename(columns=str.lower)
        lr_stops_df = lr_stops_df.loc[:,['id', 'mode', 'route_name', 'stop_name', 'near_node', 'geometry']]  
                
        # Light Rail subset: the 4 Green Line stops carrying all 4 Green Line routes
        all4_lrStop_df = copy.deepcopy(lr_stops_df.loc[lr_stops_df['stop_name'].isin(
                                       ['PARK STREET', 'BOYLSTON', 'ARLINGTON', 'COPLEY'])])   
        # Remove un-needed fields
        all4_lrStop_df = all4_lrStop_df.loc[:,['id', 'mode', 'route_name', 'stop_name', 'near_node', 'geometry']]
        
        # Comment from notebook: Just get Nodes from Stops
        nodes_df = copy.deepcopy(all_stops_df.groupby('near_node').first().reset_index()) # this makes it one record per node id
        nodes_df = nodes_df[['near_node', 'geometry']]
        
        # Prep for generating bus service stop buffers:
        # Merge nodes with the bus service headway data
        nodes_df = pd.merge(left=nodes_df, right=hdwy_df, how='left', left_on='near_node', right_on='near_node')

        # Cleanup artifacts of the merge:
        nodes_df = nodes_df.rename(columns={'node_x' : 'node'})
        
        # Drop any record with 3 or more NULL values in the _joined_ columns 
        nodes_df = nodes_df.dropna(how = 'any', thresh=3) 
        nodesf5_df = nodes_df.query('am_hdwy <= 5') # this filters so headway < 5min
        nodesf15_df = nodes_df.query('am_hdwy <= 15') # this filters so headway < 15min   
        
        # Bus routes used to identify SUBURBAN TAZes
        sub_routes_df = copy.deepcopy(routes_df.loc[routes_df['mode'].isin([MODE_LOCAL_BUS,
                                                                            MODE_EXPRESS_BUS,
                                                                            MODE_LIGHT_RAIL,
                                                                            MODE_HEAVY_RAIL,
                                                                            MODE_RTA_LOCAL_BUS])])
        
        # Calculate 'Population+Employment' Density for TAZs: Density = (Population + Employment)/Area in Sq Mi
        # TDM23: Deal with TAZes for which we have no Pop or Emp data
        # NOte: The 'x != x' test is the Pythonic way to test for a NULL or NaN value.
        self.logger.info("timestamp 1")

        taz_df['tot_pop'] = taz_df.apply(lambda row: 0 if row['tot_pop'] != row['tot_pop'] else row['tot_pop'], axis=1)
        taz_df['tot_emp'] = taz_df.apply(lambda row: 0 if row['tot_emp'] != row['tot_emp'] else row['tot_emp'], axis=1)
        taz_df['Pop_Emp_Density'] = (taz_df['tot_pop']+taz_df['tot_emp'])/(taz_df['land_area'])
        
        # Sanity check: Report number of TAZes with 0 'Pop_Emp_Density'
        no_pe_density_df = taz_df[taz_df['Pop_Emp_Density'].isnull()]
        self.logger.debug('Number of records with NULL Pop_Emp_Density (should be 0): ' + str(len(no_pe_density_df)))
        
        zero_pe_density_df = taz_df[taz_df['Pop_Emp_Density']==0]
        self.logger.debug('Number of TAZes with Pop-Emp Density == 0: ' + str(len(zero_pe_density_df)))
        
        # 5-way classificaiton of each TAZ's 'population + employment' density:
        #     0 --> no data
        #     1 --> high
        #     2 --> high medium
        #     3 --> low medium
        #     4 --> low
        #
        # This classification is used only for 'sanity checking' calculated results.
        # Unlike the Jupyter notebook from which it was derived, this code relies
        # upon explicit tests of the Pop_Emp_Density attribute of TAZes rather than
        # this classification.

        def classify_pop_emp_density(row):
            if row['Pop_Emp_Density'] >= POP_EMP_DENSITY_THRESHOLD_FOR_DENSE_URBAN:
                retval = POP_EMP_DENSITY_HIGH
            elif row['Pop_Emp_Density'] >= POP_EMP_DENSITY_THRESHOLD_FOR_URBAN:
                retval = POP_EMP_DENSITY_HIGH_MEDIUM
            elif row['Pop_Emp_Density'] >= POP_EMP_DENSITY_THRESHOLD_FOR_FRINGE_URBAN:
                retval = POP_EMP_DENSITY_LOW_MEDIUM
            elif row['Pop_Emp_Density'] > 0:
                retval = POP_EMP_DENSITY_LOW
            else:
                retval = POP_EMP_DENSITY_NO_DATA
            return retval
        # end_def
        # self.logger.info("timestamp 2")
        self.status_updater(self.status_pct[2],"access density: classifying pop_emp_density")
        taz_df['Den_Flag'] = taz_df.apply(lambda row: classify_pop_emp_density(row), axis=1)
        
        # Final step in 'prepping' the taz_df for transit_access_density calculation:
        # Add a column to contain the T-A-D classification code.
        taz_df['access_density'] = POP_EMP_DENSITY_UNDEFINED

        return taz_df, routes_df, stops_df, all_stops_df, \
                hr_stops_df, cr_stops_df, lr_stops_df, all4_lrStop_df, \
                nodes_df, nodesf5_df, nodesf15_df, sub_routes_df
    # end_def initialize_for_tad_calc()
    
    def calc_cbd_tazes(self, taz_df, hr_stops_df, all4_lrStop_df):
        """
        calc_cbd_tazes: Determine which TAZes meet the criteria for being classified as 'CBD'(Central Business District')
        inputs:         taz_df, hr_stops_df, all4_lrStop_df
        outputs:        Updated taz_df
        side effects:   None
        returns:        List of TAZes classified as 'CBD'
        """
        # Criteria for being a 'CBD' TAZ: 
        #     the TAZ is within 1/2 mile of multiple heavy-rail rapid transit stops 
        # OR
        #     the TAZ is within 1/4 mile of the 4 'core' green line stops 
        # AND
        #    more than 50% of the TAZ's area is within the union of these 1/2- and 1/4-mile buffers

        # Make a copy of hr_stops to buffer
        hr_buf = copy.deepcopy(hr_stops_df)

        # Buffer the heavy rail rapid transit stops
        # Keep attribute data by replacing the geometry column
        hr_buf['geometry'] = hr_buf.buffer(HALF_MILE_BUFFER)
        
        # Group by line and dissolve
        hr_buf_dis = hr_buf.dissolve(by='route_name').reset_index()
        # Keep only the fields of interest
        hr_buf_dis = hr_buf_dis[['mode', 'route_name', 'geometry']]
        
        # Make a copy of lr_stops to buffer
        lr4_buf = copy.deepcopy(all4_lrStop_df)

        # Buffer the 'subset' light rail stops
        lr4_buf['geometry'] = lr4_buf.buffer(QUARTER_MILE_BUFFER)

        # Group and dissolve
        lr4_buf_dis = lr4_buf.dissolve(by='mode').reset_index()
        # Keep only the fields of interest
        lr4_buf_dis = lr4_buf_dis[['mode', 'route_name', 'geometry']].reset_index()

        # Get individual geo-dataframes for each of the 3 heavy rail rapid transit lines 
        # Each such line can be identified by the ROUTE_NAME field, but note that there are distinct ROUTE_NAME values.
        # For example these are the possible 'ROUTE_NAMEs' that identify the Red line:
        #    1. 'Red Line (Alewife - Ashmont):Red'
        #    2. 'Red Line (Ashmont - Alewife):Red'
        #    3. 'Red Line (Alewife - Braintree):R'
        #    4. 'Red Line (Braintree - Alewife):R'
        # Note that the trailing "R's" (rather than a trailing 'Red') in (3) and (4)
        red = hr_buf_dis[hr_buf_dis['route_name'].str.startswith('Red Line')]
        blue = hr_buf_dis[hr_buf_dis['route_name'].str.startswith('Blue Line')]
        orange = hr_buf_dis[hr_buf_dis['route_name'].str.startswith('Orange Line')]

        # Get all overlaps between route buffers
        rb = gpd.overlay(red, blue, how='intersection')
        ob = gpd.overlay(orange, blue, how='intersection')
        ro = gpd.overlay(red, orange, how='intersection')

        # Put overlaps into one geodataframe with overlaps not overlapping (union)
        rbob = gpd.overlay(rb, ob, how='union')
        rbobro = gpd.overlay(rbob, ro, how='union', keep_geom_type=False)

        # Add in the 4 stops of the Green Line light rail carrying all 4 Green Line branches ('B', 'C', 'D', and 'E')
        rbobro = rbobro.append(lr4_buf)

        # Turn into a single multi part polygon, i.e., a geodataframe with one row
        #
        # The next statement isolates Red, Orange, Blue, and the 4 'main' stops of the Green Line.
        # This is accomplished by  setting the 'mode' for these records to 
        # the synthetic value 'red_orange_blue_green_4', and dissolving on the 'mode' field.
        rbobro['mode'] = 'red_orange_blue_green_4'
        rbobro = rbobro.dissolve(by='mode').reset_index()
        # Get only fields we need
        rbobro = rbobro[['mode', 'geometry']]

        # Select the TAZes that intersect with 'red_orange_blue_green_4'
        taz_hr = gpd.overlay(taz_df, rbobro, how='intersection')

        self.logger.debug("Number of TAZes in 'rapid transit buffer': " + str(len(taz_hr)))

        # Caculate the total area of these polygons: each of which is either a TAZ or a portion of one
        taz_hr['area_of_intersection'] = taz_hr.area
        taz_df['taz_area'] = taz_df.area
        
        # Remove un-needed columns from 'taz_hr' before merging taz_df with it
        taz_hr = taz_hr.loc[:, ['taz_id', 'area_of_intersection', 'geometry']]

        # Join the 'TAZes intersecting with the "R,B,O,G4"' dataframe (i.e., taz_hr') to the main TAZ dataframe,
        # and calculate the percentage of each TAZ that intersects with the "R,B,O,G4" dataframe.

        taz_df = pd.merge(taz_df, taz_hr, how='left', left_on='taz_id', right_on='taz_id')

        # Indicate 'CBD' TAZes:
        # If more than 50% of a TAZ intersects with the "R,B,O,G4" buffer, flag it as a 'CBD' TAZ. 
        taz_df['pct_cbd_hr_lr'] = taz_df['area_of_intersection'] / taz_df['taz_area']

        # Record the TAZ as a CBD TAZ
        self.logger.info("timestamp 3")
        taz_df['access_density'] = taz_df.apply(lambda row: TAD_CLASS_CBD if row['pct_cbd_hr_lr'] > 0.5 else row['access_density'], axis=1)
        self.logger.debug("\tNumber of CBD TAZes = " + str(len(taz_df[taz_df['access_density'] == TAD_CLASS_CBD])))
        
        # Turn 'taz_df' back into a geodataframe:
        # The 'merge' above merged two geodataframes, each with a 'geometry' column.
        # The result of the merge was a vanilla dataframe with a 'geometry_x' and a 'geometry_y' column.
        # The dataframe has to have a (single) 'geometry' field in order to be a geodataframe and be usable
        # in geographic calculations. Rename the 'geometry_x' column to 'geometry'.
        #
        taz_df = taz_df.rename(columns={'geometry_x': 'geometry'})
        taz_df = taz_df.drop(columns=['geometry_y'])
        taz_df = gpd.GeoDataFrame(taz_df)

        # Make sure that taz_df doesn't have duplicates because of light rail and heavy rail being different buffer polygons.
        # First sort so that when deleting duplicates, we delete the ones that didn't pass CBD muster
        taz_df = taz_df.sort_values(by=['access_density'], ascending=False)
        # Delete duplicates
        taz_df = taz_df.drop_duplicates('taz_id')

        cbd_taz_df = taz_df[taz_df['access_density'] == TAD_CLASS_CBD]
        cbd_taz_list = cbd_taz_df['taz_id'].tolist()
        return cbd_taz_list, taz_df
    # end_def calc_cbd_tazes()
    
    def calc_dense_urban_tazes(self, taz_df, hr_stops_df, lr_stops_df):
        """
        calc_dense_urban_tazes: Determine which TAZes meet the criteria for being classified as a 'Dense Urban TAZ'
        inputs:         taz_df, hr_stops_df, lr_stops_df
        outputs:        N/A
        side effects:   Updates taz_df
        returns:        List of TAZes classified as 'Dense Urban', Updated taz_df    
        """
        dense_urban_taz_list = [] 

        working_taz_df = copy.deepcopy(taz_df)

        working_taz_df = working_taz_df[working_taz_df['access_density'] == 0]        
        self.logger.debug('\tcalc_dense_urban: length of working_taz_df after initial culling: ' + str(len(working_taz_df)))
        
        working_taz_df = working_taz_df[working_taz_df['Pop_Emp_Density'] > POP_EMP_DENSITY_THRESHOLD_FOR_DENSE_URBAN]
        self.logger.debug('\tcalc_dense_urban: Number of candidate TAZes with P+E density > 10,000 = ' + str(len(working_taz_df)))
        
        hr_buf = copy.deepcopy(hr_stops_df) 
        lr_buf = copy.deepcopy(lr_stops_df)
        
        # Perform the buffering
        hr_buf['geometry'] = hr_buf.buffer(HALF_MILE_BUFFER)
        lr_buf['geometry'] = lr_buf.buffer(HALF_MILE_BUFFER)
        
        # Dissolve each of the buffer dataframes, in each case creating a dataframe with a single record
        hr_buf_dis_du = hr_buf.dissolve().reset_index()
        lr_buf_dis = lr_buf.dissolve().reset_index()

        taz_intersect_hr_list = []
        taz_intersect_lr_list = []

        for index, row in gpd.overlay(hr_buf_dis_du, working_taz_df, how='intersection').iterrows():
            taz_intersect_hr_list.append(row['taz_id'])

        for index, row in gpd.overlay(lr_buf_dis, working_taz_df, how='intersection').iterrows():
            taz_intersect_lr_list.append(row['taz_id'])

        dense_urban_taz_list = taz_intersect_hr_list + taz_intersect_lr_list

        # Unique-ify the list
        dense_urban_taz_list = list(set(dense_urban_taz_list))        
        self.logger.info("timestamp 4")
        taz_df['access_density'] = taz_df.apply(lambda row: TAD_CLASS_DENSE_URBAN if row['taz_id'] in dense_urban_taz_list else row['access_density'], axis=1)
        
        self.logger.debug("\tNumber of 'dense urban' TAZes = " + str(len(dense_urban_taz_list)))
        return dense_urban_taz_list, taz_df
    # end_def calc_dense_urban_tazes()
    
    def calc_urban_tazes(self, taz_df, cr_stops_df, nodesf5_df):
        """
        calc_urban_tazes: Determine which TAZes meet the criteria for being classified as an 'Urban TAZ'
        inputs:         taz_df, cr_stops_df, nodesf5_df
        outputs:        N/A
        side effects:   Updates taz_df
        returns:        List of TAZes classified as 'Urban', Updated taz_df    
        """
        urban_taz_list = [] 
        
        working_taz_df = copy.deepcopy(taz_df)
        working_taz_df = working_taz_df[working_taz_df['access_density'] == 0]        
        self.logger.debug('\tcalc_urban: length of working_taz_df after initial culling: ' + str(len(working_taz_df)))
        
        # DEBUG/TRACE
        temp_df = working_taz_df[working_taz_df['Pop_Emp_Density'] > 10000]
        self.logger.debug('\tcalc_urban: Number of candidate TAZes with P+E density > 10,000 = ' + str(len(temp_df)))
        
        # Pop+Emp density must be > 7,500 per sqmi (this includes > 10,000 per sqmi
        working_taz_df = working_taz_df[working_taz_df['Pop_Emp_Density'] > POP_EMP_DENSITY_THRESHOLD_FOR_URBAN]
        self.logger.debug('\tcalc_urban: Number of candidate TAZes with P+E density > 7,500 = ' + str(len(working_taz_df)))
        
        # Set up to perform calculation
        cr_buf = copy.deepcopy(cr_stops_df) 
        nodesf5_buf = copy.deepcopy(nodesf5_df)   # Bus stops with < 5 minute headways

        # Create the buffers
        cr_buf['geometry'] = cr_buf.buffer(HALF_MILE_BUFFER)
        nodesf5_buf['geometry'] = nodesf5_buf.buffer(HALF_MILE_BUFFER)
        
        # Dissolve each of the buffer dataframes, in each case creating a dataframe with a single record
        cr_buf_dis = cr_buf.dissolve().reset_index()
        nodesf5_buf_dis = nodesf5_buf.dissolve().reset_index()
        
        taz_intersect_cr_list = []
        taz_intersect_f5_list = []
        # self.logger.info("timestamp 5")
        self.status_updater(self.status_pct[5], "access density: calculating urban_tazes")

        for index, row in gpd.overlay(cr_buf_dis, working_taz_df, how='intersection').iterrows():
            taz_intersect_cr_list.append(row['taz_id'])

        # self.logger.info("timestamp 5.1")
        for index, row in gpd.overlay(nodesf5_buf_dis, working_taz_df, how='intersection').iterrows():
            taz_intersect_f5_list.append(row['taz_id'])

        # self.logger.info("timestamp 5.2")
        urban_taz_list = taz_intersect_cr_list + taz_intersect_f5_list
        
        # Unique-ify the list
        urban_taz_list = list(set(urban_taz_list))


        taz_df['access_density'] = taz_df.apply(lambda row: TAD_CLASS_URBAN if row['taz_id'] in urban_taz_list else row['access_density'], axis=1)
        self.logger.debug("\tNumber of 'urban' TAZes = " + str(len(urban_taz_list)))
        # self.logger.info("timestamp 5.3")

        return urban_taz_list, taz_df
    # end_def calc_urban_tazes()

    def calc_fringe_urban_tazes(self, taz_df, cr_stops_df, hr_stops_df, lr_stops_df, nodesf15_df):
        """
        calc_fringe_urban_tazes: Determine which TAZes meet the criteria for being classified as a 'Fringe Urban TAZ'
        inputs:         taz_df, cr_stops_df, hr_stops_df, lr_stops_df, nodesf15_df
        outputs:        N/A
        side effects:   taz_df
        returns:        List of TAZes classified as 'Fringe Urban', Updated taz_df   
        """    
        fringe_urban_taz_list = []  

        working_taz_df = copy.deepcopy(taz_df)
        working_taz_df = working_taz_df[working_taz_df['access_density'] == 0]        
        self.logger.debug('\tcalc_fringe_urban: length of working_taz_df after initial culling: ' + str(len(working_taz_df)))  
            
        cr_buf = copy.deepcopy(cr_stops_df) 
        hr_buf1 = copy.deepcopy(hr_stops_df)
        lr_buf = copy.deepcopy(lr_stops_df)        
        nodesf15_buf = copy.deepcopy(nodesf15_df)  

        # Create buffers
        cr_buf['geometry'] = cr_buf.buffer(HALF_MILE_BUFFER)
        hr_buf1['geometry'] = hr_buf1.buffer(ONE_MILE_BUFFER) # Note that the heavy rail rapid transit buffer here is 1 mile
        lr_buf['geometry'] = lr_buf.buffer(ONE_MILE_BUFFER) 
        nodesf15_buf['geometry'] = nodesf15_buf.buffer(HALF_MILE_BUFFER)

        # Dissolve each of the buffer dataframes, in each case creating a dataframe with a single record
        cr_buf_dis = cr_buf.dissolve().reset_index()
        hr_buf_dis_fu = hr_buf1.dissolve().reset_index()
        lr_buf_dis = lr_buf.dissolve().reset_index()
        nodesf15_buf_dis = nodesf15_buf.dissolve().reset_index()

        taz_intersect_cr_list = []
        taz_intersect_hr_list = [] 
        taz_intersect_lr_list = []
        taz_intersect_f15_list = []
        
        # Compute the list of tazes in the fringe_urban list.
        # We do this in two stages, as there are two conditions that identify 'fringe urban' TAZes:
        # 
        # Condition 1: TAZ is within 1 mile of rail rapid transit (heavy rail or light rail)
        # OR
        # Condition 2: TAZ is within 1/2 mile of light rail rapid transit or commuter rail
        #              AND
        #              TAZ has a Population+Employment density > 5,000 per square mile
        
        # Evaluate Condition #1: 
        
        for index, row in gpd.overlay(hr_buf_dis_fu, working_taz_df, how = 'intersection').iterrows():
            taz_intersect_hr_list.append(row['taz_id'])
        
        for index, row in gpd.overlay(lr_buf_dis, working_taz_df, how = 'intersection').iterrows():
            taz_intersect_lr_list.append(row['taz_id'])
         
        self.logger.debug('Number of TAZes in heavy-rail buffer: ' + str(len(taz_intersect_hr_list)))
        
        # TAZes in the taz_intersect_hr_list and taz_intersect_lr_list
        # can be put into the taz_fringe_urban_list without further qualification:
        fringe_urban_taz_list_set_1 = taz_intersect_hr_list + taz_intersect_lr_list
        # Unique-ify the list
        fringe_urban_taz_list_set_1 = list(set(fringe_urban_taz_list_set_1))
        
        # Evaluate Condition #2:
        #     TAZes in the taz_intersect_cr_list and taz_intersect_f15_list must be further culled:
        #     only those with a 'Den_Flag' value of POP_EMP_DENSITY_MEDIUM, 
        #     i.e., Pop_Emp_Density > 5,000 per sqmi will pass muster
        
        working_taz_subset_df = copy.deepcopy(working_taz_df)
        working_taz_subset_df = working_taz_subset_df[working_taz_subset_df['Pop_Emp_Density'] > POP_EMP_DENSITY_THRESHOLD_FOR_FRINGE_URBAN]
        
        self.logger.debug('\tcalc_fringe_urban: Number of candidate TAZes with P+E density > 5,000: ' + str(len(working_taz_subset_df)))
                        
        for index, row in gpd.overlay(cr_buf_dis, working_taz_subset_df, how = 'intersection').iterrows():
            taz_intersect_cr_list.append(row['taz_id'])
               
        for index, row in gpd.overlay(nodesf15_buf_dis, working_taz_subset_df, how = 'intersection').iterrows():
            taz_intersect_f15_list.append(row['taz_id'])
            
        fringe_urban_taz_list_set_2 = taz_intersect_cr_list + taz_intersect_f15_list
        # Unique-ify the list
        fringe_urban_taz_list_set_2 = list(set(fringe_urban_taz_list_set_2))  
        self.logger.debug('\tNumber of TAZes meeting fringe-urban criterion #1: ' + str(len(fringe_urban_taz_list_set_1)))
        
        # Compute the complete return value
        fringe_urban_taz_list = fringe_urban_taz_list_set_1 + fringe_urban_taz_list_set_2
        fringe_urban_taz_list = list(set(fringe_urban_taz_list))
        self.logger.debug('\tTotal number of fringe-urban TAZes: ' + str(len(fringe_urban_taz_list)))
        
        # self.logger.info("timestamp 6")
        self.status_updater(self.status_pct[6],"access density: calculating access_density")
        taz_df['access_density'] = taz_df.apply(lambda row: TAD_CLASS_FRINGE_URBAN if row['taz_id'] in fringe_urban_taz_list else row['access_density'], axis=1)

        self.logger.info("timestamp 6.1")
        
        return fringe_urban_taz_list, taz_df
    # end_def calc_fringe_urban_tazes()

    def calc_suburban_tazes(self, taz_df, sub_routes_df):
        """
        calc_suburban_tazes: Determine which TAZes meet the criteria for being classified as a 'Suburban TAZ'
        inputs:         taz_df, sub_routes_df
        outputs:        N/A
        side effects:   Updates taz_df
        returns:        List of TAZes classified as 'Suburban', Updated taz_df
        """

        suburban_taz_list = []
        working_taz_df = copy.deepcopy(taz_df)
        working_taz_df = working_taz_df[working_taz_df['access_density'] == 0]
        self.logger.debug('\tcalc_suburban: length of working_taz_df after initial culling: ' + str(len(working_taz_df)))

        # self.logger.info("timestamp 7.0.1")
        self.status_updater(self.status_pct[8], "access density: calculating suburban_tazes dissolving...")
        sub_buf = copy.deepcopy(sub_routes_df)
        sub_buf['geometry'] = sub_buf.buffer(HALF_MILE_BUFFER)
        sub_buf_dis = sub_buf.dissolve().reset_index()

        # self.logger.info("timestamp 7")
        self.status_updater(self.status_pct[9], "access density: calculating suburban_tazes overlay")
        tazid = gpd.overlay(sub_buf_dis, working_taz_df, how='intersection')[['fare', 'taz_id']]
        for fare, tazid in tazid.itertuples(index=False):
            suburban_taz_list.append(tazid)

        self.logger.debug('\tNumber of suburban TAZes: ' + str(len(suburban_taz_list)))
        taz_df['access_density'] = taz_df.apply(lambda row: TAD_CLASS_SUBURBAN if row['taz_id'] in suburban_taz_list else row['access_density'], axis=1)
        return suburban_taz_list, taz_df
    # end_def calc_suburban_tazes()
    
    def calc_rural_tazes(self, taz_df):
        """
        calc_rural_tazes: Determine which TAZes meet the criteria for being classified as a 'Rural TAZ'
        inputs:         taz_df
        outputs:        N/A
        side effects:   Updates taz_df
        returns:        List of TAZes classified as 'Rural'    
        """
        rural_taz_list = []  # We will compute function return value into this list

        working_taz_df = copy.deepcopy(taz_df)
        self.logger.info("timestamp 8.0")
        working_taz_df = working_taz_df[working_taz_df['access_density'] == 0]     
        self.logger.debug('\tcalc_rural: length of working_taz_df after initial (and only) culling: ' + str(len(working_taz_df)))  
        
        rural_taz_list = working_taz_df['taz_id'].tolist()
        self.logger.debug('\nNumber of rural TAZes: ' + str(len(rural_taz_list)))

        self.status_updater(self.status_pct[11],"access density: calculating rural_tazes")
        taz_df['access_density'] = taz_df.apply(lambda row: TAD_CLASS_RURAL if row['taz_id'] in rural_taz_list else row['access_density'], axis=1)  
        
        return rural_taz_list, taz_df 
    # end_def calc_rural_tazes()

    def load_transit_access_density(self, path_tr_acc):
        """
        [Populate the access_density DB table from the transit_access CSV file.]
        """
        tr_acc_df = pd.read_csv(path_tr_acc)
        return tr_acc_df
    # end_def load_transit_access_density()

    def export_data(self, dataframe, csv_file_path = '', sql_table_name = ''):
        """
        Exports a DataFrames to an SQL table and a CSV file.
        
        Parameters:
        - dataframe: Pandas DataFrame to export.
        - file_path: File path for the output CSV file.
        - sql_table_name: Name of the SQL table to which the DataFrame should be appended.
        """
        # If file_path is not empty then write to_csv
        self.logger.debug(f"Exporting DataFrame to {csv_file_path}")
        if csv_file_path != '':
            dataframe.to_csv(csv_file_path, index=False) 

        #If sql_table_name is not empty, then write to sql
        self.logger.debug(f"Populating '{sql_table_name}' table.")
        if sql_table_name != '':
            dataframe.to_sql(name=sql_table_name, con=self.db_conn, if_exists="append", index=False)    

        return None
    # end_def export_data()

    def calc_terminal_times(self, tr_acc_df, path_tt_lk_up):
        """
        [calculate terminal times (auto OVTT access/egress time)]
        """
        lkup = pd.read_csv(path_tt_lk_up, dtype={'access_density':'Int64'})

        term_times = pd.merge(tr_acc_df, lkup, how="left", on="access_density")
        term_times = term_times.drop(columns=['access_density', 'access_density_label'])

        return term_times
    # end_def calc_terminal_times()

    def print_summary_info(self, tr_acc_df):
        # Collect and print some summary information:
        n_cbd = len(tr_acc_df[tr_acc_df['access_density'] == 1])
        n_dense_urb = len(tr_acc_df[tr_acc_df['access_density'] == 2])
        n_urb = len(tr_acc_df[tr_acc_df['access_density'] == 3])
        n_fringe_urb = len(tr_acc_df[tr_acc_df['access_density'] == 4])
        n_sub = len(tr_acc_df[tr_acc_df['access_density'] == 5])
        n_rural = len(tr_acc_df[tr_acc_df['access_density'] == 6])
        
        data = { 'Transit Access Density' : ['CBD', 'Dense Urban', 'Urban', 'Fringe Urban', 'Suburban', 'Rural'],
                 'TAZs'              : [n_cbd, n_dense_urb, n_urb, n_fringe_urb, n_sub, n_rural] }
        summary_df = pd.DataFrame(data)
        self.logger.debug("Contents of transit access density summary DF:\n")
        self.logger.debug(summary_df.head(10))
         
        return summary_df
    # end_def print_summary_info()

# end_class pre_processor()

if __name__ == "__main__":
    pp = access_density()
    # pp.calculate_transit_access_density()
