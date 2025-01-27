import os
from dbfread import DBF
import pandas as pd
from .base import disagg_model

class export_transit_activity_summary(disagg_model):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)    
        if "init" in kwargs and kwargs["init"]:
            self.init_logger()
        else:
            pass

    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """

        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        # Try statement setup for compatibility with TransCAD
        try:
            routes_df, stops_df, onoff_df = self.retrieve_files()

            trn_act_summary_df = self.process_transit_activity_data(routes_df, stops_df, onoff_df)

            # Export the DataFrame to SQL and CSV
            self.export_data(trn_act_summary_df,
                                self.args['OutputFolder'] + '\\_summary\\trn\\' + 'transit_activity_summary.csv')
            
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
    # end_def run

    def retrieve_files(self):
        """
        Retrieves routes, stops, and on-off data from specified files.
        Returns dataframes for each file type.
        """
        
        path_routes = os.path.join(self.args['OutputFolder'], '_networks', 'routes.dbf')
        path_stops = os.path.join(self.args['OutputFolder'], '_networks', 'stops.dbf')

        encoding = 'latin1'

        # Read input data: Transit routes and stops
        routes_dbf = DBF(path_routes, encoding=encoding)
        routes_df = pd.DataFrame(iter(routes_dbf))
        stops_dbf = DBF(path_stops, encoding=encoding)
        stops_df = pd.DataFrame(iter(stops_dbf))
        
        # Get all files in the directory and retrieve onoff_*.csv files
        on_off_dir = os.path.join(self.args['OutputFolder'], '_assignment')
        all_files = os.listdir(on_off_dir)
        csv_files = [f for f in all_files if f.startswith('onoff') and f.endswith('.csv')]

        # Dictionary to store DataFrames, with the CSV file names as keys
        on_off_dfs = {}

        # Iterate over each CSV file and read it into a DataFrame, with file name as keys
        for file_name in csv_files:
            file_path = os.path.join(on_off_dir, file_name)
            df = pd.read_csv(file_path)
            # Parses file name of 'onoff_' and '.csv' texts
            on_off_dfs[file_name[6:-4]] = df

        # Initialize an empty DataFrame for the merged data
        onoff_df = pd.DataFrame()

        for df_name, df in on_off_dfs.items():
            # Remove 'Layover' and any other unwanted columns
            df = df[['STOP', 'ROUTE', 'On', 'Off']]

            # If merged_df is empty, initialize it with the first DataFrame
            if onoff_df.empty:
                onoff_df = df.set_index(['STOP', 'ROUTE'])
                onoff_df.columns = [f"{df_name}_{col}" for col in onoff_df.columns]
            else:
                # Set index to ['STOP', 'ROUTE'] for the merge
                df.set_index(['STOP', 'ROUTE'], inplace=True)
                df.columns = [f"{df_name}_{col}" for col in df.columns]
                # Perform an outer join to merge the current DataFrame with the merged DataFrame
                onoff_df = onoff_df.join(df, how='outer')

        # Reset index to turn 'STOP' and 'ROUTE' back into columns
        onoff_df.reset_index(inplace=True)

        # Calculate totals specifically for 'On' and 'Off' columns
        on_columns = [col for col in onoff_df.columns if '_On' in col]
        off_columns = [col for col in onoff_df.columns if '_Off' in col]

        onoff_df['Total_On'] = onoff_df[on_columns].sum(axis=1)
        onoff_df['Total_Off'] = onoff_df[off_columns].sum(axis=1)

        # Define the order of the first few columns
        first_columns = ['STOP', 'ROUTE', 'Total_On', 'Total_Off']

        # Get the rest of the columns and sort them in descending order, excluding the first few columns
        rest_of_columns = sorted([col for col in onoff_df.columns if col not in first_columns], reverse=True)

        # Combine the columns into the final desired order
        final_column_order = first_columns + rest_of_columns

        # Reindex the DataFrame with the final column order
        onoff_df = onoff_df[final_column_order]
        onoff_df.rename(columns={'ROUTE': 'ROUTE_ID', 'STOP': 'STOP_ID'}, inplace=True)

        return routes_df, stops_df, onoff_df
    # end_def retrieve_files

    def process_transit_activity_data(self, routes_df, stops_df, onoff_df):
        """
        Processes and merges routes, stops, and on-off dataframes.
        Returns a final merged dataframe with NaN values filled.
        """

        # Trim dataframes
        routes_df = routes_df[['ROUTE_ID', 'ROUTE_NAME', 'DIR', 'MODE']].copy()
        stops_df = stops_df[['ROUTE_ID', 'STOP_ID', 'STOP_NAME', 'NEAR_NODE']].copy()

        # # Convert ROUTE_ID and STOP_ID in routes_df and stops_df to integers
        # routes_df['ROUTE_ID'] = routes_df['ROUTE_ID'].astype(int)
        # stops_df['ROUTE_ID'] = stops_df['ROUTE_ID'].astype(int)
        # stops_df['STOP_ID'] = stops_df['STOP_ID'].astype(int)
        # onoff_df['ROUTE_ID'] = onoff_df['ROUTE_ID'].astype(int)
        # onoff_df['STOP_ID'] = onoff_df['STOP_ID'].astype(int)

        # Join dataframes
        combined_df = pd.merge(routes_df, stops_df, on='ROUTE_ID', how='left')
        final_df = pd.merge(combined_df, onoff_df, left_on=['ROUTE_ID', 'STOP_ID'], right_on=['ROUTE_ID', 'STOP_ID'], how='left')
        # final_df = pd.merge(combined_df, merged_df, on=['ROUTE_ID', 'STOP_ID'], how='left')

        # Clean and finalize final_df formatting
        final_df.fillna(0, inplace=True)
        final_df.columns = final_df.columns.str.lower()

        return final_df
    # end_def process_transit_activity_data

    def export_data(self, dataframe, csv_file_path = ''):
        """
        Exports the given dataframe to a CSV file.
        Logs the export action.
        """
        # If file_path is not empty then write to_csv
        self.logger.debug(f"Exporting DataFrame to {csv_file_path}")
        if csv_file_path != '':
            dataframe.to_csv(csv_file_path, index=False) 

        return None
    # end_def export_data()

if __name__ == "__main__":
    pp = export_transit_activity_summary()
    