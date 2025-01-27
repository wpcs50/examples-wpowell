dk = None
import pandas as pd
import numpy as np
import ast

def run_mode(mode):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if mode == "gidk":
                print("Running function in gidk mode")
                return func(*args, **kwargs )
            elif mode == "production":
                # print("Running function in production mode")
                return func(mode, *args, **kwargs )
        return wrapper
    return decorator

globalmode = "production"
def GetLayerDB(*args):      
    """
    Summary
    This function retrieves the storage location of a layer.

    Syntax
    db_name = GetLayerDB(layer_name)

    Argument
    layer_name: the name of the layer whose storage location is to be determined.
    
    Returns
    A string indicating the layer type and its corresponding storage location:

    Geographic file: name of the principal file (e.g., .DBD for standard geographic file).
    Image file: name of the principal image file (e.g., .SID for MrSid image).
    ODBC layer: name of the data source.
    Oracle layer: name of the service.
    file:///C:/Program%20Files/TransCAD%209.0/Help/GISDK/dk/GetLayerDB.htm
    """
    obj = dk.GetLayerDB(*args)
    return obj

@run_mode(mode=globalmode)
def GetRouteStops(*args):
    """
    Summary
    This function retrieves a list of stops associated with a given route.

    Syntax
    stops = GetRouteStops(rs_layer, name, attributes)

    Argument

    rs_layer: the layer name of the route system.
    name: the name of the route.
    attributes: a boolean value indicating whether to include attribute values in the returned results.
    Returns
    An array of stops associated with the route, or Null if there are no stops. Each stop is represented by an element in the array with the following structure:

    Stop ID: an integer value that uniquely identifies the stop.
    Link ID: an integer value that identifies the link associated with the stop.
    Pass count: an integer value indicating the number of times the route passes through the stop.
    Milepost: a double value representing the distance of the stop from the starting point of the route.
    Stop location: a coordinate value representing the latitude and longitude of the stop location.
    (Optional) Attribute values: an array of attribute values associated with the stop.
    Physical stop ID: an integer value that uniquely identifies the physical stop.
    """ 
    if "production" in args:
        obj = dk.GetRouteStops(*args[1:])
        df = pd.DataFrame.from_records(obj).sort_values(by=3).reset_index(drop=True)
        df[4] = df[4].apply(lambda x : dk.ShowArray(x))
        df.columns = ["Stop ID","Link ID","Pass count","Milepost",
                    "Stop location","Attribute"]
        df_stop_route = df.copy()
        return df_stop_route
    else:
        obj = dk.GetRouteStops(*args)
        return obj
    

@run_mode(mode=globalmode)
def GetRouteLinks(*args):
    """
    Summary: Returns a list of links that define a route.

        Syntax: links = GetRouteLinks(string rs_layer, string name)

    Argument:

        rs_layer: The layer name of the route system
        name: The name of the route
    Returns:
        Null if no links are present or an array of link definitions.
        Each link definition is an array with the structure:
            Link ID (integer)
            Traverse direction (1 for forward or -1 for reverse) (integer)
            Side of link: "Left", "Right" (or null) (string)
            Start Milepost (or null) (double)
            End Milepost (or null) (double)
            Null (no longer used) (N/A)
    """ 
    if "production" in args:
        obj = dk.GetRouteLinks(*args[1:])
        df = pd.DataFrame.from_records(obj,
                                       columns=["Link ID","Traverse direction",
                                                "Side of link","Start Milepost",
                                                "End Milepost","Null" ])
        return df
    else:
        obj = dk.GetRouteLinks(*args)
        return obj