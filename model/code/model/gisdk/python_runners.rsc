macro "InitializePYClass" (args)
//
    pyObj = CreateObject("PYRunner", args)
    return(pyObj)
endmacro

class "PYRunner" (args)
    init do
        self.export_config_file(args)
        self.ConfigurationPath = self.get_config_file_path(args)

        py = CreateObject("PythonScript", {Environment: args.condaBat + " activate " + args.condaEvn})
        self.set_error_file(args,py)
        py.SetRunDisplayMode("hidden")
        self.py = py
        self.fn = args.[Base Folder] + "code\\runner.py"
    enditem

    done do 
        self.py = null
    enditem

    macro "TransitAccessDensity" do
    // Run components implemented in python

        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).TransitAccessDensity(cfg)

        return(1)
    endItem

    macro "ExportTransitActivitySummary" do
    // Run components implemented in python

        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).ExportTransitActivitySummary(cfg)

        return(1)
    endItem

    macro "EmploymentAccess" do
    // Calculate employment accessibility

        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).EmploymentAccess(cfg)

        return(1)
    endItem    

    macro "VehicleAvailability" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        SetStatus(2, "VA component is running", )
        self.py.WithFile(fn).VehicleAvailability(cfg)
        SetStatus(2, "VA component runs to the completion", )
        return(1)
    endItem

    macro "LoadSQLDB" do
    // Create sqlite DB and populate it with input data
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).Init_InputFileCheck(cfg)
        return(1)
    endItem

    macro "WorkFromHome" (Level,Cell) do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).WorkFromHome({"json_file": cfg})
        return(1)
    endItem

    macro "GenerateResidentInternalTrips" (hboVal) do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).TripGeneration({"json_file": cfg, "hbo": hboVal})
        return(1)
    endItem

    macro "Peak_NonPeak" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).PeakNonpeak({"json_file": cfg})
        return(1)
    endItem

    macro "Aggregate" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).Aggregate({"json_file": cfg})
        return(1)
    endItem

    macro "TruckTripGeneration" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).TruckTripGeneration({"json_file": cfg})
        return(1)
    endItem    

    macro "ExternalTripGeneration" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).ExternalTripGeneration({"json_file": cfg})
        return(1)
    endItem    

    macro "SpecialGeneratorTripGeneration" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).SpecialGeneratorTripGeneration({"json_file": cfg})
        return(1)
    endItem    

    macro "AirportTripGeneration" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).AirportTripGeneration({"json_file": cfg})
        return(1)
    endItem    

    macro "HBUTripGeneration" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).HBUTripGeneration({"json_file": cfg})
        return(1)
    endItem    

    macro "AirQuality" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).AirQuality({"json_file": cfg})
        return(1)
    endItem    

    macro "AggregateMetricValues" do
    // Run components implemented in python
        fn = self.fn
        cfg = self.ConfigurationPath
        self.py.WithFile(fn).AggregateMetricValues({"json_file": cfg})
        return(1)
    endItem    

    // private methods
    private macro "export_config_file" (Args) do
        //### Purpose:             Export configuration file to config.json for python functions
        //##  Flow Chart Location: 
        //##  Note:                
        //##  Inputs:
        //### UI Parameters:
        cfg_file = Args.[Config File]

        out_js = ArrayToJson(Args)
        ptr = OpenFile(cfg_file, "w")
        WriteLine(ptr, out_js)
        CloseFile(ptr)
    enditem

    private macro "get_config_file_path" (Args) do
        cfg_file = Args.[Config File]
        cfg_path = SplitPath(cfg_file)
        cfg_path = Substitute(cfg_path[1] + cfg_path[2] + cfg_path[3]+cfg_path[4], "\\", "/",)
        return(cfg_path)
    enditem

    private macro "set_error_file" (Args,py) do
        //### Purpose:      Redirect the standard error file location
        //##  Flow Chart Location: 
        //##  Note:  get py object from parameters           
        //##  Inputs:
        //### UI Parameters:
        err_file = Args.[OutputFolder] + '\\_logs\\' + "py.err"
        py.SetStdErrFile(err_file)
        out_file = Args.[OutputFolder] + '\\_logs\\' + "out.log"
        py.SetStdOutput(out_file)
        // TODO: create parameter for silent running to avoid showing the task dialog that shows up with the link to the error file
        py.SetDisplayErrorDbox(1)
    enditem

endclass



// python_runners.rsc
macro "Transit Access Density"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    ok = Args.PY.TransitAccessDensity()
    ok = 1
    Return(ok)
endmacro

macro "export_transit_activity_summary"  (Args)
// Macro is called to be call appropriate code in PYRunner
    if Args.DryRun = 1 then Return(1)
    ok = Args.PY.ExportTransitActivitySummary()
    ok = 1
    Return(ok)
endmacro

macro "Employment Accessibility"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    ok = Args.PY.EmploymentAccess()
    ok = 1
    Return(ok)   
endmacro

macro "Initialize and Load SQL DB"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    ok = Args.PY.LoadSQLDB()
    ok = 1
    Return(ok) // macro needs to return 1 for success or 0 for failure
endmacro

macro "Vehicle Availability"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.VehicleAvailability()
    ok = 1
    Return(ok)
endmacro

macro "Work from Home"  (Args)
//

    if Args.DryRun = 1 then Return(1)
    Args.PY.WorkFromHome( )
    ok = 1
    Return(ok)
endmacro

macro "Generate Resident Internal Trips"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.GenerateResidentInternalTrips( Args.[WFH HBO] )
    ok = 1
    Return(ok)
endmacro

macro "Peak NonPeak"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.Peak_NonPeak(  )
    ok = 1
    Return(ok)
endmacro

macro "Aggregate and Balance Trips"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.Aggregate(  )
    ok = 1
    Return(ok)
endmacro

macro "Truck Trip Generation"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.TruckTripGeneration(  )
    ok = 1
    Return(ok)
endmacro

macro "External Trip Generation"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.ExternalTripGeneration(  )
    ok = 1
    Return(ok)
endmacro

macro "Special Trip Generation"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.SpecialGeneratorTripGeneration(  )
    ok = 1
    Return(ok)
endmacro

macro "Airport Trip Generation"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.AirportTripGeneration(  )
    ok = 1
    Return(ok)
endmacro

macro "HBU Trip Generation"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.HBUTripGeneration(  )
    ok = 1
    Return(ok)
endmacro

macro "Aggregate Metric Values"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.AggregateMetricValues(  )
    ok = 1
    Return(ok)
endmacro

macro "Aggregate Metric Values"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.AggregateMetricValues(  )
    ok = 1
    Return(ok)
endmacro

macro "AirQuality Analysis"  (Args)
//
    if Args.DryRun = 1 then Return(1)
    Args.PY.AirQuality(  )
    ok = 1
    Return(ok)
endmacro