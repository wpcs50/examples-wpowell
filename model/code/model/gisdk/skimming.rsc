
//#   "skimming.rsc"
macro "Skim Highway AM MD"  (Args)
// Helper function to skim AM and MD highway network
    hwy_dbd              = Args.[Highway]  
    out_dir              = Args.OutputFolder    
    log_level            = Args.loglevel  

   if Args.DryRun = 1 then Return(1)
    ok = 1

    for tod in {"am", "md"} do
      for mode in {"da", "sr"} do
         hwy_net = Args.("Highway Net - " + tod)
         hwy_skim = runmacro("get_highway_mode_skim_file", out_dir, tod, mode)
         ok = runmacro("skim_highway_mode", hwy_dbd, hwy_net, hwy_skim, mode, tod)
         ok = runmacro("rename_skim_cores", hwy_skim, mode)
      end

      // combine da and sr skims
      comb_mat = runmacro("combine_mode_skims", Args, tod)

      // add rs fare core
      if (comb_mat <> null) then ok = runmacro("calc_rs_fare", Args, comb_mat)
      else ok = 0

    end

    // delete single skims
    if (!({"DEBUG"} contains log_level)) then do
      for tod in {"am", "md"} do
         for mode in {"da", "sr"} do
            hwy_skim = runmacro("get_highway_mode_skim_file", out_dir, tod, mode)
            DeleteFile(hwy_skim)
         end
      end
    end

    return(ok)

endmacro

macro "skim_highway_mode" (hwy_dbd, hwy_net, hwy_skim, mode, tod) 
// Skim drive alone or shared ride links
   
   filter = "truck_only = 1"
   if (mode = "da") then filter = filter + " | hov_only_" + tod + " = 1"

   // update network fields and filter by mode
   netobj = CreateObject("Network.Update", {Network: hwy_net})
   netobj.DisableLinks({Type: "BySet", Filter: filter})
   ok = netobj.Run()

   // run by mode
   obj = CreateObject("Network.Skims")
   obj.LoadNetwork (hwy_net)
   obj.LayerDB = hwy_dbd
   obj.Origins = "int_zone = 1 | ext_zone = 1"
   obj.Destinations = "int_zone = 1 | ext_zone = 1"
   obj.Minimize = "time"
   if (mode = "da") then obj.AddSkimField({"Length", "All"}) 
   obj.AddSkimField({"toll_auto", "All"})
   //obj.MaxCost = 120
   obj.OutputMatrix({MatrixFile: hwy_skim, Matrix: "skim", Compression : true, ColumnMajor : false})
   ok = obj.Run()
   //m = obj.GetResults().Data.[Output Matrix]


   // set intrazonals
   cores = {"time"}
   if (mode = "da") then cores = InsertArrayElements(cores, 1,{"Length (skim)"}) 

   for mat_core in cores do
      mat_opts = null
      mat_opts.MatrixFile = hwy_skim
      mat_opts.Matrix = mat_core
      obj = CreateObject("Distribution.Intrazonal")
      obj.SetMatrix(mat_opts)
      obj.OperationType = "Replace"
      obj.Factor = 0.5
      obj.TreatMissingAsZero = true
      obj.Neighbours = 3
      ok = obj.Run()
   end

   // intrazonal tolls are zero
   m = OpenMatrix(hwy_skim,)
   OpenMatrixFileHandle(m, "w")
   mc = CreateMatrixCurrency(m, "toll_auto (skim)",,,)
   FillMatrix(mc,,,{"Copy",0},{{"Diagonal","Yes"}})
   CloseMatrixFileHandle(m)

   // enable all links after skimming
   netobj = CreateObject("Network.Update", {Network: hwy_net})
   netobj.EnableLinks({Type: "BySet", Filter: filter})
   ok = netobj.Run()
   return(ok)
endmacro

macro "rename_skim_cores" (hwy_skim, mode) 
// Rename default skim cores to standard names

   ok = 1 
   mObj = CreateObject("Matrix", hwy_skim)
   if (lower(mode) = "da") then 
      mObj.RenameCores({CurrentNames: {"Length (skim)"}, NewNames: {"dist"}})
   mObj.RenameCores({CurrentNames: {"time", "toll_auto (skim)"}, NewNames: {mode + "_time", mode + "_toll"}})
   return(ok)
endmacro

macro "combine_mode_skims" (Args, tod)
// Combine DA and SR skims into a single file by tod
   out_dir              = Args.OutputFolder 

   ok = 1 
   da_skim = runmacro("get_highway_mode_skim_file", out_dir, tod, "da")
   sr_skim = runmacro("get_highway_mode_skim_file", out_dir, tod, "sr")
   comb_skim = Args.("HighwaySkims - " + tod)

   da_m = OpenMatrix(da_skim,)
   sr_m = OpenMatrix(sr_skim,)

   da_mc = CreateMatrixCurrencies(da_m,,,)
   sr_mc = CreateMatrixCurrencies(sr_m,,,)

   comb_mat = ConcatMatrices({da_m, sr_m}, "True",
                              {{"File Name", comb_skim},
                              {"Label", "highway " + tod}})
   return(comb_mat)
endmacro   

macro "calc_rs_fare" (Args, mat)
   // Calculate ridesource fare core
   ok = 1

   min_fare = Args.[RS Min Fare]
   base_fare = Args.[RS Base Fare]
   dist_fare = Args.[RS Dist Fare]
   time_fare = Args.[RS Time Fare]

   AddMatrixCore(mat,"rs_fare")
   mc = CreateMatrixCurrencies(mat,,,)
   mc.rs_fare := max(min_fare, base_fare + mc.dist * dist_fare + mc.sr_time * time_fare)

   // tnc availability sensitivity test
   if (Args.[TNC Fare Wait Adjustment] <> 1) then do
      mc.rs_fare := mc.rs_fare * Args.[TNC Fare Wait Adjustment ]
   end

   mc = null
   mat = null

   return(ok)

endmacro

macro "Skim PK NP Transit Walk"  (Args)
// Helper function to skim AM and MD transit walk network
    trn_rts              = Args.[Transit]
    out_dir              = Args.OutputFolder      
   if Args.DryRun = 1 then Return(1)   
    ok = 1

    for tod in {"am", "md"} do
      trn_net = runmacro("get_transit_network_file", out_dir, tod)
      skim_mtx = Args.("TransitWalkSkims - " + tod)
      
      ok = runmacro("set_transit_network", Args, trn_rts, trn_net, tod, "tw")
      ok = runmacro("skim_transit_walk", Args, trn_rts, trn_net, skim_mtx)
    end

    return(ok)

endmacro

macro "Skim PK NP Transit Auto"  (Args)
// Helper function to skim AM and MD transit auto network
    trn_rts              = Args.[Transit]
    out_dir              = Args.OutputFolder      
   if Args.DryRun = 1 then Return(1)
   
   //##  code block //:
   ok = 1

   for tod in {"am", "md"} do
      trn_net = runmacro("get_transit_network_file", out_dir, tod)
      skim_mtx = Args.("TransitAutoSkims - " + tod)
      parkUsage = Args.("TransitParkUsage - " + tod)
      
      ok = runmacro("set_transit_network", Args, trn_rts, trn_net, tod, "ta_acc")
      ok = runmacro("skim_transit_auto", Args, trn_rts, trn_net, skim_mtx, parkUsage, )
    end

    return(ok)

endmacro


macro "Skim PK NP Logan Express"  (Args)
// Helper function to skim Logan Express Service
    trn_rts              = Args.[Transit]
    out_dir              = Args.OutputFolder      
   if Args.DryRun = 1 then Return(1)
   
   //##  code block //:
   ok = 1

   for tod in {"am", "md"} do
      trn_net = runmacro("get_transit_network_file", out_dir, tod)
      skim_mtx = out_dir + "\\_skim\\lx_" + tod + ".mtx"
      parkUsage = out_dir + "\\_skim\\lx_park_usage_" + tod + ".bin"
      
      ok = runmacro("set_transit_network", Args, trn_rts, trn_net, tod, "lx")
      ok = runmacro("skim_transit_auto", Args, trn_rts, trn_net, skim_mtx, parkUsage, "_lx")

      if ({"DEBUG","FULL"} contains Args.loglevel) then do 
         omx_file = runmacro("get_transit_omx_skim_file", out_dir, tod, "lx")
         runmacro("export_skim_to_omx", skim_mtx, omx_file, "gen_cost")
      end
    end

    return(ok)

endmacro

macro "skim_transit_walk" (Args, trn_rts, trn_net, skim_mtx)
// Skim transit walk network

   // core names to be updated
   walk_core_names = {{"Generalized Cost", "gen_cost"},
                  //{"Fare", "fare"},
                  {"Number of Transfers","xfer"},
                  {"In-Vehicle Time","ivtt"},
                  {"Initial Wait Time","iwait"},
                  {"Transfer Wait Time","xwait"},
                  {"Access Walk Time","walk"},
                  //{"Egress Walk Time",}, // will be combined with access walk
                  //{"Transfer Walk Time",}, // will be combined with access walk
                  {"In-Vehicle Distance", "tdist"}}
   skim_vars = {"Generalized Cost", 
                               "Fare",
                               "Number of Transfers",
                               "In-Vehicle Time",
                               "Dwelling Time",
                               "Initial Wait Time",
                               "Transfer Wait Time",
                               "Access Walk Time",
                               "Egress Walk Time",
                               "Transfer Walk Time",
                               "In-Vehicle Distance"}                  

   debug_skim_vars = {"Local Bus.ttime", "Express Bus.ttime", "Bus Rapid.ttime", "Light Rail.ttime",
                               "Heavy Rail.ttime", "Commuter Rail.ttime","Ferry.ttime","Shuttle.ttime","RTA Local Bus.ttime","Regional Bus.ttime"}

   debug_core_names = {{"ttime (Local Bus)", "ivtt_lbus"},
                  {"ttime (Express Bus)", "ivtt_xbus"}, 
                  {"ttime (Bus Rapid)", "ivtt_brt"},
                  {"ttime (Light Rail)","ivtt_lrt"},
                  {"ttime (Heavy Rail)", "ivtt_hrt"},
                  {"ttime (Commuter Rail)","ivtt_cr"},
                  {"ttime (Ferry)","ivtt_fr"},
                  {"ttime (Shuttle)","ivtt_sh"},
                  {"ttime (RTA Local Bus)","ivtt_rta"},
                  {"ttime (Regional Bus)","ivtt_rb"}}

   // record submode level skims in DEBUG and INFO modes
   // Including all to have access to mode specific IVT for emat
   if (({"DEBUG","FULL"} contains Args.loglevel) |
         Args.[Transit HRT Time Adjustment] <> 1) then do 
      walk_core_names = walk_core_names + debug_core_names
      skim_vars = skim_vars + debug_skim_vars
   end                  

   obj = CreateObject("Network.TransitSkims")
   obj.Method = "PF"
   obj.LayerRS = trn_rts
   obj.LoadNetwork( trn_net )
   obj.OriginFilter = "int_zone = 1 | ext_zone = 1"
   obj.DestinationFilter = "int_zone = 1 | ext_zone = 1"
   obj.SkimVariables = skim_vars

   obj.OutputMatrix({MatrixFile: skim_mtx, Matrix: "Transit_Walk", Compression : true, ColumnMajor : false})
   ok = obj.Run()
   
   mtx = obj.GetResults().Data.[Skim Matrix]

   res = obj.GetResults()
   if !ok then ShowArray(res)


      // update core names
      m = OpenMatrix(skim_mtx, )
      mc = CreateMatrixCurrencies(m,,,)

      // combine walk times
      mc.[Access Walk Time] := mc.[Access Walk Time] + mc.[Egress Walk Time] + mc.[Transfer Walk Time]
      DropMatrixCore(m, "Egress Walk Time")
      DropMatrixCore(m, "Transfer Walk Time")

      // combine dwell with ivtt
      mc.[In-Vehicle Time] := mc.[In-Vehicle Time] + mc.[Dwelling Time]
      DropMatrixCore(m, "Dwelling Time")

      // sensitivity test - adjust tolls
      if Args.[Transit Fare Adjustment] <> 1 then do 
         mc.Fare := mc.Fare * Args.[Transit Fare Adjustment]
      end    

      // sensitivity test - adjust IVT
      if Args.[Transit HRT Time Adjustment] <> 1 then do 
         mc.[In-Vehicle Time] := mc.[In-Vehicle Time] + 
                                 (nz(mc.[ttime (Heavy Rail)]) * (Args.[Transit HRT Time Adjustment] - 1))
      end    

      for i in walk_core_names do 
         SetMatrixCoreName(m, i[1], i[2])
      end

   RunMacro("G30 File Close All") // flush out all changes

   // set generalized costs to zero to indicate no transit
   m = OpenMatrix(skim_mtx, )
   gc_mc = CreateMatrixCurrency(m, "gen_cost",,,)
   gc_mc := Nz(gc_mc)
   
   RunMacro("G30 File Close All") // flush out all changes

   return(ok)   

endmacro


macro "skim_transit_auto" (Args, trn_rts, trn_net, skim_mtx, parkUsageTable, lx)
// Skim transit auto network

    vot                  = Args.[Value Of Time] // $ per ivtt min
    drv_time_fact        = Args.[TransitPath_GlobalWeights].("DriveTimeFactor").Value // ivtt min per drive min
    drv_time_val         = vot * drv_time_fact // $ per drive min       

   auto_core_names = {{"Generalized Cost", "gen_cost"},
                  //{"Fare", "fare"}, 
                  //{"auto_cost", "auto_cost"},                  
                  {"Number of Transfers","xfer"},
                  {"In-Vehicle Time","ivtt"},
                  {"Initial Wait Time","iwait"},
                  {"Transfer Wait Time","xwait"},
                  {"Egress Walk Time","walk"},
                  //{"Transfer Walk Time",}, // will be combined with egress walk
                  {"In-Vehicle Distance", "tdist"},                             
                  {"Access Drive Distance", "ddist"},
                  {"Access Drive Time", "dtime"}}
   skim_vars = {"Generalized Cost", 
                              "Fare",
                              "Number of Transfers",
                              "In-Vehicle Time",
                              "Dwelling Time",
                              "Initial Wait Time",
                              "Transfer Wait Time",
                              "Egress Walk Time",
                              "Transfer Walk Time",
                              "Access Drive Time",                          
                              "In-Vehicle Distance",                               
                              "Access Drive Distance",
                              "auto_cost"}

   debug_skim_vars = {"Local Bus.ttime", "Express Bus.ttime", "Bus Rapid.ttime", "Light Rail.ttime",
                     "Heavy Rail.ttime", "Commuter Rail.ttime","Ferry.ttime","Shuttle.ttime","RTA Local Bus.ttime","Regional Bus.ttime"}

   debug_core_names = {{"ttime (Local Bus)", "ivtt_lbus"},
                  {"ttime (Express Bus)", "ivtt_xbus"}, 
                  {"ttime (Bus Rapid)", "ivtt_brt"},
                  {"ttime (Light Rail)","ivtt_lrt"},
                  {"ttime (Heavy Rail)", "ivtt_hrt"},
                  {"ttime (Commuter Rail)","ivtt_cr"},
                  {"ttime (Ferry)","ivtt_fr"},
                  {"ttime (Shuttle)","ivtt_sh"},
                  {"ttime (RTA Local Bus)","ivtt_rta"},
                  {"ttime (Regional Bus)","ivtt_rb"}}

   // record submode level skims in DEBUG and INFO modes
   // Including all to have access to mode specific IVT for emat
   if (({"DEBUG","FULL"} contains Args.loglevel) |
         Args.[Transit HRT Time Adjustment] <> 1) then do
      auto_core_names = auto_core_names + debug_core_names
      skim_vars = skim_vars + debug_skim_vars
   end

   obj = CreateObject("Network.TransitSkims")
   obj.Method = "PF"
   obj.LayerRS = trn_rts
   obj.LoadNetwork( trn_net )
   obj.OriginFilter = "int_zone = 1 | ext_zone = 1"
   obj.DestinationFilter = "int_zone = 1 | ext_zone = 1"
   obj.SkimVariables = skim_vars
   obj.AccessParkTable = parkUsageTable
   obj.OutputMatrix({MatrixFile: skim_mtx, Matrix: "Transit_Auto" + lx, Compression : true, ColumnMajor : false})
   ok = obj.Run()
   
   mtx = obj.GetResults().Data.[Skim Matrix]

   res = obj.GetResults()
   if !ok then ShowArray(res)

      // update core names
      m = OpenMatrix(skim_mtx, )
      mc = CreateMatrixCurrencies(m,,,)

      // combine walk times
      mc.[Egress Walk Time] := nz(mc.[Egress Walk Time]) + nz(mc.[Transfer Walk Time])
      DropMatrixCore(m, "Transfer Walk Time")

      // combine dwell with ivtt
      mc.[In-Vehicle Time] := mc.[In-Vehicle Time] + mc.[Dwelling Time]
      DropMatrixCore(m, "Dwelling Time")    

      // correct skim zero bug
      mc.auto_cost := nz(mc.auto_cost)    

      // remove cost from drive time
      mc.[Access Drive Time] := nz(mc.[Access Drive Time]) - nz(mc.auto_cost)/drv_time_val

      // sensitivity test - adjust tolls
      if Args.[Transit Fare Adjustment] <> 1 then do 
         mc.Fare := mc.Fare * Args.[Transit Fare Adjustment]
      end          

      // sensitivity test - adjust IVT
      if Args.[Transit HRT Time Adjustment] <> 1 then do 
         mc.[In-Vehicle Time] := mc.[In-Vehicle Time] + 
                                 (nz(mc.[ttime (Heavy Rail)]) * (Args.[Transit HRT Time Adjustment] - 1))
      end          

      for i in auto_core_names do 
         SetMatrixCoreName(m, i[1], i[2])
    end

    RunMacro("G30 File Close All") // flush out all changes

    // set generalized costs to zero to indicate no transit
    m = OpenMatrix(skim_mtx, )
    gc_mc = CreateMatrixCurrency(m, "gen_cost",,,)
    gc_mc := Nz(gc_mc)
    
    RunMacro("G30 File Close All") // flush out all changes

    return(ok)   

endmacro


macro "Skim NonMotorized Network" (Args)
// Skim walk-bike networks
    ok = 1
    if Args.DryRun = 1 then Return(1)

    out_dir = Args.OutputFolder 
    nm_dbd  = Args.[NonMotorized Links]
    nm_net = runmacro("get_nm_network_file", out_dir)
    nm_skim = Args.[NonMotorizedSkim]

    obj = CreateObject("Network.Skims")
    obj.LoadNetwork (nm_net)
    obj.LayerDB = nm_dbd
    obj.Origins = "Centroids_Only = 1"
    obj.Destinations = "Centroids_Only = 1"
    obj.Minimize = "Length"
   
    obj.OutputMatrix({MatrixFile: nm_skim, Matrix: "skim", Compression : true, ColumnMajor : false})
    ok = obj.Run()
    
    // set intrazonals
    cores = {"Length"}
    for mat_core in cores do
        mat_opts = null
        mat_opts.MatrixFile = nm_skim
        mat_opts.Matrix = mat_core
        obj = CreateObject("Distribution.Intrazonal")
        obj.SetMatrix(mat_opts)
        obj.OperationType = "Replace"
        obj.Factor = 0.5
        obj.TreatMissingAsZero = false
        obj.Neighbours = 3
        ok = obj.Run()
    end

    // update core names
    m = OpenMatrix(nm_skim,)
    OpenMatrixFileHandle(m, "w")
    SetMatrixCoreName(m, "Length",      "dist")
    CloseMatrixFileHandle(m)
    return(ok) 
    
endmacro


