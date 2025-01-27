
//#   "assignment.rsc"
// UI caller macros
macro "Highway Assignment Coordination" (Args)
//
   if Args.DryRun = 1 then Return(1)

   return(true)
endmacro

macro "Highway Assignment - AM" (Args)
// Helper function to assign am highway trip tables
   if Args.DryRun = 1 then Return(1)   
   ok = 1
   ok = runmacro("highway_assignment", Args, "am")
   return(ok)
endmacro

macro "Highway Assignment - MD" (Args)
// Helper function to assign md highway trip tables
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   ok = runmacro("highway_assignment", Args, "md")
   return(ok)
endmacro

macro "Highway Assignment - PM" (Args)
// Helper function to assign pm highway trip tables
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   ok = runmacro("highway_assignment", Args, "pm")
   return(ok)
endmacro

macro "Highway Assignment - NT" (Args)
// Helper function to assign NT highway trip tables
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   ok = runmacro("highway_assignment", Args, "nt")
   return(ok)
endmacro


macro "highway_assignment"  (Args, tod)
// Load da, sr, and truck vehicle trips onto highway network
   out_dir              = Args.OutputFolder                        //ui:output folder
   hwy_dbd              = Args.[Highway]                           //ui:highway database
   assign_iter          = Args.[Number of Assignment Iterations]   //ui: Number of Assignment Iterations
   rel_gap              = Args.[Relative Gap Threshold]
   cls                  = Args.[Assign Classes]
   hwy_net              = Args.("Highway Net - " + tod)
   trip_tab             = Args.("Veh Trips - " + tod)
   
   //### Outputs:
   flowtable =  Args.("HighwayFlows - " + tod)         //ot: time of day flows
   iteration_log = Args.("Highway AssignLog - " + tod)   //ot: assignment log

   //##  code block //:
   ok = 1

   // hov zipper lane access (AM open is 1, PM open is 2)
   zipper_excl = "(peak_link = 1 | peak_link = 2)"
   if (tod = "am") then zipper_excl = "peak_link = 2"
   if (tod = "pm") then zipper_excl = "peak_link = 1"

   base_excl = zipper_excl + " | available = 0 | transit_only = 1 | walk_bike_only = 1 | pnr_link = 1"
   sr_excl = base_excl + " | truck_only = 1"
   da_excl = sr_excl + " | hov_only_" + tod + " = 1"
   ltrk_excl = base_excl + " | hov_only_" + tod + " = 1"
   mtrk_excl = ltrk_excl + " | small_veh_only = 1"
   htrk_excl = mtrk_excl + " | no_heavy_truck = 1"


   obj = CreateObject("Network.Assignment")
   obj.ResetClasses()
   if (Args.AssignThreads > 0) then obj.ForceThreads = Args.AssignThreads
   obj.LayerDB =  hwy_dbd
   obj.LoadNetwork(hwy_net)
   obj.Iterations =  assign_iter
   obj.Convergence =  rel_gap
   obj.SetConjugates(3)
   obj.DelayFunction = {Function:  "BPR.vdf", Fields: { "ff_time",  "capacity",  "alpha",  "beta",  "None"}}
   obj.DemandMatrix( trip_tab)
   obj.MSAFeedback({Flow: "__MSAFlow", Time: "__MSATime", Iteration: Args.Iteration})
   //obj.TollMatrix({MatrixFile: TollMatrixFile, Matrix: TollMatrix, RowIndex: RowIndex, ColIndex: ColumnIndex})
   obj.AddClass({Demand: 'da', 
                  PCE: cls.("da").PCE, 
                  VOI: cls.("da").VOT, 
                  LinkTollField: "toll_auto", 
                  ExclusionFilter: da_excl})
   obj.AddClass({Demand: 'sr', 
                  PCE: cls.("sr").PCE, 
                  VOI: cls.("sr").VOT, 
                  LinkTollField: "toll_auto", 
                  ExclusionFilter: sr_excl})
   obj.AddClass({Demand: 'ltrk', 
                  PCE: cls.("lt").PCE, 
                  VOI: cls.("lt").VOT, 
                  LinkTollField: "toll_lt_trk", 
                  ExclusionFilter: ltrk_excl})
   obj.AddClass({Demand: 'mtrk', 
                  PCE: cls.("mt").PCE, 
                  VOI: cls.("mt").VOT, 
                  LinkTollField: "toll_md_trk", 
                  ExclusionFilter: mtrk_excl})
   obj.AddClass({Demand: 'htrk', 
                  PCE: cls.("ht").PCE, 
                  VOI: cls.("ht").VOT, 
                  LinkTollField: "toll_hv_trk", 
                  ExclusionFilter: htrk_excl})   
   //obj.AddQuery({Name: CriticalLinkQueryName, Query: CriticalLinkQuery})
   //obj.CriticalMatrix({MatrixFile: CriticalMatrixFileName, Matrix: CriticalMatrixCoreName})

   if Args.[Run Select Query from Menu] = 1 then do 
      highway_sq = Args.[Highway Select Query File]
      output_file = Args.[Highway Select Query Output Folder] + "\\highway_sq_" + tod + ".mtx"
      obj.CriticalQueryFile = highway_sq
      obj.CriticalMatrix({MatrixFile: output_file, Matrix: "results"})

      flowtable = Args.[Highway Select Query Output Folder] + "\\flows_" + tod + ".bin"          //ot: time of day flows
      iteration_log = Args.[Highway Select Query Output Folder] + "\\assign_log_" + tod + ".bin"   //ot: assignment log

   end

   obj.FlowTable =  flowtable
   obj.IterationLog = iteration_log
   ok = obj.Run()
   res = obj.GetResults()

   // record RMSE for feedback
   Args.("rmse_" + tod) = res.Data.[MSA RMSE]
   Args.("prmse_" + tod) = res.Data.[MSA PERCENT RMSE]

   return(ok)
   
endmacro

macro "Highway Summary" (Args)
// Attach highway assignment results to network dbd for analysis
   if Args.DryRun = 1 then Return(1)   
   ok = 1
   time_periods         = Args.[TimePeriods]   

   tod_fields = ParseString(Args.[hwy_tod_fields], ",")
   daily_fields = ParseString(Args.[hwy_summary_fields], ",")
   
   runmacro("add_assignment_result_fields", Args, time_periods, tod_fields, daily_fields)

   for tod in time_periods do 
      runmacro("add_highway_tod_results_to_dbd", Args, tod, tod_fields)
   end

   runmacro("derive_daily_highway_summary", Args, daily_fields, time_periods)
   
   // attach counts and calculate volume differences
   if Args.hwy_valid = 1 then do
      runmacro("attach_highway_counts", Args)
   end

   return(ok)
endmacro

macro "attach_highway_counts" (Args)
// Attach highway counts and calculate volume differences
   hwy_dbd              = Args.[Highway]
   cnt_file             = Args.hwy_cnt

   // add count fields
   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
   {node_lyr, link_lyr} = objLyrs.Layers

   lyr_obj = CreateObject("CC.ModifyTableOperation", link_lyr)
   lyr_obj.FindOrAddField("obs_vol_daily","Real",12,2)
   lyr_obj.FindOrAddField("diff_vol_daily","Real",12,2)
   lyr_obj.FindOrAddField("absd_vol_daily","Real",12,2)
   lyr_obj.Apply()

   // join count data
   cnt_vw = OpenTable("Flow", "CSV", {cnt_file})
   link_cnt_vw = JoinViews("link_cnt", link_lyr + ".count_id", cnt_vw + ".count_id",)  

   cnt_v = GetDataVector(link_cnt_vw + "|", "aadt_19",)
   SetDataVector(link_cnt_vw + "|", "obs_vol_daily", cnt_v,)      

   // calculate differences
   vol_v = GetDataVector(link_cnt_vw + "|", "tot_vol_daily",)
   diff_v = nz(vol_v) - cnt_v // purposely leave nulls in count
   absd_v = abs(diff_v)

   SetDataVector(link_cnt_vw + "|", "diff_vol_daily", diff_v,)    
   SetDataVector(link_cnt_vw + "|", "absd_vol_daily", absd_v,)    

endmacro


macro "add_assignment_result_fields" (Args, time_periods, tod_fields, daily_fields)
// Create fields on highway dbd to populate with results

   hwy_dbd              = Args.[Highway]

   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
   {node_lyr, link_lyr} = objLyrs.Layers
   
   lyr_obj = CreateObject("CC.ModifyTableOperation", link_lyr)

   for field in tod_fields do 
      for tod in time_periods do 
         for dir in {"ab","ba"} do 
            lyr_obj.FindOrAddField(dir + "_" + field + "_" + tod,"Real",12,2)
         end
      end
   end  

   for field in daily_fields do 
      for dir in {"ab","ba","tot"} do 
         lyr_obj.FindOrAddField(dir + "_" + field + "_daily","Real",12,2)
      end
   end     

   lyr_obj.Apply() 
endmacro

macro "add_highway_tod_results_to_dbd" (Args, tod, tod_fields)
// Populate TOD assignment result fields

   hwy_dbd              = Args.[Highway]
   flow_tab = Args.("HighwayFlows = " + tod)

   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
   {node_lyr, link_lyr} = objLyrs.Layers

   flow_vw = OpenTable("Flow", "FFB", {Args.("HighwayFlows - " + tod)})
   link_flow_vw = JoinViews("link_flows", link_lyr + ".id", flow_vw + ".id1",)   

   for field in tod_fields do 
      for dir in {"ab","ba"} do 
         src_v = GetDataVector(link_flow_vw + "|", dir + "_" + field,)
         SetDataVector(link_flow_vw + "|", dir + "_" + field + "_" + tod,src_v,)
      end
   end
   return(true)

endmacro

macro "derive_daily_highway_summary" (Args, daily_fields, time_periods)
// Populate Daily assignment result fields

   hwy_dbd              = Args.[Highway]
   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
   {node_lyr, link_lyr} = objLyrs.Layers

   for field in daily_fields do
      src_fields = ParseString(Args.[hwy_summary_source].(field).Source, ",")

      // get daily field - initialize to zero
      id_v = GetDataVector(link_lyr + "|", "id",)
      zero_v = Vector(id_v.length, "Integer", {{"Constant", 0}})
      day_ab_v = zero_v
      day_ba_v = zero_v
      day_tot_v = zero_v

      // loop time periods
      for tod in time_periods do
         for src in src_fields do 
            tod_ab_v = GetDataVector(link_lyr + "|", "ab_" + src + "_" + tod,)
            tod_ba_v = GetDataVector(link_lyr + "|", "ba_" + src + "_" + tod,)

            day_ab_v = nz(day_ab_v) + nz(tod_ab_v)
            day_ba_v = nz(day_ba_v) + nz(tod_ba_v)
         end
      end

      day_tot_v = day_ab_v + day_ba_v
      SetDataVector(link_lyr + "|", "ab_" + field + "_daily",day_ab_v,)
      SetDataVector(link_lyr + "|", "ba_" + field + "_daily",day_ba_v,)
      SetDataVector(link_lyr + "|", "tot_" + field + "_daily",day_tot_v,)
   end // daily fields

   return(true)
endmacro

macro "Transit PnR Shadow Costs" (Args)
// Run PnR to convergence with parking capacity
   if Args.DryRun = 1 then Return(1)
   iter                 = Args.Iteration   
   run_mode             = Args.[TransitParking]
   pnr                  = Args.transit_pnr_pfe
   rmse_thr             = pnr.("RMSE_Threshold").Value   
   ok = 1  

   if (run_mode = "Dynamic Shadow Costs") then do

      // run to convergence
      while ((Args.PnR_Iteration <= Args.PnR_Max_Iteration) & 
               (Args.rmse_pnr > rmse_thr)) do 

         ok = runmacro("pnr_assign_shadowcost_update", Args)

         // log iteration
         status_str = "PnR Iteration: " + i2s(iter) + "_" + 
                        i2s(Args.PnR_Iteration) + " - RMSE: " + r2s(Args.rmse_pnr)
         runmacro("write_log_file", Args.OutputFolder, "pnr_converge.txt", status_str)    

         Args.PnR_Iteration = Args.PnR_Iteration + 1         
      end

      // model converged               
      if (Args.rmse_pnr < rmse_thr) then do 
         Args.ExitMessage = "PnR Converged after " + i2s(pnr_iter) + " Iterations"
      end   

      // max iterations
      if Args.PnR_Iteration >= Args.PnR_Max_Iteration then do 
         Args.ExitMessage = "Model Stopped after " + i2s(pnr_iter) + " Iterations"
      end   

      // reset
      Args.PnR_Iteration = 1
      Args.rmse_pnr = 999
   end

   Return(ok)

endmacro


macro "pnr_assign_shadowcost_update" (Args)
// Run AM pnr access assignment and update shadow costs
   if Args.DryRun = 1 then Return(1)
   out_dir              = Args.OutputFolder     
   trn_rts              = Args.[Transit]
   hwy_dbd              = Args.[Highway]
   vot                  = Args.[Value Of Time]
   drv_time_fact        = Args.[TransitPath_GlobalWeights].("DriveTimeFactor").Value // ivtt min per drive min
   drv_time_val         = vot * drv_time_fact // $ per drive min       
   run_mode             = Args.[TransitParking]

   trip_tab = Args.("Per Trips - am")
   trn_net = runmacro("get_transit_network_file", out_dir, "am")
   ok = runmacro("transit_assignment", Args, trip_tab, "am", "ta_acc") 

   // get auto assigned matrix from assignment and update dbd
   tag = runmacro("get_transit_file_tag", "ta_acc", "am")
   auto_mat = out_dir + "\\_assignment\\pnr\\pnr_" + tag + ".mtx"   
   runmacro("set_pnr_pfe_trips", Args, auto_mat, Args.PnR_Iteration)
   
   // calculate shadow costs, apply to dbd, update network
   ok = runmacro("calc_pnr_shadow_cost", Args, hwy_dbd, run_mode)
   ok = runmacro("calc_auto_link_impedance", hwy_dbd, drv_time_val, "am")
   ok = runmacro("build_transit_network", trn_rts, trn_net, "am")

   if (({"DEBUG"} contains Args.loglevel)) then do 
      runmacro("log_pnr_demand_cost", out_dir, hwy_dbd, Args.PnR_Iteration)
   end  

   return(ok)
endmacro


// calculate trips to each pnr lot and record on highway dbd
macro "set_pnr_pfe_trips" (Args, dacc_mat_file, pnr_iter)
// Sets PNR demand on highway dbd for calculation of pnr shadow costs
   out_dir              = Args.OutputFolder
   hwy_dbd              = Args.[Highway]
   
   // calc trips per pnr
   dacc_mat          = OpenMatrix(dacc_mat_file,)
   dacc_mc           = CreateMatrixCurrency(dacc_mat,"Drive Access Demand",,,)
      
   id_v = GetMatrixVector(dacc_mc,{{"Index","Column"}})
   trips_v = GetMatrixVector(dacc_mc,{{"Marginal","Column Sum"}})

   pnr_file = GetTempFileName("*.bin")
   if (({"DEBUG"} contains Args.loglevel)) then do 
      pnr_file = out_dir + "\\_assignment\\pnr\\pnr_raw_demand_" + i2s(pnr_iter) + ".bin"
   end  
   
   flds = {{"pnr_lot",     "Integer", 10, 0, },
           {"pnr_trips",   "Real",    15, 4, }}
   obj = CreateObject("CC.Table")
   tab = obj.Create({
               FileName: pnr_file, 
               FieldSpecs: flds, 
               AddEmptyRecords: trips_v.length, 
               DeleteExisting: True})
   dacc_vw = tab.View 

   SetDataVector(dacc_vw + "|","pnr_lot",id_v,) 
   SetDataVector(dacc_vw + "|","pnr_trips",trips_v,) 

   // attach to dbd
   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

   link_pnr = JoinViews("link_pnr", GetFieldFullSpec(link_lyr, "pnr_node"), GetFieldFullSpec(dacc_vw, "pnr_lot"), ) 
   dem_v = GetDataVector(link_pnr + "|", "pnr_trips",)
   sav_v = GetDataVector(link_pnr + "|", "pnr_pfe_trips",)

   // check convergence
   if (pnr_iter > 0) then do 
      c = CreateObject("Model.Statistics")
      v = c.rmse({Method: "Vectors", Observed: sav_v, Predicted: dem_v})
      rmse = v.RMSE
      Args.rmse_pnr = rmse
   end

   // msa pnr demand
   if (pnr_iter > 1) then do 
      dem_v = nz(dem_v) * (1 / pnr_iter) + nz(sav_v) * (1 - (1 / pnr_iter)) 
   end

   SetDataVector(link_pnr + "|", "pnr_pfe_trips",dem_v,)   

   return(1)
endmacro

macro "log_pnr_demand_cost" (out_dir, hwy_dbd, iter)
// Write out pnr links with capacity, demand and costs per iteration

   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
      {node_lyr, link_lyr} = objLyrs.Layers   

   // only report for pnr lots
   SetView(link_lyr)
   SelectByQuery("pnr_lots", "Several", "Select * where pnr_node > 0 and pnr_capacity > 0", ) 

   arr_v = GetDataVectors(link_lyr + "|pnr_lots", {"ID","pnr_node","pnr_parking_cost","pnr_capacity","pnr_pfe_trips","pnr_shadow_cost"},) 

   // write out report
   flds = {{"id",     "Integer", 10, 0, }
            ,{"pnr_node",   "Integer", 10, 0, }
            ,{"pnr_parking_cost",   "Integer", 10, 0, }
            ,{"pnr_capacity",   "Integer", 10, 0, }
            ,{"pnr_pfe_trips",  "Real",    15, 4, }
            ,{"pnr_shadow_cost",  "Real",    15, 4, }
         }
   obj = CreateObject("CC.Table")
   tab = obj.Create({
               FileName: out_dir + "\\_assignment\\pnr\\pnr_msa_demand_" + i2s(iter) + ".bin", 
               FieldSpecs: flds, 
               AddEmptyRecords: arr_v[1].length, 
               DeleteExisting: True})
   pnr_vw = tab.View 

   SetDataVectors(pnr_vw+"|",{
                  {"id",               arr_v[1]}
                  ,{"pnr_node",        arr_v[2]}
                  ,{"pnr_parking_cost",arr_v[3]}
                  ,{"pnr_capacity",    arr_v[4]}
                  ,{"pnr_pfe_trips",    arr_v[5]}
                  ,{"pnr_shadow_cost", arr_v[6]}
                  },)

   tab = null

endmacro


macro "Transit Auto Access Assignment" (Args)
// Helper function to assign transit auto access trips
   if Args.DryRun = 1 then Return(1)

   // delete the transit summary file
   summaryFile = Args.TransitSummaries
   if GetFileInfo(summaryFile) <> null then
      DeleteFile(summaryFile)

   for tod in Args.[TimePeriods] do 
      trip_tab = Args.("Per Trips - " + tod)
      ok = runmacro("transit_assignment", Args, trip_tab, tod, "ta_acc")             
   end

   return(ok)
endmacro

macro "Transit Auto Egress Assignment" (Args)
// Helper function to assign transit auto egress trips
   if Args.DryRun = 1 then Return(1)

   for tod in Args.[TimePeriods] do 
      trip_tab = Args.("Per Trips - " + tod)
      ok = runmacro("transit_assignment", Args, trip_tab, tod, "ta_egr")      
   end

   return(ok)
endmacro


macro "Transit Walk Assignment" (Args)
// Helper function to assign transit walk trips
   if Args.DryRun = 1 then Return(1)

   for tod in Args.[TimePeriods] do 
      trip_tab = Args.("Per Trips - " + tod)
      ok = runmacro("transit_assignment", Args, trip_tab, tod, "tw")
   end

   return(ok)
endmacro

macro "Logan Express Assignment" (Args)
// Helper function to assign logan express trips
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp = "air"
   periods = {"pk", "np"}  

   for per in periods do 
      // single assignment per period
      lx_tag = runmacro("get_pa_file_tag", purp, per, "lx")          
      lx_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, lx_tag)      
      
      if per = "pk" then tod = "am" else tod = "md"
      ok = runmacro("transit_assignment", Args, lx_file, tod, "lx")
   end

   return(ok)
endmacro

macro "transit_assignment"(Args, trip_tab, tod, mode)
// Load person transit trips onto transit network
   out_dir              = Args.OutputFolder     
   trn_rts              = Args.[Transit]
   hwy_dbd              = Args.[Highway]

   // set network for transit mode
   trn_net = runmacro("get_transit_network_file", out_dir, tod)
   ok = runmacro("set_transit_network", Args, trn_rts, trn_net, tod, mode)

   // assignment outputs
   tag = runmacro("get_transit_file_tag", mode, tod)
   flow_tab          = out_dir + "\\_assignment\\flow_" + tag + ".bin"     
   walk_flow_tab     = out_dir + "\\_assignment\\walk_flow_" + tag + ".bin"      
   onoff_tab         = out_dir + "\\_assignment\\onoff_" + tag + ".bin"     
   link_flow_tab     = out_dir + "\\_assignment\\link_flow_" + tag + ".bin"   
   auto_mat          = out_dir + "\\_assignment\\pnr\\pnr_" + tag + ".mtx"   

   obj = CreateObject("Network.PublicTransportAssignment", {RS: trn_rts, NetworkName: trn_net})
   obj.ODLayerType = "Node"
   obj.Method = "PF"
   obj.FlowTable = flow_tab
   obj.WalkFlowTable = walk_flow_tab
   obj.OnOffTable = onoff_tab
   obj.AddDemandMatrix({Class: mode, Matrix: {MatrixFile: trip_tab, Matrix: mode}})
   obj.TransitLinkFlowsTable = link_flow_tab
   obj.OutputSummary({FileName: Args.TransitSummaries, Description: tag, DeleteExisting: false})

   if (mode = 'ta_acc') then 
      obj.DriveAccessDemandMatrix = auto_mat
   else if (mode = 'ta_egr') then do
      obj.DriveEgressDemandMatrix = auto_mat
   end

   ok = obj.Run()
   results = obj.GetResults().Data
   
   Return(ok)
endmacro

macro "transit_sq_assignment" (Args, trip_tab, tod, mode)

   RunMacro("TCB Init")
   //RunMacro("G30 File Close All")
   
   // Load person transit trips onto transit network 
   out_dir             = Args.[Transit Select Query Output Folder]
   trn_rts             = Args.Transit
   hwy_dbd             = Args.[Highway]
   transit_sq          = Args.[Transit Select Query File]
   

   // set network for transit mode
   trn_net = runmacro("get_transit_network_file", Args.OutputFolder, tod)
   ok = runmacro("set_transit_network", Args, trn_rts, trn_net, tod, mode)

   // assignment outputs
   tag               = runmacro("get_transit_file_tag", mode, tod)
   flow_tab          = out_dir + "\\flow_" + tag + ".bin"     
   walk_flow_tab     = out_dir + "\\walk_flow_" + tag + ".bin"      
   onoff_tab         = out_dir + "\\onoff_" + tag + ".bin"     
   output_file       = out_dir + "\\transit_sq_" + tag + ".mtx"

   // open trip matrix 
   trip_tab_obj =  CreateObject("Matrix", trip_tab)

   // get core references
   trip_tab_mc = trip_tab_obj.GetCores()

   // get currency OD matrix
   trip_tab_curr = trip_tab_mc.(mode)

   Opts.Input.[Transit RS] = trn_rts
   Opts.Input.Network = trn_net
   Opts.Input.[OD Matrix Currency] = trip_tab_curr
   Opts.Global.[OD Layer Type] = 2
   Opts.Global.CriticalQueryFile = transit_sq
   Opts.Flag.critFlag = 1
   Opts.Output.[Flow Table] = flow_tab
   Opts.Output.[Walk Flow Table] = walk_flow_tab 
   Opts.Output.[OnOff Table] = onoff_tab
   Opts.Output.[Critical Matrix].[File Name] = output_file

   ret_value = RunMacro("TCB Run Procedure", 4, "Transit Assignment PF", Opts)
   
   return(1)

endmacro

//placeholder
macro "parking_cap"(Args)
//

   return(1)
endmacro

macro "Transit Summary" (Args)
// Produce transit flow and pnr summaries
   if Args.DryRun = 1 then Return(1)
   ok = 1
   
   runmacro("transit_flow_summary", Args)
   runmacro("transit_pnr_summary", Args)

   Return (ok)

endmacro

macro "transit_pnr_summary" (Args)
// Summarize pnr demand
   out_dir              = Args.OutputFolder   
   hwy_dbd              = Args.[Highway]
   time_periods         = Args.[TimePeriods]
   out_tab              = out_dir + "\\_summary\\trn\\pnr_demand"
   //TODO: extend with drive egress once assignment is able to output those tables

   // create and initialize summary table
   auto_mat = OpenMatrix(out_dir + "\\_assignment\\pnr\\pnr_ta_acc_am.mtx",)  
   dacc_mc = CreateMatrixCurrency(auto_mat,"Drive Access Demand",,,)
   id_v = GetMatrixVector(dacc_mc,{{"Index","Column"}})

   flds = {{"id",             "Integer", 10, 0, },
           {"station",        "String", 40, , },
           {"capacity",       "Integer", 10, 0, },
           {"am_acc",         "Real",    15, 4, },
           {"md_acc",         "Real",    15, 4, },
           {"pm_acc",         "Real",    15, 4, },
           {"nt_acc",         "Real",    15, 4, },
           {"daily_acc",      "Real",    15, 4, }
           }
   obj = CreateObject("CC.Table")
   tab = obj.Create({
               FileName: out_tab + ".bin", 
               FieldSpecs: flds, 
               AddEmptyRecords: id_v.length, 
               DeleteExisting: True})
   pnr_vw = tab.View 
   SetDataVector(pnr_vw + "|","id",id_v,)

   // load pnr lot data from nodes
   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
   {node_lyr, link_lyr} = objLyrs.Layers

   node_pnr = JoinViews("node_pnr", GetFieldFullSpec(pnr_vw, "id"), GetFieldFullSpec(node_lyr, "id"),  {"I"})
   name_v = GetDataVector(node_pnr + "|", "Station_Name",)
   park_v = GetDataVector(node_pnr + "|", "parking",)

   SetDataVector(pnr_vw + "|", "station",name_v,)   
   SetDataVector(pnr_vw + "|", "capacity",park_v,)   

   // load data from demand tables 
   daily_v = Vector(id_v.length, "float", {{"Constant", 0},{"Row Based", "True"}})
   for tod in time_periods do
      auto_mat = OpenMatrix(out_dir + "\\_assignment\\pnr\\pnr_ta_acc_" + tod + ".mtx",)
      dacc_mc = CreateMatrixCurrency(auto_mat,"Drive Access Demand",,,)
      trips_v = GetMatrixVector(dacc_mc,{{"Marginal","Column Sum"}})
      daily_v = daily_v + trips_v
      SetDataVector(pnr_vw + "|",tod + "_acc",trips_v,)
   end 

   SetDataVector(pnr_vw + "|","daily_acc",daily_v,) 
   runmacro("convert_bin_to_csv",out_tab + ".bin", out_tab + ".csv")

endmacro


macro "transit_flow_summary" (Args)
// Load person transit trips onto transit network
 
   out_dir              = Args.OutputFolder   
   hwy_dbd              = Args.[Highway]

   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
   {node_lyr, link_lyr} = objLyrs.Layers

   id_v = GetDataVector(link_lyr + "|", "id",)
   fc_v = GetDataVector(link_lyr + "|", "func_class",)

   // create file to assemble transit flows to
   indexed = 1
   tran_summ_file = out_dir + "\\_summary\\trn\\trn_link_flow.bin"
   TableFields = {{"ID1", "Integer", 8, 0, indexed,},
                  {"func_class", "Integer", 8, 0, !indexed,}}
   summ_obj = CreateObject("CC.Table")
   table = summ_obj.Create({ FileName: tran_summ_file, FieldSpecs: TableFields, 
                        DeleteExisting: True, AddEmptyRecords: id_v.length})
   summ_vw = table.View 
   SetDataVectors(summ_vw + "|",{
                  {"ID1"             ,id_v},
                  {"func_class"     ,fc_v}},)         

   //
   // add columns in output table
   //
   SetView(summ_vw)

   // daily totals
   for attr in {"transitflow","walk_flow","drive_flow"} do 
      for dir in {"ab","ba"} do 
         summ_obj.AddField(dir + "_" + attr + "_tot", "Real", 12, 3)
      end
   end    

   // tod totals
   for tod in Args.[TimePeriods] do 
      for tmode in {'','_ta_acc','_ta_egr','_tw','_lx'} do 
         for attr in {"transitflow","walk_flow","drive_flow"} do 
            for dir in {"ab","ba"} do 
               summ_obj.AddField(dir + tmode + "_" + attr + "_" + tod, "Real", 12, 3)
            end
         end      
      end
   end

   //   
   // set tod mode values
   //
   for tod in Args.[TimePeriods] do 
      for tmode in {'ta_acc','ta_egr','tw','lx'} do 

         // logan express only assigns pk/np (am, md)
         if (tmode='lx') & ({'pm','nt'} contains tod) then continue

         tag = runmacro("get_transit_file_tag", tmode, tod)
         flow_file = out_dir + "\\_assignment\\link_flow_" + tag + ".bin" 

         flow_tab = OpenTable("flow_tab", "FFB", {flow_file},)
         join_vw = JoinViews("flow_join", summ_vw + ".id1", flow_tab + ".id1",)      

         for dir in {"ab","ba"} do 
            // tw doesn't have a "walk_flow" attribute, nor auto_flow
            attr_arr = {"transitflow","walk_flow","drive_flow"}
            if tmode = 'tw' then attr_arr = {"transitflow","walk_flow"} 

            for attr in attr_arr do 
               in_attr = attr
               if (tmode = 'tw') & (attr = 'walk_flow') then in_attr = "nontransitflow"
               flow_v = GetDataVector(join_vw + "|", dir + "_" + in_attr,)
               SetDataVector(join_vw + "|", dir + "_" + tmode + "_" + attr + "_" + tod,flow_v,)
            end
         end
         CloseView(join_vw)
      end // mode
   end // tod

   // sum to totals by tod and daily
   SetView(summ_vw)
   for dir in {"ab","ba"} do 
      for attr in {"transitflow","walk_flow","drive_flow"} do 
         
         tot_v = GetDataVector(summ_vw + "|", dir + "_" + attr + "_tot",) 
              
         for tod in Args.[TimePeriods] do 
            
            tot_tod_v = GetDataVector(summ_vw + "|", dir + "_" + attr + "_" + tod,) 

            for tmode in {'ta_acc','ta_egr','tw','lx'} do
               flow_v = GetDataVector(summ_vw + "|", dir + "_" + tmode + "_" + attr + "_" + tod,)
               tot_tod_v = nz(tot_tod_v) + nz(flow_v)
            end // mode
            
            tot_v = nz(tot_v) + nz(tot_tod_v)
            SetDataVector(summ_vw + "|", dir + "_" + attr + "_" + tod,tot_tod_v,)

         end // tod
         SetDataVector(summ_vw + "|", dir + "_" + attr + "_tot",tot_v,)
      end // attr
   end // dir
endmacro

