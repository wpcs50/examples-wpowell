//#   "utilities.rsc"

macro "Check Convergence" (Args)
// Check RMSE from feedback assignments and loop or end model

   if Args.DryRun = 1 then do // dry run up to max iterations
      Args.[rmse_am] = 1
      Args.[rmse_md] = 1
   end

   // no feedback for assignment only
   if Args.run_mode != "Full Run" then do 
      return(1)
   end

   // return value of 1 to end, 2 to feedback
   RetValue = 2
 
   if Args.Iteration = null then Args.Iteration = 1
   if Args.MaxIterations = null then Args.MaxIterations = 1

   // only have % RMSE on later iterations
   if Args.Iteration > 1 then do 
      rmse_am = Args.[prmse_am]
      rmse_md = Args.[prmse_md]
   end 
   else do 
      rmse_am = 100
      rmse_md = 100
   end   


   status_str = "Feedback Iteration: " + i2s(Args.Iteration) + 
                  " - AM RMSE: " + r2s(Args.[rmse_am]) + 
                  " - MD RMSE: " + r2s(Args.[rmse_md]) + 
                  " - AM %RMSE: " + r2s(rmse_am) + 
                  " - MD %RMSE: " + r2s(rmse_md) + 
                  " time: " + FormatDateTime(CreateTime(),)

   runmacro("write_log_file", Args.OutputFolder, "converge.txt", status_str)

   SetStatus(4, status_str,)

   // model converged               
   if ((rmse_am < Args.[FeedbackConvergence]) and 
         (rmse_md < Args.[FeedbackConvergence])) then do 
      RetValue = 1 
      Args.ExitMessage = "Model Converged after " + i2s(Args.Iteration) + " Iterations"
   end

   // max iterations
   if Args.Iteration >= Args.MaxIterations then do 
      RetValue = 1
      Args.ExitMessage = "Model Stopped after " + i2s(Args.Iteration) + " Iterations"
   end

   // user control
   if Args.AskToContinue then do
      end_run = RunDbox("G30 Confirm", "Iteration " + String(Args.Iteration) + 
               " - AM RMSE: " + r2s(rmse_am) + " - MD RMSE: " + r2s(rmse_md) + ". Stop?")
      
      if end_run then do 
         RetValue = 1
         Args.ExitMessage = "Model Stopped by User"
      end
   end

   // save feedback files
   if ((RetValue = 2) and (Args.[Save Feedback Data])) then do 
      runmacro("save_feedback_files", Args)
   end

   if (RetValue = 2) then Args.Iteration = Args.Iteration + 1

  return(RetValue)

endmacro

macro "save_feedback_files" (Args)
// Save intermediate feedback files to subfolder

   iter_dir = Args.OutputFolder  + "\\_feedback\\iter_" + i2s(Args.Iteration)
   src_dir = Args.OutputFolder

   dirs = {"_assignment","_skim",
            "_demand\\mc","_demand\\td","_demand\\tod"}

   for i = 1 to dirs.length do
      CreateDirectory(iter_dir + "\\" + dirs[i])
      o = CreateObject("CC.Directory", src_dir + "\\" + dirs[i])
      o.Copy(iter_dir + "\\" + dirs[i]) 
   end

endmacro


macro "Model Setup" (Args)
// Copy Necessary Files to Output Folders
   if Args.DryRun = 1 then Return(1)

   runmacro("copy_files_to_scenario", Args)

   if Args.run_mode != "Full Run" then do 
      runmacro("copy_assign_only_files", Args)
   end

   Return(1)

endmacro

macro "copy_files_to_scenario" (Args)
// Copy files that will be modified from the inputs to the scenario folder
   out_dir              = Args.out_dir           //ui:output folder
   hwy_dbd_in           = Args.[Highway Input]         //ui:highway geodb filename
   tst_rts_in           = Args.[Transit Input]        //ui:transit rts filename
   //### Outputs:
   hwy_dbd              = Args.[Highway]         //ot: highway database
   trn_rts              = Args.[Transit]          //ot: transit route system
   if Args.DryRun = 1 then Return(1)

   // Copy highway
   CopyDatabase(hwy_dbd_in, hwy_dbd) 
   // also copy the rs to the output folder
   obj = CreateObject("DataManager")
   rsMaster = tst_rts_in
   rsTarget = trn_rts
   obj.AddDataSource("RS", {FileName: rsMaster, DataType: "RS"})
    
   opts.TargetRS = rsTarget
   opts.settings.Geography = hwy_dbd
   opts.settings.Label = "CTPS RS"
   opts.settings.DefaultSide = "Right"
   opts.settings.Reload = 0
   obj.CopyRouteSystem("RS", opts)
   obj = null

   ok = 1
quit:   
   Return(ok)

endmacro

macro "copy_assign_only_files" (Args)
//
   // copy files needed for assignment only runs

   // trip tables
   src_dir = Args.cnstr_source + "\\_demand\\tod"
   dest_dir = Args.OutputFolder + "\\_demand\\tod"

   // TODO: test that src_dir exists, otherwise crashes tc
   o = CreateObject("CC.Directory", src_dir)
   o.Copy(dest_dir) 

   // area type (to set vdf)
   at_src = Args.cnstr_source + "\\_networks\\access_density.csv"
   at_dest = Args.OutputFolder + "\\_networks\\access_density.csv"
   CopyFile(at_src, at_dest)

   // taz index (for summaries)
   ti_src = Args.cnstr_source + "\\_networks\\taz_index.csv"
   ti_dest = Args.OutputFolder + "\\_networks\\taz_index.csv"
   CopyFile(ti_src, ti_dest)

   // highway network (for transit-only assignments)
   for tod in Args.[TimePeriods] do 
      net_src = Args.cnstr_source + "\\_networks\\hwy_" + tod + ".net"
      net_dest = Args.OutputFolder + "\\_networks\\hwy_" + tod + ".net"
      CopyFile(net_src, net_dest)
   end

   // logan express
   periods = {"pk", "np"}  
   for per in periods do 
      lx_tag = runmacro("get_pa_file_tag", "air", per, "lx")          
      src_file = runmacro("get_segment_mc_trip_file", Args.cnstr_source, lx_tag)    
      dst_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, lx_tag)    
      CopyFile(src_file, dst_file)
   end

endmacro

macro "Export Reports" (Args)
// Export transcad reports
   out_dir              = Args.[Output Folder]
   rep_file = out_dir + "\\_summary\\summary_report.html"

   html = RunMacro("ConvertXML_to_HTML", {XML: out_dir + "\\CTPS_TDM23-Run-Report.xml", XmlType: "Report", OutFileName: rep_file}) // Report, Summary - Log, ErrorLog
   LaunchDocument(html,)

   Return(1)

endmacro


macro "Export Transit Network" (Args)
// Export transit network to shape files for preprocessors
   trn_rts              = Args.[Transit]
   out_dir              = Args.[Output Folder]
   if Args.DryRun = 1 then Return(1)

   objLyrs = CreateObject("AddRSLayers", {FileName: trn_rts})
   // open up and have access to node, line, route, and stops layer
   node_lyr = objLyrs.NodeLayer
   line_lyr = objLyrs.LineLayer
   rs_layer = objLyrs.RouteLayer
   stops_layer = objLyrs.StopLayer

   // stops layer
   stop_fields = GetFields(stops_layer, "All")
   route_fields = GetFields(rs_layer, "All")

   ExportArcViewShape(stops_layer, 
                     out_dir + "\\_networks\\stops.shp", {{"fields", stop_fields[1]},
                        {"Projection","nad83:2001",{"_cdist=1000","_limit=100"}}}) 

   ExportArcViewShape(rs_layer, 
                     out_dir + "\\_networks\\routes.shp", {{"fields", route_fields[1]},
                        {"Projection","nad83:2001",{"_cdist=1000","_limit=100"}}}) 

   Return(1)

endmacro

macro "Export TAZ Demographics"(Args)
// Pull TAZ demographics data from Sqlite DB
   if Args.DryRun = 1 then Return(1)
   
   out_dir = Args.OutputFolder
   taz_csv_file = Args.[TAZ Demographic Data]
   
   db_file = out_dir + "\\tdm23.db"

   obj = CreateNativeObject("SQLite")
   obj.Connect(db_file) 
   qry = "SELECT t.taz_id as taz_id, type, town, state, mpo, subregion, corridor, ring, district, total_area, land_area, " +
	   "(CASE a.access_density WHEN  1 THEN 'CBD' WHEN 2 THEN 'Dense Urban' WHEN 3 THEN 'Urban' " +
		"WHEN 4 THEN 'Fringe Urban' WHEN 5 THEN 'Suburban' WHEN 6 THEN 'Rural' ELSE NULL END) AS access_density, " +
      "e.college_total AS hedu,  IFNULL(f.jobs,0) AS jobs, IFNULL(f.households,0) AS households, " + 
      "IFNULL(f.population,0) AS population " +
      "FROM MA_taz_geography AS t " + 
      "LEFT JOIN access_density AS a ON t.taz_id = a.taz_id " +
      "LEFT JOIN enrollment AS e ON t.taz_id = e.taz_id " + 
      "LEFT JOIN ( SELECT taz_id, sum(area_fct*jobs) as jobs, " +
         "sum(area_fct*households) as households, sum(area_fct*population) as population " +
         "FROM (SELECT b.taz_id, b.block_id, area_fct, IFNULL(j.total_jobs,0) AS jobs, " +
            "IFNULL(h.households,0) AS households, IFNULL(h.population,0) AS population " +
            "FROM taz_block_allocation AS b LEFT JOIN jobs AS j ON b.block_id = j.block_id " +
            "LEFT JOIN (SELECT block_id, COUNT(hid) as households, SUM (persons) AS population FROM hh GROUP BY block_id) " +
            "AS h ON b.block_id = h.block_id) " +
         "GROUP BY taz_id) AS f on t.taz_id = f.taz_id "
   view = obj.RetrieveQueryResults({ViewName: "taz_data", Query: qry, MaxStringLength: 30, 
      FieldTypes: {"integer", "string",  "string", "string", "string", "string", "integer", 
         "integer", "integer", "float", "float",  "string", "integer", "float", "float", "float"}})
   ExportView("taz_data|","CSV",taz_csv_file,,{{"CSV Header", "True"}})

   Return (1)

endmacro

macro "Export Highway Network" (Args)
// Export highway network to shape files for postrocessors
   hwy_dbd              = Args.[Highway]
   out_dir              = Args.[Output Folder]
   if Args.DryRun = 1 then Return(1)

   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
   // open up and have access to node and line layer
   {node_lyr, link_lyr} = objLyrs.Layers

   // fields
   node_fields = GetFields(node_lyr, "All")
   link_fields = {"ID","Dir","Length","a_node","b_node","taz_id","urban","street_name","func_class","fac_type","ff_time"}
   //GetFields(link_lyr, "All")

   ExportArcViewShape(node_lyr, 
                     out_dir + "\\_networks\\nodes.shp", {{"fields", node_fields[1]},
                        {"Projection","nad83:2001",{"_cdist=1000","_limit=100"}}}) 

   ExportArcViewShape(link_lyr, 
                     out_dir + "\\_networks\\links.shp", {{"fields", link_fields},
                        {"Projection","nad83:2001",{"_cdist=1000","_limit=100"}}}) 

   // Export BIN file for AQ process
   bin_file = out_dir  + "\\_networks\\LinksNodes"  + ".bin"  
   csv_file = out_dir + "\\_networks\\LinksNodes"  + ".csv"  
   runmacro("convert_bin_to_csv", bin_file, csv_file)
   
   Return(1)

endmacro

macro "Calculate Stop Service Frequency" (Args)
// Export transit network to shape files for preprocessors
   trn_rts              = Args.[Transit]
   out_dir              = Args.[Output Folder]
   if Args.DryRun = 1 then Return(1)
   if Args.acc_load = 1 then Return(1) // stop frequencies are only needed for calculating zonal values
   
   stop_hdwy_csv = out_dir + "\\_networks\\stop_hdwy.csv"
   indexed = 1

   objLyrs = CreateObject("AddRSLayers", {FileName: trn_rts})
   node_lyr = objLyrs.NodeLayer
   line_lyr = objLyrs.LineLayer
   rs_layer = objLyrs.RouteLayer
   stops_layer = objLyrs.StopLayer

   // get transit frequency per stop
   sr_vw = JoinViews("sr_vw", stops_layer + ".Route_ID", rs_layer + ".Route_ID",)
	SetView(sr_vw)   

   n = SelectByQuery("available", "Several", 
      "Select * where Routes.available = 1 and Stops.available = 1",)

   nnode_v     = GetDataVector(sr_vw + "|available", "near_node", )
   am_hdwy_v   = GetDataVector(sr_vw + "|available", "headway_am", )
   am_freq_v   = nz(180 / am_hdwy_v) // 6:30-9:30 AM peak period      

   // record out to file to make it easy to aggregate by near_node
   stop_freq_file = GetTempFileName(".bin")    
   TableFields = {{"near_node", "Integer", 8, 0, indexed,},
                  {"am_freq", 'Real (8 bytes)',12, 3, !indexed,}}
   obj = CreateObject("CC.Table")
   table = obj.Create({ FileName: stop_freq_file, FieldSpecs: TableFields, 
                        DeleteExisting: True, AddEmptyRecords: n})
   stop_freq_vw = table.View     

   SetDataVectors(stop_freq_vw + "|",{
                  {"near_node"   ,nnode_v},
                  {"am_freq"     ,am_freq_v}},)

   // Aggregate frequencies across all routes and calculate combined headway
   agg_freq_file = GetTempFileName(".bin") 
   agg_freq_vw = AggregateTable("agg_freq_vw", stop_freq_vw + "|", 
                                 "FFB", agg_freq_file, 
                                 "near_node", {{"am_freq","SUM",}},)

   nnode_agg_v     = GetDataVector(agg_freq_vw + "|", "near_node", )
   am_freq_agg_v   = GetDataVector(agg_freq_vw + "|", "am_freq", )
   am_hdwy_agg_v   = nz(180 / am_freq_agg_v)

   // Create aggregate headway table (needs to be .bin first, then export to csv)
   stop_hdwy_file = GetTempFileName(".bin") 
   TableFields = {{"near_node", "Integer", 8, 0, indexed,},
                  {"am_hdwy", 'Real (8 bytes)',12, 3, !indexed,}}
   obj = CreateObject("CC.Table")
   table = obj.Create({ FileName: stop_hdwy_file, FieldSpecs: TableFields, 
                        DeleteExisting: True, AddEmptyRecords: nnode_agg_v.Length})
   stop_hdwy_vw = table.View   
   SetDataVectors(stop_hdwy_vw + "|",{
                  {"near_node"   ,nnode_agg_v},
                  {"am_hdwy"     ,am_hdwy_agg_v}},)   

   ExportView(stop_hdwy_vw + "|", "CSV",stop_hdwy_csv,,{{"CSV Header","True"}})

   Return(1)

endmacro

macro "Export Skims to OMX" (Args)
// Export highway and transit skims to OMX
   time_periods         = Args.[TimePeriods]
   out_dir              = Args.[Output Folder]
   if Args.DryRun = 1 then Return(1)   
   //TODO: remove to be all 4 periods (test with just am, md)
   time_periods = {"am","md"}

   ok = 1
   
   for tod in time_periods do 

      omx_file = runmacro("get_highway_omx_skim_file", out_dir, tod, mode)
      mtx_file = Args.("HighwaySkims - " + tod)
      runmacro("export_skim_to_omx", mtx_file, omx_file, "dist")
      
      omx_file = runmacro("get_transit_omx_skim_file", out_dir, tod, "ta")
      mtx_file = Args.("TransitAutoSkims - " + tod)
      runmacro("export_skim_to_omx", mtx_file, omx_file, "gen_cost")

      omx_file = runmacro("get_transit_omx_skim_file", out_dir, tod, "tw")
      mtx_file = Args.("TransitWalkSkims - " + tod)
      runmacro("export_skim_to_omx", mtx_file, omx_file, "gen_cost")
      
   end
   
   omx_file = out_dir + "\\_skim\\nm_daily.omx"
   mtx_file = Args.[NonMotorizedSkim]
   runmacro("export_skim_to_omx", mtx_file, omx_file, "dist")   

   return(ok)

endmacro

macro "Export Skims to OMX - Init" (Args)
// Export highway and transit skims to OMX for initialization steps
   time_periods         = Args.[TimePeriods]
   out_dir              = Args.[Output Folder]
   if Args.DryRun = 1 then Return(1)   
   if Args.acc_load = 1 then Return(1) // skims are only needed for calculating zonal values
   time_periods = {"am"}

   ok = 1

   for tod in time_periods do 

      omx_file = runmacro("get_highway_omx_skim_file", out_dir, tod, mode)
      mtx_file = Args.("HighwaySkims - " + tod)
      runmacro("export_skim_to_omx", mtx_file, omx_file, "dist")
      
      omx_file = runmacro("get_transit_omx_skim_file", out_dir, tod, "tw")
      mtx_file = Args.("TransitWalkSkims - " + tod)
      runmacro("export_skim_to_omx", mtx_file, omx_file, "gen_cost")
      
   end

   return(ok)

endmacro

// exports from mtx to omx
macro "export_skim_to_omx" (mtx_file, omx_file, root_mc)
//

   ok = 1
   
   // add simple index of number sequence (needed for OMX outputs)
   m = OpenMatrix(mtx_file,)
   curr_idx = GetMatrixIndex(m)
   index_ids = GetMatrixIndexIDs(m,curr_idx[1])
   mat_size = ArrayLength(index_ids)
   seq_idx = Vector(mat_size,"Short",{{"Sequence",1,1}})

   idx_t = GetTempFileName(".bin")
   idx_vw = CreateTable("idx_vw", idx_t,"FFB",
               {{"old_idx", "Integer", 10, 0, "True"},
               {"new_idx", "Integer", 10, 0,}})

   rh = AddRecords(idx_vw, null, null, {{"Empty Records", mat_size}})
   SetDataVector(idx_vw + "|","old_idx" ,ArrayToVector(index_ids),)
   SetDataVector(idx_vw + "|","new_idx" ,seq_idx,)

   mobj = CreateObject("Caliper.Matrix")
   mobj.AddIndex({MatrixFile: mtx_file, 
                ViewName: idx_vw,
                Dimension: "Both", 
                OriginalID: "old_idx",
                NewID: "new_idx",
                IndexName: "ID"})

   mc = CreateMatrixCurrency(m,root_mc,,,)
   CopyMatrix(mc,{{"File Name", omx_file}, 
                    {"OMX", True},
                    {"File Based", "Yes"}})
   return (ok)
endmacro

macro "Generate Warm Start Speeds" (Args)
// Export AM and MD link speeds for warm start
   hwy_dbd              = Args.[Highway]
   out_dir              = Args.[Output Folder]
   scen_name            = Args.Scenario
   if Args.DryRun = 1 then Return(1)

   objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
   // open up and have access to node and line layer
   {node_lyr, link_lyr} = objLyrs.Layers
   id_v = GetDataVector(link_lyr + "|", "ID",)
   len_v = GetDataVector(link_lyr + "|", "Length",)

   for tod in {"am","md"} do 
      speed_file = out_dir + "\\_networks\\" + scen_name + "_" + tod + "_speeds.bin"

      speed_table_vw = CreateTable("WarmSpeeds", speed_file, "FFB",
        {	{"ID1",         "Integer", 10, 0, "True"},
            {"AB_Speed",    "Real", 15, 4, },
            {"BA_Speed",    "Real", 15, 4, }} )      

      rh = AddRecords(speed_table_vw, null, null, {{"Empty Records", id_v.Length}})

      ab_time_v = GetDataVector(link_lyr + "|", "ab_MSAtime_" + tod,)
      ba_time_v = GetDataVector(link_lyr + "|", "ba_MSAtime_" + tod,)
      ab_v = len_v / (ab_time_v / 60)
      ba_v = len_v / (ba_time_v / 60)

      SetDataVector(speed_table_vw + "|", "ID1",id_v,)
      SetDataVector(speed_table_vw + "|", "AB_Speed",ab_v,)
      SetDataVector(speed_table_vw + "|", "BA_Speed",ba_v,)
      CloseView("WarmSpeeds")
   end
endmacro

macro "Export Highway Assignment" (Args)
// Export highway assignment results for summary reports
   out_dir              = Args.[Output Folder]
   if Args.DryRun = 1 then Return(1)

   for tod in Args.[TimePeriods] do 
      flow_in = Args.("HighwayFlows - " + tod) 
      flow_out = out_dir + "\\_assignment\\flows_" + tod + ".csv"  
      runmacro("convert_bin_to_csv", flow_in, flow_out)
   end


   Return(1)
endmacro

macro "Rerun Dist Reports" (Args)
// Rerun reports with final skims
   out_dir              = Args.[Output Folder]
   if Args.DryRun = 1 then Return(1)

   for tod in Args.[TimePeriods] do 
      flow_in = Args.("HighwayFlows - " + tod) 
      flow_out = out_dir + "\\_assignment\\flows_" + tod + ".csv"  
      runmacro("convert_bin_to_csv", flow_in, flow_out)
   end


   Return(1)
endmacro

macro "Export Transit Assignment" (Args)
// Export transit assignment results for summary reports
   out_dir              = Args.[Output Folder]
   if Args.DryRun = 1 then Return(1)

   for tod in Args.[TimePeriods] do 
      for mode in {"tw","ta_acc","ta_egr"} do 

         tag = runmacro("get_transit_file_tag", mode, tod)
         onoff_in = out_dir + "\\_assignment\\onoff_" + tag + ".bin"  
         onoff_out = out_dir + "\\_assignment\\onoff_" + tag + ".csv"  

         runmacro("convert_bin_to_csv", onoff_in, onoff_out)

         // export flows for runtimes
         if mode = "tw" then do
            flow_in = out_dir + "\\_assignment\\flow_" + tag + ".bin"  
            flow_out = out_dir + "\\_assignment\\flow_" + tag + ".csv"  

            runmacro("convert_bin_to_csv", flow_in, flow_out)            
         end
      end
   end


   Return(1)
endmacro

macro "Generate Summaries" (Args)
// Generate summary table for reports / post processors
   runmacro("aggregate_summary_trip_tables", Args)
   runmacro("export_transit_activity_summary", Args)
endmacro

macro "convert_bin_to_csv" (bin_file, csv_file)
//
   tc_vw = OpenTable("Flow", "FFB", {bin_file})
   ExportView(tc_vw + "|","CSV",csv_file,,{{"CSV Header", "True"}})
endmacro

macro "Clean Up Intermediate Files" (Args)
// Remove intermediate files that were used during speed feedback
   if Args.DryRun = 1 then Return(1)

   if ({"LEAN"} contains Args.loglevel) then do 

      // truck and external TOD files
      for tod in Args.[TimePeriods] do 
         for trip in {"trk","ext"} do
            tags = runmacro("get_od_file_tag", trip, tod)
            od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)

            if (GetFileInfo(od_file)<>null) then DeleteFile(od_file)
         end // trk/ext
      end // tod
   end // only run if not debug mode

   Return(1)
endmacro

macro "trip_length_distribution_summary" (Args, trip_mobj, tag, tld)
// Calculate trip length distribution summary and coincidence ratios
   out_dir = Args.OutputFolder

   skim_file = Args.("HighwaySkims - am")
   sk_mobj = CreateObject("Matrix", skim_file)
   sk_mc = sk_mobj.GetCores()        
   trip_mc = trip_mobj.GetCores()
   cores = trip_mobj.GetCoreNames()

   idx_tag = Substitute(tag,"pa_","",1)

   for seg in cores do
      for imp in {"dist","da_time"} do 

         //mout_file = out_dir + "\\_summary\\tld_" + tag + "_" + seg + "_" + imp + ".mtx"
         mout_file = GetTempFileName(".mtx")
      
         objTLD = CreateObject("Distribution.TLD")
         objTLD.StartValue = 0
         objTLD.BinSize = 1
         objTLD.TripMatrix = trip_mc.(seg)
         objTLD.ImpedanceMatrix = sk_mc.(imp)
         objTLD.OutputMatrix( mout_file )
         objTLD.Run()
         res = objTLD.GetResults()    
         //ShowArray(res.Data)

         // record stats for all segments
         tld.(idx_tag).(seg).(imp).av = res.Data.AvTripLength
         tld.(idx_tag).(seg).(imp).mn = res.Data.MinTripLength
         tld.(idx_tag).(seg).(imp).mx = res.Data.MaxTripLength

         // only export distribution for all segments or if for mode choice
         if ((seg = "all") | (Position(tag,"mc_")>0) | (Position(tag,"trk")>0) | (Position(tag,"air")>0)) then do 
            
            name = tag
            if (Position(tag,"mc_")>0) then name = tag + "_" + seg
            if (Position(tag,"trk")>0) then name = tag + "_" + seg
            if (Position(tag,"air")>0) then name = tag + "_" + seg

            opts = null
            opts.FileName =  out_dir + "\\_summary\\tld\\" + name + "_" + imp + ".csv"
            opts.OutputMode = "Matrix"
            res.Data.MatrixObj.ExportToTable(opts)
         end

         // only calculate distance dist for mode choice
         if (Position(tag,"mc_")>0) then break
      end // impedance
   end // segments

   Return(ok)
endmacro

macro "sum_pknp_matrices" (out_dir, pk_tag, np_tag)
//
   // sums all cores in the input matrices (must be the same)
   // returns the matrix object

   pk_file = runmacro("get_segment_pa_file", out_dir, pk_tag)
   np_file = runmacro("get_segment_pa_file", out_dir, np_tag)
   pk_mobj = CreateObject("Matrix", pk_file)
   np_mobj = CreateObject("Matrix", np_file)
   pk_mc = pk_mobj.GetCores()
   np_mc = np_mobj.GetCores()
   cores = pk_mobj.GetCoreNames()

   dy_mat = pk_mobj.CloneMatrixStructure({FileName: GetTempFileName(".mtx"),  
                                             MatrixLabel: purp + "_daily", 
                                             CloneSource: pk_file //,MemoryOnly: true 
                                             })
   dy_mobj = CreateObject("Matrix", dy_mat)                                                
   dy_mc = dy_mobj.GetCores()

   for core in cores do
      dy_mc.(core) := nz(pk_mc.(core)) + nz(np_mc.(core))
   end

   Return(dy_mobj)
endmacro


macro "aggregate_matrix_by_geography" (out_dir, m_obj, tag, geo)
//

   out_file = out_dir + "\\_summary\\geo\\" + tag + "_" + geo + ".csv"

   if (geo = "access_density") then do 
      idx_file = out_dir + "\\_networks\\access_density.csv"
   end
   else idx_file = out_dir + "\\_networks\\taz_index.csv"

   taz_idx_vw = OpenTable("taz_idx_vw", "CSV", {idx_file,})
   agg_info = {"Data": taz_idx_vw, "MatrixID": "taz_id", "AggregationID": geo}
   mout_file = {"MatrixFile": GetTempFileName("*.mtx"), "MatrixLabel": geo}

   agg_mat = m_obj.Aggregate({"Matrix": mout_file,
                        "Method": "Sum",
                        "Rows": agg_info, 
                        "Cols": agg_info})
   
   // create all sum (except for vehicles which includes both da,sr and auto)
   if (tag <> 'od_veh') then do
      coreNames = agg_mat.GetCoreNames()
      targetCore = "Sum of all"
      coreNames = ArrayExclude(coreNames, {targetCore})
      if coreNames <> null then 
         agg_mat.Sum(targetCore) // combined matrix (will replace all sum if exists)
   end
   agg_mat.ExportToTable({"FileName": out_file, "OutputMode":"Table"})   

endmacro


macro "calculate_trip_origins" (trip_mobj, summ_file)
// Calculate intrazonals
   trip_mc = trip_mobj.GetCores()
   cores = trip_mobj.GetCoreNames()

   taz_v = trip_mobj.GetVector({Core: cores[1], Index:"Column"})

   indexed = 1
   to_bin_file = GetTempFileName(".bin")
   
   // build field for each core
   TableFields = {{"taz_id", "Integer", 8, 0, indexed}}
   for seg in cores do
      TableFields = TableFields + {{seg, "Real",12, 3, !indexed,}}
   end

   summ_obj = CreateObject("CC.Table")
   table = summ_obj.Create({ FileName: to_bin_file, FieldSpecs: TableFields, 
                        DeleteExisting: True, AddEmptyRecords: taz_v.length})
   summ_vw = table.View 
   SetDataVector(summ_vw + "|","taz_id",taz_v,)

   for seg in cores do
      marg_v = trip_mobj.GetVector({Core: seg, Marginal:"Column Sum"})
      SetDataVector(summ_vw + "|", seg, marg_v,)
   end // trip tables

   // export to csv
   runmacro("convert_bin_to_csv",to_bin_file, summ_file)

   Return(ok)
endmacro


macro "calculate_intrazonals" (Args, trip_mobj, tag, intr)
// Calculate intrazonals
   out_dir = Args.OutputFolder

   trip_mc = trip_mobj.GetCores()
   cores = trip_mobj.GetCoreNames()

   for seg in cores do
      diag = VectorToArray(trip_mobj.GetVector({Core: seg, Diagonal:"Column"}))
      col_sum = VectorToArray(trip_mobj.GetVector({Core: seg, Marginal:"Row Sum"}))
      intrazonal = Sum(diag)/(Sum(col_sum) + 0.0001) // missing segments will cause div by zero error

      intr.(tag).(seg) = intrazonal
   end // segments

   Return(ok)
endmacro

// Macros to return files generated by the model
macro "get_transit_network_file" (out_dir, tod)
//

   filename = out_dir + "\\_networks\\transit_" + tod + ".tnw"
   return(filename)
endmacro

macro "get_transit_omx_skim_file" (out_dir, tod, mode)
//

   filename = out_dir + "\\_skim\\" + mode + "_" + tod + ".omx"
   return(filename)
endmacro

macro "get_highway_mode_skim_file" (out_dir, tod, mode)
//

   if (mode <> null) then mode = mode + "_"
   filename = out_dir + "\\_skim\\hwy_"  + mode + tod + ".mtx"
   return(filename)
endmacro

macro "get_highway_omx_skim_file" (out_dir, tod, mode)
//

   if (mode <> null) then mode = mode + "_"
   filename = out_dir + "\\_skim\\hwy_"  + mode + tod + ".omx"
   return(filename)
endmacro

macro "get_nm_network_file" (out_dir)
//

   filename = out_dir + "\\_networks\\nm_daily.net" 
   return(filename)
endmacro


macro "get_od_file_tag" (type, tod)
//
   // type is purpose string, or veh, cv, per, ext
   tag = type + "_" + tod
   return(tag)

endmacro

macro "get_pa_file_tag" (purp, per, seg)
//

   if (seg <> null) then seg = seg + "_"
   tag = purp + "_" + seg + per
   return(tag)

endmacro

macro "get_transit_file_tag" (mode, tod)
//
   tag = mode + "_" + tod
   return(tag)

endmacro

macro "get_segment_dc_prob_file" (out_dir, tag)
//

   filename = out_dir + "\\_demand\\td\\prob_"  + tag + ".mtx"
   return(filename)
endmacro

macro "get_segment_mc_prob_file" (out_dir, tag)
//

   filename = out_dir + "\\_demand\\mc\\prob_"  + tag + ".mtx"
   return(filename)
endmacro

macro "get_segment_mc_logsum_file" (out_dir, tag)
//

   filename = out_dir + "\\_demand\\mc\\ls_"  + tag + ".mtx"
   return(filename)
endmacro

macro "get_segment_mc_trip_file" (out_dir, tag)
//

   filename = out_dir + "\\_demand\\mc\\mc_"  + tag + ".mtx"
   return(filename)
endmacro

macro "get_od_trip_file" (out_dir, tag)
//

   filename = out_dir + "\\_demand\\tod\\od_"  + tag + ".mtx"
   return(filename)
endmacro

macro "get_segment_pa_file" (out_dir, tag)
//

   filename = out_dir + "\\_demand\\td\\pa_"  + tag + ".mtx"
   return(filename)
endmacro

macro "get_rs_od_file" (out_dir, rev_nonrev)
//

   filename = out_dir + "\\_demand\\tod\\od_rs_"  + rev_nonrev + ".mtx"
   return(filename)
endmacro

macro "get_purpose_mc_nest" (Args, purp)
//

   nest = null

   if ({"hbw","hbu","hbsr","hbpb"} contains purp) then nest = Args.[HB Full Alt Nests]  
   if ({"hbsc"} contains purp) then nest = Args.[HBSC Nests]  
   if ({"nhbw","nhbnw"} contains purp) then nest = Args.[NHBW NHBNW Nests]  
   if ({"air_resb","air_resl","air_visb","air_visl"} contains purp) then nest = Args.(purp + " Nests")  

   return(nest)

endmacro

macro "write_log_file" (out_dir, logfile, text)
//

   filename = out_dir + "\\_logs\\" + logfile

   fptr = OpenFile(filename, "a+")
   WriteLine(fptr, text)
   CloseFile(fptr)

endmacro

macro "export_arr_to_json"(arr, outfile)
//

   out_js = ArrayToJson(arr)
   ptr = OpenFile(outfile, "w")
   WriteLine(ptr, out_js)
   CloseFile(ptr)

endmacro

macro "placeholder" (Args)
//

   return(1)
endmacro