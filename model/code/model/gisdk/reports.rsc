//#   "reports.rsc"

macro "ReportWorkFromHome"  (Args)
// Export summary report for Work from Home component
   ok = 1

   log_folder = Args.[OutputFolder] + "\\_summary\\zonal\\"
   file_worker_state = log_folder + "wfh_summary_worker_by_state.csv"
   file_worker_mpo = log_folder + "wfh_summary_worker_by_mpo.csv"
   file_job_state = log_folder + "wfh_summary_job_by_state.csv"
   file_job_mpo = log_folder + "wfh_summary_job_by_mpo.csv"

   obj = CreateObject("Flowchart.Reports.Table", { Title: "Work from Home"})
   obj.AddTable({Name: file_worker_state, Title: "Remote Workers by State", CompleteTable: true})
   obj.AddTable({Name: file_worker_mpo, Title: "Remote Workers by MPO", CompleteTable: true})
   obj.AddTable({Name: file_job_state, Title: "Remote Jobs by State", CompleteTable: true})
   obj.AddTable({Name: file_job_mpo, Title: "Remote Jobs by MPO", CompleteTable: true})
   
   report = obj.Run()
   return (report) 

endmacro


macro "ReportVehicleAvailability"  (Args)
//
   folder = Args.[OutputFolder] + "\\_summary\\zonal\\"
   va_summ = folder + 'vehicle_availability_summary.csv'

   obj = CreateObject("Flowchart.Reports.Table", { Title: "Vehicle Availability"})
   obj.AddTable({Name: va_summ, Title: "Vehicles Available", CompleteTable: true})
   
   report = obj.Run()
   return (report) 

endmacro

macro "ReportTripGeneration"  (Args)
//
   folder = Args.[OutputFolder] + "\\_summary\\trips\\"

   tp_summ = folder + 'trip_production_summary.csv'
   ta_summ = folder + 'trip_attraction_summary.csv'
   tg_summ = folder + 'trip_generation_summary.csv'

   obj = CreateObject("Flowchart.Reports.Table", { Title: "Trip Generation"})
   obj.AddTable({Name: tp_summ, Title: "Trip Production Rates", CompleteTable: true})
   obj.AddTable({Name: ta_summ, Title: "Trip Attraction Rates", CompleteTable: true})
   obj.AddTable({Name: tg_summ, Title: "Trip Generation Summary", CompleteTable: true})
   
   report = obj.Run()
   return (report) 

endmacro

macro "ReportZonalDataInit"  (Args)
//
   folder = Args.[OutputFolder] + "\\_summary\\zonal\\"
   per_summ = folder + 'person_table_summary.csv'
   hh_summ = folder + 'household_table_summary.csv'
   emp_summ = folder + 'block_sed_summary.csv'
   enr_summ = folder + 'enrollment_summary.csv'
   park_summ = folder + 'parking_summary.csv'
   accden_summ = folder + 'access_density_summary.csv'

   obj = CreateObject("Flowchart.Reports.Table", { Title: "Zonal Data"})
   obj.AddTable({Name: hh_summ, Title: "Households", CompleteTable: true})
   obj.AddTable({Name: per_summ, Title: "Persons", CompleteTable: true})
   obj.AddTable({Name: emp_summ, Title: "Employment", CompleteTable: true})
   obj.AddTable({Name: enr_summ, Title: "Enrollment", CompleteTable: true})
   obj.AddTable({Name: park_summ, Title: "Parking", CompleteTable: true})
   obj.AddTable({Name: accden_summ, Title: "Access Density", CompleteTable: true})   
   
   report = obj.Run()
   return (report) 

endmacro

macro "ReportModeChoice" (Args)
//
   purposes = Args.[Trip Purp]
   purp_mode = Args.[Purpose Segments]
   periods = {"pk", "np"}

   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)   

   obj = CreateObject("Flowchart.Reports.ModeChoice", { Title: "Mode Choice Report"})
   ok = 1

   ref_purp = purposes[1]
   ref_seg = ParseString(purp_mode.(ref_purp).Segments, ",")

   tags = runmacro("get_pa_file_tag", ref_purp, "pk", ref_seg[1])
   ref_mat = runmacro("get_segment_mc_trip_file",Args.OutputFolder, tags)   
   ref_mobj = CreateObject("Matrix", ref_mat)

   sum_tag = runmacro("get_pa_file_tag", "hhpurp", "daily",)
   sum_file = runmacro("get_segment_mc_trip_file",Args.OutputFolder, sum_tag)   

   sum_mobj = ref_mobj.CopyStructure({FileName: sum_file, 
                                          Label: "all trips", 
                                          Cores: {"da","s2","s3","ta","tw","bk","wk","rs","sb"}})     
   sum_mc = sum_mobj.GetCores()      

   for purp in purposes do    
      segments = ParseString(purp_mode.(purp).Segments, ",")
      modes = ParseString(purp_mode.(purp).Modes, ",")
         
      for per in periods do 
         for seg in segments do 
            seg_tag = runmacro("get_pa_file_tag", purp, per, seg)
            seg_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, seg_tag)
            seg_mobj = CreateObject("Matrix", seg_file)
            seg_mc = seg_mobj.GetCores()

            for m in modes do
               sum_mc.(m) := nz(sum_mc.(m)) + nz(seg_mc.(m)) 
            end
            if ({"INFO", "DEBUG"} contains Args.loglevel) then do 
               obj.AddSegment({Title: seg_tag, MatrixFile: seg_file}) 
            end
         end // segments
      end // periods
   end //purposes

   obj.AddSegment({Title: "All Household Trips", MatrixFile: sum_file}) 

   // HBU - TODO: move to separate report
   if ({"FULL", "DEBUG"} contains Args.loglevel) then do 
      purp = "hbu"
      segments = ParseString(purp_mode.(purp).Segments, ",")
      modes = ParseString(purp_mode.(purp).Modes, ",")
         
      for per in periods do 
         for seg in segments do 
            seg_tag = runmacro("get_pa_file_tag", purp, per, seg)
            seg_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, seg_tag)
            seg_mobj = CreateObject("Matrix", seg_file)
            seg_mc = seg_mobj.GetCores()

            obj.AddSegment({Title: seg_tag, MatrixFile: seg_file}) 
         end // segments
      end // periods
   end

   // Airport - TODO: move to separate report
   if ({"FULL", "DEBUG"} contains Args.loglevel) then do 
      purp = "air"
      segments = ParseString(purp_mode.(purp).Segments, ",")
      modes = ParseString(purp_mode.(purp).Modes, ",")
         
      for per in periods do 
         for seg in segments do 
            seg_tag = runmacro("get_pa_file_tag", purp, per, seg)
            seg_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, seg_tag)
            seg_mobj = CreateObject("Matrix", seg_file)
            seg_mc = seg_mobj.GetCores()

            obj.AddSegment({Title: seg_tag, MatrixFile: seg_file}) 
         end // segments
      end // periods
   end

   report = obj.Run()
   return (report) 

endmacro 

macro "ReportRideSource" (Args)
//
   purposes = Args.[Trip Purp]
   periods = {"pk", "np"}

   obj = CreateObject("Flowchart.Reports.TLD", { Title: "Ride Source Non-Revenue Report"})

   rev_file = runmacro("get_rs_od_file", Args.OutputFolder, "rev")              
   nrev_file = runmacro("get_rs_od_file", Args.OutputFolder, "nonrev")    
   skim = {MatrixFileName: Args.("HighwaySkims - am"), Matrix: "dist", RowIndex: "Origin", ColIndex: "Destination"}

   obj.AddReport({Title: "Revenue Vehicle Trips", TripMatrix: rev_file, TripCores: null, TimeMatrix: skim}) 
   obj.AddReport({Title: "NonRevenue Vehicle Trips", TripMatrix: nrev_file, TripCores: null, TimeMatrix: skim}) 

   report = obj.Run()
   return (report) 
endmacro 

macro "ReportTimeofDay" (Args)
//

   per_summ_file = Args.OutputFolder + "\\_summary\\trips\\per_trips_daily.mtx"
   veh_summ_file = Args.OutputFolder + "\\_summary\\trips\\veh_trips_daily.mtx"

   // if summary tables not created, generate them
   if GetFileInfo(per_summ_file) = null then runmacro("aggregate_summary_trip_tables", Args)

   obj = CreateObject("Flowchart.Reports.ModeChoice", { Title: "Time of Day Report"})

   obj.AddSegment({Title: "Daily Vehicle Trips", MatrixFile: veh_summ_file}) 
   obj.AddSegment({Title: "Daily Person Trips", MatrixFile: per_summ_file})    
   
   if ({"FULL", "DEBUG"} contains Args.loglevel) then do   

      for tod in Args.[TimePeriods] do 
         obj.AddSegment({Title: tod + " Vehicle Trips", MatrixFile: Args.("Veh Trips - " + tod)}) 
         obj.AddSegment({Title: tod + " Person Trips", MatrixFile: Args.("Per Trips - " + tod)}) 
      end
   end

    report = obj.Run()
    return (report) 
endmacro 


macro "ReportTripDistribution" (Args)
//
   purposes = Args.[Trip Purp]
   periods = {"pk", "np"}

   obj = CreateObject("Flowchart.Reports.TLD", { Title: "Trip Distribution Report"})

   sum_tag = runmacro("get_pa_file_tag", "all", "all",)
   sum_file = runmacro("get_segment_pa_file", Args.OutputFolder, sum_tag)  
   skim = {MatrixFileName: Args.("HighwaySkims - am"), Matrix: "dist", RowIndex: "Origin", ColIndex: "Destination"}
   obj.AddReport({Title: "Trip Distribution", TripMatrix: sum_file, TripCores: null, TimeMatrix: skim}) 
   
   if ({"FULL", "DEBUG"} contains Args.loglevel) then do   
      for purp in purposes do           
         for per in periods do   
            tag = runmacro("get_pa_file_tag", purp, per,)
            pa_mat = runmacro("get_segment_pa_file",Args.OutputFolder, tag)  

            if per = "pk" then tod = "am" else tod = "md" 
            skim = {MatrixFileName: Args.("HighwaySkims - " + tod), Matrix: "dist", RowIndex: "Origin", ColIndex: "Destination"}
            obj.AddReport({Title: purp + " " + per, TripMatrix: pa_mat, TripCores: null, TimeMatrix: skim}) 
         end
      end
   end
      
   report = obj.Run()
   return (report) 
endmacro 

macro "ReportCommercialVehicles" (Args)
//
   purp_segs = Args.[Purpose Segments]
   trk_classes = ParseString(purp_segs.("trk").Modes, ",")

   obj = CreateObject("Flowchart.Reports.TLD", { Title: "Truck Distribution Report"})

   comb_mat_file = runmacro("get_segment_pa_file", Args.OutputFolder, "trk_daily")
   skim = {MatrixFileName: Args.("HighwaySkims - am"), Matrix: "dist", RowIndex: "Origin", ColIndex: "Destination"}
   obj.AddReport({Title: "Truck Trips", TripMatrix: comb_mat_file, TripCores: trk_classes, TimeMatrix: skim}) 

   report = obj.Run()
   return (report) 
endmacro 


macro "ReportExternalTravel" (Args)
//
   purp_segs = Args.[Purpose Segments]
   modes = ParseString(purp_segs.("ext").Modes, ",")

   obj = CreateObject("Flowchart.Reports.TLD", { Title: "External Distribution Report"})

   comb_mat_file = runmacro("get_segment_pa_file", Args.OutputFolder, "ext_daily")
   skim = {MatrixFileName: Args.("HighwaySkims - am"), Matrix: "dist", RowIndex: "Origin", ColIndex: "Destination"}
   obj.AddReport({Title: "External Trips", TripMatrix: comb_mat_file, TripCores: modes, TimeMatrix: skim}) 

   report = obj.Run()
   return (report) 
endmacro 

macro "ReportSpecialGenerators" (Args)
//
   purp_segs = Args.[Purpose Segments]
   segments = ParseString(purp_segs.("spcgen").Segments, ",")
   periods = {"pk", "np"}

   obj = CreateObject("Flowchart.Reports.TLD", { Title: "Special Generator Distribution Report"})

   for per in periods do 
      comb_mat_file = runmacro("get_segment_pa_file", Args.OutputFolder, "spcgen_" + per)
      skim = {MatrixFileName: Args.("HighwaySkims - am"), Matrix: "dist", RowIndex: "Origin", ColIndex: "Destination"}
      obj.AddReport({Title: "Special Generators " + per, TripMatrix: comb_mat_file, TripCores: segments, TimeMatrix: skim}) 
   end

   report = obj.Run()
   return (report) 
endmacro 

macro "ReportAirportGroundAccess" (Args)
//
   purp_segs = Args.[Purpose Segments]
   segments = ParseString(purp_segs.("air").Segments, ",")
   periods = {"pk", "np"}

   obj = CreateObject("Flowchart.Reports.TLD", { Title: "Airport Ground Access Distribution Report"})

   for per in periods do 
      comb_mat_file = runmacro("get_segment_pa_file", Args.OutputFolder, "air_" + per)
      skim = {MatrixFileName: Args.("HighwaySkims - am"), Matrix: "dist", RowIndex: "Origin", ColIndex: "Destination"}
      obj.AddReport({Title: "Airport Ground Access " + per, TripMatrix: comb_mat_file, TripCores: segments, TimeMatrix: skim}) 
   end

   report = obj.Run()
   return (report) 
endmacro 


macro "ReportHBU" (Args)
//
   purp_segs = Args.[Purpose Segments]
   segments = ParseString(purp_segs.("hbu").Segments, ",")
   periods = {"pk", "np"}

   obj = CreateObject("Flowchart.Reports.TLD", { Title: "HBU Distribution Report"})

   for per in periods do 
      comb_mat_file = runmacro("get_segment_pa_file", Args.OutputFolder, "hbu_" + per)
      skim = {MatrixFileName: Args.("HighwaySkims - am"), Matrix: "dist", RowIndex: "Origin", ColIndex: "Destination"}
      obj.AddReport({Title: "Home Based University " + per, TripMatrix: comb_mat_file, TripCores: segments, TimeMatrix: skim}) 
   end

   report = obj.Run()
   return (report) 
endmacro 

macro "ReportAMMDHighwayAssignment" (Args)
//
    o = CreateObject("Flowchart.Reports.Assignment",  {Title: "Assignments by Facility Type"})
    // o.LineData({LineDB: Args.HWYDB, LineAttributes: {TableName, "ID"}}) 
    o.LineData({LineDB: Args.[Highway]}) 
    o.AddAssignClass({  Title: "AM Peak Assignment by Facility Type", 
                        FlowTable: { Args.[HighwayFlows - am] , "ID1"}, 
                        Flow: { "AB_Flow", "BA_Flow" }, 
                        FreeTime: "ff_time", 
                        LoadedTime: {"AB_Time", "BA_Time"}, 
                        GroupBy: "fac_type"})            
    o.AddAssignClass({  Title: "Midday Assignment by Facility Type", 
                        FlowTable: { Args.[HighwayFlows - md] , "ID1"}, 
                        Flow: { "AB_Flow", "BA_Flow" }, 
                        FreeTime: "ff_time", 
                        LoadedTime: {"AB_Time", "BA_Time"}, 
                        GroupBy: "fac_type"})                                            
    report = o.Run()
    return(report)
endmacro 

macro "ReportPMNTHighwayAssignment" (Args)
//
    o = CreateObject("Flowchart.Reports.Assignment",  {Title: "Assignments by Facility Type"})
    // o.LineData({LineDB: Args.HWYDB, LineAttributes: {TableName, "ID"}}) 
    o.LineData({LineDB: Args.[Highway]}) 
    o.AddAssignClass({  Title: "PM Peak Assignment by Facility Type", 
                        FlowTable: { Args.[HighwayFlows - pm] , "ID1"}, 
                        Flow: { "AB_Flow", "BA_Flow" }, 
                        FreeTime: "ff_time", 
                        LoadedTime: {"AB_Time", "BA_Time"}, 
                        GroupBy: "fac_type"})            
    o.AddAssignClass({  Title: "Overnight Assignment by Facility Type", 
                        FlowTable: { Args.[HighwayFlows - nt] , "ID1"}, 
                        Flow: { "AB_Flow", "BA_Flow" }, 
                        FreeTime: "ff_time", 
                        LoadedTime: {"AB_Time", "BA_Time"}, 
                        GroupBy: "fac_type"})                                            
    report = o.Run()
    return(report)
endmacro 

macro "TransitAssignmentReport" (Args)
//

   // Summarize all assignments
   obj = CreateObject("Table", {FileName: Args.TransitSummaries})
   view = obj.GetView()
   rec.Description = "Totals"
   rec.AssignedDemand = obj.Statistics({FieldName: "AssignedDemand"})
   rec.NotAssignedDemand = obj.Statistics({FieldName: "NotAssignedDemand"})
   rec.PersonDistancePT = obj.Statistics({FieldName: "PersonDistancePT"})
   rec.PersonDistanceWalk = obj.Statistics({FieldName: "PersonDistanceWalk"})
   rec.PersonTimeWalk = obj.Statistics({FieldName: "PersonTimeWalk"})
   rec.PersonTimePT = obj.Statistics({FieldName: "PersonTimePT"})
   rec.MaxRidership = obj.Statistics({FieldName: "MaxRidership", Method: "Max"})
   AddRecord(view, rec)
   obj = null

   o = CreateObject("Flowchart.Reports.Table",  {Title:  "Transit Assignment Results"})
   o.AddTable({ Title: "PT Assignment", Name: Args.TransitSummaries, CompleteTable: true})
   report = o.Run()
   return(report)
endmacro
