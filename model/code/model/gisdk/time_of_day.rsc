
//#   "time_of_day.rsc"
macro "Household TOD"  (Args)
// Convert household trips from PA to OD by TOD
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   purposes = Args.[Trip Purp]
   periods = {"pk", "np"}
   per_tod = {"pk": {"am","pm"}, "np": {"md","nt"}}
   purp_mode = Args.[Purpose Segments]

   //ShowArray({GetNumMatrixThreads()})
   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)

   mobj = CreateObject("Caliper.Matrix")

   for purp in purposes do
      modes = ParseString(purp_mode.(purp).Modes, ",")
      segments = ParseString(purp_mode.(purp).Segments, ",")

      for per in periods do 
         tod_factors = Args.("Time of Day Factors - " + per)

         // same tod factors apply across all segments, combine trips first
         seg_tag = runmacro("get_pa_file_tag", purp, per, segments[1])          
         seg_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, seg_tag)
         comb_mat = mobj.CloneMatrixStructure({MatrixLabel: purp, 
                                                CloneSource: seg_file, 
                                                MemoryOnly: true })
         comb_mobj = CreateObject("Matrix", comb_mat)
         comb_mc = comb_mobj.GetCores()

         // combine all segments
         for seg in segments do
            seg_tag = runmacro("get_pa_file_tag", purp, per, seg)
            seg_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, seg_tag)
            seg_mobj = CreateObject("Matrix", seg_file)
            seg_mc = seg_mobj.GetCores()

            for m in modes do
               comb_mc.(m) := nz(comb_mc.(m)) + nz(seg_mc.(m))
            end
         end

         // apply tod factors      
         for tod in per_tod.(per) do 
            runmacro("pa_to_od", Args, comb_mobj, seg_file, purp, modes, tod_factors, tod)
         end
      end // periods
   end // purposes

   Return(ok)
endmacro

macro "HBU TOD"  (Args)
// Convert hbu from PA to OD by TOD
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   periods = {"pk", "np"}
   per_tod = {"pk": {"am","pm"}, "np": {"md","nt"}}
   purp_mode = Args.[Purpose Segments]
   purp = "hbu"

   //ShowArray({GetNumMatrixThreads()})
   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)

   mobj = CreateObject("Caliper.Matrix")

   modes = ParseString(purp_mode.(purp).Modes, ",")
   segments = ParseString(purp_mode.(purp).Segments, ",")

   for per in periods do 
      tod_factors = Args.("hbu_tod_" + per)

      // Only one segment, to support more will need to combine or treat like airport trips
      for seg in segments do
         seg_tag = runmacro("get_pa_file_tag", purp, per, seg)
         seg_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, seg_tag)
         seg_mobj = CreateObject("Matrix", seg_file)

         // apply tod factors      
         for tod in per_tod.(per) do 
            runmacro("pa_to_od", Args, seg_mobj, seg_file, purp, modes, tod_factors, tod)
         end
      end // segments
   end // periods

   Return(ok)
endmacro

macro "Airport TOD"  (Args)
// Convert airport ground access trips from PA to OD by TOD
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   periods = {"pk", "np"}
   per_tod = {"pk": {"am","pm"}, "np": {"md","nt"}}
   purp_mode = Args.[Purpose Segments]
   purp = "air"

   //ShowArray({GetNumMatrixThreads()})
   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)

   mobj = CreateObject("Caliper.Matrix")

   // logan express is assigned in pa format, but converted to OD anyway for reporting
   modes = ParseString(purp_mode.(purp).Modes, ",")
   segments = ParseString(purp_mode.(purp).Segments, ",")

   for per in periods do 
      tod_factors = Args.("air_tod_" + per)

      // unlike hh trips, the airport segments have distinct tod factors
      for seg in segments do
         seg_tag = runmacro("get_pa_file_tag", purp, per, seg)
         seg_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, seg_tag)
         seg_mobj = CreateObject("Matrix", seg_file)

         // apply tod factors      
         for tod in per_tod.(per) do 
            runmacro("pa_to_od", Args, seg_mobj, seg_file, purp + "_" + seg, modes, tod_factors, tod)
            runmacro("airport_pickup_deadhead", Args, purp, seg, tod)
         end
      end // segments
   end // periods

   Return(ok)
endmacro

macro "airport_pickup_deadhead" (Args, purp, seg, tod)
//
   // create table of deadhead trip to pick up / drop-off airport passenger
   pu_occ = Args.Airport_Occupancies.(seg).[PU Occupancy]
   tags = runmacro("get_od_file_tag", purp + "_" + seg, tod)
   od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)
   od_mobj = CreateObject("Matrix",od_file)

   od_mobj.AddCores({"pu_dh"})
   od_t_mobj = od_mobj.Transpose()
   od_mc = od_mobj.GetCore("pu_dh")
   od_t_mc = od_t_mobj.GetCore("pu")

   od_mc := od_t_mc / pu_occ

endmacro

macro "pa_to_od" (Args, pa_mobj, ref_mat_file, purp, modes, tod_factors, tod)
//

   tags = runmacro("get_od_file_tag", purp, tod)
   od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)
   ta_modes = null

   // if transit-auto is a mode, need to record as access / egress
   if modes contains "ta" then do
      simple_modes = ArrayExclude(modes,{"ta"})
      ta_modes = {"ta_acc","ta_egr"}
   end
   else do 
      simple_modes = modes
   end
   
   // create output od and transposed pa matrix objects
   ref_mobj = CreateObject("Matrix", ref_mat_file)
   
   od_mobj = ref_mobj.CopyStructure({FileName: od_file, 
                                          Cores: simple_modes + ta_modes, 
                                          Label: "od_trips_" + tags},{FileName: ref_mat_file})
   
   /* TODO: replace with memory only once bug is fixed
   od_mobj = pa_mobj.CopyStructure({FileName: od_file, 
                                          Cores: simple_modes + ta_modes, 
                                          Label: "od_trips_" + tags})                                             
   */                                          
   pa_t_mobj = pa_mobj.Transpose()

   // production to attraction 
   tod_pa = tod_factors.(tod + "-pa").(purp)
   tod_ap = tod_factors.(tod + "-ap").(purp)

   runmacro("apply_tod_fact", pa_mobj, od_mobj, tod_pa, simple_modes)
   runmacro("apply_tod_fact", pa_t_mobj, od_mobj, tod_ap, simple_modes)

   if modes contains "ta" then do
      // transit auto
      acc_mc = od_mobj.GetCore("ta_acc")
      ta_mc = pa_mobj.GetCore("ta")
      acc_mc := ta_mc * tod_pa  
   
      egr_mc = od_mobj.GetCore("ta_egr")
      ta_t_mc = pa_t_mobj.GetCore("ta")
      egr_mc := ta_t_mc * tod_ap  
   end

endmacro

macro "apply_tod_fact" (pa_mobj, od_mobj, fact, modes)
//

   od_mc = od_mobj.GetCores()
   pa_mc = pa_mobj.GetCores()

   for m in modes do 
      od_mc.(m) := nz(od_mc.(m)) + nz(pa_mc.(m)) * fact
   end // modes
endmacro

macro "Truck TOD"  (Args)
// Convert truck trips from PA to OD by TOD
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   purp_segs = Args.[Purpose Segments]
   trk_classes = ParseString(purp_segs.("trk").Modes, ",")

   runmacro("daily_pa_to_od", Args, "trk", trk_classes)

   Return(ok)
endmacro

macro "External TOD"  (Args)
// Convert external trips from PA to OD by TOD
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   purp_segs = Args.[Purpose Segments]
   modes = ParseString(purp_segs.("ext").Modes, ",")

   runmacro("daily_pa_to_od", Args, "ext", modes)

   Return(ok)
endmacro

macro "daily_pa_to_od" (Args, purp, modes)
// Convert pa trips from PA to OD by TOD

   tod_factors = Args.[Time of Day Factors - Daily]
   periods = Args.TimePeriods
   tpurp = purp + "_daily" 

   pa_file = runmacro("get_segment_pa_file", Args.OutputFolder, tpurp)
   pa_mobj = CreateObject("Matrix", pa_file)
   pa_t_mobj = pa_mobj.Transpose()   
   pa_mc = pa_mobj.GetCores()
   pa_t_mc = pa_t_mobj.GetCores()

   for tod in periods do 
      tags = runmacro("get_od_file_tag", purp, tod)
      od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)

      od_mobj = pa_mobj.CopyStructure({FileName: od_file, 
                                    Cores: modes, 
                                    Label: "od_trips_" + tags})
      od_mc = od_mobj.GetCores()

      for md in modes do 
         // production to attraction 
         tod_pa = tod_factors.(tod + "-pa").(md)
         tod_ap = tod_factors.(tod + "-ap").(md)
         od_mc.(md) := nz(od_mc.(md)) + nz(pa_mc.(md)) * tod_pa + nz(pa_t_mc.(md)) * tod_ap
      end // modes
   end //tod

   Return(ok)
endmacro

macro "Person and Vehicle Trips"  (Args)
// Assemble OD trips into vehicle and person trip tables for assignment
   if Args.DryRun = 1 then Return(1)   
   ok = 1

   time_periods = Args.[TimePeriods]
   purposes = Args.[Trip Purp]
   purp_mode = Args.[Purpose Segments]

   veh_cores = {"da","sr","ltrk","mtrk","htrk"}
   per_cores = {"auto","wk","bk","ta_acc","ta_egr","tw", "sb", "lx"}

   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)   

   // matrix structure - initialize and hand off so we can delete the hbw matrix
   tags = runmacro("get_od_file_tag", "hbw", "am")
   ref_mat = runmacro("get_od_trip_file",Args.OutputFolder, tags)      
   per_mobj = CreateObject("Matrix", ref_mat)          

   for tod in time_periods do 

      veh_mobj = null
      veh_mobj = per_mobj.CopyStructure({FileName: Args.("Veh Trips - " + tod), 
                                          Label: "veh_trips_" + tod, 
                                          Cores: veh_cores})    
      per_mobj = null
      per_mobj = veh_mobj.CopyStructure({FileName: Args.("Per Trips - " + tod), 
                                          Label: "per_trips_" + tod, 
                                          Cores: per_cores})                                                                                   

      per_mc = per_mobj.GetCores()
      veh_mc = veh_mobj.GetCores()

      // Person and Vehicle Trips
      // household trips by purpose
      for purp in purposes do
         modes = ParseString(purp_mode.(purp).Modes, ",")
         runmacro("add_household_trips_for_assignment", Args, per_mc, veh_mc, purp, modes, tod)   
      end 

      // hbu
      hbu_modes = ParseString(purp_mode.("hbu").Modes, ",")
      runmacro("add_household_trips_for_assignment", Args, per_mc, veh_mc, "hbu", hbu_modes, tod)        
      
      // Airport ground access trips
      air_purps = ParseString(purp_mode.("air").Segments, ",") 
      for purp in air_purps do
         runmacro("add_airport_trips_for_assignment", Args, per_mc, veh_mc, purp, tod)       
      end // airport trips  

      // Vehicle Trips Only
      // External trips
      runmacro("add_external_trips_for_assignment", Args, veh_mc, tod)    

      // Commercial vehicle trips
      runmacro("add_truck_trips_for_assignment", Args, veh_mc, tod)    

      // ride sourcing
      runmacro("add_rs_trips_for_assignment", Args, veh_mc, tod) 

   end // time periods

   Return(ok)
endmacro

macro "add_household_trips_for_assignment" (Args, per_mc, veh_mc, purp, modes, tod)
// Combine modes from household purposes into assignment tables
   s2_occ = Args.HH_Occupancies.(purp).[SR2 Occupancy]
   s3p_occ = Args.HH_Occupancies.(purp).[SR3p Occupancy]

   tags = runmacro("get_od_file_tag", purp, tod)
   od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)
   od_mobj = CreateObject("Matrix", od_file)
   od_mc = od_mobj.GetCores()

   // vehicles
   veh_mc.da := nz(veh_mc.da) + nz(od_mc.da)
   veh_mc.sr := nz(veh_mc.sr) + nz(od_mc.s2) / s2_occ + nz(od_mc.s3) / s3p_occ
   
   // persons
   per_mc.auto := nz(per_mc.auto) + nz(od_mc.da) + nz(od_mc.s2) + nz(od_mc.s3)
   per_mc.wk := nz(per_mc.wk) + nz(od_mc.wk)
   per_mc.bk := nz(per_mc.bk) + nz(od_mc.bk)
   per_mc.tw := nz(per_mc.tw) + nz(od_mc.tw)

   // not all purposes have rs, ta or sb modes
   if modes contains "ta" then do
      per_mc.ta_acc := nz(per_mc.ta_acc) + nz(od_mc.ta_acc)
      per_mc.ta_egr := nz(per_mc.ta_egr) + nz(od_mc.ta_egr)
   end

   if modes contains "sb" then do
      per_mc.sb := nz(per_mc.sb) + nz(od_mc.sb)
   end

   // only need to record person RS trips, vehicles are compiled in non-revenue distribution
   if modes contains "rs" then do
      per_mc.auto := nz(per_mc.auto) + nz(od_mc.rs)
   end  

   od_mobj = null
   od_mc = null 
   if ({"STANDARD","LEAN"} contains Args.loglevel) then do 
      DeleteFile(od_file)
   end

endmacro

macro "add_airport_trips_for_assignment" (Args, per_mc, veh_mc, seg, tod)
// Combine modes from airport trips into assignment tables

   dp_occ = Args.Airport_Occupancies.(seg).[DP Occupancy]
   rc_occ = Args.Airport_Occupancies.(seg).[RC Occupancy]
   pu_occ = Args.Airport_Occupancies.(seg).[PU Occupancy]

   tags = runmacro("get_od_file_tag", "air_" + seg, tod)
   od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)
   od_mobj = CreateObject("Matrix", od_file)
   od_t_mobj = od_mobj.Transpose() 
   od_mc = od_mobj.GetCores()

   // vehicles
   veh_mc.sr := nz(veh_mc.sr) + nz(od_mc.dp) / dp_occ + nz(od_mc.rc) / rc_occ + nz(od_mc.pu) / pu_occ

   // pick-up deadheading trips are assumed to be drive-alone
   veh_mc.da := nz(veh_mc.da) + nz(od_mc.pu_dh)
   
   // persons
   per_mc.auto := nz(per_mc.auto) + nz(od_mc.dp) + nz(od_mc.rc) + nz(od_mc.pu) + nz(od_mc.rs)
   per_mc.tw := nz(per_mc.tw) + nz(od_mc.tw)

   // transit access and egress
   per_mc.ta_acc := nz(per_mc.ta_acc) + nz(od_mc.ta_acc)
   per_mc.ta_egr := nz(per_mc.ta_egr) + nz(od_mc.ta_egr)

   // logan express
   per_mc.lx := nz(per_mc.lx) + nz(od_mc.lx)   

   od_mobj = null
   od_mc = null
   if ({"STANDARD","LEAN"} contains Args.loglevel) then do 
      DeleteFile(od_file)
   end   

endmacro

macro "add_truck_trips_for_assignment" (Args, veh_mc, tod)
// Combine modes from truck trips into assignment tables
   purp_mode = Args.[Purpose Segments]
   trk_classes = ParseString(purp_mode.("trk").Modes, ",")    
   tags = runmacro("get_od_file_tag", "trk", tod)
   od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)
   od_mobj = CreateObject("Matrix", od_file)
   od_mc = od_mobj.GetCores()

   for trk in trk_classes do 
      veh_mc.(trk) := nz(veh_mc.(trk)) + nz(od_mc.(trk))
   end

endmacro

macro "add_external_trips_for_assignment" (Args, veh_mc, tod)
// Combine modes from external trips into assignment tables
   purp_mode = Args.[Purpose Segments]
   ext_classes = ParseString(purp_mode.("ext").Modes, ",")    
   tags = runmacro("get_od_file_tag", "ext", tod)
   od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)
   od_mobj = CreateObject("Matrix", od_file)
   od_mc = od_mobj.GetCores()      
   for ext in ext_classes do 
      if ext = "auto" then do
         veh_mc.da := nz(veh_mc.da) + nz(od_mc.(ext))
      end
      else veh_mc.(ext) := nz(veh_mc.(ext)) + nz(od_mc.(ext))
   end   

endmacro

macro "add_rs_trips_for_assignment" (Args, veh_mc, tod)
// Combine ride sourcing revenue and non-revenue trips into assignment tables

   // TNC vehicle trips (revenue and non-revenue)
   rs_rev_file = runmacro("get_rs_od_file", Args.OutputFolder, "rev")              
   rs_nrev_file = runmacro("get_rs_od_file", Args.OutputFolder, "nonrev")                       
   rs_rev_mobj = CreateObject("Matrix", rs_rev_file)
   rs_nrev_mobj = CreateObject("Matrix", rs_nrev_file)
   rs_rev_mc = rs_rev_mobj.GetCores()
   rs_nrev_mc = rs_nrev_mobj.GetCores()    

   //revenue to shared ride, non-revenue to DA
   veh_mc.da := nz(veh_mc.da) + nz(rs_nrev_mc.(tod))
   veh_mc.sr := nz(veh_mc.sr) + nz(rs_rev_mc.(tod))

endmacro

macro "Prepare LX Assignment" (Args)
// Helper function to build logan express trips for assignment
   ok = 1
   if Args.DryRun = 1 then Return(1)      

   purp_mode = Args.[Purpose Segments]
   purp = "air"
   periods = {"pk", "np"}
   
   segments = ParseString(purp_mode.(purp).Segments, ",")   

   ref_tag = runmacro("get_pa_file_tag", purp, "pk", segments[1])          
   ref_mat = runmacro("get_segment_mc_trip_file", Args.OutputFolder, ref_tag)
   ref_mobj = CreateObject("Matrix", ref_mat)      

   for per in periods do 
      // single assignment per period
      lx_tag = runmacro("get_pa_file_tag", purp, per, "lx")          
      lx_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, lx_tag)      
      lx_mobj = ref_mobj.CopyStructure({FileName: lx_file, 
                                       Label: "LoganExpress-" + per, 
                                       Cores: {"lx"}})              
      lx_mc = lx_mobj.GetCores()

      // combine all segments
      for seg in segments do
         seg_tag = runmacro("get_pa_file_tag", purp, per, seg)
         seg_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, seg_tag)
         seg_mobj = CreateObject("Matrix", seg_file)
         seg_mc = seg_mobj.GetCores()

         lx_mc.lx := nz(lx_mc.lx) + nz(seg_mc.lx)
      end
   end

   return(ok)
endmacro


macro "Summarize TOD" (Args)
// Assemble OD trips into vehicle and person trip tables for assignment
   out_dir = Args.OutputFolder
   time_periods = Args.[TimePeriods]

   if Args.DryRun = 1 then Return(1)   
   ok = 1   

   tod_rep_file = out_dir + "\\_summary\\trips\\tod_summ.json"
   tod_rep_arr = {}   

   for tod in time_periods do 

      veh_mobj = CreateObject("Matrix", Args.("Veh Trips - " + tod)) 
      runmacro("summarize_tod", veh_mobj, "veh", tod_rep_arr, tod)

      per_mobj = CreateObject("Matrix", Args.("Per Trips - " + tod)) 
      runmacro("summarize_tod", per_mobj, "per", tod_rep_arr, tod)
   end

   runmacro("export_arr_to_json",tod_rep_arr, tod_rep_file)

   return(ok)

endmacro

macro "summarize_tod" (mobj, type, tod_rep_arr, tod)
//

   cores = mobj.GetCoreNames()
   for core in cores do
      stats = mobj.GetMatrixStatistics(core)
      tod_rep_arr.(tod).(type).(core) = stats.Sum
   end

endmacro

macro "aggregate_summary_trip_tables"  (Args)
// Aggregate OD trip tables for reports
    ok = 1

    time_periods = Args.[TimePeriods]
    out_dir = Args.OutputFolder

    thr_status = CreateObject("Utils.Currency")
    SetNumMatrixThreads(Args.MatrixThreads)    

    veh_cores = {"auto","mtrk","htrk","all","da","sr"}
    per_cores = {"auto","nonm","trn","sb"}
    veh_core_match = {{"da","sr","ltrk","mtrk","htrk"},
                      {"da","sr","auto","mtrk","htrk"}}
    per_core_match = {{"auto",  "wk",  "bk","ta_acc","ta_egr","tw", "sb", "lx"},
                      {"auto","nonm","nonm", "trn",    "trn",  "trn", "sb", "trn"}}

    // matrix structure 
    ref_mat_obj = CreateObject("Matrix", Args.("Veh Trips - am"))

    // daily trip tables
    per_summ_file = Args.OutputFolder + "\\_summary\\trips\\per_trips_daily.mtx"
    veh_summ_file = Args.OutputFolder + "\\_summary\\trips\\veh_trips_daily.mtx"

    per_summ_mobj = ref_mat_obj.CopyStructure({FileName: per_summ_file, 
                                                Cores: per_cores, 
                                                Label: "person_trips"})
    veh_summ_mobj = ref_mat_obj.CopyStructure({FileName: veh_summ_file, 
                                                Cores: veh_cores, 
                                                Label: "vehicle_trips"})

    for tod in time_periods do 
        veh_mobj = CreateObject("Matrix", Args.("Veh Trips - " + tod))
        per_mobj = CreateObject("Matrix", Args.("Per Trips - " + tod))

        for i = 1 to veh_core_match[1].length do 
            sourceCore = veh_mobj.GetCore(veh_core_match[1][i])
            destCore = veh_summ_mobj.GetCore(veh_core_match[2][i])
            destCore := nz(destCore) + nz(sourceCore)
        end

        for i = 1 to per_core_match[1].length do 
            sourceCore = per_mobj.GetCore(per_core_match[1][i])
            destCore = per_summ_mobj.GetCore(per_core_match[2][i])
            destCore := nz(destCore) + nz(sourceCore)
        end
    end

   // sum all trips for reports
   per_summ_mobj.Sum("all")
   veh_summ_mobj.Sum("all")

   // populate auto core with da and sr
   veh_mc = veh_summ_mobj.GetCores()
   veh_mc.auto := nz(veh_mc.da) + nz(veh_mc.sr) + nz(veh_mc.auto)

   // summarize taz origins
   runmacro("calculate_trip_origins",per_summ_mobj, Args.OutputFolder + "\\_summary\\trips\\per_trip_origins.csv")
   runmacro("calculate_trip_origins",veh_summ_mobj, Args.OutputFolder + "\\_summary\\trips\\veh_trip_origins.csv")

   // aggregate to geography
   geoarray = {"state","mpo","district","town_state"}
   for geo in geoarray do    
      runmacro("aggregate_matrix_by_geography", out_dir, per_summ_mobj, "od_pers", geo)
      runmacro("aggregate_matrix_by_geography", out_dir, veh_summ_mobj, "od_veh", geo)
   end   
   Return(ok)
endmacro





 

