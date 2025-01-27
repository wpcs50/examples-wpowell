
//#   "mode_choice.rsc"
macro "Prepare Mode Choice Inputs" (Args)
// Prepare files for mode choice calculation
   if Args.DryRun = 1 then Return(1)
   ok = 1

   runmacro("assemble_mc_taz_data", Args)
   return(ok)

endmacro

macro "assemble_mc_taz_data" (Args)
// Pull and assemble TAZ data from Sqlite DB
   db_file = Args.OutputFolder + "\\tdm23.db"
   pfact = Args.Parking_Factors

   obj = CreateNativeObject("SQLite")
   obj.Connect(db_file)

   qry = "Select t.taz_id, terminal_time_p, terminal_time_a, rs_wait_time, access_density, " + 
         " cost_hr, cost_dr, cost_mr, walkability / 1000 AS int_density, 1 as cnst " + 
         " FROM terminal_times t JOIN access_density ad ON t.taz_id = ad.taz_id " + 
         "JOIN parking p ON t.taz_id = p.taz_id JOIN walkbike wb ON t.taz_id = wb.taz_id"
         
   view = obj.RetrieveQueryResults({ViewName: "taz_data", Query: qry, MaxStringLength: 30, 
                     FieldTypes: {"integer", 
                                 "float", 
                                 "float", 
                                 "float", 
                                 "integer", 
                                 "float", 
                                 "float", 
                                 "float", 
                                 "float",
                                 "integer"}})

   taz_t = CreateObject("Table",{View: "taz_data"})  
   taz_v = taz_t.GetDataVectors()                               

   // for each purpose, generate the parking cost data
   pcost_fields = {}
   for purp in pfact do 
      cost_field = purp[1] + "_pcost"
      cost_type = purp[2].[Cost Attribute]
      cost_fact = purp[2].[Factor]

      cost_v = taz_v.(cost_type) * cost_fact

      taz_t.AddField({FieldName: cost_field})
      taz_t.SetDataVectors({FieldData: {{cost_field,cost_v}}})
   end

   taz_t.Export({FileName: Args.[MC TAZ Data]})
endmacro

macro "Mode Choice Logsums"  (Args)
// Calculate logsums and shares by mode for each taz interchange
   if Args.DryRun = 1 then Return(1)
   purp_mode = Args.[Purpose Segments]
   purposes = Args.[Trip Purp]
   avail = Args.ModeChoiceAvail
   periods = {"pk", "np"}
   ok = 1

   for purp in purposes do    
      segments = ParseString(purp_mode.(purp).Segments, ",")
      nests = runmacro("get_purpose_mc_nest", Args, purp)
         
      for per in periods do 
         for seg in segments do
            ok = runmacro("calc_mode_choice", Args, nests, avail, purp, per, seg)
         end
      end
   end
   Return(ok)
endmacro

macro "Airport Mode Shares"  (Args)
// Calculate probabilities by mode for each taz interchange
   ok = 1
   if Args.DryRun = 1 then Return(1)
   purp_mode = Args.[Purpose Segments]
   periods = {"pk", "np"}
   purp = "air"
   avail = null // no non-motorized modes for airport
   segments = ParseString(purp_mode.(purp).Segments, ",")

   // prepare data (if haven't run household logsums yet)
   if (GetFileInfo(Args.[MC TAZ Data])=null) then do 
      runmacro("assemble_mc_taz_data", Args) 
   end

   // calculate mode shares     
   for per in periods do 
      for seg in segments do
         nests = runmacro("get_purpose_mc_nest", Args, purp + "_" + seg)
         ok = runmacro("calc_mode_choice", Args, nests, avail, purp + "_" + seg, per,)
      end
   end

   Return(ok)
endmacro

macro "HBU Mode Shares"  (Args)
// Calculate probabilities by mode for each taz interchange
   ok = 1
   if Args.DryRun = 1 then Return(1)
   purp_mode = Args.[Purpose Segments]
   periods = {"pk", "np"}
   purp = "hbu"
   nests = runmacro("get_purpose_mc_nest", Args, purp)
   avail = Args.ModeChoiceAvail 
   segments = ParseString(purp_mode.(purp).Segments, ",")

   // prepare data (if haven't run household logsums yet)
   if (GetFileInfo(Args.[MC TAZ Data])=null) then do 
      runmacro("assemble_mc_taz_data", Args)    
   end
         
   for per in periods do 
      for seg in segments do
         ok = runmacro("calc_mode_choice", Args, nests, avail, purp, per, seg)
      end
   end

   Return(ok)
endmacro

macro "calc_mode_choice" (Args, nests, avail, purp, per, seg)
// Calculate logsum and mode choice probabilities
   ok = 1

   tag = runmacro("get_pa_file_tag", purp, per, seg)
   if per = "pk" then tod = "am" else tod = "md"
   if seg = null then seg = "*"

   // Creating the NLM Object
   obj = CreateObject("PMEChoiceModel", {ModelName: tag})
   obj.Segment = seg 
   obj.OutputModelFile = Args.OutputFolder + "\\_demand\\mc\\modelfiles\\" + tag + ".mdl"
   substituteStrings = {{"{WLK_MPM}", string(Args.[Walk Speed] / 60)},
                        {"{BK_MPM}", string(Args.[Bike Speed] / 60)},
                        {"{WLK_DIST}", string(Args.[Walk Speed])}, // 1 hour max walk
                        {"{BK_DIST}", string(Args.[Bike Speed])},  // 1 hour max bike                      
                        {"{AOC}", string(Args.[Auto Operating Costs])},
                        {"{S2_OCC}", string(nz(Args.HH_Occupancies.(purp).[SR2 Occupancy]))},
                        {"{S3_OCC}", string(nz(Args.HH_Occupancies.(purp).[SR3p Occupancy]))},
                        {"{DP_TERM}", string(Args.[air_tt].("dp").TerminalTime)},
                        {"{RC_TERM}", string(Args.[air_tt].("rc").TerminalTime)},
                        {"{RS_TERM}", string(Args.[air_tt].("rs").TerminalTime)},
                        {"{RS_FEE}", string(Args.[rs_fee])},
                        {"{RESB_PARK}", string(Args.[airport parking].("resb").DailyCost)},
                        {"{RESL_PARK}", string(Args.[airport parking].("resl").DailyCost)}
                        }
   
   // Add model sources. The source names appaer in the utility spec (e.g. SkimDA.Time, TAZ.IntersectionDensity)
   obj.AddTableSource({SourceName: "TAZ", File: Args.[MC TAZ Data], IDField: "taz_id"})
   
   obj.AddMatrixSource({SourceName: "SkimRD", 
                        File: Args.("HighwaySkims - " + tod), 
                        RowIndex: "Origin", 
                        ColIndex: "Destination"})

   obj.AddMatrixSource({SourceName: "SkimTA", 
                        File: Args.("TransitAutoSkims - " + tod), 
                        RowIndex: "RCIndex", 
                        ColIndex: "RCIndex"})

   obj.AddMatrixSource({SourceName: "SkimTW", 
                        File: Args.("TransitWalkSkims - " + tod), 
                        RowIndex: "RCIndex", 
                        ColIndex: "RCIndex"})

   obj.AddMatrixSource({SourceName: "SkimLX", 
                        File: Args.OutputFolder + "\\_skim\\lx_" + tod + ".mtx", 
                        RowIndex: "RCIndex", 
                        ColIndex: "RCIndex"})

   obj.AddMatrixSource({SourceName: "SkimNM", 
                        File: Args.[NonMotorizedSkim], 
                        RowIndex: "Origin", 
                        ColIndex: "Destination"})
      
   obj.AddAlternatives({AlternativesTree: nests})
   obj.AddUtility({UtilityFunction: Args.(purp + "_Utility"), 
                     AvailabilityExpressions: avail,
                     SubstituteStrings: substituteStrings})
   obj.AddPrimarySpec({Name: "SkimRD"})

   // Output options
   output_opts = {Probability: runmacro("get_segment_mc_prob_file", Args.OutputFolder, tag)}

   // only need logsums for household purposes
   if (ArrayExclude(Args.[Trip Purp], {'hbsc'}) contains purp) then do 
      output_opts = output_opts + {Logsum: runmacro("get_segment_mc_logsum_file", Args.OutputFolder, tag)}
   end

   if ({"DEBUG","FULL"} contains Args.loglevel) then do 
      output_opts = output_opts + {Utility: Args.OutputFolder + "\\_demand\\mc\\Utility" + tag + ".mtx"}
   end

   obj.AddOutputSpec(output_opts)
   
   ret = obj.Evaluate()
   if !ret then
      Throw("MC model failed for: " + tag) 

   return(ok)

endmacro

macro "Household Mode Choice" (Args)
// Manage mode choice application for household trips
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp_mode = Args.[Purpose Segments]
   purposes = Args.[Trip Purp]
   periods = {"pk", "np"}
   ok = 1

   for purp in purposes do    
      segments = ParseString(purp_mode.(purp).Segments, ",")
         
      for per in periods do 
         for seg in segments do
            runmacro("apply_mode_share", Args, purp, per, seg)
         end
      end
   end


   Return(ok)

endmacro


macro "Airport Mode Choice" (Args)
// Apply mode choise shares to the person trip tables for standard trips
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp_mode = Args.[Purpose Segments]
   purp = "air"
   periods = {"pk", "np"}
   
   segments = ParseString(purp_mode.(purp).Segments, ",")
         
   for per in periods do 
      for seg in segments do
         runmacro("apply_mode_share", Args, purp, per, seg)
      end
   end

   Return(ok)

endmacro

macro "HBU Mode Choice" (Args)
// Apply mode choise shares to the person trip tables for standard trips
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp_mode = Args.[Purpose Segments]
   purp = "hbu"
   periods = {"pk", "np"}
   
   segments = ParseString(purp_mode.(purp).Segments, ",")
         
   for per in periods do 
      for seg in segments do
         runmacro("apply_mode_share", Args, purp, per, seg)
      end
   end

   Return(ok)

endmacro

macro "apply_mode_share" (Args, purp, per, seg)
// Create mode trip table for given purpose, tod, segment
   purp_mode = Args.[Purpose Segments]

   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)   

   td_tag = runmacro("get_pa_file_tag", purp, per)
   mc_tag = runmacro("get_pa_file_tag", purp, per, seg)

   // estimated shares and trip tables
   shr_file = runmacro("get_segment_mc_prob_file", Args.OutputFolder, mc_tag)
   shr_mobj = CreateObject("Matrix", shr_file)
   trip_file = runmacro("get_segment_pa_file", Args.OutputFolder, td_tag)
   trip_mobj = CreateObject("Matrix", trip_file)   

   // output trips by mode
   mode_file = runmacro("get_segment_mc_trip_file", Args.OutputFolder, mc_tag)
   mode_mobj = shr_mobj.CopyStructure({FileName: mode_file, 
                                       Label: "per_trips_" + mc_tag})                           

   mode_mc = mode_mobj.GetCores()
   shr_mc = shr_mobj.GetCores()
   trip_mc = trip_mobj.GetCores()

   modes = shr_mobj.GetCoreNames()

   for m in modes do
      mode_mc.(m) := shr_mc.(m) * trip_mc.(seg)
   end

   shr_mobj = null
   shr_mc = null
   if ({"STANDARD","LEAN"} contains Args.loglevel) then do 
      DeleteFile(shr_file)
   end
endmacro

macro "Household Mode Summaries" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purposes = Args.[Trip Purp]
   out_dir = Args.OutputFolder
   shr_arr = {}
   tld_arr = {}
   shr_file = out_dir + "\\_summary\\trips\\hh_trip_mc.json"
   tld_file = out_dir + "\\_summary\\tld\\hh_trip_mc_tld.json"     

   runmacro("mode_share_summaries", Args, purposes, shr_arr, tld_arr)
   runmacro("export_arr_to_json",shr_arr, shr_file)
   runmacro("export_arr_to_json",tld_arr, tld_file)

   Return(ok)
endmacro


macro "AGA Mode Summaries" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purposes = {"air"}
   out_dir = Args.OutputFolder
   shr_arr = {}
   tld_arr = {}
   shr_file = out_dir + "\\_summary\\trips\\air_trip_mc.json"
   tld_file = out_dir + "\\_summary\\tld\\air_trip_mc_tld.json"   

   runmacro("mode_share_summaries", Args, purposes, shr_arr, tld_arr)
   runmacro("export_arr_to_json",shr_arr, shr_file)
   runmacro("export_arr_to_json",tld_arr, tld_file)

   Return(ok)
endmacro


macro "HBU Mode Summaries" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purposes = {"hbu"}
   out_dir = Args.OutputFolder
   shr_arr = {}
   tld_arr = {}
   shr_file = out_dir + "\\_summary\\trips\\hbu_trip_mc.json"
   tld_file = out_dir + "\\_summary\\tld\\hbu_trip_mc_tld.json"

   runmacro("mode_share_summaries", Args, purposes, shr_arr, tld_arr)
   runmacro("export_arr_to_json",shr_arr, shr_file)
   runmacro("export_arr_to_json",tld_arr, tld_file)

   Return(ok)
endmacro


macro "mode_share_summaries" (Args, purposes, shr_arr, tld_arr)
// 

   purp_segs = Args.[Purpose Segments]
   out_dir = Args.OutputFolder

   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)   
   
   for purp in purposes do 
      segments = ParseString(purp_segs.(purp).Segments, ",")
      modes = ParseString(purp_segs.(purp).Modes, ",")
      
      // create combined results across vehicle segments
      if (segments.length > 1) then do
         ref_tag = runmacro("get_pa_file_tag", purp, "pk", segments[1])
         ref_file = runmacro("get_segment_mc_trip_file", out_dir, ref_tag)
         ref_mobj = CreateObject("Matrix", ref_file)
         all_mobj = ref_mobj.CopyStructure({OutputFile: GetTempFileName(".mtx"),  
                                                   Label: purp + "_daily", 
                                                   Cores: modes
                                                   })                                             
         all_mc = all_mobj.GetCores()
      end

      for seg in segments do
         // combine trips from both time periods
         pk_tag = runmacro("get_pa_file_tag", purp, "pk", seg)
         np_tag = runmacro("get_pa_file_tag", purp, "np", seg)
         pk_file = runmacro("get_segment_mc_trip_file", out_dir, pk_tag)
         np_file = runmacro("get_segment_mc_trip_file", out_dir, np_tag)
         pk_mobj = CreateObject("Matrix", pk_file)
         np_mobj = CreateObject("Matrix", np_file)
         pk_mc = pk_mobj.GetCores()
         np_mc = np_mobj.GetCores()
         cores = pk_mobj.GetCoreNames()

         dy_mat = pk_mobj.CloneMatrixStructure({FileName: GetTempFileName(".mtx"),  
                                                   MatrixLabel: purp + "_" + seg + "_daily", 
                                                   CloneSource: pk_file //,MemoryOnly: true 
                                                   })
         dy_mobj = CreateObject("Matrix", dy_mat)                                                
         dy_mc = dy_mobj.GetCores()
      
         // write out model region totals
         for core in cores do
            dy_mc.(core) := nz(pk_mc.(core)) + nz(np_mc.(core))
            stats = dy_mobj.GetMatrixStatistics(core)

            // write out specific segments
            if (segments.length > 1) then do
               shr_arr.(purp).(seg).(core).trips = stats.Sum
               
               // build up combined matrix as we go
               all_mc.(core) := nz(all_mc.(core)) + nz(dy_mc.(core))
            end 
            else do // set combined to daily for single segments
               all_mobj = dy_mobj
            end
         end
      end

      // calculate for all segments
      cores = all_mobj.GetCoreNames()
      for core in cores do
         stats = all_mobj.GetMatrixStatistics(core)
         shr_arr.(purp).("all").(core).trips = stats.Sum
      end      

      // calculate trip length distribution by mode
      runmacro("trip_length_distribution_summary", Args, all_mobj, "mc_"  + purp, tld_arr)

      // aggregate by geography
      geoarray = {"state","mpo","access_density","subregion" ,"corridor","ring","district"}
      for geo in geoarray do 
         runmacro("aggregate_matrix_by_geography", out_dir, all_mobj, "mc_"  + purp, geo)
      end
   end
endmacro


