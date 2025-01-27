
//#   "distribution.rsc"
macro "Prepare Distribution Inputs" (Args)
// Create distribution calculation files
   if Args.DryRun = 1 then Return(1)
   ok = 1

   // create identity matrix for intrazonals
   ref_mobj = CreateObject("Matrix", Args.[HighwaySkims - am])
   iz_file = Args.OutputFolder + "\\_demand\\td\\iz.mtx"

   iz_mobj = ref_mobj.CopyStructure({FileName: iz_file, 
                                    Cores: {"iz"}, 
                                    Label: "identity"})

   iz_mobj.SetVector({Core: "iz", Vector: 0, FillAll: "Rows"})
   iz_mobj.SetVector({Core: "iz", Vector: 1, Diagonal: "Diagonal"})

   Return(ok)
endmacro

macro "Household Distribution" (Args)
// UI helper to distribute household trips
   if Args.DryRun = 1 then Return(1)
   ok = 1

   time_periods = {"pk", "np"}
   purposes = Args.[Trip Purp]
   purp_segs = Args.[Purpose Segments]
   hbw_inc_segs = {"inc1","inc2","inc3","inc4"} // TODO: pull from parameters

   for purp in purposes do 

      segments = ParseString(purp_segs.(purp).Segments, ",")

      for per in time_periods do 
         for seg in segments do

            if (purp = 'hbsc') then do 
               
               prod_field = runmacro("get_pa_file_tag", purp, per, seg)
               attr_field = purp
               out_mat = runmacro("get_segment_pa_file", Args.OutputFolder, prod_field)

               // distribution file will be cleaned up in assembly - only save balanced if in debug mode
               if ({"DEBUG"} contains Args.loglevel) then do 
                  pa_bal_file = Args.OutputFolder + "\\_demand\\td\\" + prod_field + "_balanced.bin"
               end
               else pa_bal_file = GetTempFileName("*.bin")
               
               // balance to productions
               runmacro("assemble_and_balance_pa", pa_bal_file, Args.[hh_prod], Args.[hh_attr], prod_field, attr_field)
               ok = runmacro("gravity_distribution", Args, pa_bal_file, prod_field, attr_field, per, purp + "-all", out_mat)

               continue
            end

            for inc in hbw_inc_segs do

               if (purp <> 'hbw') then inc = null
               ok = runmacro("destination_choice", Args, purp, inc, per, seg)
               
               if (inc = null) then break // only run once for non-hbw

            end // income
            
            // clean up logsum file
            if ({"STANDARD","LEAN"} contains Args.loglevel) then do 
               ls_tag = runmacro("get_pa_file_tag", purp, per, seg)
               DeleteFile(runmacro("get_segment_mc_logsum_file", Args.OutputFolder, ls_tag))
            end 
         end // segments
      end // periods
   end // purposes

   Return(ok)
endmacro

macro "destination_choice" (Args, purp, inc, per, seg)
//

   ok = 1

   // logsums aren't segmented by worker income, but attractions are
   ls_tag = runmacro("get_pa_file_tag", purp, per, seg)

   if (inc <> null) then inc = "_" + inc
   dc_tag = runmacro("get_pa_file_tag", purp + inc, per, seg)

   if per = "pk" then tod = "am" else tod = "md"
   if seg = null then seg = "*"

   // Create the NLM Object
   obj = CreateObject("PMEChoiceModel", {ModelName: dc_tag})
   obj.Segment = seg
   obj.OutputModelFile = Args.OutputFolder + "\\_demand\\td\\ModelFiles\\" + dc_tag + ".dcm"
   
   obj.AddTableSource({SourceName: "Productions", File: Args.[hh_prod], IDField: "taz_id"})
   obj.AddTableSource({SourceName: "Attractions", File: Args.[hh_attr], IDField: "taz_id"})

   obj.AddMatrixSource({SourceName: "SkimRD", 
                     File: Args.("HighwaySkims - " + tod), 
                     RowIndex: "Origin", 
                     ColIndex: "Destination"})
   obj.AddMatrixSource({SourceName: "SkimTW", 
                     File: Args.("TransitWalkSkims - " + tod), 
                     RowIndex: "RCIndex", 
                     ColIndex: "RCIndex"})
   obj.AddMatrixSource({SourceName: "IntraZonal", 
                        File: Args.OutputFolder + "\\_demand\\td\\iz.mtx", 
                        RowIndex: "Rows", 
                        ColIndex: "Columns"})                
   obj.AddMatrixSource({SourceName: "MCLogsum", 
                     File: runmacro("get_segment_mc_logsum_file", Args.OutputFolder, ls_tag),
                     RowIndex: "Origins", 
                     ColIndex: "Destinations"})
   
   obj.AddDestinations({DestinationsSource: "SkimRD", DestinationsIndex: "Destination"})

   substituteStrings = {{"[Purpose]", purp + inc}}
   obj.AddUtility({UtilityFunction: Args.(purp + "DestUtility"), 
                        SubstituteStrings: substituteStrings})

   obj.AddPrimarySpec({Name: "SkimRD"})
   obj.AddTotalsSpec({Name: "Productions", ZonalField: dc_tag}) 

   // Choose output options
   output_opts = {Probability: runmacro("get_segment_dc_prob_file", Args.OutputFolder, dc_tag),
               Totals: runmacro("get_segment_pa_file", Args.OutputFolder, dc_tag)}
   obj.AddOutputSpec(output_opts)
   
   ret = obj.Evaluate()
   if !ret then
      Throw("DC model failed for: " + dc_tag) 

   return(ok)
endmacro

macro "assemble_and_balance_pa" (pa_bal_file, prod_file, attr_file, prod_field, attr_field)
// Combine and balance attractions to productions for gravity distribution

   obj = CreateObject("Generation.Balance")
   obj.AddDatasource({ TableNames: {{ Name: prod_file, Field: "taz_id" }, { Name: attr_file, Field: "taz_id"}}} )
   obj.OutputFile = pa_bal_file
   obj.AddPurpose({Production: prod_field, Attraction: attr_field})
   ok = obj.Run()
endmacro

macro "gravity_distribution" (Args, pa_bal_file, prod_field, attr_field, per, purpseg, out_mat)
// Gravity distribution
   dist_terms = Args.[Gravity Dist]
   constr = dist_terms.(purpseg).Constraint
   func = dist_terms.(purpseg).Function
   imp_core = dist_terms.(purpseg).Impedance
   terms = ParseString(dist_terms.(purpseg).Parameter, ",")
   if per = "pk" then tod = "am" else tod = "md"   
   skim_mat = {MatrixFile: Args.("HighwaySkims - " + tod), Matrix: imp_core,
                 RowIndex: "Origin", ColIndex: "Destination"}     

   obj = CreateObject("Distribution.Gravity")
   obj.ResetPurposes()
   obj.DataSource = {TableName:  pa_bal_file}
   
   if func = "Exponential" then do 
      obj.AddPurpose({Name:  "Total", 
                     Production:  prod_field, 
                     Attraction:  attr_field, 
                     ConstraintType: constr, 
                     ImpedanceMatrix:  skim_mat, 
                     Exponential: s2r(terms[1])}) 
      end
   else if func = "Gamma" then do 
      obj.AddPurpose({Name:  "Total", 
                     Production:  prod_field, 
                     Attraction:  attr_field, 
                     ConstraintType: constr, 
                     ImpedanceMatrix:  skim_mat, 
                     Gamma: {s2r(terms[1]),s2r(terms[2]),s2r(terms[3])}
                     }) 
      end
   else do 
      throw("unknown distribution function " + func + " for " + prod_field)
      return(0)
   end

   obj.OutputMatrix({MatrixFile: out_mat, Matrix: prod_field, Compression : true, ColumnMajor : false})
   //obj.CalculateTLD = true
   ok = obj.Run()
   return(ok)

endmacro

macro "Truck Distribution" (Args)
// Helper UI macro for truck distribution
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp_mode = Args.[Purpose Segments]
   modes = ParseString(purp_mode.("trk").Modes, ",")

   ok = runmacro("other_trip_distribution", Args, "trk", modes, "daily")

   Return(ok)
endmacro

macro "External Distribution" (Args)
// Helper UI macro for external trip distribution
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp_mode = Args.[Purpose Segments]
   ext_purps = ParseString(purp_mode.("ext").Segments, ",")

   for purp in ext_purps do
      purp = "ext_" + purp
      modes = ParseString(purp_mode.(purp).Modes, ",")
      if purp = "ext_ee" then do
         ok = runmacro("ee_trip_factoring", Args, modes)  
      end
      else do // ei, eair
         ok = runmacro("other_trip_distribution", Args, purp, modes, "daily")
      end
   end     

   // combine external segments
   runmacro("combine_ext_distribution", Args, ext_purps)
  
   Return(ok)
endmacro


macro "SpcGen Distribution" (Args)
// Helper UI macro for special generator distribution
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp_mode = Args.[Purpose Segments]
   spc_segs = ParseString(purp_mode.("spcgen").Segments, ",")

   for per in {"pk","np"} do 
      ok = runmacro("other_trip_distribution", Args, "spcgen", spc_segs, per) 
   end
  
   Return(ok)
endmacro


macro "Airport Distribution" (Args)
// Helper UI macro for airport ground access distribution
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp_mode = Args.[Purpose Segments]
   air_segs = ParseString(purp_mode.("air").Segments, ",")

   for per in {"pk","np"} do 
      ok = runmacro("other_trip_distribution", Args, "air", air_segs, per) 
   end
  
   Return(ok)
endmacro


macro "HBU Distribution" (Args)
// Helper UI macro for hbu distribution
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp_mode = Args.[Purpose Segments]
   hbu_segs = ParseString(purp_mode.("hbu").Segments, ",")

   for per in {"pk","np"} do 
      ok = runmacro("other_trip_distribution", Args, "hbu", hbu_segs, per) 
   end
  
   Return(ok)
endmacro

macro "other_trip_distribution" (Args, purp, segments, per)
// Distributes trips at a daily level with segments by mode
   //                         purp = trk, ext_ei, ext_eair, spcgen, air, hbu
   //                         segments - how to group trips for distribution, needs to match Gravity Dist rows
   tpurp = purp + "_" + per
   if (per="daily") then pknp = "pk" else pknp = per

   if ({"DEBUG","FULL"} contains Args.loglevel) then do 
      pa_file = Args.OutputFolder + "\\_demand\\td\\" + tpurp + "_pa.bin"
   end
   else pa_file = GetTempFileName("*.bin")   
   runmacro("assemble_other_trip_pa",Args, pa_file, segments, purp + "_trip", per)

   mat_arr = null
   for ms in segments do 
               
      prod_field = ms + "_p"
      attr_field = ms + "_a"
      out_mat = runmacro("get_segment_pa_file", Args.OutputFolder, ms)     
      ok = runmacro("gravity_distribution", Args, pa_file, prod_field, attr_field, pknp, purp + "-" + ms, out_mat)

      // rename core to ms
      mat_obj = CreateObject("Matrix", out_mat)
      mat_obj.RenameCores({CurrentNames: {"Total"}, NewNames: {ms}})
      mat_arr = mat_arr + {mat_obj.GetMatrixHandle()}
      mat_obj = null
   end // modes

   // assemble into single distribution file
   comb_mat_file = runmacro("get_segment_pa_file", Args.OutputFolder, tpurp)
   cm = ConcatMatrices(mat_arr,"True", {{"File Name", comb_mat_file}, {"Label", tpurp + "_pa"}})

   // delete single distribution files
   mat_arr = null
   if ({"STANDARD","LEAN"} contains Args.loglevel) then do 
      for ms in segments do 
         temp_file = runmacro("get_segment_pa_file", Args.OutputFolder, ms)
         DeleteFile(temp_file)
      end
   end

   Return(ok)
endmacro

macro "assemble_other_trip_pa" (Args, pa_file, segments, table, per)
// Pull and assemble other trip pa data from Sqlite DB
   db_file = Args.OutputFolder + "\\tdm23.db"
   obj = CreateNativeObject("SQLite")
   obj.Connect(db_file)

   // build query for just selected segments
   qry = "Select taz_id "
   pa_segs = null
   for ms in segments do 
      for dir in {"p","a"} do 
         sdir = ms + "_" + dir
         qry = qry + ", " + sdir
         pa_segs = pa_segs + {sdir}
      end
   end
   qry = qry + " FROM " + table

   // time of day
   if (per <> "daily") then qry = qry + " WHERE peak = " + if (per="pk") then "1" else "0" 

   view = obj.RetrieveQueryResults({ViewName: "trip_data", Query: qry, MaxStringLength: 30})

   ExportView("trip_data|","FFB",pa_file,
      {"taz_id"} + pa_segs,)
   CloseView("trip_data")

endmacro


macro "combine_ext_distribution" (Args, ext_purps)
// Combine ei, eair, and ee into a single matrix for tod
   purp_mode = Args.[Purpose Segments]

   // initialize matrix with ei
   modes = ParseString(purp_mode.("ext").Modes, ",")
   ref_mat = runmacro("get_segment_pa_file",Args.OutputFolder, "ext_ei_daily")   
   ref_mobj = CreateObject("Matrix", ref_mat)  

   sum_file = runmacro("get_segment_pa_file", Args.OutputFolder, "ext_daily")   
   sum_mobj = ref_mobj.CopyStructure({FileName: sum_file, 
                                          Cores: modes, 
                                          Label: "pa_trips_ext"})     
   ref_mobj = null
   sum_mc = sum_mobj.GetCores()

   for purp in ext_purps do 
      purp = "ext_" + purp
      tpurp = purp + "_daily"

      modes = ParseString(purp_mode.(purp).Modes, ",")
      ext_mat = runmacro("get_segment_pa_file",Args.OutputFolder, tpurp)   
      ext_mobj = CreateObject("Matrix", ext_mat)

      if (purp = "ext_ee") then do // set external mat to full taz set
         ext_mobj.SetColIndex("TAZ")
         ext_mobj.SetRowIndex("TAZ")
      end

      ext_mc = ext_mobj.GetCores()
      for mode in modes do 
         sum_mc.(mode) := nz(sum_mc.(mode)) + nz(ext_mc.(mode))   
      end
      ext_mobj = null
      ext_mc = null

   if ({"STANDARD","LEAN"} contains Args.loglevel) then do 
         DeleteFile(runmacro("get_segment_pa_file",Args.OutputFolder, tpurp))
      end
   end

endmacro

macro "ee_trip_factoring" (Args, segments)
// Apply IPF to external to external target volumes
   ok = 1

   ipf_seed_file = Args.[ext_ext_seed]
   purp = "ext_ee"
   tpurp = purp + "_daily"
   db_table = purp + "_trip"

   if ({"DEBUG","FULL"} contains Args.loglevel) then do 
      pa_file = Args.OutputFolder + "\\_demand\\td\\" + tpurp + "_pa.bin"
   end
   else pa_file = GetTempFileName("*.bin")   

   out_mat_file = runmacro("get_segment_pa_file", Args.OutputFolder, tpurp)  

   // Read external station targets
   db_obj=null
   db_file = Args.OutputFolder + "\\tdm23.db"
   db_obj = CreateNativeObject("SQLite")
   db_obj.Connect(db_file)

   // build query for just selected segments
   qry = "Select taz_id "
   pa_segs = null
   for ms in segments do 
      for dir in {"p","a"} do 
         sdir = ms + "_" + dir
         qry = qry + ", " + sdir
         pa_segs = pa_segs + {sdir}
      end
   end
   qry = qry + " FROM " + db_table

   view = db_obj.RetrieveQueryResults({ViewName: "trip_data", Query: qry, MaxStringLength: 30})

   ExportView("trip_data|","FFB",pa_file,
      {"taz_id"} + pa_segs,)
   CloseView("trip_data")

   // Run IPF
   ipf_seed = {MatrixFile: ipf_seed_file, RowIndex: "taz_id", ColIndex: "taz_id"}

   obj = null
   obj = CreateObject("Distribution.IPF") 
   obj.ResetPurposes()
   obj.DataSource = {TableName: pa_file}
   obj.BaseMatrix = ipf_seed
   for ms in segments do 
      obj.AddPurpose({Name: ms, Production: ms + "_p", Attraction: ms + "_a" } )
   end
   obj.OutputMatrix({MatrixFile: out_mat_file, MatrixLabel : purp, 
      Compression: true, ColumnMajor: false})
   ok = obj.Run()

   r = obj.GetResult()
   obj = null

   // Expand to entire zone system
   taz_qry = "SELECT taz_id FROM MA_taz_geography"
   view = db_obj.RetrieveQueryResults({ViewName: "taz_id", Query: taz_qry, MaxStringLength: 30})

   out_mat = OpenMatrix(out_mat_file,)
   matObj = CreateObject("Caliper.Matrix")

   TAZIndex = matObj.AddIndex({Matrix: out_mat, IndexName: "TAZ",
                ViewName: "taz_id", Dimension: "Both",
                OriginalID: "taz_id", NewID: "taz_id",
                ExtendedIndex: true})
   return(ok)

endmacro

macro "Assemble Trips for MC"  (Args)
// Combine PA trips for mode choice
   if Args.DryRun = 1 then Return(1)
   ok = 1

   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)   

   time_periods = {"pk", "np"}
   purposes = Args.[Trip Purp]
   purp_segs = Args.[Purpose Segments]
   hbw_inc_segs = {"inc1","inc2","inc3","inc4"} // TODO: pull from parameters

   // matrix structure 
   tags = runmacro("get_pa_file_tag", "hbw_inc1", "pk", "zv")
   ref_mat = runmacro("get_segment_pa_file",Args.OutputFolder, tags)   
   ref_mobj = CreateObject("Matrix", ref_mat)

   for purp in purposes do 

      segments = ParseString(purp_segs.(purp).Segments, ",")

      for per in time_periods do 

         comb_tag = runmacro("get_pa_file_tag", purp, per,)
         comb_file = runmacro("get_segment_pa_file", Args.OutputFolder, comb_tag) 

         comb_mobj = ref_mobj.CopyStructure({FileName: comb_file, 
                                                Cores: segments, 
                                                Label: "pa_trips_" + comb_tag})  
         ref_mobj = null
         ref_mobj = comb_mobj // after first tod, leverage previous tod
         comb_mc = comb_mobj.GetCores()

         for seg in segments do 
            for inc in hbw_inc_segs do

               if (purp <> 'hbw') then inc = null else inc = "_" + inc
               
               runmacro("combine_td_segment", Args, purp + inc, per, seg, comb_mc)
              
               if (inc = null) then break // only run once for non-hbw

            end // income
         end // segments
      end // periods
   end //purposes

   Return(ok)
endmacro

macro "combine_td_segment" (Args, purp, per, seg, comb_mc)
//

   seg_tag = runmacro("get_pa_file_tag", purp, per, seg)
   seg_file = runmacro("get_segment_pa_file", Args.OutputFolder, seg_tag)   

   seg_mobj = CreateObject("Matrix", seg_file)
   seg_mc = seg_mobj.GetCore("Total")

   comb_mc.(seg) := nz(comb_mc.(seg)) + nz(seg_mc)

   // delete segment distribution files
   seg_mobj = null
   seg_mc = null
   if ({"STANDARD","LEAN"} contains Args.loglevel) then do 
      DeleteFile(seg_file)
      if (purp <> "hbsc") then DeleteFile(runmacro("get_segment_dc_prob_file", Args.OutputFolder, seg_tag))
   end

endmacro

macro "Combine SpcGen Trips"  (Args)
// Add special generator trips to household trips for MC
   if Args.DryRun = 1 then Return(1)
   ok = 1

   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)   

   time_periods = {"pk", "np"}
   purp_segs = Args.[Purpose Segments]

   spc_segs = ParseString(purp_segs.("spcgen").Segments, ",")

   for per in time_periods do     
      spc_pa_file = runmacro("get_segment_pa_file", Args.OutputFolder, "spcgen_" + per)
      spc_mobj = CreateObject("Matrix", spc_pa_file)
      spc_mc = spc_mobj.GetCores()

      for purp in spc_segs do 
         hh_segments = ParseString(purp_segs.(purp).Segments, ",")
         hh_pa_tag = runmacro("get_pa_file_tag", purp, per,)
         hh_pa_file = runmacro("get_segment_pa_file", Args.OutputFolder, hh_pa_tag) 
         hh_mobj = CreateObject("Matrix", hh_pa_file)

         // add special generator trips to sufficient vehicles if segmented by vehicles
         hh_seg = if (ArrayLength(hh_segments)>1) then "sv" else "all"
         hh_mc = hh_mobj.GetCore(hh_seg)         
         hh_mc := nz(hh_mc) + nz(spc_mc.(purp))
      end
   end
   Return(ok)
endmacro

macro "Distribute Non-Revenue Trips"  (Args)
// Distribute Ride-Sourcing non-revenue trips
   time_periods         = Args.[TimePeriods]
   purposes             = Args.[Trip Purp]
   purp_mode            = Args.[Purpose Segments]
   out_dir              = Args.OutputFolder

   if Args.DryRun = 1 then Return(1)   
   ok = 1

   thr_status = CreateObject("Utils.Currency")
   SetNumMatrixThreads(Args.MatrixThreads)   

   // build matrices
   tags = runmacro("get_od_file_tag", "hbw", "am")
   ref_mat = runmacro("get_od_trip_file",out_dir, tags)      
   ref_mobj = CreateObject("Matrix", ref_mat)                  

   rev_file = runmacro("get_rs_od_file", out_dir, "rev")              
   nrev_file = runmacro("get_rs_od_file", out_dir, "nonrev")    

   rev_mobj = ref_mobj.CopyStructure({FileName: rev_file, 
                                       Label: "rs_revenue_trips", 
                                       Cores: time_periods})    

   nrev_mobj = ref_mobj.CopyStructure({FileName: nrev_file, 
                                       Label: "rs_non_revenue_trips", 
                                       Cores: time_periods})   

   rev_mc = rev_mobj.GetCores()
   nrev_mc = nrev_mobj.GetCores()
   taz_v = rev_mobj.GetVector({Core: "am", Index: "Row"})                                       

   // table of marginals for distribution of non-revenue trips
   if ({"DEBUG","FULL"} contains Args.loglevel) then do 
      rev_marg_file = out_dir + "\\_demand\\tod\\rs_rev_marg.bin"
   end
   else rev_marg_file = GetTempFileName("*.bin")   
   
   flds = {{"taz_id",        "Integer", 8, 0, "True"},
            {"am_o",          "Real", 12, 3, },
            {"am_d",          "Real", 12, 3, },
            {"md_o",          "Real", 12, 3, },
            {"md_d",          "Real", 12, 3, },
            {"pm_o",          "Real", 12, 3, },
            {"pm_d",          "Real", 12, 3, },
            {"nt_o",          "Real", 12, 3, },
            {"nt_d",          "Real", 12, 3, }}
   obj = CreateObject("CC.Table")
   tab = obj.Create({
               FileName: rev_marg_file, 
               FieldSpecs: flds, 
               AddEmptyRecords: taz_v.length, 
               DeleteExisting: True})
   rev_marg_vw = tab.View             

   SetDataVector(rev_marg_vw + "|", "taz_id", rev_mobj.GetVector({Core: "am", Index: "Row"}),)

   // combine all ride-sourcing revenue vehicle trips by time period
   for tod in time_periods do 

      // household trips by purpose
      for purp in purposes do
         modes = ParseString(purp_mode.(purp).Modes, ",")
         if modes contains "rs" then do
            rs_occ = Args.HH_Occupancies.(purp).[RS Occupancy]
            runmacro("combine_rs_trip", Args, rev_mc, purp, tod, rs_occ)
         end
      end // household trips

      // hbu trips
      modes = ParseString(purp_mode.("hbu").Modes, ",")   

      if modes contains "rs" then do 
         rs_occ = Args.HH_Occupancies.("hbu").[RS Occupancy]
         runmacro("combine_rs_trip", Args, rev_mc, "hbu", tod, rs_occ)
      end // hbu trips

      // airport trips by purpose
      air_purps = ParseString(purp_mode.("air").Segments, ",") 
      modes = ParseString(purp_mode.("air").Modes, ",")   

      if modes contains "rs" then do 
         for purp in air_purps do
            rs_occ = Args.Airport_Occupancies.(purp).[RS Occupancy]
            runmacro("combine_rs_trip", Args, rev_mc, "air_" + purp, tod, rs_occ)
         end
      end // airport trips

      // calculate marginals
      nrev_d = rev_mobj.GetVector({Core: tod, Marginal: "Row Sum"})
      nrev_o = rev_mobj.GetVector({Core: tod, Marginal: "Column Sum"})

      SetDataVector(rev_marg_vw + "|", tod + "_o", nrev_o,)
      SetDataVector(rev_marg_vw + "|", tod + "_d", nrev_d,)

      // distribute non-revenue
      tod_nrev_file = runmacro("get_rs_od_file", out_dir, "nonrev_" + tod)    
      runmacro("gravity_distribution", Args, 
               rev_marg_file, tod + "_o", tod + "_d", per, "rs-nonrev-all",
               tod_nrev_file)

      // add to total
      nrev_tod_mobj = CreateObject("Matrix", tod_nrev_file)
      nrev_tod_mc = nrev_tod_mobj.GetCore("Total")
      nrev_mc.(tod) := nz(nrev_mc.(tod)) + nz(nrev_tod_mc)

      nrev_tod_mobj = null
      nrev_tod_mc = null
      if ({"STANDARD","LEAN"} contains Args.loglevel) then do 
         DeleteFile(tod_nrev_file)
      end        

   end // revenue and non-revenue vehicle trips by time period

   // sum all trips for reports
   rev_mobj.Sum("Daily")
   nrev_mobj.Sum("Daily")

   // write summaries for reports
   runmacro("rs_trip_reports", Args, rev_mobj, nrev_mobj)

   Return(ok)
endmacro

macro "combine_rs_trip" (Args, rev_mc, purp, tod, rs_occ)
//
   tags = runmacro("get_od_file_tag", purp, tod)
   od_file = runmacro("get_od_trip_file",Args.OutputFolder, tags)
   od_mobj = CreateObject("Matrix", od_file)
   od_mc = od_mobj.GetCore("rs")

   rev_mc.(tod) := nz(rev_mc.(tod)) + nz(od_mc)/rs_occ
endmacro

macro "rs_trip_reports" (Args, rev_mobj, nrev_mobj)
//
   // report files
   out_dir = Args.OutputFolder
   rs_taz_file = out_dir + "\\_summary\\trips\\rs_trip_ends" 
   rs_rep_file = out_dir + "\\_summary\\trips\\rs_summ.json"
   rs_rep_arr = {}

   // export trips by TOD
   cores = rev_mobj.GetCoreNames()
   for core in cores do
      rev_stats = rev_mobj.GetMatrixStatistics(core)
      nrev_stats = nrev_mobj.GetMatrixStatistics(core)

      rs_rep_arr.rev.(core) = rev_stats.Sum
      rs_rep_arr.nrev.(core) = nrev_stats.Sum
   end

   // calculate deadheading VMT
   runmacro("calculate_rs_vmt", Args, rev_mobj, nrev_mobj, rs_rep_arr)
   runmacro("export_arr_to_json",rs_rep_arr, rs_rep_file)

   // export trips by zone for municipality summaries
   trip_o_v = rev_mobj.GetVector({Core: "Daily", Marginal:"Row Sum"})
   trip_d_v = rev_mobj.GetVector({Core: "Daily", Marginal:"Column Sum"})
   taz_v = rev_mobj.GetVector({Core: "Daily", Index:"Row"})
   
   flds = {{"taz_id",        "Integer", 8, 0, True},
            {"trip_o",          "Real", 12, 3, },
            {"trip_d",          "Real", 12, 3, }}

   obj = CreateObject("CC.Table")
   tab = obj.Create({
               FileName: rs_taz_file + ".bin", 
               FieldSpecs: flds, 
               AddEmptyRecords: taz_v.length, 
               DeleteExisting: True})
   rev_taz_vw = tab.View             

   SetDataVectors(rev_taz_vw+"|", {{"taz_id",taz_v},
                                     {"trip_o",trip_o_v},
                                     {"trip_d",trip_d_v}},)   
   tab = null

   runmacro("convert_bin_to_csv", rs_taz_file + ".bin", rs_taz_file + ".csv")
endmacro

macro "calculate_rs_vmt" (Args, rev_mobj, nrev_mobj, rs_rep_arr)
//
   // calculate ratio of ride-sourcing deadhead VMT

   // use AM skims
   out_dir = Args.OutputFolder
   skim_mobj = CreateObject("Matrix", Args.[HighwaySkims - am])
   dist_mc = skim_mobj.GetCore("dist")
   rev_mc = rev_mobj.GetCore("Daily")
   nrev_mc = nrev_mobj.GetCore("Daily")

   o = CreateObject("Caliper.Matrix")
   rs_mout = o.CloneMatrixStructure({MatrixLabel: "rs", 
                                    CloneSource: {dist_mc}, 
                                    //MatrixFile: out_dir + "rs_mat.mtx"
                                    MemoryOnly: true 
                                    })
   rs_mobj = CreateObject("Matrix", rs_mout)

   rs_mobj.AddCores({'rev','nrev'})
   rs_mc = rs_mobj.GetCores()

   rs_mc.rev := dist_mc * rev_mc
   rs_mc.nrev := dist_mc * nrev_mc

   rev_vmt = VectorToArray(rs_mobj.GetVector({Core: "rev", Marginal:"Row Sum"}))
   nrev_vmt = VectorToArray(rs_mobj.GetVector({Core: "nrev", Marginal:"Row Sum"}))

   rs_rep_arr.rev.vmt = Sum(rev_vmt)
   rs_rep_arr.nrev.vmt = Sum(nrev_vmt)

endmacro


macro "Build Combined Table for Reports"  (Args)
// Combine All Distribution Trips for Reports
   ok = 1
   if Args.DryRun = 1 then Return(1)
   time_periods = {"pk", "np"}
   
   // matrix structure 
   tags = runmacro("get_pa_file_tag", "hbw", "pk",)
   ref_mat = runmacro("get_segment_pa_file",Args.OutputFolder, tags)   
   ref_mobj = CreateObject("Matrix", ref_mat)

   comb_tag = runmacro("get_pa_file_tag", "all", "all",)
   comb_file = runmacro("get_segment_pa_file", Args.OutputFolder, comb_tag) 

   comb_mobj = ref_mobj.CopyStructure({FileName: comb_file, 
                                          Cores: {"hb","nhb","air","ext","trk"}, 
                                          Label: "pa_trips_" + comb_tag})  
   comb_mc = comb_mobj.GetCores()   
   
   hb_purps = {"hbw","hbsc","hbpb","hbsr","hbu"}
   for purp in hb_purps do 
      for per in time_periods do 
         tags = runmacro("get_pa_file_tag", purp, per,)
         pa_mat_file = runmacro("get_segment_pa_file",Args.OutputFolder, tags)   
         runmacro("combine_matrix", comb_mc.hb, pa_mat_file)
      end // periods
   end //purposes

   nhb_purps = {"nhbw","nhbnw"}
   for purp in nhb_purps do 
      for per in time_periods do 
         tags = runmacro("get_pa_file_tag", purp, per,)
         pa_mat_file = runmacro("get_segment_pa_file",Args.OutputFolder, tags)   
         runmacro("combine_matrix", comb_mc.nhb, pa_mat_file)
      end // periods
   end //purposes

   for purp in {"ext","trk"} do 
      tags = runmacro("get_pa_file_tag", purp, "daily",)
      pa_mat_file = runmacro("get_segment_pa_file",Args.OutputFolder, tags)   
      runmacro("combine_matrix", comb_mc.(purp), pa_mat_file)
   end //purposes   

   purp = "air"
   for per in time_periods do 
      tags = runmacro("get_pa_file_tag", purp, per,)
      pa_mat_file = runmacro("get_segment_pa_file",Args.OutputFolder, tags)   
      runmacro("combine_matrix", comb_mc.air, pa_mat_file)
   end // periods   

   Return(ok)
endmacro

macro "combine_matrix" (out_mc, in_mat_file)
//
   // helper function to walk through matrix core and add to summary core
   in_mobj = CreateObject("Matrix", in_mat_file)         
   cores = in_mobj.GetCoreNames()

   for core in cores do 
      in_mc = in_mobj.GetCore(core)
      out_mc := nz(out_mc) + nz(in_mc)
   end // cores
endmacro

macro "Household Trip Aggregation" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1   
   out_dir = Args.OutputFolder
   purposes = Args.[Trip Purp]

   // aggregate pa files from trip distribution and export for reports
   geoarray = {"state","mpo","access_density","subregion" ,"corridor","ring","district"}
   for geo in geoarray do 
      for purp in purposes do 
         pk_tag = runmacro("get_pa_file_tag", purp, "pk",)
         np_tag = runmacro("get_pa_file_tag", purp, "np",)
         trip_obj = runmacro("sum_pknp_matrices", out_dir, pk_tag, np_tag) 

         runmacro("aggregate_matrix_by_geography", out_dir, trip_obj, "pa_"  + purp, geo)
      end
   end
   Return(ok)
endmacro

macro "HBU Trip Aggregation" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1   
   out_dir = Args.OutputFolder
   purposes = {"hbu"}

   // aggregate pa files from trip distribution and export for reports
   geoarray = {"state","mpo","access_density","subregion" ,"corridor","ring","district"}
   for geo in geoarray do 
      for purp in purposes do 
         pk_tag = runmacro("get_pa_file_tag", purp, "pk",)
         np_tag = runmacro("get_pa_file_tag", purp, "np",)
         trip_obj = runmacro("sum_pknp_matrices", out_dir, pk_tag, np_tag) 

         runmacro("aggregate_matrix_by_geography", out_dir, trip_obj, "pa_"  + purp, geo)
      end
   end
   Return(ok)
endmacro

macro "Ext Trip Aggregation" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1   
   out_dir = Args.OutputFolder
   purposes = {"ext"}

   // aggregate pa files from trip distribution and export for reports
   geoarray = {"state","mpo","access_density","subregion" ,"corridor","ring","district"}
   for geo in geoarray do 
      for purp in purposes do 
         pa_tag = runmacro("get_pa_file_tag", purp, "daily",)
         pa_file = runmacro("get_segment_pa_file", out_dir, pa_tag)
         trip_obj = CreateObject("Matrix", pa_file)

         runmacro("aggregate_matrix_by_geography", out_dir, trip_obj, "pa_"  + purp, geo)
      end
   end
   Return(ok)
endmacro

macro "AGA Trip Aggregation" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1   
   out_dir = Args.OutputFolder
   purposes = {"air"}

   // aggregate pa files from trip distribution and export for reports
   geoarray = {"state","mpo","access_density","subregion" ,"corridor","ring","district"}
   for geo in geoarray do 
      for purp in purposes do 
         pk_tag = runmacro("get_pa_file_tag", purp, "pk",)
         np_tag = runmacro("get_pa_file_tag", purp, "np",)
         trip_obj = runmacro("sum_pknp_matrices", out_dir, pk_tag, np_tag) 

         runmacro("aggregate_matrix_by_geography", out_dir, trip_obj, "pa_"  + purp, geo)
      end
   end
   Return(ok)
endmacro


macro "Truck Trip Aggregation" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1   
   out_dir = Args.OutputFolder
   purposes = {"trk"}

   // aggregate pa files from trip distribution and export for reports
   geoarray = {"state","mpo","access_density","subregion" ,"corridor","ring","district"}
   for geo in geoarray do 
      for purp in purposes do 
         pa_tag = runmacro("get_pa_file_tag", purp, "daily",)
         pa_file = runmacro("get_segment_pa_file", out_dir, pa_tag)
         trip_obj = CreateObject("Matrix", pa_file)

         runmacro("aggregate_matrix_by_geography", out_dir, trip_obj, "pa_"  + purp, geo)
      end
   end
   Return(ok)
endmacro

macro "HBW Income Geographic Distribution" (Args)
// Summarize hbw trip distributions by income segment
   if Args.DryRun = 1 then Return(1)
   if ({"STANDARD","LEAN"} contains Args.loglevel) then Return(1)

   ok = 1      
   out_dir = Args.OutputFolder
   purp_segs = Args.[Purpose Segments]
   time_periods = {"pk", "np"}   
   hbw_inc_segs = {"inc1","inc2","inc3","inc4"} // TODO: pull from parameters
   geoarray = {"state","mpo","access_density","subregion" ,"corridor","ring","district"}

   tags = runmacro("get_pa_file_tag", "hbw_inc1", "pk", "zv")
   ref_mat = runmacro("get_segment_pa_file",Args.OutputFolder, tags)   
   ref_mobj = CreateObject("Matrix", ref_mat)
   segments = ParseString(purp_segs.("hbw").Segments, ",")

   for inc in hbw_inc_segs do 
      // first combine files across segments
      for per in time_periods do 
         purp = "hbw_" + inc

         comb_tag = runmacro("get_pa_file_tag", purp, per,)
         comb_file = runmacro("get_segment_pa_file", Args.OutputFolder, comb_tag) 

         comb_mobj = ref_mobj.CopyStructure({FileName: comb_file, 
                                                Cores: segments, 
                                                Label: "pa_trips_" + comb_tag})  
         comb_mc = comb_mobj.GetCores()

         for seg in segments do 
            runmacro("combine_td_segment", Args, purp, per, seg, comb_mc)
         end // segments
      end // periods

      // now combine across time periods
      pk_tag = runmacro("get_pa_file_tag", purp, "pk",)
      np_tag = runmacro("get_pa_file_tag", purp, "np",)
      trip_obj = runmacro("sum_pknp_matrices", out_dir, pk_tag, np_tag)

      for geo in geoarray do       
         runmacro("aggregate_matrix_by_geography", out_dir, trip_obj, "pa_"  + purp, geo)
      end
   end // income segment

   Return(ok)   
endmacro

macro "Household Trip Length Distribution" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purposes = Args.[Trip Purp]
   out_dir = Args.OutputFolder
   tld_arr = {}
   tld_file = out_dir + "\\_summary\\tld\\hh_trip_tld.json"
   int_arr = {}
   int_file = out_dir + "\\_summary\\tld\\hh_trip_intraz.json"
   
   runmacro("pa_tld_intrz_summaries", Args, purposes, tld_arr, int_arr)
   runmacro("export_arr_to_json",tld_arr, tld_file)
   runmacro("export_arr_to_json",int_arr, int_file)

   Return(ok)
endmacro

macro "HBU Trip Length Distribution" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purposes = {"hbu"}
   out_dir = Args.OutputFolder
   tld_arr = {}
   tld_file = out_dir + "\\_summary\\tld\\hbu_trip_tld.json"
   int_arr = {}
   int_file = out_dir + "\\_summary\\tld\\hbu_trip_intraz.json"
   
   runmacro("pa_tld_intrz_summaries", Args, purposes, tld_arr, int_arr)
   runmacro("export_arr_to_json",tld_arr, tld_file)
   runmacro("export_arr_to_json",int_arr, int_file)

   Return(ok)
endmacro


macro "AGA Trip Length Distribution" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purposes = {"air"}
   out_dir = Args.OutputFolder
   tld_arr = {}
   tld_file = out_dir + "\\_summary\\tld\\air_trip_tld.json"
   int_arr = {}
   int_file = out_dir + "\\_summary\\tld\\air_trip_intraz.json"

   runmacro("pa_tld_intrz_summaries", Args, purposes, tld_arr, int_arr)
   runmacro("export_arr_to_json",tld_arr, tld_file)
   runmacro("export_arr_to_json",int_arr, int_file)

   Return(ok)
endmacro

macro "SpcGen Trip Length Distribution" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purposes = {"spcgen"}
   out_dir = Args.OutputFolder
   tld_arr = {}
   tld_file = out_dir + "\\_summary\\tld\\spcgen_trip_tld.json"
   int_arr = {}
   int_file = out_dir + "\\_summary\\tld\\spcgen_trip_intraz.json"

   runmacro("pa_tld_intrz_summaries", Args, purposes, tld_arr, int_arr)
   runmacro("export_arr_to_json",tld_arr, tld_file)
   runmacro("export_arr_to_json",int_arr, int_file)

   Return(ok)
endmacro


macro "Truck Trip Length Distribution" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp = "trk"
   out_dir = Args.OutputFolder
   tld_arr = {}
   tld_file = out_dir + "\\_summary\\tld\\trk_trip_tld.json"
   int_arr = {}
   int_file = out_dir + "\\_summary\\tld\\trk_trip_intraz.json"

   pa_tag = runmacro("get_pa_file_tag", purp, "daily",)
   pa_file = runmacro("get_segment_pa_file", out_dir, pa_tag)
   dy_mobj = CreateObject("Matrix", pa_file)   
   
   runmacro("trip_length_distribution_summary", Args, dy_mobj, "pa_" + purp, tld_arr)
   runmacro("calculate_intrazonals", Args, dy_mobj, purp, int_arr)

   runmacro("export_arr_to_json",tld_arr, tld_file)
   runmacro("export_arr_to_json",int_arr, int_file)

   Return(ok)
endmacro

macro "Ext Trip Length Distribution" (Args)
// 
   if Args.DryRun = 1 then Return(1)
   ok = 1

   purp = "ext"
   out_dir = Args.OutputFolder
   tld_arr = {}
   tld_file = out_dir + "\\_summary\\tld\\ext_trip_tld.json"

   pa_tag = runmacro("get_pa_file_tag", purp, "daily",)
   pa_file = runmacro("get_segment_pa_file", out_dir, pa_tag)
   dy_mobj = CreateObject("Matrix", pa_file)   
   
   runmacro("trip_length_distribution_summary", Args, dy_mobj, "pa_" + purp, tld_arr)

   runmacro("export_arr_to_json",tld_arr, tld_file)

   Return(ok)
endmacro

macro "pa_tld_intrz_summaries" (Args, purp_list, tld_arr, int_arr)
// Run tld and intrazonal summaries, populate passed arrays
   out_dir = Args.OutputFolder

   for purp in purp_list do 
      // combine trips from both time periods
      pk_tag = runmacro("get_pa_file_tag", purp, "pk",)
      np_tag = runmacro("get_pa_file_tag", purp, "np",)
      dy_mobj = runmacro("sum_pknp_matrices", out_dir, pk_tag, np_tag)

      cores = dy_mobj.GetCoreNames()

      // include all if vehicle segments
      if (cores.length > 1) then do
         dy_mobj.Sum("all")
      end

      runmacro("trip_length_distribution_summary", Args, dy_mobj, "pa_" + purp, tld_arr)
      runmacro("calculate_intrazonals", Args, dy_mobj, purp, int_arr)
   end

endmacro


