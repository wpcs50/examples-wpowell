
//#   "equity_measures.rsc"

macro "Get IntraZonalCores" (Args)
// Export taz level cores for interazonal trip
    if Args.DryRun = 1 then Return(1)
    ok = 1

    out_dir = Args.[OutputFolder] 
    mtx_file = out_dir + "\\_summary\\trips\\veh_trips_daily.mtx"
    mtx_mobj = CreateObject("Matrix", mtx_file)

    trip_mc = mtx_mobj.GetCores()
    cores = mtx_mobj.GetCoreNames()
    v_trips = {}
    v_trips_keys = {"auto","mtrk","htrk"}
    
    v_auto = {}
    v_auto = mtx_mobj.GetVector({Core: "auto",  Diagonal: "Row"})
    v_trck = Vector(v_auto.length, "Float", {{"Constant",0}})

    for veh in {"mtrk","htrk"} do
        v_trck = v_trck + mtx_mobj.GetVector({Core: veh,  Diagonal: "Row"})
    end


    dim pairs[2,2] 
    i = 1
    for core in {"da_time","dist"} do 
        j = 1
        for tod  in {"am","md"} do
            mtx_file = Args.("HighwaySkims - " + tod)
            obj = CreateObject("Matrix", mtx_file)
            pairs[i][j] = obj.GetVector({Core: core,  Diagonal: "Row"})
            j = j + 1
        end
        i = i + 1
    end


    taz_v = mtx_mobj.GetVector({Core: "auto", Index: "Row"})     
    rev_marg_file = GetTempFileName("*.bin")  
    flds = {    {"taz_id",        "Integer", 8, 0, "True"},
                {"auto",          "Real", 12, 3, },
                {"trk",          "Real", 12, 3, },
                {"time_am",           "Real", 12, 3, },
                {"time_md",           "Real", 12, 3, },
                {"dist_am",           "Real", 12, 3, },
                {"dist_md",           "Real", 12, 3, }

                }
    obj = CreateObject("CC.Table")
    tab = obj.Create({
                FileName: rev_marg_file, 
                FieldSpecs: flds, 
                AddEmptyRecords: taz_v.length, 
                DeleteExisting: True})
    view_name = tab.View             

    SetDataVector(view_name + "|", "taz_id", taz_v,)
    SetDataVector(view_name + "|", "auto"  , v_auto,)
    SetDataVector(view_name + "|", "trk"   , v_trck,)
    SetDataVector(view_name + "|", "time_am",  pairs[1][1],)
    SetDataVector(view_name + "|", "time_md",  pairs[1][2],)
    SetDataVector(view_name + "|", "dist_am",  pairs[2][1],)
    SetDataVector(view_name + "|", "dist_md",  pairs[2][2],)

	ExportView(view_name + "|", "CSV",out_dir + "\\_summary\\trips\\veh_trips_daily.csv",,{{"CSV Header","True"}})
	CloseView(view_name)


    return(ok)
endmacro

macro "calc_bus_count" (headway,tod)
// Supporting function to convert bus count based on headway and time of day

        
        if (headway = 0) then
            bus_cnt = 0
        else do
            if ({"am"} contains tod) then 
                bus_cnt =  3 *60  / headway  
            else if  ({"md"} contains tod) then 
                bus_cnt = 5.5*60  / headway
            else if  ({"pm"} contains tod) then 
                bus_cnt = 4  *60  /  headway
            else if ({"nt"} contains tod ) then 
                bus_cnt = 11.5 *60 /headway
            
        end

    return (bus_cnt)
endmacro


macro "Transit RouteLinks" (Args)
// Data preparation for transit emission
    if Args.DryRun = 1 then Return(1)    
    ok = 1

    out_dir = Args.[OutputFolder] 
    mtx_file = out_dir + "\\_summary\\trips\\veh_trips_daily.mtx"

    {rt_lyr, stop_lyr, ph_lyr} = RunMacro("TCB Add RS Layers", Args.[Transit], "ALL",)

    binfile = GetLayerDB(rt_lyr) + "R.bin"
    view_name = OpenTable(rt_lyr,"FFB", {binfile,})
    fds = GetFields(view_name,"All")
    dfroute = GetDataVectors(rt_lyr+"|",fds[1],  )

    seq_vec = Vector(dfroute[2].length, "Integer", {{"Sequence",   1, 1}})

    cnt = 1 
    for row in seq_vec do
        route_n   = dfroute[2][row]
        rlinks = GetRouteLinks(rt_lyr, route_n)
        for row_rt in rlinks do
            cnt = cnt + 1
        end
    end
    dim pairs[4,4*cnt] 

    ix = 0
    for tod in {"am","md","pm","nt"} do 
        for row in seq_vec do 
            headway   = dfroute[14][row] +  dfroute[15][row] +  dfroute[16][row] +  dfroute[17][row]  // tod 14-17
            route_n   = dfroute[2][row] 
            modekey   = dfroute[4][row] //  row["Mode"] 
            fuel_type = dfroute[12][row] // row["fuel_type"]
            
            rlinks = GetRouteLinks(rt_lyr, route_n)

            for row_rt in rlinks do
                ix = ix + 1
                id      = row_rt[1]
                bus_cnt = runmacro("calc_bus_count",headway,tod)
                if (TypeOf(fuel_type) <> "string" ) then  
                    fuel_type =  "na" //String(fuel_type)
                pairs[1][ix] = id
                pairs[2][ix] = bus_cnt
                pairs[3][ix] = fuel_type
                pairs[4][ix] = modekey
            end
        end
    end


    rev_marg_file = GetTempFileName("*.bin")  
    flds = {    {"ID",                "Float"  ,20, 0,, },  //link_id
                {"bus_cnt",          "Float"  ,20, 0,, },
                {"fuel_type",        "String" ,40,0,, },
                {"mode",             "Float"  ,20, 0,, }
                }
    obj = CreateObject("CC.Table")
    tab = obj.Create({
                FileName: rev_marg_file, 
                FieldSpecs: flds, 
                AddEmptyRecords: pairs[1].length, 
                DeleteExisting: True})
    view_name = tab.View    


    SetDataVector(view_name + "|", "ID",         A2V(pairs[1]) ,)
    SetDataVector(view_name + "|", "bus_cnt",    A2V(pairs[2]) ,)
    SetDataVector(view_name + "|", "fuel_type",  A2V(pairs[3]) ,)
    SetDataVector(view_name + "|", "mode",       A2V(pairs[4]) ,)
    
	ExportView(view_name + "|", "CSV",out_dir + "\\_summary\\trn\\trn_trips.csv",,{{"CSV Header","True"}})
	CloseView(view_name)
    
    return(ok)
endmacro

macro "Get Equity Metrics by TAZ" (Args)
// Calculate equity measures by taz
    if Args.DryRun = 1 then Return(1)    
    ok = 1

    out_dir = Args.[OutputFolder]
    taz_demogr_file = Args.[TAZ Demographic Data]
    metrics_taz_file = Args.[Equity Metrics by TAZ] // output

    la = Args.Accessibility_Metrics.[Target Field].Length
    lm = Args.Mobility_Metrics.[Target Field].Length
    dim vetor_array[la+lm+1], out_fields_array[la+lm+1], table_fields[la+lm+1], pairs[la+lm+1]

    // get taz_id
    view_name = OpenTable("view name 1","CSV", {taz_demogr_file, })
    v_taz_id = GetDataVector(view_name+"|", "TAZ_ID", {{"Sort Order", {{"TAZ_ID", "Ascending"}}}}) 
    CloseView(view_name)
    vetor_array[1] = v_taz_id
    out_fields_array[1] = "taz_id"
    table_fields[1] = {"taz_id", "Integer", 8, 0, indexed,}
    pairs[1] = {out_fields_array[1], vetor_array[1]}

    // Calculate Accessibility
    for a = 1 to la do
    vec = RunMacro ("Calculate Accessibility",Args.[OutputFolder],
                    Args.Accessibility_Metrics.Mode[a],
                    Args.Accessibility_Metrics.[Time of Day][a],
                    Args.Accessibility_Metrics.[Time Threshold][a],
                    Args.Accessibility_Metrics.[Source File][a],
                    Args.Accessibility_Metrics.[Source Field][a],
                    Args.Accessibility_Metrics.[Presence Type][a])
    vetor_array[a+1] = vec
    out_fields_array[a+1] = Args.Accessibility_Metrics.[Target Field][a]
    table_fields[a+1] = {out_fields_array[a+1], "Real", 12, 3, !indexed,}
    pairs[a+1] = {out_fields_array[a+1], vetor_array[a+1]}
    end

    // Calculate Mobility
    for m = 1 to lm do
    vec = RunMacro ("Calculate Mobility",Args.[OutputFolder],
                    Args.Mobility_Metrics.Mode[m],
                    Args.Mobility_Metrics.[Time of Day][m])
    vetor_array[m+la+1] = vec
    out_fields_array[m+la+1] = Args.Mobility_Metrics.[Target Field][m]
    table_fields[m+la+1] = {out_fields_array[m+la+1], "Real",12, 3, !indexed,}
    pairs[m+la+1] = {out_fields_array[m+la+1], vetor_array[m+la+1]}
    end

	// write into .csv
	temp_file = GetTempFileName(".bin") // needs to be .bin first, then export to .csv
    obj = CreateObject("CC.Table")
	table = obj.Create({ FileName: temp_file, FieldSpecs: table_fields, 
						DeleteExisting: True, AddEmptyRecords: v_taz_id.Length})
	view_name2 = table.View   
	SetDataVectors(view_name2 + "|", pairs,)  
	ExportView(view_name2 + "|", "CSV",metrics_taz_file,,{{"CSV Header","True"}})
	CloseView(view_name2)

    return(ok)
endmacro

macro "Calculate Accessibility"(out_dir, mode, tod, threshold, in_file, in_field, bool_type)
//
    /***
    //### Purpose:             Calculate accessibility metrics 
    //##  Flow Chart Location: 
    //##  Note: 
	//## INPUTS: 
                                out_dir
                                mode = highway(default)/"transit"(if specified)
                                tod = am/md
                                threshold = int (in minutes)
                                in_file = .csv file for input data
                                in_field
                                bool_type = 0 (for counts, default)/1 (for presence, if specified)
	//## OUTPUTS:               v_access = accessibility vector by taz_id (returned)
	***/

    // get taz_id and field vectors
    if Args.DryRun = 1 then Return(1)    
    view_name = OpenTable("view name 1","CSV", {in_file, })
    v_taz_id = GetDataVector(view_name+"|", "TAZ_ID", {{"Sort Order", {{"TAZ_ID", "Ascending"}}}}) 
    v_taz_field = GetDataVector(view_name+"|", in_field, {{"Sort Order", {{"TAZ_ID", "Ascending"}}}}) 
    CloseView(view_name)

    // mc.time_bool: whether or not 0 < travel_time <= threshold
    if (mode = "transit") then do
        skim_file = out_dir + "\\_skim\\" + "tw_" + tod + ".mtx"
        mat_handle = OpenMatrix(skim_file,)
        AddOrFindMatrixCore(mat_handle, "travel_time")
        AddOrFindMatrixCore(mat_handle, "time_bool")
        AddOrFindMatrixCore(mat_handle, "accessibility") 
        mc = CreateMatrixCurrencies(mat_handle,,,)
        mc.travel_time := mc.walk + mc.iwait + mc.ivtt + mc.xwait 
        // mc.time_bool := ((mc.travel_time <= threshold) and (mc.travel_time > 0))
        // introducing decimal factor for decay at turning point
        mc.time_bool := Min(Max((threshold - mc.travel_time)/1 + 0.5, 0), 1)
        end
    else do // mode as "highway"
        skim_file = out_dir + "\\_skim\\" + "hwy_" + tod + ".mtx"
        mat_handle = OpenMatrix(skim_file,)
        AddOrFindMatrixCore(mat_handle, "time_bool")
        AddOrFindMatrixCore(mat_handle, "accessibility") 
        mc = CreateMatrixCurrencies(mat_handle,,,)
        // mc.time_bool := ((mc.sr_time <= threshold) and (mc.sr_time > 0))
        // introducing decimal factor for decay at turning point
        mc.time_bool := Min(Max((threshold - mc.sr_time)/1 + 0.5, 0), 1)
    end

    // calulate accessibility_count from taz to taz
    mc.accessibility := mc.time_bool * v_taz_field

    // get vector of from taz (to all)
    mat = CreateObject("Matrix", mat_handle)
    v_access = mat.GetVector({Core: "accessibility", Marginal: "Row Sum"})

    // convert accessibility_count to accessibility_presence (if bool_type=1)
    if (bool_type = 1) then 
        v_access = (v_access > 0)

	// close views and handles
	mat_handle = null

	// Return accessibility vector
	return(v_access)
endmacro

macro "Calculate Mobility"(out_dir, mode, tod)
//
	/*** 
    //### Purpose:              Calculate mobility in terms of average travel time from taz 
    //##  Flow Chart Location: 
    //##  Note: 
	//##  INPUTS: 
                                out_dir
                                mode = highway(default)/"transit"(if specified)
                                tod = am/md
	//## OUTPUTS:                v_avg_out_time = vector of average travel time from taz (returned)
	***/
    if Args.DryRun = 1 then Return(1)
    od_skim_file = out_dir + "\\_demand\\tod\\" + "od_per_" + tod + ".mtx"

    if (mode = "transit") then do
        skim_file = out_dir + "\\_skim\\" + "tw_" + tod + ".mtx"
        mat_handle = OpenMatrix(skim_file,)
        AddOrFindMatrixCore(mat_handle, "travel_time")
        AddOrFindMatrixCore(mat_handle, "simple_VHT") // for tw only
        mc = CreateMatrixCurrencies(mat_handle,,,)
        mc.travel_time := mc.walk + mc.iwait + mc.ivtt + mc.xwait // no need to repeat

        // get vector of "o" (trips from taz (to all))
        od_mat_handle = OpenMatrix(od_skim_file,)
        od_mat = CreateObject("Matrix", od_mat_handle)
        v_o = od_mat.GetVector({Core: "tw", Marginal: "Row Sum"})

        // get matrix currency of travel time
        od_mc = CreateMatrixCurrency(od_mat_handle,"tw",,,)
        end
    else do // mode as "highway"
        skim_file = out_dir + "\\_skim\\" + "hwy_" + tod + ".mtx"
        mat_handle = OpenMatrix(skim_file,)
        AddOrFindMatrixCore(mat_handle, "travel_time")
        AddOrFindMatrixCore(mat_handle, "simple_VHT") // for (auto * sr_time) only
        mc = CreateMatrixCurrencies(mat_handle,,,) 
        mc.travel_time := mc.sr_time // do not fill null with zero

        // get vector of "o" (trips from taz (to all))
        od_mat_handle = OpenMatrix(od_skim_file,)
        od_mat = CreateObject("Matrix", od_mat_handle)
        v_o = od_mat.GetVector({Core: "auto", Marginal: "Row Sum"})

        // get matrix currency of travel time
        od_mc = CreateMatrixCurrency(od_mat_handle,"auto",,,) 
    end

    // calulate simple_VHT from taz to taz
    mc.simple_VHT := mc.travel_time * od_mc

    // get vector of simple_VHT from taz (to all)
    mat = CreateObject("Matrix", mat_handle)
    v_simple_VHT = mat.GetVector({Core: "simple_VHT", Marginal: "Row Sum"})

    // get vector of average_travel_time_for_slected_mode_from_taz
    v_avg_out_time = v_simple_VHT / v_o // null if v_o = 0

	// close handles
	od_mat_handle = null
	mat_handle = null
	
	// Return accessibility vector
	return(v_avg_out_time)
endmacro