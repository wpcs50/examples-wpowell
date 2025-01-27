
//#   "networks.rsc"
macro "Initialize Highway DBD"  (Args)
// Create fields and set values on input highway dbd for highway assignment and skimming
    hwy_dbd              = Args.[Highway]
   if Args.DryRun = 1 then Return(1)
    ok = 1
 
    runmacro("create_link_fields", Args)
    runmacro("set_roadway_link_types", hwy_dbd)
    runmacro("derive_bus_lanes", Args)
    for tod in Args.[TimePeriods] do
        runmacro("set_initial_highway_congested_times", Args, tod)
    end

    // sensitivity tests
    if Args.[Highway Toll Adjustment] != 1.0 then do
        runmacro("adjust_highway_tolls", Args)
    end

    Return(ok)
endmacro

macro "create_link_fields" (Args)
// Add link fields to be populated with model set/derived values
    hwy_dbd              = Args.[Highway]
    time_periods         = Args.[TimePeriods]

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers
    
    lyr_obj = CreateObject("CC.ModifyTableOperation", link_lyr)

    // categorical fields
    lyr_obj.FindOrAddField("a_node", 'Integer (4 bytes)',8,0)
    lyr_obj.FindOrAddField("b_node", 'Integer (4 bytes)',8,0)
    lyr_obj.FindOrAddField("access_density", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("urban", 'Integer (2 bytes)',2,0)    

    // restriction fields
    lyr_obj.FindOrAddField("hov_only_am", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("hov_only_md", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("hov_only_pm", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("hov_only_nt", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("truck_only", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("small_veh_only", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("no_heavy_truck", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("pnr_link", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("transit_only", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("no_walk_bike", 'Integer (2 bytes)',2,0)
    lyr_obj.FindOrAddField("walk_bike_only", 'Integer (2 bytes)',2,0)    
    lyr_obj.FindOrAddField("transit_walk", 'Integer (2 bytes)',2,0)

    // vdf fields
    lyr_obj.FindOrAddField("alpha",     'Real (8 bytes)',12,3)
    lyr_obj.FindOrAddField("beta",      'Real (8 bytes)',12,3)
    lyr_obj.FindOrAddField("ff_speed",  'Real (8 bytes)',12,3)
    lyr_obj.FindOrAddField("ff_time",   'Real (8 bytes)',12,3)
    lyr_obj.FindOrAddField("capacity",  'Integer (4 bytes)',8,0)
    lyr_obj.FindOrAddField("walk_time", 'Real (8 bytes)',12,3)
    lyr_obj.FindOrAddField("auto_cost", 'Real (8 bytes)',12,3) 
    
    // transit parking capacity fields
    lyr_obj.FindOrAddField("pnr_node", 'Real (8 bytes)',12,3)
    lyr_obj.FindOrAddField("pnr_capacity", 'Integer (4 bytes)',8,0) 
    lyr_obj.FindOrAddField("pnr_pfe_trips", 'Real (8 bytes)',12,3)       
    lyr_obj.FindOrAddField("pnr_shadow_cost",'Real (8 bytes)',12,3)

    for tod in time_periods do
        lyr_obj.FindOrAddField("ab_capacity_" + tod, 'Integer (4 bytes)',8,0)
        lyr_obj.FindOrAddField("ba_capacity_" + tod, 'Integer (4 bytes)',8,0)
        lyr_obj.FindOrAddField("ab_lanes_" + tod, 'Real (4 bytes)',6,2)
        lyr_obj.FindOrAddField("ba_lanes_" + tod, 'Real (4 bytes)',6,2)
    end

    // highway assignment times and transit network attributes
    // only nneded for peak (am) and non-peak (md)
    for tod in time_periods do
        lyr_obj.FindOrAddField("ab_MSAtime_" + tod,'Real (8 bytes)',12,3)    
        lyr_obj.FindOrAddField("ba_MSAtime_" + tod,'Real (8 bytes)',12,3)
        lyr_obj.FindOrAddField("ab_MSAtime_cost_" + tod,'Real (8 bytes)',12,3)    
        lyr_obj.FindOrAddField("ba_MSAtime_cost_" + tod,'Real (8 bytes)',12,3)                    
        lyr_obj.FindOrAddField("ab_trn_time_" + tod,'Real (8 bytes)',12,3)    
        lyr_obj.FindOrAddField("ba_trn_time_" + tod,'Real (8 bytes)',12,3)        
    end       
    
    lyr_obj.Apply()    

endmacro

macro "set_roadway_link_types" (hwy_dbd)
// Set attributes on roadway layer useful for highway assignment

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    // set anode and bnode fields
    anode = CreateNodeField(link_lyr, "node_from", "Nodes.ID", "From", )
    bnode = CreateNodeField(link_lyr, "node_to", "Nodes.ID", "To", )
    anode_v = GetDataVector(link_lyr + "|", anode,)
    bnode_v = GetDataVector(link_lyr + "|", bnode,)

    SetDataVector(link_lyr + "|","a_node",anode_v,)            
    SetDataVector(link_lyr + "|","b_node",bnode_v,)  

    // set link types   
    fc_v = GetDataVector(link_lyr + "|", "func_class",)
    peak_hov_v = GetDataVector(link_lyr + "|", "peak_hov",)
    trk_size_v = GetDataVector(link_lyr + "|", "max_truck_size",)
    
    zero_v = Vector(fc_v.length, "Integer", {{"Constant", 0}})
    one_v = Vector(fc_v.length, "Integer", {{"Constant", 1}})
    seventy_v = Vector(fc_v.length, "Integer", {{"Constant", 70}})

    no_wb_v = if(fc_v = 1 |
                 fc_v = 2 | 
                (   fc_v > 50 & 
                    fc_v <> 90)) then one_v else zero_v
    only_wb_v = if(fc_v = 20 |
                    fc_v = 21 | 
                    fc_v = 30 | 
                    fc_v = 40 | 
                    fc_v = 41 | 
                    fc_v = 42) then one_v else zero_v
    only_trk_v  = if(fc_v = 60) then one_v else zero_v
    only_auto_v = if((trk_size_v = 1 | trk_size_v = 2) & only_wb_v = 0) then one_v else zero_v
    no_htrk_v   = if(trk_size_v = 3) then one_v else zero_v
    pnr_v       = if(fc_v = 70) then one_v else zero_v
    only_trn_v  = if(fc_v = 71 |
                    fc_v = 100 | 
                    fc_v = 101 | 
                    fc_v = 102) then one_v else zero_v

    // hov by time of day
    only_hov_am_v = if(peak_hov_v = 1 | peak_hov_v = 3) then one_v else zero_v
    only_hov_pm_v = if(peak_hov_v = 2 | peak_hov_v = 3) then one_v else zero_v

    only_hov_daily_v  = if(fc_v = 50 & peak_hov_v = 0) then one_v else zero_v
    only_hov_am_v = min(only_hov_am_v + only_hov_daily_v, 1)
    only_hov_pm_v = min(only_hov_pm_v + only_hov_daily_v, 1)    

    trn_wlk_v = seventy_v // set all links to walk mode, will use no_walk_bike to restrict

    SetDataVector(link_lyr + "|","no_walk_bike",no_wb_v,)
    SetDataVector(link_lyr + "|","walk_bike_only",only_wb_v,)
    SetDataVector(link_lyr + "|","hov_only_am",only_hov_am_v,)
    SetDataVector(link_lyr + "|","hov_only_md",only_hov_daily_v,)
    SetDataVector(link_lyr + "|","hov_only_pm",only_hov_pm_v,)
    SetDataVector(link_lyr + "|","hov_only_nt",only_hov_daily_v,)
    SetDataVector(link_lyr + "|","truck_only",only_trk_v,)
    SetDataVector(link_lyr + "|","small_veh_only",only_auto_v,)
    SetDataVector(link_lyr + "|","no_heavy_truck",no_htrk_v,)
    SetDataVector(link_lyr + "|","pnr_link",pnr_v,)
    SetDataVector(link_lyr + "|","transit_only",only_trn_v,)
    SetDataVector(link_lyr + "|","transit_walk",trn_wlk_v,)

endmacro


macro "adjust_highway_tolls" (Args)
// Adjust highway tolls by factor
    hwy_dbd             = Args.[Highway]
    toll_adj            = Args.[Highway Toll Adjustment]

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    auto_v = GetDataVector(link_lyr + "|", "toll_auto",)
    ltrk_v = GetDataVector(link_lyr + "|", "toll_lt_trk",)
    mtrk_v = GetDataVector(link_lyr + "|", "toll_md_trk",)
    htrk_v = GetDataVector(link_lyr + "|", "toll_hv_trk",)

    auto_v = auto_v * toll_adj
    ltrk_v = ltrk_v * toll_adj
    mtrk_v = mtrk_v * toll_adj
    htrk_v = htrk_v * toll_adj

    SetDataVector(link_lyr + "|","toll_auto",auto_v,)
    SetDataVector(link_lyr + "|","toll_lt_trk",ltrk_v,)
    SetDataVector(link_lyr + "|","toll_md_trk",mtrk_v,)
    SetDataVector(link_lyr + "|","toll_hv_trk",htrk_v,)

endmacro

macro "derive_bus_lanes" (Args)
// Derive change in number of lanes based on bus lane code
    hwy_dbd              = Args.[Highway]
    bus_lane_file        = Args.[Bus Lane Definitions]

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    id_v = GetDataVector(link_lyr + "|", "id",)
    bl_v = nz(GetDataVector(link_lyr + "|", "bus_lane",))

    zero_v = Vector(id_v.length, "Integer", {{"Constant", 0}})
    one_v = Vector(id_v.length, "Integer", {{"Constant", 1}})

    if (!Args.[Enable Bus Lanes]) then do 
        bl_v = zero_v
    end

    tod_v    = floor(bl_v / 100) //10^2 = time of day
    dir_v    = floor(((bl_v - tod_v * 100)/10)) // 10^1 = direction 
    gp_v   = if(bl_v = 0) then bl_v else mod(bl_v, 2) // 10^0 = lane used
    
    am_v = if(tod_v < 3 | tod_v = 4) then one_v else zero_v
    pm_v = if(tod_v = 1 | tod_v > 2) then one_v else zero_v
    np_v = if(tod_v = 1) then one_v else zero_v

    ab_v = if(dir_v = 1 | dir_v = 2) then one_v else zero_v
    ba_v = if(dir_v = 1 | dir_v = 3) then one_v else zero_v

    // peak direction (4 - ab inbound; 5 - ba inbound)
    pk1dir_v = if(tod_v = 4 & dir_v = 4) then one_v else zero_v
    pk2dir_v = if(tod_v = 4 & dir_v = 5) then one_v else zero_v

    // vectors indicating if a bus lane is active by direction and tod
    am_ab_v = am_v * ab_v + pk1dir_v
    am_ba_v = am_v * ba_v + pk2dir_v
    md_ab_v = np_v * ab_v
    md_ba_v = np_v * ba_v    
    pm_ab_v = pm_v * ab_v + pk2dir_v
    pm_ba_v = pm_v * ba_v + pk1dir_v    
    nt_ab_v = np_v * ab_v
    nt_ba_v = np_v * ba_v

    bus_lane_vw = CreateTable("BusLane", bus_lane_file, "FFB",
        {	{"id",          "Integer", 8, 0, "True"},
            {"bl",          "Integer", 4, 0, },
            {"bl_gp",       "Integer", 2, 0, },
            {"bl_am_ab",    "Integer", 2, 0, },
            {"bl_am_ba",    "Integer", 2, 0, },
            {"bl_md_ab",    "Integer", 2, 0, },
            {"bl_md_ba",    "Integer", 2, 0, },
            {"bl_pm_ab",    "Integer", 2, 0, },
            {"bl_pm_ba",    "Integer", 2, 0, },
            {"bl_nt_ab",    "Integer", 2, 0, },
            {"bl_nt_ba",    "Integer", 2, 0, }} )

    rh = AddRecords(bus_lane_vw, null, null, {{"Empty Records", id_v.Length}})

    SetDataVector(bus_lane_vw + "|","id"     ,id_v,)
    SetDataVector(bus_lane_vw + "|","bl"     ,bl_v,)
    SetDataVector(bus_lane_vw + "|","bl_gp"     ,gp_v,)
    SetDataVector(bus_lane_vw + "|","bl_am_ab"  ,am_ab_v,)
    SetDataVector(bus_lane_vw + "|","bl_am_ba"  ,am_ba_v,)
    SetDataVector(bus_lane_vw + "|","bl_md_ab"  ,md_ab_v,)
    SetDataVector(bus_lane_vw + "|","bl_md_ba"  ,md_ba_v,)
    SetDataVector(bus_lane_vw + "|","bl_pm_ab"  ,pm_ab_v,)
    SetDataVector(bus_lane_vw + "|","bl_pm_ba"  ,pm_ba_v,)
    SetDataVector(bus_lane_vw + "|","bl_nt_ab"  ,nt_ab_v,)
    SetDataVector(bus_lane_vw + "|","bl_nt_ba"  ,nt_ba_v,)
    CloseView("BusLane")

endmacro

macro "set_initial_highway_congested_times" (Args, tod)
// Set initial congested times from input file
    hwy_dbd              = Args.[Highway]
    ff_speed             = Args.[Free Flow Speed]
    speed_file           = Args.("Init Speeds - " + tod)
    ok = 1

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    length_v = GetDataVector(link_lyr + "|", "length",)
    ffspd_input_v = GetDataVector(link_lyr + "|", "ff_speed_input",)
    ff_spd_v = if(nz(ffspd_input_v) > 0) then ffspd_input_v else ff_speed

    // only use inputs for am and md, pm and nt are set in speed feedback
    if (({"am", "md"} contains tod) & GetFileInfo(speed_file) <> null) then do 
        spd_input_vw = OpenTable("SpeedInput", "FFB", {speed_file})
        link_spd_vw = JoinViews("link_speed", link_lyr + ".id", spd_input_vw + ".ID1",)

        // if input speed is non-zero, use it, else use free flow speed
        for dir in {"ab", "ba"} do 
            in_spd_v = GetDataVector(link_spd_vw + "|", dir + "_Speed",) 
            spd_v = if(nz(in_spd_v) > 0) then in_spd_v else ff_spd_v

            time_v = (length_v / spd_v) * 60
            SetDataVector(link_spd_vw + "|", dir + "_MSAtime_" + tod, time_v,) 
        end
    end 
    else do // pm and nt
        for dir in {"ab", "ba"} do 
            time_v = (length_v / ff_spd_v) * 60
            SetDataVector(link_lyr + "|", dir + "_MSAtime_" + tod, time_v,)
        end
    end

    return (ok)

endmacro

// build network
macro "Build Highway Network" (Args)
// Helper function to build highway networks by time of day
    hwy_dbd              = Args.[Highway]
    pen_tab              = Args.[Turn Penalties]    
    out_dir              = Args.OutputFolder   
    time_periods         = Args.[TimePeriods]   
   if Args.DryRun = 1 then Return(1)

    ok = 1

    for tod in time_periods do
        hwy_net = Args.("Highway Net - " + tod)
        ok = runmacro("build_highway_network", Args, hwy_dbd, hwy_net, tod, pen_tab)
    end

    return(ok)

endmacro


// build the highway network file
macro "build_highway_network" (Args, hwy_dbd, hwy_net, tod, pen_tab)
// Builds and sets the highway network

    gl_tpen = Args.global_turn_pen
    lt_tpen = Args.[Link Type Penalties]

    netObj = CreateObject("Network.Create")
    netObj.LayerDB = hwy_dbd
    netObj.LengthField = "Length"
    
    filter = "available > 0 & transit_only = 0 & walk_bike_only = 0"
    netObj.Filter = filter

    netObj.AddLinkField({Name: "ff_time", Field: "ff_time", IsTimeField : true, DefaultValue: 1})
    netObj.AddLinkField({Name: "time", Field: {"ab_MSAtime_" + tod, "ba_MSAtime_" + tod}, IsTimeField : true, DefaultValue: 1})
    netObj.AddLinkField({Name: "alpha", Field: "alpha", DefaultValue: 0.15})
    netObj.AddLinkField({Name: "beta", Field: "beta", DefaultValue: 4})

    netObj.AddLinkField({Name: "capacity", Field: {"ab_capacity_" + tod, "ba_capacity_" + tod}, DefaultValue: 1800})
    
    netObj.AddLinkField({Name: "toll_auto", Field: "toll_auto"})
    netObj.AddLinkField({Name: "toll_lt_trk", Field: "toll_lt_trk"})
    netObj.AddLinkField({Name: "toll_md_trk", Field: "toll_md_trk"})
    netObj.AddLinkField({Name: "toll_hv_trk", Field: "toll_hv_trk"})
    netObj.AddLinkField({Name: "func_class", Field: "func_class"})
    netObj.LinkTypeInfo({Label: "TYPENO", LayerField: "fac_type"})
    netObj.OutNetworkName = hwy_net
    ret_value = netObj.Run()
    netHandle = netObj.GetResults().Data.NetworkHandle

    // Set network
    netSetObj = null
    netSetObj = CreateObject("Network.Settings", {Network: netHandle})
    netSetObj.CentroidFilter = "int_zone = 1 | ext_zone = 1"
    netSetObj.LinkTollFilter = "toll_auto > 0"
    //netSetObj.EntryExitTollFilter = TollCorridorFilter
    Penalties = {Table: pen_tab, PenaltyField: "Penalty",  // link specific in table
                PenaltyByLinkType: lt_tpen, // link type penalties
                Left: gl_tpen.("Left").Value, 
                Right: gl_tpen.("Right").Value, 
                Through: gl_tpen.("Through").Value, 
                Uturn: gl_tpen.("Uturn").Value} // global turn penalties
    netSetObj.SetPenalties(Penalties)    
    netSetObj.UseLinkTypes = true
    ret_value = netSetObj.Run()

    return(ret_value)
endmacro

// build network
macro "Update Highway Network" (Args)
// Helper function to build highway networks by time of day
    hwy_dbd              = Args.[Highway]
    pen_tab              = Args.[Turn Penalties]    
    out_dir              = Args.OutputFolder   
    time_periods         = Args.[TimePeriods]   
   if Args.DryRun = 1 then Return(1)

    ok = 1

    for tod in time_periods do
        hwy_net = Args.("Highway Net - " + tod)
        ok = runmacro("update_highway_network", Args, hwy_net, tod)
    end

    return(ok)

endmacro

// update the highway network file
macro "update_highway_network" (Args, hwy_net, tod)
// Updates and sets the highway network with new congested times

    netObj = CreateObject("Network.Update", {Network: hwy_net}) 
    netObj.UpdateLinkField({Name: "time", Field: {"ab_MSAtime_" + tod, "ba_MSAtime_" + tod}, IsTimeField : true, DefaultValue: 1})  
    ok = netObj.Run()

endmacro


macro "Roadway VDF" (Args)
// Set roadway capacity, alpha, beta parameters
    hwy_dbd              = Args.[Highway]

   if Args.DryRun = 1 then Return(1)

    ok = 1

    runmacro("set_link_area_type", Args)
    runmacro("initialize_roadway_link_vdf", Args)

    return(ok)

endmacro


macro "set_link_area_type" (Args)
// Set roadway link access density and urbanized area
    hwy_dbd         = Args.[Highway] 
    out_dir         = Args.OutputFolder   

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers    

    // set area type
    accden_file = out_dir + "\\_networks\\access_density.csv"
    accden_vw = OpenTable("AT", "CSV", {accden_file})
    link_at_vw = JoinViews("link_at", link_lyr + ".taz_id", accden_vw + ".taz_id",)

    at_v = GetDataVector(link_at_vw + "|", accden_vw + ".access_density",)            
    SetDataVector(link_at_vw + "|",link_lyr + ".access_density",at_v,)

    // set uza
    taz_file = out_dir + "\\_networks\\taz_index.csv"
    taz_vw = OpenTable("taz", "CSV", {taz_file})
    link_taz_vw = JoinViews("link_taz", link_lyr + ".taz_id", taz_vw + ".taz_id",)
    // there is a repeated field 'urban' in the view 
    urb_v = GetDataVector(link_taz_vw + "|", GetFieldFullSpec(link_taz_vw, "Taz.urban"),)            
    SetDataVector(link_taz_vw + "|", GetFieldFullSpec(link_lyr, "Links.urban"),urb_v,)

endmacro

macro "initialize_roadway_link_vdf" (Args)
// Set roadway link vdf parameters (setup for highway assignment)
    hwy_dbd         = Args.[Highway] 
    bus_lane_file   = Args.[Bus Lane Definitions]
    spd_cap_file    = Args.[Speed-capacity Filename]
    abc_inputs      = Args.vdf_abc_inputs
    ff_speed        = Args.[Free Flow Speed] 
    walk_speed      = Args.[Walk Speed] 
    time_periods    = Args.[TimePeriods]
    cap_facts       = Args.[Capacity Factors]
    shld_benefit    = Args.[Shoulder Lane Equivalent]

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    // set alpha, beta, capacity attributes by lookup table
    spd_cap_vw = OpenTable("SpdCap", "CSV", {spd_cap_file}) 
    link_cap_vw = JoinViewsMulti("link_cap", {link_lyr + ".fac_type",link_lyr + ".urban"}, {spd_cap_vw + ".fac_type",spd_cap_vw + ".urban"},)    

    alpha_lu_v = GetDataVector(link_cap_vw + "|", "alpha_lu",)
    beta_lu_v = GetDataVector(link_cap_vw + "|", "beta_lu",)
    cap_lu_v = GetDataVector(link_cap_vw + "|", "capacity_lu",)

    alpha_v = alpha_lu_v
    beta_v = beta_lu_v   
    cap_v = cap_lu_v

    if abc_inputs != 0 then do 
        alpha_input_v = GetDataVector(link_cap_vw + "|", "alpha_input",)
        alpha_v = if (nz(alpha_input_v) > 0) then alpha_input_v else alpha_v
        
        beta_input_v = GetDataVector(link_cap_vw + "|", "beta_input",)
        beta_v = if (nz(beta_input_v) > 0) then beta_input_v else beta_v

        cap_input_v = GetDataVector(link_cap_vw + "|", "capacity_input",)
        cap_v = if (nz(cap_input_v) > 0) then cap_input_v else cap_v
    end

    SetDataVector(link_cap_vw + "|","alpha",alpha_v,)
    SetDataVector(link_cap_vw + "|","beta",beta_v,)       
    SetDataVector(link_cap_vw + "|","capacity",cap_v,)       

    // free-flow speed uses the input value, or posted, then lookup

    ffspd_input_v = GetDataVector(link_cap_vw + "|", "ff_speed_input",)
    postspd_v = GetDataVector(link_cap_vw + "|", "posted_speed",)
    ffspd_lu_v = GetDataVector(link_cap_vw + "|", "speed_lu",)
    ffspd_v = if (nz(ffspd_input_v) > 0) then ffspd_input_v else 
                if (nz(postspd_v) > 0) then postspd_v else 
                if (nz(ffspd_lu_v) > 0) then ffspd_lu_v else ff_speed
    SetDataVector(link_cap_vw + "|","ff_speed",ffspd_v,)  
   
    // free flow time
    length_v = GetDataVector(link_lyr + "|", "length",)
    ff_time_v = (length_v / ffspd_v) * 60
    SetDataVector(link_lyr + "|","ff_time",ff_time_v,) 

    // walk time
    wlk_input_v = GetDataVector(link_lyr + "|", "walk_time_input",)
    wlk_time_v = if (nz(wlk_input_v) > 0) then wlk_input_v else 
        (length_v / walk_speed) * 60     
    SetDataVector(link_lyr + "|","walk_time",wlk_time_v,) 

    // set effective number of lanes with bus lane
    // TODO: error flag if effective number of lanes is zero wehre numlanes > 0
    bus_lane_vw = OpenTable("BusLane", "FFB", {bus_lane_file})
    link_bus_vw = JoinViews("link_buslanes", link_lyr + ".id", bus_lane_vw + ".id",)

    gp_v = GetDataVector(link_bus_vw + "|", "bl_gp",)

    for dir in {"ab", "ba"} do         
    
        num_ln_v = GetDataVector(link_bus_vw + "|", dir + "_lanes",)

        for tod in time_periods do

            bl_v = GetDataVector(link_bus_vw + "|", "bl_" + tod + "_" + dir,)
                       
            // take a lane if active and uses general purpose - ONLY if there is a lane to take
            eff_ln_v = if(num_ln_v > 1) then (num_ln_v - (bl_v * gp_v)) else num_ln_v
            SetDataVector(link_bus_vw + "|", dir + "_lanes_" + tod,eff_ln_v,)
            
        end
    end
    CloseView(link_bus_vw)

    // set effective number of lanes with shoulder use
    shoulder_v = GetDataVector(link_lyr + "|", "shoulder_use",)

    for dir in {"ab", "ba"} do 
        num_ln_am_v = GetDataVector(link_lyr + "|", dir + "_lanes_am",)
        num_ln_pm_v = GetDataVector(link_lyr + "|", dir + "_lanes_pm",)

        eff_ln_am_v = if (shoulder_v = 1) then num_ln_am_v + shld_benefit else num_ln_am_v
        eff_ln_pm_v = if (shoulder_v = 2) then num_ln_pm_v + shld_benefit else num_ln_pm_v

        SetDataVector(link_lyr + "|", dir + "_lanes_am",eff_ln_am_v,)
        SetDataVector(link_lyr + "|", dir + "_lanes_pm",eff_ln_pm_v,)
    end
    
    // set effective number of lanes with zipper lane use
    zipper_v = GetDataVector(link_lyr + "|", "peak_link",)

    for dir in {"ab", "ba"} do
        for tod_peak_code in {{"am", 3}, {"pm", 4}} do
            tod = tod_peak_code[1]
            peak_code = tod_peak_code[2] 

            num_ln_v = GetDataVector(link_lyr + "|", dir + "_lanes_" + tod,)
            eff_ln_v = if (zipper_v = peak_code) then (num_ln_v - 1) else num_ln_v
            SetDataVector(link_lyr + "|", dir + "_lanes_" + tod, eff_ln_v,)
        end
    end

    // per period capacity
    for dir in {"ab", "ba"} do         
        capacity_v = GetDataVector(link_lyr + "|", "capacity",)

        for tod in time_periods do
            tod_cap_fct = cap_facts.(tod).CapacityFactor
            num_ln_v = GetDataVector(link_lyr + "|", dir + "_lanes_" + tod,)
            
            per_cap_v = capacity_v * tod_cap_fct * num_ln_v
            SetDataVector(link_lyr + "|", dir + "_capacity_" + tod,per_cap_v,)
            
        end
    end
endmacro


macro "Update Congested Times - Feedback" (Args)
// Update congested times after AM and MD highway assignment
    out_dir              = Args.OutputFolder                       
    hwy_dbd              = Args.[Highway]       
   if Args.DryRun = 1 then Return(1)                 

    ok = 1
    for tod in {"am","md"} do 
        hwy_net = Args.("Highway Net - " + tod)
        ok = runmacro("update_highway_congested_times", hwy_dbd, hwy_net, tod)
    end

    return(ok)

endmacro

macro "Update Congested Times - Final" (Args)
// Update congested times after PM and NT highway assignment
    out_dir              = Args.OutputFolder                       
    hwy_dbd              = Args.[Highway]       
   if Args.DryRun = 1 then Return(1)                  

    ok = 1
    for tod in {"pm","nt"} do 
        hwy_net = Args.("Highway Net - " + tod)  
        ok = runmacro("update_highway_congested_times", hwy_dbd, hwy_net, tod)
    end

    return(ok)

endmacro

// pull congested times from networks and attach to dbd
macro "update_highway_congested_times" (hwy_dbd, hwy_net, tod)
// Attach congested times from highway assignment to update transit times
    ok = 1

    outputfile = GetTempFileName(".bin")
    //outputfile = "C:\\Projects\\temp\\temp.bin" //GetTempFileName(".bin")
    Options = null
    Options.[Flow Fields] = {"__MSATime"}
    Options.[Write To] = {outputfile, "FFB", "test" + tod}
    ActiveNetwork = ReadNetwork(hwy_net)
    net_vw = CreateTableFromNetworkVars(ActiveNetwork, Options)

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers
    hwy_time_vw = JoinViews("hwy_time", link_lyr + ".id", net_vw + ".id1",)    

    for dir in {"ab", "ba"} do 
        time_v = GetDataVector(hwy_time_vw + "|", dir + "___MSATime",)
        SetDataVector(hwy_time_vw + "|", dir + "_MSAtime_" + tod, time_v,)
    end

    return (ok)

endmacro

macro "Initialize Transit RTS" (Args)
// Add fields and initialize values for transit route system
    hwy_dbd              = Args.[Highway]
    out_dir              = Args.OutputFolder     
    run_mode             = Args.[TransitParking]
    init_pnr_demand      = Args.[Init PnR Demand - am]    
    if Args.DryRun = 1 then Return(1)        
    ok = 1

    // add fields
    runmacro("create_route_fields", Args)

    // SENS_TESTS
    if Args.[Transit HRT Time Adjustment] <> 1.0 then do
        runmacro("adjust_hrt_headway",Args)
    end

    // calculate transit pnr costs - only done for AM
    ok = runmacro("set_pnr_node_capacity", hwy_dbd)

    // confirm warm start is available, otherwise demand will be zero as init
    if (GetFileInfo(init_pnr_demand) <> null) then do
        ok = runmacro("set_pnr_pfe_trips", Args, init_pnr_demand, 0)
    end

    ok = runmacro("calc_pnr_shadow_cost", Args, hwy_dbd, run_mode)
   
    if (({"DEBUG"} contains Args.loglevel)) then do 
        runmacro("log_pnr_demand_cost", out_dir, hwy_dbd, 0)
    end

    return(ok)
endmacro

macro "create_route_fields" (Args)
// Add route fields to be populated with model set/derived values
    trn_rts         = Args.[Transit] 
    time_periods    = Args.[TimePeriods]  

    objLyrs         = CreateObject("AddRSLayers", {FileName: trn_rts})
    rs_layer        = objLyrs.RouteLayer
    
    lyr_obj = CreateObject("CC.ModifyTableOperation", rs_layer)

    // categorical fields
    for tod in time_periods do 
        lyr_obj.FindOrAddField("iwait_" + tod, 'Real (8 bytes)',12,3)
    end

    lyr_obj.Apply() 

endmacro

macro "adjust_hrt_headway" (Args)
// adjust heavy rail headways for sensitivity testing
    trn_rts         = Args.[Transit] 
    time_periods    = Args.[TimePeriods]   
    hrt_adj         = Args.[Transit HRT Time Adjustment]
    objLyrs         = CreateObject("AddRSLayers", {FileName: trn_rts})
    rs_layer        = objLyrs.RouteLayer

    SetView(rs_layer)
    SelectByQuery("hrt", "Several", "Select * where mode = 5", ) 

    for tod in time_periods do 
        hd_v = GetDataVector(rs_layer + "|hrt", "headway_" + tod,)
        hd_v = hd_v * hrt_adj
        SetDataVector(rs_layer + "|hrt","headway_" + tod,hd_v,)  
    end

endmacro

macro "set_pnr_node_capacity" (hwy_dbd)
// Associates PnR node attributes to PnR links

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
        {node_lyr, link_lyr} = objLyrs.Layers

    a_v = GetDataVector(link_lyr + "|", "a_node",)
    b_v = GetDataVector(link_lyr + "|", "b_node",)
    neg_v = Vector(a_v.length, "Integer", {{"Constant", -1}})

    // determine PnR lot id
    a_pnr = CreateNodeField(link_lyr, "node_from", "Nodes.pnr_lot", "From", )
    b_pnr = CreateNodeField(link_lyr, "node_to", "Nodes.pnr_lot", "To", )
    apnr_v = GetDataVector(link_lyr + "|", a_pnr,)
    bpnr_v = GetDataVector(link_lyr + "|", b_pnr,)

    pnr_v = if (nz(apnr_v) > 0) then a_v 
            else if (nz(bpnr_v) > 0) then b_v else neg_v
    SetDataVector(link_lyr + "|", "pnr_node", pnr_v,)

    // set parking capacity
    link_node = JoinViews("link_node", 
                    GetFieldFullSpec(link_lyr, "pnr_node"), 
                    GetFieldFullSpec(node_lyr, "ID"), )
    
    SetView(link_node)
    SelectByQuery("pnr_lots", "Several", "Select * where walk_bike_only = 0 & pnr_lot > 0 & parking > 0", ) 

    cap_v = GetDataVector(link_node + "|pnr_lots", "parking",) 
    SetDataVector(link_node + "|pnr_lots", "pnr_capacity",cap_v,)

    return(1)

endmacro


macro "calc_pnr_shadow_cost" (Args, hwy_dbd, run_mode)
// Updates and sets shadow cost to PnR lots
    ok = 1
    pnr = Args.transit_pnr_pfe
    alpha = pnr.("Alpha").Value
    beta = pnr.("Beta").Value
    pnr_occ = pnr.("PnROccupancy").Value
    max_factor = pnr.("MaxFactor").Value

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    cap_v = GetDataVector(link_lyr + "|", "pnr_capacity",) 
    zero_v = Vector(cap_v.length, "Integer", {{"Constant", 0}})

    if (run_mode = "Unconstrained PnR Parking") then pnr_shd_v = zero_v 
    else do
        dem_v = GetDataVector(link_lyr + "|", "pnr_pfe_trips",)
        pnr_shd_v = if (nz(cap_v)>0) 
            then 
                min(round((alpha * pow((nz(dem_v) / (cap_v * pnr_occ)), beta)),3), max_factor) 
            else 0
    end

    SetDataVector(link_lyr + "|", "pnr_shadow_cost",pnr_shd_v,)

    return(ok)
endmacro

// initialize transit dbd
macro "Set Transit Link Time" (Args)
// Sets times on transit specific links
    hwy_dbd              = Args.[Highway]
   if Args.DryRun = 1 then Return(1)    
    ok = 1

    ok = runmacro("set_transit_time_input", hwy_dbd)

    return(ok)

endmacro

macro "set_transit_time_input" (hwy_dbd)
// Set transit time input for all transit only links
    ok = 1

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    ff_time_v = GetDataVector(link_lyr + "|", "ff_time",)

    // set transit time input to free flow speed for transit only links
    trn_time_input_v =  GetDataVector(link_lyr + "|", "transit_time_input",)
    trn_time_input_v = if (only_trn_v > 0 & nz(trn_time_input_v) = 0) then 
                        ff_time_v else trn_time_input_v
    SetDataVector(link_lyr + "|", "transit_time_input", trn_time_input_v,)

    return(ok)
endmacro

// initialize transit dbd
macro "Set Transit Link Impedance" (Args)
// Sets transit times for transit assignment and skimming
    hwy_dbd              = Args.[Highway]
    out_dir              = Args.OutputFolder     
    bus_lane_file        = Args.[Bus Lane Definitions] 
    trn_spd_factor       = Args.[Transit Speed Factors]
    vot                  = Args.[Value Of Time] // $ per ivtt min
    drv_time_fact        = Args.[TransitPath_GlobalWeights].("DriveTimeFactor").Value // ivtt min per drive min
    drv_time_val         = vot * drv_time_fact // $ per drive min    

   if Args.DryRun = 1 then Return(1)    
    ok = 1

    for tod in Args.[TimePeriods] do

        // calculate transit mode travel times
        ok = runmacro("calc_transit_link_time", hwy_dbd, bus_lane_file, trn_spd_factor, tod)

        // calculate transit auto impedance (time + cost)
        ok = runmacro("calc_auto_link_impedance", hwy_dbd, drv_time_val, tod)

    end

    return(ok)

endmacro

macro "calc_transit_link_time" (hwy_dbd, bus_lane_file, trn_spd_factor, tod)
// Derive transit link travel time from inputs and congested times
    ok = 1 

    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    // set attributes
    bus_lane_vw = OpenTable("BusLane", "FFB", {bus_lane_file})
    link_bus_vw = JoinViews("link_buslanes", link_lyr + ".id", bus_lane_vw + ".id",)
    
    fc_v =              GetDataVector(link_bus_vw + "|", "func_class",)
    bl_v =              GetDataVector(link_bus_vw + "|", "bl",)
    trn_time_input_v =  GetDataVector(link_bus_vw + "|", "transit_time_input",)
    ff_time_v =         GetDataVector(link_lyr + "|", "ff_time",)
 
    ab_time_v =        GetDataVector(link_bus_vw + "|", "ab_MSAtime_" + tod,)
    ba_time_v =        GetDataVector(link_bus_vw + "|", "ba_MSAtime_" + tod,)
    bl_ab =             GetDataVector(link_bus_vw + "|", "bl_" + tod + "_ab",)
    bl_ba =             GetDataVector(link_bus_vw + "|", "bl_" + tod + "_ba",)

    // only use input transit time if 
    //      a. not a bus lane or 
    //      b. a bus lane enabled by direction and tod
    //
    // if a bus lane is set and no transit time set, use free flow time
    // 
    ab_trn_time_v = if(bl_v > 0) then
                    if (trn_time_input_v > 0) then trn_time_input_v * bl_ab else ff_time_v * bl_ab
                    else trn_time_input_v
    ba_trn_time_v = if(bl_v > 0) then
                    if (trn_time_input_v > 0) then trn_time_input_v * bl_ba else ff_time_v * bl_ba
                    else trn_time_input_v

    //Congested Speed Factors
    ab_trn_time_v = if (nz(ab_trn_time_v) > 0) then ab_trn_time_v else ab_time_v / trn_spd_factor
    ba_trn_time_v = if (nz(ba_trn_time_v) > 0) then ba_trn_time_v else ba_time_v / trn_spd_factor
    ab_trn_time_v = if (nz(ab_trn_time_v) > 0) then ab_trn_time_v else 0.001
    ba_trn_time_v = if (nz(ba_trn_time_v) > 0) then ba_trn_time_v else 0.001

    SetDataVector(link_bus_vw + "|","ab_trn_time_" + tod,   ab_trn_time_v,)
    SetDataVector(link_bus_vw + "|","ba_trn_time_" + tod,   ba_trn_time_v,)

    return (ok)
endmacro

macro "calc_auto_link_impedance" (hwy_dbd, drv_time_val, tod)
// Calculate travel times and costs for transit auto impedance

    ok = 1
    objLyrs = CreateObject("AddDBLayers", {FileName: hwy_dbd})
    {node_lyr, link_lyr} = objLyrs.Layers

    toll_v =            GetDataVector(link_lyr + "|", "toll_auto",)
    pnr_v =             GetDataVector(link_lyr + "|", "pnr_parking_cost",)
    pnr_pen_v =         GetDataVector(link_lyr + "|", "pnr_penalty",)
    pnr_sh_v =          GetDataVector(link_lyr + "|", "pnr_shadow_cost",)
    ab_time_v =        GetDataVector(link_lyr + "|", "ab_MSAtime_" + tod,)
    ba_time_v =        GetDataVector(link_lyr + "|", "ba_MSAtime_" + tod,)
    
    cost_v = nz(toll_v) + nz(pnr_v) + nz(pnr_pen_v)
    
    // only include shadow cost for AM and MD (not PM and NT)
    if ({"am","md"} contains tod) then cost_v = cost_v + nz(pnr_sh_v)

    SetDataVector(link_lyr + "|","auto_cost", cost_v,)

    // drive component of transit-auto trips (combine travel time with cost)
    ab_time_cost_v = nz(ab_time_v) + (nz(cost_v) / drv_time_val)
    ba_time_cost_v = nz(ba_time_v) + (nz(cost_v) / drv_time_val)
    ab_time_cost_v = if (nz(ab_time_cost_v) > 0) then ab_time_cost_v else 0.001
    ba_time_cost_v = if (nz(ba_time_cost_v) > 0) then ba_time_cost_v else 0.001

    SetDataVector(link_lyr + "|","ab_MSAtime_cost_" + tod, ab_time_cost_v,)
    SetDataVector(link_lyr + "|","ba_MSAtime_cost_" + tod, ba_time_cost_v,)

    return (ok)
endmacro

macro "Build PK NP Transit Network" (Args)
// Helper function to build transit networks by time of day
    trn_rts              = Args.[Transit]
    out_dir              = Args.OutputFolder      
   if Args.DryRun = 1 then Return(1) 
    ok = 1

    // build network
    for tod in {"am","md"} do 
        trn_net = runmacro("get_transit_network_file", out_dir, tod)
        runmacro("build_transit_network", trn_rts, trn_net, tod)
        runmacro("set_stop_to_stop_runtimes", trn_rts, trn_net)
    end
    return (ok)

endmacro


macro "Build All Transit Networks" (Args)
// Helper function to build transit networks by time of day
    trn_rts              = Args.[Transit]
    out_dir              = Args.OutputFolder      
   if Args.DryRun = 1 then Return(1) 
    ok = 1

    // build network
    for tod in Args.[TimePeriods] do 
        trn_net = runmacro("get_transit_network_file", out_dir, tod)
        runmacro("build_transit_network", trn_rts, trn_net, tod)
        runmacro("set_stop_to_stop_runtimes", trn_rts, trn_net)
    end
    return (ok)

endmacro

macro "build_transit_network" (trn_rts, trn_net, tod)
// Build transit networks

    netObj = CreateObject("Network.CreatePublic")
    netObj.LayerRS = trn_rts
    netObj.OutNetworkName = trn_net
    netObj.StopToNodeTagField = "near_node"
    netObj.IncludeWalkLinks = true
    netObj.RouteFilter = "available > 0 & headway_" + tod + " > 0"
    netObj.StopFilter = "available > 0 & DistanceToNextStop != 0"
    netObj.WalkLinkFilter = "no_walk_bike = 0 & available > 0"
    netObj.DriveLinkFilter = "walk_bike_only = 0 & truck_only = 0 & transit_only = 0 & available > 0"
    netObj.IncludeDriveLinks = true

    netObj.UseModes({TransitModeField: "Mode", NonTransitModeField: "transit_walk"})

    netObj.AddRouteField({Name: "headway_am",   Field: "headway_am"})
    netObj.AddRouteField({Name: "headway_md",   Field: "headway_md"})
    netObj.AddRouteField({Name: "headway_pm",   Field: "headway_pm"})
    netObj.AddRouteField({Name: "headway_nt",   Field: "headway_nt"})  
    netObj.AddRouteField({Name: "fare_type",    Field: "fare_type"})
    netObj.AddRouteField({Name: "fare",         Field: "fare"})
    netObj.AddRouteField({Name: "fare_core",    Field: "fare_core"})

    netObj.AddStopField({Name: "time_next",     Field: "time_next"})
    netObj.AddStopField({Name: "fare_zone",     Field: "fare_zone"})

    netObj.AddLinkField({Name: "Length", TransitFields: "Length", NonTransitFields: "Length"})
    netObj.AddLinkField({Name: "ttime", 
                        TransitFields: {"ab_trn_time_" + tod, "ba_trn_time_" + tod}, 
                        NonTransitFields: "walk_time" })

    // drive time values
    netObj.AddLinkField({Name: "drv_timecost", 
                        TransitFields: {"ab_MSAtime_cost_" + tod, "ba_MSAtime_cost_" + tod}, 
                        NonTransitFields: {"ab_MSAtime_cost_" + tod, "ba_MSAtime_cost_" + tod}})                     

    netObj.AddLinkField({Name: "auto_cost", TransitFields: "auto_cost", NonTransitFields: "auto_cost"})
    
    netObj.AddNodeField({Name: "pnr_lot", Field: "pnr_lot"})
    netObj.AddNodeField({Name: "parking", Field: "parking"})
    
    ok = netObj.Run()
    res = netObj.GetResults()
    if !ok then ShowArray(res)
    return(ok)   

endmacro


macro "set_stop_to_stop_runtimes" (trn_rts, trn_net)
// Update stop to stop runtimes from inputs in stop layer

    ok = 1
    objLyrs = null
    objLyrs = CreateObject("AddRSLayers", {FileName: trn_rts})
    RouteLayer = objLyrs.RouteLayer
    StopLayer = objLyrs.StopLayer
    //stop_vw = GetLayerView(StopLayer)
    SetView(StopLayer)

    // TODO: clean up and confirm no more elegant way to subset a layer
    // subset stop layer for only stops available with travel time to next
    Stop2Stop_File = GetTempFileName()
    n = SelectByQuery("Stop2Stop", "Several", "Select * where nz(time_next) > 0 & available > 0" ,)

    if n > 0 then do 
        {V_Stop_ID,V_TT_Stop2Stop}  = GetDataVectors(StopLayer + "|Stop2Stop", {"ID", "time_next"}, )
        Stop2Stop_vw = CreateTable("Stop2Stop", Stop2Stop_File, "FFB",
        {	{"ID", "Integer", 8, null, "Yes"},
            {"time_next", "Real", 12, 2, "No"} } )

        rh = AddRecords(Stop2Stop_vw, null, null, {{"Empty Records", V_Stop_ID.Length}})

        SetDataVectors(Stop2Stop_vw + "|", {	{"ID", 	V_Stop_ID}, {"time_next", V_TT_Stop2Stop}} , )
        
        // read the transit network file
        net = ReadNetwork(trn_net)
        stop_id_fld ="ID"
        stop_time_fld = "time_next"
        net_time_fidx = 2                    // index (1-based) of tnw trn_time TIME field
        REPLACE = 1
        // update stop-to-stop travel times
        ok = UpdateTransitLinks(net, Stop2Stop_vw, stop_id_fld, {{stop_time_fld, net_time_fidx}}, REPLACE, null)
    end
    
    CloseView(StopLayer)
    RunMacro("G30 File Close All")
    return(ok)

endmacro

macro "set_transit_network" (Args, trn_rts,trn_net, tod, userclass)
// Set transit network by time of day and mode
    mode_table = Args.[Transit Mode Table]
    transfer_file = Args.[Transit Transfer Table]
    zonal_fares = Args.[Transit Fare Table]
    path_thr = Args.[Transit Path Thresholds]
    penalties = Args.[Transit Path Penalties]
    global_wgts = Args.[TransitPath_GlobalWeights]
    mode_wgts = Args.[TransitPath_ModeWeights]
    path_comb = Args.[TransitPath_Combination]
    vot = Args.[Value Of Time]
    pnr = Args.transit_pnr_pfe

    ok = 1

    pknp = if (tod = 'am' | tod = 'pm') then 
        'pk' 
    else 
        'np'

    o = CreateObject("Network.SetPublicPathFinder", {RS: trn_rts, NetworkName: trn_net})
    o.UserClasses = {"tw", "ta_acc", "ta_egr", "lx"}
    o.CurrentClass = userclass
    o.CentroidFilter = "int_zone = 1 | ext_zone = 1"
    o.LinkImpedance = "ttime"
    o.DriveTime = "drv_timecost" 
    o.Parameters({
        MaxTripCost: path_thr.("MaxTripCost").Value,
        MaxTransfers: path_thr.("MaxTransfers").Value,
        VOT: vot,
        MidBlockOffset: 1, 
        InterArrival: 0.5
        })
    o.AccessControl({
        PermitWalkOnly: false,
        StopAccessField: null,
        MaxWalkAccessPaths: 10,
        WalkAccessNodeField: null
        })
    o.Combination({
        CombinationFactor: path_comb.("CombinationFactor").Value,
        Walk: path_comb.("WalkFactor").Value,
        Drive: path_comb.("DriveFactor").Value
        //ModeField: null,
        //WalkField: null
        })
    o.StopTimeFields({ /*
        InitialPenalty: null,
        TransferPenalty: null,
        DwellOn: null,
        DwellOff: null */
        })
    o.RouteTimeFields({
        Headway: "headway_" + tod
        //InitialWaitOverride: "iwait_" + tod
        //Layover: null,
        //DwellOn: null,
        //DwellOff: null
        })
    o.ModeTable({
        TableName: mode_table,
        ModesUsedField: {"tw_modes", "ta_modes", "ta_modes", "lx_modes"},
        SpeedField: "speed",
        OnlyCombineSameMode: true,
        FreeTransfers: 0
    })
    o.ModeTimeFields({
        DwellOn: "dwell_" + pknp,
        MaxTransferWait: "max_xfer_time"
    })
    o.ModeTransfers({
        TableName: transfer_file,
        FromMode: "from",
        ToMode: "to",
        AtStop: "stop",
        PenaltyTime: "wait",
        Fare: "fare",
        Prohibition: "prohibit",
        FareMethod: "Add"
        })
    o.TimeGlobals({
        //Headway: 14,
        InitialPenalty: 0,
        TransferPenalty: {penalties.("TransferPenalty - walk - " + pknp).Value,
                          penalties.("TransferPenalty - auto - " + pknp).Value,
                          penalties.("TransferPenalty - auto - " + pknp).Value,
                          penalties.("TransferPenalty - lx").Value},
        MaxInitialWait: path_thr.("MaxInitialWait").Value,
        MaxTransferWait: path_thr.("MaxTransferWait").Value,
        //MinInitialWait: 2,
        //MinTransferWait: 2,
        Layover: 15, 
        //DwellOn: 0.25,
        //DwellOff: 0.25,
        MaxAccessWalk: path_thr.("MaxAccessWalk").Value,
        MaxEgressWalk: path_thr.("MaxEgressWalk").Value,
        MaxModalTotal: path_thr.("MaxModalTotal").Value
    })
    o.RouteWeights({/*
        Fare: null,
        Time: null,
        InitialPenalty: null,
        TransferPenalty: null,
        InitialWait: null,
        TransferWait: null,
        Dwelling: null */
    })
    o.ModeWeights({
        Time: mode_wgts.("Time").Value,
        Dwelling: mode_wgts.("Dwelling").Value,
        InitialWait: mode_wgts.("InitialWait").Value,
        TransferWait: mode_wgts.("TransferWait").Value
    })       
    o.GlobalWeights({/*
        Time: 1,
        InitialPenalty: 1,
        TransferPenalty: 1,
        InitialWait: 2,
        TransferWait: 2,
        Dwelling: 1,*/
        WalkTimeFactor: global_wgts.("WalkTimeFactor").Value,
        Fare: global_wgts.("Fare").Value,
        DriveTimeFactor: global_wgts.("DriveTimeFactor").Value
    })
    o.Fare({
        Type: "Mixed", // Flat, Zonal, Mixed
        FareValue: .99,
        RouteFareField: "fare",
        RouteFareTypeField: "fare_type",
        RouteFareCoreField: "fare_core",
        ModeFareField: "fare",
        ModeFareTypeField: "fare_type",
        ModeFareCoreField: "fare_core_name",
        ZonalFareMethod: "ByRoute",
        StopFareZone: "fare_zone",
        FareMatrix: zonal_fares
        })
        
    o.DriveAccess({
        InUse: {false, true, false, true},
        MaxDriveTime: path_thr.("MaxDriveTime").Value,
        MaxParkToStopTimeField: mode_wgts.("ParkToStopTime").Value,
        MaxParkToStopTime: path_thr.("MaxParkToStopTime").Value,    
        ParkingNodes: "pnr_lot > 0 & parking > " + String(path_thr.("MinParkingCapacity").Value),
        ParkingNodeCapacity: {
            Alpha: pnr.("Alpha").Value, 
            Beta: pnr.("Beta").Value, 
            //Capacity: 100, 
            CapacityField: "parking"}
        //PermitAllWalk: false,
        //AllowWalkAccess: false   
    })

    // reverse origins and destinations for egress parking
    parkUsageTable = Args.("TransitParkUsage - " + 'am')

    egressOpts = {
        InUse: {false, false, true, false},
        MaxDriveTime: path_thr.("MaxDriveTime").Value,
        MaxStopToParkTimeField: mode_wgts.("ParkToStopTime").Value,
        MaxStopToParkTime: path_thr.("MaxParkToStopTime").Value,        
        ParkingNodes: "pnr_lot > 0 & parking > " + String(path_thr.("MinParkingCapacity").Value)
    }
    if GetFileInfo(parkUsageTable) <> null then do
        tempUsageTable = GetTempFileName("*.bin")
        CopyTableFiles(NULL, "FFB", parkUsageTable, NULL, tempUsageTable, NULL)
        dm = CreateObject("DataManager")
        vw = dm.AddDataSource("p", {FileName: tempUsageTable})
        
        oFld = CreateObject("CC.Field", GetFieldFullSpec(vw, "ORIGIN"))
        oFld.Rename("______ORIGIN________")
        oFld = CreateObject("CC.Field", GetFieldFullSpec(vw, "DESTINATION"))
        oFld.Rename("ORIGIN")
        oFld = CreateObject("CC.Field", GetFieldFullSpec(vw, "______ORIGIN________"))
        oFld.Rename("DESTINATION")
        egressOpts.ParkingUsageTable = {,,tempUsageTable,}
    end
   
    o.DriveEgress(egressOpts)

    ok = o.Run()
    res = o.GetResults()
    if !ok then ShowArray(res)
    return(ok) 

endmacro

macro "Build NonMotorized Network" (Args)
// Build the non motorized network file
    // Flow Chart Location: Initialization
    //##  Inputs:  nm dbd (pedestrian links from OSM)
    //## Outputs:  nm network file
    if Args.DryRun = 1 then Return(1)
    ok = 1

    out_dir = Args.OutputFolder 
    nm_dbd  = Args.[NonMotorized Links]
    nm_net = runmacro("get_nm_network_file", out_dir)

    netObj = CreateObject("Network.Create")
    netObj.LayerDB = nm_dbd
    netObj.LengthField = "Length"

    netObj.NetworkName = nm_net
    ok = netObj.Run()
    res = netObj.GetResults()
    if !ok then ShowArray(res)

    // Set network
    netSetObj = null
    netSetObj = CreateObject("Network.Settings")
    netSetObj.LayerDB = nm_dbd
    netSetObj.LoadNetwork(nm_net)
    netSetObj.CentroidFilter = "Centroids_Only = 1"
    netSetObj.UseLinkTypes = false

    ok = netSetObj.Run()
    res = netSetObj.GetResults()
    if !ok then ShowArray(res)
    return(ok) 

endmacro


