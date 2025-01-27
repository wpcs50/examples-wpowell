
Class "Visualize.Menu.Items"

    init do 
        self.runtimeObj = CreateObject("Model.Runtime")
    enditem 
    
    Macro "GetMenus" do
        Menus = {
					{ ID: "M1", Title: "Show Selected Param Info" , Macro: "SelectedParamInfo" }
				}
        Return(Menus)
    enditem 

    Macro "SelectedParamInfo" do
        ShowArray({ SelectedParamInfo: self.runtimeObj.GetSelectedParamInfo() })
        enditem  
 
EndClass


Macro "OpenParamFile" (Args,Result)
Body:
	mr = CreateObject("Model.Runtime")
	curr_param = mr.GetSelectedParamInfo()
	result = mr.OpenFile(curr_param.Name)
EndMacro


MenuItem "CTPS_TDM23 Menu Item" text: "CTPS_TDM23"
    menu "CTPS_TDM23 Menu"

menu "CTPS_TDM23 Menu"
    init do
	runtimeObj = CreateObject("Model.Runtime")
	curr_param = runtimeObj.GetSelectedParamInfo() 
	menu_items = {"Show Map", "Show Matrix", "Show Table"}
	if curr_param = null then
		DisableItems(menu_items)
	status = curr_param.Status
	if status = "Missing" then DisableItems(menu_items)
	else if status = "Exists" then do
		type = curr_param.Type
		if type = "NETWORK" then type = "MAP"
		menu_item = "Show " + Proper(type)
		DisableItems(menu_items)
		EnableItem(menu_item)
		end

    {, scen} = runtimeObj.GetScenario()
    if scen = null then 
        DisableItem("Select Query Analysis Menu Item")
    else 
        EnableItem("Select Query Analysis Menu Item")
    EndItem // end of init

    MenuItem "Show Map" text: "Show Map"
        do 
        RunMacro("OpenParamFile")
        enditem 

    MenuItem "Show Matrix" text: "Show Matrix"
        do 
        RunMacro("OpenParamFile")
        enditem 

    MenuItem "Show Table" text: "Show Table"
        do 
        RunMacro("OpenParamFile")
        enditem 

    MenuItem "Select Query Analysis Menu Item" text: "Select Query Analysis"
        do
        mr = CreateObject("Model.Runtime") // runtimeObj cannot be accessed here 
        dbox_res = RunDbox("config query", mr) 
        ready = dbox_res[1]

        if ready = 1 then do
            Args = mr.GetValues()
            Args.[Run Select Query from Menu] = 1
            mode = dbox_res[2] // 1 for highway, 2 for transit
            qry_file = dbox_res[3]
            res_folder = dbox_res[4] 

            if mode = 1 then do
                Args.[Highway Select Query File] = qry_file 
                Args.[Highway Select Query Output Folder] = res_folder
                for tod in {"am", "md", "pm", "nt"} do
                    ok = mr.RunCode("highway_assignment", Args, tod)
                end
                if ok =1 then
                    ShowMessage("Highway Select Query Analysis finished.")
                end
            else if mode = 2 then do
                Args.[Transit Select Query File] = qry_file 
                Args.[Transit Select Query Output Folder] = res_folder

                for tod in {"am", "md", "pm", "nt"} do
                    trip_tab = Args.("Per Trips - " + tod)
                    for mode in {"ta_acc", "ta_egr", "tw"} do
                        ok = mr.RunCode("transit_sq_assignment", Args, trip_tab, tod, mode)
                    end
                end
            
                for per in {"pk", "np"} do
                    lx_tag = mr.RunCode("get_pa_file_tag", "air", per, "lx")          
                    trip_tab = mr.RunCode("get_segment_mc_trip_file", Args.OutputFolder, lx_tag)

                    if per = "pk" then tod = "am" else tod = "md"
                    ok = mr.RunCode("transit_sq_assignment", Args, trip_tab, tod, "lx")
                end

                if ok = 1 then
                    ShowMessage("Transit Select Query Analysis finished.")
            end
        end
        enditem

EndMenu 

DBox "config query" (mr)  , , 77, 12 Title: "Configure Select Query Analysis" 
    Init do
        DisableItem("Select file")
        DisableItem("Select folder")
        DisableItem("Run")

        {, scen} = mr.GetScenario()
        Args = mr.GetValues()
        init_dir = Args.OutputFolder + "\\_assignment\\"
    EndItem

    Text "Selected scenario: " 1, 1, 20, 1
    Text "Show scenario" after, same, 54, 1 Framed Align: right variable: scen 

    Radio List 1, 3, 75, 2 Prompt: "Run select query analysis for" Variable: mode 
        Radio Button 30, 4 Prompt: "Highway" 
            do EnableItem("Select file") EndItem // mode = 1 
        Radio Button 55, 4 Prompt: "Transit"
            do EnableItem("Select file") EndItem // mode = 2 
    
    Text "Select query file (*.qry): " 1, 6, 20, 1
    Text "Show query file" after, same, 50, 1 Framed Align: right variable: qry_file
    Button "Select file" after, same, 2, 1 Prompt: "..." 
        Do
            on escape do
                goto step1
                end
            qry_file = ChooseFile({{"Query file (*.qry)", "*.qry"}}, "Choose a Query File", 
                        {"Initial Directory": init_dir})
            EnableItem("Select folder")
            step1:
            on escape do end 
        EndItem 

    Text "Select result folder: " 1, 8, 20, 1
    Text "Show result folder" after, same, 50, 1 Framed Align: right variable: res_folder
    Button "Select folder" after, same, 2, 1 Prompt: "..."
        Do
            on escape do
                goto step2
                end
            res_folder = ChooseDirectory("Choose a Folder", {"Initial Directory": init_dir} )
            EnableItem("Run")
            step2:
            on escape do end
        EndItem 

    Button "Run" 53, 10, 10, 1 
        Do
            return({1,mode,qry_file,res_folder}) 
        EndItem 
    // Add a Cancel button. The Cancel keyword allows the user to press Esc.
    Button "Cancel" 65, same, 10, 1 Cancel
        Do
            return({,,,})
        EndItem 
EndDBox
