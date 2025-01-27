
from asyncio.log import logger
from pickle import TRUE
import pandas as pd 
from pathlib import Path
from .base import disagg_model
import yaml
import numpy as np
import os
from dbfread import DBF
from math import floor
from tqdm.notebook import tqdm
tqdm.pandas()

from utilities.pygisdk.cntxtmng import TC
from utilities.pygisdk import MatrixClass
from utilities.pygisdk import rts
from utilities.pygisdk.table import bin
from openpyxl.styles import Alignment



class util:
    def get_ext_file_from(any_file,to_suf="csv"):
        path_name = os.path.split(any_file)
        suffix = path_name[1].split(".")[1]
        omx_file = os.path.join(path_name[0] , path_name[1].replace(suffix,to_suf))
        return omx_file
    
    @staticmethod
    def html_styling(df="df",to_excel=None
                            ,headers = None
                            ,to_html=None
                            ,to_str=None
                            ,title = "Summary of Highway Air Quality (Daily)"
                            ,s_name = "Sheet1"):
        """_summary_

        Args:
            df (str, optional): _description_. Defaults to "df".
            to_excel (_type_, optional): _description_. Defaults to None.
            headers (_type_, optional): {'VMT<br>(mi)': 'VMT (mi)',
                                        'CVMT<br>(mi)': 'CVMT (mi)',
                                        'VHT<br>(hr)': 'VHT (hr)',
                                        'CO<br>(kg)': 'CO (kg)',
                                        'NO<br>(kg)': 'NO (kg)',
                                        'SO<br>(kg)': 'SO (kg)',
                                        'VOC<br>(kg)': 'VOC (kg)',
                                        'CO2<br>(kg)': 'CO2 (kg)'} . Defaults to None.
            to_html (_type_, optional): _description_. Defaults to None.
            to_str (_type_, optional): _description_. Defaults to None.
            title (str, optional): _description_. Defaults to "Summary of Highway Air Quality (Daily)".
            s_name (str, optional): _description_. Defaults to "Sheet1".

        Returns:
            _type_: _description_
        """
        styles = [
            dict(selector="tr:hover",
                        props=[("background", "#f4f4f4")]),
            dict(selector="tr:nth-child(even)",
                        props=[("background", "#f2f2f2")]), 
            dict(selector="th", props=[
                                    #  ("color", "#fff"),
                                    ("border", "1px solid #eee"),
                                    ("padding", "6px 8px"),
                                    ("border-collapse", "collapse"),
                                    ("background", "#d5e1e1"),
                                    ("text-transform", "uppercase"),
                                    ("font-size", "8px")
                                    ]),
                    ]
        
        table = df.style.set_properties(
                        **{
                        # 'background-color': '#f2f2f2',                                                   
                        'color': 'black',    
                        'font-size': '8pt',
                        'text-align': "center",
                        'width': '6vw',
                        }).set_table_styles(styles).format('{:,.0f}')
                        #.background_gradient(axis=0)
                        #.bar() #.hide_index()
        table.set_caption(title)

        if to_excel is not None:
            if headers is not None:
                df = df.rename(**headers) 
                table = df.style.set_properties(
                        **{                                           
                        'color': 'black',    
                        'font-size': '8pt',
                        'text-align': "center",
                        'width': '6vw',
                        }).set_table_styles(styles).format('{:,.0f}')
                table.set_caption(title)

            table.to_excel(to_excel,sheet_name=s_name, engine='openpyxl')
            
            #modifyng output by style - wrap
            writer = to_excel
            workbook  = writer.book
            worksheet = writer.sheets[s_name]
            for row in worksheet.iter_rows():
                for cell in row:
                    cell.alignment = Alignment(wrapText=True,horizontal="center")

        if to_html is not None:
            with open(to_html,"w") as f:                
                f.write( table.to_html() )

        if to_str is not None:
            return table.to_html()
        
def bpr_func(x,ffs=60,alpha=.15,beta=4) : 
    r"""BPR volume delay function input: 
        volume over capacity
        ```md
        $$ 
            \begin{aligned}
            Spd &= Spd_{ff} / (1 + \alpha (\frac{q}{q_{qc}} )^\beta ) \\
            Spd &= Spd_{ff} / (1 + \alpha x^\beta ) \\
            Spd_{ff}    &=  Spd (1 + \alpha x^\beta ) 
            \end{aligned}
        $$ 
    

    ---
    Args:
        x     (_type_):   volume over capacity
        ffs   (_type_):   Free-flow speed
        alpha (_type_):   Alpha coefficient, which was assigned a value of .15 in the original BPR curve
        beta  (_type_):   Beta coefficient, the exponent of the power function, which was assigned a value of 4 in the original BPR curve
    Returns:
        cgs     _type_:   Congestion speed
    """
    
    cgs = ffs / (1 + alpha*np.power(x,beta))
    return cgs
    
class air_quality(disagg_model):
    EW_mass_tipping = 2000

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        logger = self.add_logger(name=__name__)
        self.logger = logger
        
        self.emrate = pd.read_csv(self.args["emis_rate_hwy"]
                                  ).set_index( ["emass"] + ["level_%s"%i for i in range(5)]
                                              ).to_dict(orient="index")
        self.bsrate = pd.read_csv(self.args["emis_rate_trn"],index_col=[0]
                                  ).to_dict(orient="index")        
        self.tod_hrs = {"am":3 , "md":5.5, "pm": 4, "nt":11.5 }
        self.tod = ["am","md","pm","nt"]
        self.dirs = ["AB","BA"]
        self.auto = ["da","sr"]
        self.trk  = ["ltrk","mtrk","htrk"]
        self.vmap = {"Auto": self.auto , "Truck": self.trk }
        self.link_metrics   = [ i.upper() for i in self.args["Air Quality Fields"].split(",")]
        self.itaz_metrics   = [ i.upper() for i in self.args["Air Quality Fields"].split(",") if i.upper() != "CVMT" ]
        self.eqty_metrics   = self.args["Selected Air Quality Fields"].split(",") 
        self.unit_lookup = {    "VMT" : "VMT<br>(mi)" ,
                                "CVMT": "CVMT<br>(mi)" ,
                                "VHT" : "VHT<br>(hr)" ,
                                "CO"  : "CO<br>(kg)"  ,
                                "NO"  : "NO<br>(kg)"  , 
                                "SO"  : "SO<br>(kg)"  , 
                                "VOC" : "VOC<br>(kg)" ,
                                "CO2" : "CO2<br>(kg)" , }
        self.link_flows = {}
        self.link_geo = os.path.join(self.args["OutputFolder"],"_networks\links.dbf")
        self.link_att = os.path.join(self.args["OutputFolder"],"_networks\LinksNodes.csv")
        self.VC_thred = float(self.args["v_c thres"])

        self.mapdict = {1 : 'BRMPO',
                        2 : 'MVPC',
                        3 : 'NMCOG',
                        4 : 'OCPC',
                        5 : 'MRPC',
                        6 : 'CMRPC',
                        7 : 'SRPEDD',
                        8 : 'CCC',
                        9 : 'MVC',
                        10: 'NPEDC',
                        11: 'PVPC',
                        12: 'FRCOG',
                        13: 'BRPC',
                        99: 'NHARI'}
        self.FC = {1, 2} | set(range(47, 70,1)) | set(range(71, 100,1))
        self.taz_type_file = os.path.join(self.args["OutputFolder"],"_networks\\taz_index.csv") 
        taz_df = pd.read_csv(self.taz_type_file)[["taz_id","mpo"]]
        taz_df["mpo"] = taz_df["mpo"].map(self.mapdict)
        self.mpo_taz = taz_df.rename(columns={"taz_id":"TAZ_ID"})

        ## hw intrazonal 
        self.veh_summ_file = os.path.join(self.args["OutputFolder"],"_summary\\trips","veh_trips_daily.mtx")
        self.hwskm = {  
                        "am" : self.args["HighwaySkims - am"],
                        "md" : self.args["HighwaySkims - md"],
                        }
        
        ## transit
        self.rtsystm = self.args["Transit"]

        ## outputs
        self.highway_link  = self.args["emis_hwy_by_link"]
        self.highway_taz   = self.args["emis_hwy_by_taz"] 
        self.hw_mpo        = self.args["emis_hwy_by_mpo"] 
        self.highway_intra = self.args["emis_hwy_intra"] 
        self.transit_link  = self.args["emis_trn_by_link"] 
        self.transit_mode  = self.args["emis_trn_by_mode"] 

        ### handoff to Equity analysis
        self.hw_trn_taz    = self.args["Air Quality Metrics by TAZ"] 


    def __get_trn_inputs(self, trncd = True):
        
        table = DBF(self.link_geo)
        subseg = ['ID', 'LENGTH', 'TAZ_ID', 'FUNC_CLASS']
        self.df_geo = pd.DataFrame(iter(table))[ subseg]

        if trncd:
            with TC(name="transit") as dk:
                rt_lyr, stop_lyr, ph_lyr = dk.RunMacro("TCB Add RS Layers", self.rtsystm, "ALL",None)
                
                rts.dk = dk
                stop_db = rts.GetLayerDB(stop_lyr)
                tb_stop = bin(file=stop_db,view= stop_lyr,conn=dk)
                
                binfile = rts.GetLayerDB(rt_lyr) + "R.bin"
                tb_route = bin(file=binfile,view= rt_lyr,conn=dk)

                self.df_stop = tb_stop.GetDataframe()
                self.df_route = tb_route.GetDataframe()
        else:
            rt_lyr = None
            pass

        return rt_lyr


    def tod2hours(self):
        
        furl = self.args["hr_veh_trip"]
        df_veh = pd.read_csv(furl)

        tod = {"am":np.r_[ 6:10], 
                "md":np.r_[10:15],
                "pm":np.r_[15:20], 
                "nt":np.r_[:6,20:24]  }
        ## 
        relatod = None 
        for prd in self.tod:
            rela_tod = df_veh["percent"].iloc[ tod[prd] ]/ df_veh["percent"].iloc[ tod[prd] ].sum()
            relatod  = pd.concat([relatod,rela_tod])

        ##      
        cap_facts = { "am" : 2.835,
              "md" : 4.809,
              "pm" : 3.774,
              "nt" : 5.518}
        
        
        rowc = list(self.df_link.columns)
        ix_prd  = rowc.index("tod")
        ix_flow = rowc.index("%s_Flow"%"AB")
        ix_ffs  = rowc.index("ff_speed")
        ix_alpha= rowc.index("alpha")
        ix_beta = rowc.index("beta")
        ix_len  = rowc.index("LENGTH")


        rtod_dic = {}
        for prd in tod.keys():
            rela_tod = df_veh["percent"].iloc[ tod[prd] ]/ df_veh["percent"].iloc[ tod[prd] ].sum()
            rtod_dic[prd] = rela_tod
            
        lstup = []
        for row in self.df_link.itertuples(index=False):
            prd  = row[ix_prd]
            flow = row[ix_flow]
            ffs  = row[ix_ffs]
            alpha= row[ix_alpha]
            beta = row[ix_beta]
            lklen = row[ix_len] 
            
            ix_capi = rowc.index("%s_capacity_%s"%("ab",prd) )
            ab_capi = 0.0001 if row[ix_capi] == 0 else row[ix_capi]
            ix_capi = rowc.index("%s_capacity_%s"%("ba",prd) )
            ba_capi = 0.0001 if row[ix_capi] == 0 else row[ix_capi]

            rela_tod = rtod_dic[prd]
            
            hours = tod[prd]
            trow = tuple(row)
            for hr in hours:        
                fct = relatod[hr]
                r_1_13 = tuple([z * fct for z in trow[1:13]])
                
                coef1 = {"ffs":ffs,"alpha":alpha,"beta":beta ,"x": flow*fct /(ab_capi/cap_facts[prd])}
                abspd = bpr_func(**coef1)
                coef1["x"] = flow*fct /(ba_capi/cap_facts[prd])
                baspd = bpr_func(**coef1)
                
                ix_ab = rowc.index("%s_Speed"%("BA") ) + 1
                
                # replace values：AB_MSA_Time BA_MSA_Time at trow[13:15] 
                abtime = lklen/abspd # unit: hour
                batime = lklen/baspd
                
                newrow = (trow[0],)+ r_1_13 +  (abtime,batime,abspd,baspd) +  trow[ix_ab:]  + (hr,)
                lstup.append(newrow)

        dfhrlk = pd.DataFrame(lstup,columns=rowc+["hour"])
        return dfhrlk
    

    def __get_lk_inputs(self):

        df_flow = {}
        for key in self.tod:
            value = "HighwayFlows - %s"%key
            subcol = ["Flow_PCE","Flow_da","Flow_sr",
                    "Flow_ltrk", "Flow_mtrk", "Flow_htrk",
                    "MSA_Time","Speed","VOC", "Flow"]
            subseg = [ "%s_%s"%(j,i)  for i in subcol for j in self.dirs]
            self.link_flows[key] = util.get_ext_file_from(self.args[value] )
            df_flow[key] = pd.read_csv( self.link_flows[key] )[ ["ID1"]+subseg]


        table = DBF(self.link_geo)
        subseg = ['ID', 'LENGTH', 'TAZ_ID', 'FUNC_CLASS']
        self.df_geo = pd.DataFrame(iter(table))[ subseg]
        ## add "urban area zone" column 
        query_string = "SELECT * from MA_taz_geography;"
        taz_df = self.db._raw_query(qry=query_string).rename(columns={"taz_id":"TAZ_ID"})
        self.logger.debug( taz_df.columns)
        self.df_geo  = taz_df[["TAZ_ID","urban"]].merge(self.df_geo, left_on ="TAZ_ID" ,right_on= "TAZ_ID")
        
        self.hwlink =  pd.read_csv(self.link_att)
        cols = ["ID","ff_speed","alpha","beta"] + ["%s_capacity_%s"%(dir, i) for dir in ["ab","ba"] for i in self.tod]
        df_cap = self.hwlink[cols]
        self.df_geo  = pd.merge(left=df_cap,right=self.df_geo,
                        left_on='ID', right_on='ID',how="left")
        self.status_updater(3,"Loading links of all tods ")

        df_link = None
        for tod in self.tod:
            df_link_pd = pd.merge(left=df_flow[tod],right=self.df_geo,
                        left_on='ID1', right_on='ID',how="left")
            df_link_pd["tod"] = tod
            df_link = pd.concat([df_link,df_link_pd])
            # lsdic = self.tod2hours(prd=tod)
        self.df_link = df_link.fillna(0)
        self.status_updater(6,"Allocating tod volume to hourly volume")
        self.df_link =  self.tod2hours()
        self.logger.debug(self.df_link.head(20))


    def __get_iz_inputs(self, trncd = True):
        v_trips   = {"auto":None,"mtrk":None,"htrk":None}
        iz_time = {"am":None,"md":None}
        iz_dist = {"am":None,"md":None}
        self.v_trips = {}
        if trncd :
            with TC(name=2) as dk:
                mtx_file = self.veh_summ_file
                obj = dk.CreateGisdkObject("gis_ui","Matrix", mtx_file)
                mtx_mobj = MatrixClass(obj,conn=dk)
                for veh in v_trips.keys():
                    v_trips[veh] = mtx_mobj.GetVector({"Core": veh,  "Diagonal": "Row"})
                    

                for tod  in iz_time.keys():
                    mtx_file = self.hwskm[tod]
                    obj = dk.CreateGisdkObject("gis_ui","Matrix", mtx_file)
                    mtx_mobj = MatrixClass(obj,conn=dk)
                    iz_time[tod] = mtx_mobj.GetVector({"Core": "da_time",  "Diagonal": "Row"})
                    iz_dist[tod] = mtx_mobj.GetVector({"Core": "dist",     "Diagonal": "Row"})

            
            self.v_trips["Auto"]  = v_trips["auto"]
            self.v_trips["Truck"] = v_trips["mtrk"] + v_trips["htrk"]
            self.iz_time = iz_time
            self.iz_dist = iz_dist

        else:
            url = os.path.join(self.args["OutputFolder"],"_summary\\trips\\veh_trips_daily.csv") 
            df = pd.read_csv(url)

            self.v_trips   = { "Auto":df.auto.to_numpy(),"Truck":df.trk.to_numpy() }
            self.iz_time   = {"am":df.time_am.to_numpy(),"md":df.time_md.to_numpy()}
            self.iz_dist   = {"am":df.dist_am.to_numpy(),"md":df.dist_md.to_numpy()}
            self.taz_id    = df.taz_id.to_numpy()
    
    def run(self):
        """
         The standard run() method. Overrriding of run() method in the subclass of thread
        """
        
        print("Starting " + self.name)
        self.status_updater(0, "Preparing component" )
        try:    
            self.run_model()
            self.status_updater(100, "Closing component" )
            print("Exiting " + self.name)
            if self.popup == None:
                raise SystemExit()
                
            elif self.popup.runwithin == "others" :
                raise SystemExit()

        except Exception as e:
            import traceback
            errfile = self.args["OutputFolder"] +'\\_logs\\' + "py.err"
            with open(errfile,"a") as file:
                traceback.print_exc(file=file)

            self.status_updater(-1, "[Error❗]: Click cancel to check the error message %s"%str(e) )

        

    def run_model(self):
        """[load parameters and call appropriate model run method]"""
        self.highway_mpo()
        self.status_updater(85, "mpo" )

        
        self.transit_route_link(emission=False)
        self.status_updater(99, "Closing component" )

        self.logger.debug("run_summaries completed")

    @staticmethod
    def cat_spd(value):
        if value <= 72.5:
            spid = floor(value/5+1) + 1
        else:
            spid = 16
        return spid
    
    def cat_roadTypeID(self, FUNC_CLASS = 22, URBAN = 1):

        if    FUNC_CLASS       in self.FC and URBAN != 1:
            rdid = 2
        elif  FUNC_CLASS   not in self.FC and URBAN != 1:
            rdid = 3
        elif  FUNC_CLASS       in self.FC and URBAN == 1:
            rdid = 4
        elif  FUNC_CLASS   not in self.FC and URBAN == 1:
            rdid = 5
        return rdid
    

    def emission_rate(self, row,j = "AB",veh = "Truck",plt = "CO"):

        TAZ_ID,SPEED,TOD,FUNC_CLASS,URBAN, metric = row
        # TAZ ID
        emass =  1 if TAZ_ID < air_quality.EW_mass_tipping else 2
        # avgSpeedBinID
        spdid = self.cat_spd(SPEED)

        # periodID
        tod = TOD
        if tod == "am":
            tod = 1
        elif tod == "md":
            tod = 2
        elif tod == "pm":
            tod = 3
        elif tod == "nt":
            tod = 4
        
        # vehicleTypeID 
        if veh == "Auto":
            vehid = 1
        else:
            vehid = 2

        # Month ID
        if plt in ["SO"]:
            mid = 1
        else:
            mid = 7

        # roadTypeID
        rdid = self.cat_roadTypeID(FUNC_CLASS,URBAN)
        
        return emass,spdid,mid,tod,vehid,rdid
    
    @staticmethod
    def emission_rate_taz(row,tod = "am",veh = "Truck",plt = "CO"):
        self = air_quality
        # TAZ ID
        emass =  1 if row["TAZ_ID"] < air_quality.EW_mass_tipping else 2
        # avgSpeedBinID
        value = row["speed_%s"%tod] 
        spdid = self.cat_spd(value)

        # periodID
        if tod == "am":
            tod = 1
        elif tod == "md":
            tod = 2
        elif tod == "pm":
            tod = 3
        elif tod == "nt":
            tod = 4
        
        # vehicleTypeID 
        if veh == "Auto":
            vehid = 1
        else:
            vehid = 2

        # Month ID
        if plt in ["SO"]:
            mid = 1
        else:
            mid = 7

        # roadTypeID
        rdid =  4 if row["TAZ_ID"] < air_quality.EW_mass_tipping else 2
        
        return emass,spdid,mid,tod,vehid,rdid
    
    @staticmethod
    def transit_mode_map(x):
        kmap = {
                1 	: "Local Bus",
                2 	: "Express Bus",
                3 	: "Bus Rapid",
                4 	: "Light Rail",
                5 	: "Heavy Rail",
                6 	: "Commuter Rail",
                7 	: "Ferry",
                8 	: "Shuttle",
                9 	: "RTA Local Bus",
                10 	: "Regional Bus",
                    }
        key = int(x)
        try:
            mode = kmap[key]
        except:
            mode = "None"
        return mode

    def calc_rt_length(self,rlinks):
        df_rlink = pd.merge(left=rlinks,right=self.df_geo,
                left_on='Link ID', right_on='ID',how="left")
        rt_len = df_rlink["LENGTH"].sum()
        
        return rt_len
        
    def calc_bus_count(self,headway,tod):
        
        if headway == 0:
            bus_cnt = 0
        else:
            bus_cnt = self.tod_hrs["am"] * 60 / headway

        return bus_cnt

    def get_bus_rate(self,fuel_type=' Bus - Diesel'):
        
        rate =  self.bsrate[fuel_type]
        ratedic = {"VMT":1}
        ratedic.update(rate)

        return ratedic

    def calc_metric(self,ix = "VMT",mode=["da","sr"],dir="AB"):
        subseg = {}
        subseg["AB"] =  [ "AB_Flow_%s"%(i)  for i in mode ]
        subseg["BA"] =  [ "BA_Flow_%s"%(i)  for i in mode ]


        if   ix == "VMT":
            df_metric = self.df_link[subseg[dir]].sum(axis=1) * self.df_link.LENGTH
        elif ix == "VHT":
            df_VHT = self.df_link[subseg[dir]].sum(axis=1) * self.df_link["%s_MSA_Time"%dir]
            df_metric = df_VHT 
        
        return df_metric
    
    def sum_emission(self,TAZ_ID,SPEED,TOD,FUNC_CLASS,URBAN,dir,veh,plt  ,vmt):
        tup = self.emission_rate(row=[TAZ_ID,SPEED,TOD,FUNC_CLASS,URBAN,plt], j=dir,veh=veh,plt=plt )
        rate = self.emrate[tup][plt]
        return rate*vmt

    def sum_emission_trn(self,cnt,length,fuel_type,plt):

        if   fuel_type == "Diesel":
             rate_type =  " Bus - Diesel"
        elif fuel_type == "CNG":
             rate_type =  "Bus - CNG"
        elif fuel_type == "Dual":
             rate_type =  "Bus - Hybrid"
        elif fuel_type == "2.5":
             rate_type =  "Bus - Electric"
        elif fuel_type == "CommuterRail":
             rate_type =  "Commuter Rail"
        else:
             rate_type =  " Bus - Diesel"
        
        rate = self.bsrate[rate_type][plt]
        return cnt*length*rate
    
    def highway_interzonal_link(self):
        
        self.__get_lk_inputs()

        df = self.df_link[["ID","TAZ_ID","hour"]].copy()
        stat_pct = 10
        for plt in self.link_metrics:

            for veh in self.vmap.keys():
                segls = []
                for j in self.dirs:

                    if plt in ["VMT", "VHT"]:
                        self.df_link["%s_%s_%s"%(j,veh,plt)] =  self.calc_metric(ix=plt,mode=self.vmap[veh] , dir = j)
                    elif plt == "CVMT":
                        # compute congested VMT by apply VC ratio threshold;
                        self.df_link["%s_%s_%s"%(j,veh,plt)] = np.where(
                                                                    self.df_link["%s_VOC"%j] > self.VC_thred,
                                                                    self.df_link[ "%s_%s_%s"%(j,veh,"VMT")],
                                                                    0)
                    else:                         
                        self.df_link["%s_%s_%s"%(j,veh,plt)] =  np.vectorize(self.sum_emission)(
                                                                    self.df_link["TAZ_ID"], 
                                                                    self.df_link["%s_Speed"%j], 
                                                                    self.df_link["tod"], 
                                                                    self.df_link["FUNC_CLASS"], 
                                                                    self.df_link["urban"], 
                                                                    pd.Series(j, index=self.df_link.index),
                                                                    pd.Series(veh, index=self.df_link.index),
                                                                    pd.Series(plt, index=self.df_link.index),
                                                                    self.df_link["%s_%s_VMT"%(j,veh)] )
                    segment = "%s_%s_%s"%(j,veh,plt)
                    segls.append(segment)
                    stat_pct += 1
                    self.status_updater(stat_pct, "%s completed"%segment )
                
                df["%s_%s"%(veh,plt)] = self.df_link[segls].sum(axis=1)
                
        
        df_hwlk = df

        return df_hwlk
    
    def highway_interzonal_taz(self):
        
        df_hwlk = self.highway_interzonal_link()
        df_hwlk.to_csv(self.highway_link)
        df_hwtaz = df_hwlk.groupby("TAZ_ID").sum().drop(["ID","hour"],axis=1)
        df_hwtaz.to_csv(self.highway_taz)

        return df_hwtaz

    def highway_mpo(self):
        df_hwtaz_ie = self.highway_interzonal_taz()
        df_hwtaz_ia = self.highway_intrazonal_taz()
        self.status_updater(85, "highway_intrazonal_taz" )

        ## combine interzonal and intrazonal highway emission
        df_hwtaz_ie = df_hwtaz_ie.reset_index()
        df_hwtaz_ie.TAZ_ID = df_hwtaz_ie.TAZ_ID.astype(int)
        df_hwtaz_it = pd.concat([df_hwtaz_ia,df_hwtaz_ie])
        df_hwtaz = df_hwtaz_it.groupby("TAZ_ID").sum()
        
        # keep a copy to add up with transit emission
        self.taz_hwy = df_hwtaz
        
        df_hwtaz  = df_hwtaz.merge(self.mpo_taz,on="TAZ_ID")
        df_hwmpo =  df_hwtaz.groupby("mpo").sum().reset_index().drop(["TAZ_ID"],axis=1)

        ## reoder the sequence of report
        cols = [   (v,m)  for v in self.vmap.keys() for m in self.link_metrics]
        
        l = pd.wide_to_long(df_hwmpo,stubnames=["Auto","Truck"],i="mpo",j="Metrics",sep='_',suffix=r'\w+')
        tabel_sum = l.unstack()[cols].rename(columns=self.unit_lookup, level=1)

        MAmpos = [ v for k,v in self.mapdict.items() if k <20]
        tabel_sum.loc["Total_MA"]  = tabel_sum.loc[MAmpos].sum(axis=0)

        lkup_xlsx= {}
        for k,v in self.unit_lookup.items():
            lkup_xlsx[v] = v.replace("<br>", "\n")

        pd.DataFrame().to_excel("%s.xlsx"%(self.hw_mpo),sheet_name="All")
        with pd.ExcelWriter("%s.xlsx"%(self.hw_mpo), mode='a',
                                engine="openpyxl",
                                if_sheet_exists="replace"   ) as writer: 
            ## subtotal by veh type
            t1 = util.html_styling(tabel_sum,to_excel= writer,
                                     headers={"columns":lkup_xlsx,"level":1}
                                    ,to_str =1
                                    ,title = "Summary of Highway Air Quality (Daily-Subtotal)"
                                    ,s_name = "Subtotal") 
            ## total 
            tabel_sum = tabel_sum.groupby(level=1,axis=1).sum()
            t0 = util.html_styling(tabel_sum,to_excel=writer,
                                    headers={"columns":lkup_xlsx}
                                    ,to_str =1
                                    ,title = "Summary of Highway Air Quality (Daily-All)"
                                    ,s_name = "All") 
            
        with open("%s.html"%(self.hw_mpo),"w") as f:
            f.write(t0)
            f.write("<br>")
            f.write(t1)
            f.write("<a href='{}'>Download excel</a>".format("%s.xlsx"%(self.hw_mpo)) )
    
        return tabel_sum
    

    def highway_intrazonal_taz(self):
        self.__get_iz_inputs(trncd=False)

        df_iz = pd.DataFrame()
        df_iz_allday = pd.DataFrame()
        for plt in self.itaz_metrics:
            for veh in self.v_trips.keys():
                for tod in self.iz_dist.keys():
                    print (tod,veh,"completed")
                    df_iz["speed_%s"%tod] =   self.iz_dist[tod] / (self.iz_time[tod] /60)
                    if plt == "VMT":
                        df_iz["%s_%s_%s"%(tod,veh,plt)] = self.v_trips[veh] * self.iz_dist[tod]
                        df_iz["TAZ_ID"] = self.taz_id
                    elif plt == "VHT":
                        df_iz["%s_%s_%s"%(tod,veh,plt)] = self.v_trips[veh] * self.iz_time[tod] /60
                    else:
                        df_iz["%s_%s_%s"%(tod,veh,plt)] =  df_iz.progress_apply(
                            lambda row :  self.emrate[ self.emission_rate_taz(row,tod,veh,plt)
                                                        ][plt] * row["%s_%s_VMT"%(tod,veh)]            
                            ,axis=1) 
                        
                if plt == "VMT":
                    df_iz_allday["TAZ_ID"] = self.taz_id
                df_iz_allday["%s_%s"%(veh,plt)] = df_iz["%s_%s_%s"%("am",veh,plt)] + df_iz["%s_%s_%s"%("md",veh,plt)]
            
        # export
        df_iz_allday.to_csv(self.highway_intra,index=False)
        return df_iz_allday

    def transit_route_link(self,emission=False,trncd=False):
        if trncd:
            rt_lyr = self.__get_trn_inputs()
        else:
            rt_lyr = self.__get_trn_inputs(trncd=False)


        df_link_cum = self.df_geo[["ID","LENGTH","TAZ_ID"]].copy().set_index("ID")
        plts = ["VMT"] + [j for j in [i for i in self.bsrate.values()][0].keys()]

        if trncd:
            with TC(name="transit") as dk:
                rts.dk = dk
                rt_lyr, stop_lyr, ph_lyr = dk.RunMacro("TCB Add RS Layers", self.rtsystm, "ALL",None)
                ls = []
                for cnt,row in tqdm(self.df_route.iterrows()):
                    for tod in self.tod:
                        headway = row["headway_%s"%tod]
                        route_n = row["Route_Name"]
                        modekey    = row["Mode"] 
                        bus_cnt = self.calc_bus_count(headway,tod)
                        fuel_type = row["fuel_type"]
                    
                        rlinks = rts.GetRouteLinks(rt_lyr, route_n)
                        
                        for row_rt in rlinks.itertuples(index=False):
                            newrow  = {}
                            id = row_rt[0] # "Link ID"
                            newrow = {"ID":id,"bus_cnt": bus_cnt, "fuel_type":fuel_type,"mode":  modekey  }
                            ls.append(newrow)
                            
            dfrlk = pd.DataFrame(ls).set_index("ID")
            dfrlk = dfrlk.join(df_link_cum,how="left").dropna()

        else:
            url = os.path.join(self.args["OutputFolder"],"_summary\\trn\\trn_trips.csv") 
            dfrlk =  pd.read_csv(url).set_index("ID")
            dfrlk = dfrlk.join(df_link_cum,how="left").dropna()
        """
        TODO:
        NO length info for link ID
        code:  df [df.LENGTH.isnull()].index.unique()
        link ID list: [249142, 249145, 249150, 249155, 249156, 249158, 249163, 249164,249178, 249180, 249209],
        """

        for plt in plts:
            if plt == "VMT":
                dfrlk[plt] = dfrlk["bus_cnt"]* dfrlk ["LENGTH"]
            else:
                """
                Example to optimize the computing speed by vectorization:
                where the original apply function look like:
                    #  dfrlk[plt] = dfrlk["count"]* dfrlk ["LENGTH"] * self.bsrate["Bus - CNG"][plt]
                """
                
                dfrlk[plt] = np.vectorize(self.sum_emission_trn)( 
                                                            dfrlk ["bus_cnt"],
                                                            dfrlk ["LENGTH"],
                                                            dfrlk ["fuel_type"],
                                                            pd.Series(plt))

        #export
        df = dfrlk.reset_index().groupby("ID").sum()[plts+["mode"]]
        df.to_csv(self.transit_link)

        ## add transit taz level metrics to equity analysis
        dfrlk.TAZ_ID = dfrlk.TAZ_ID.astype(int)
        taz_trn = dfrlk.groupby("TAZ_ID").sum()
        if emission:
            taz_eqt = taz_trn.merge(right=self.taz_hwy,how="outer",left_index=True,right_index=True).fillna(0)
            taz_eqt["co"]   = taz_eqt[["CO" ,'Auto_CO','Truck_CO']].sum(axis=1)
            taz_eqt["no"]   = taz_eqt[["NOx",'Auto_NO','Truck_NO']].sum(axis=1)
            taz_eqt["voc"]  = taz_eqt[["VOC",'Auto_VOC','Truck_VOC']].sum(axis=1)
            taz_eqt["cvmt"] = taz_eqt[['Auto_CVMT','Truck_CVMT']].sum(axis=1)
        else:
            taz_eqt = self.taz_hwy.fillna(0)
            taz_eqt["co"]   = taz_eqt[['Auto_CO','Truck_CO']].sum(axis=1)
            taz_eqt["no"]   = taz_eqt[['Auto_NO','Truck_NO']].sum(axis=1)
            taz_eqt["voc"]  = taz_eqt[['Auto_VOC','Truck_VOC']].sum(axis=1)
            taz_eqt["cvmt"] = taz_eqt[['Auto_CVMT','Truck_CVMT']].sum(axis=1)  

        taz_eqt[self.eqty_metrics].to_csv(self.hw_trn_taz)
        
        
        df["mode"] = df["mode"].apply( self.transit_mode_map)
        df = df.groupby("mode").sum().reset_index()
        
        df.to_csv(self.transit_mode)
        return dfrlk




