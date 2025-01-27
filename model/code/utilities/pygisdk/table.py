
dk = None
import pandas as pd
import numpy as np
import os

class bin:
    """ 
    """
    

    def __init__(self,file, view ,conn):
        print("-"*60,dk)
        if dk is None:
            self.__dk = conn
            print("-"*60,"connection",self.__dk)
        else:
            self.__dk = dk
        if os.path.exists(file):
            self.file = file
        self.view   = view
        self.fields = self.GetFields()

    def GetFields(self):
        view_name = self.__dk.OpenTable(self.view,"FFB", [self.file,None])
        allfields = self.__dk.GetFields(view_name,"All")
        return allfields

    def GetDataframe(self):
        fds = self.fields[0]
        vs = self.__dk.GetDataVectors(self.view+"|",fds, None )

        records = {}
        for i,v in enumerate(vs):
            records[fds[i]] = self.__dk.v2a(v)
        
        df = pd.DataFrame.from_dict(records)

        return df