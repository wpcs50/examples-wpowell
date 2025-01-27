from . import caliperpy as cp
import psutil

def get_process_id_by_name(process_name):
    for process in psutil.process_iter():
        if process.name() == process_name:
            return process.pid
        
    return None

class TC:
    def __init__(self, name):
      self.name = name
      self.conn = None

    def __enter__(self):
      
      self.conn = cp.TransCAD.connect() 
      self.process_id = get_process_id_by_name("tcw.exe")
      print ("enter TC connection: %s---PID%s---%s"%(self.conn,self.process_id,"") )
      return self.conn 

    def __exit__(self, exc_type, exc_val, exc_tb):
        cp.TransCAD.disconnect()
        print ("exit TC connection: %s---%s"%(self.conn,self.process_id) )
        # self.conn.Close()
    

    def open(self):
      
      self.conn = cp.TransCAD.connect() 
      return self.conn 

    def close(self, exc_type, exc_val, exc_tb):
        self.conn.disconnect()