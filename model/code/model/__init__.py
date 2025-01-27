
def set_envn(func):
    """
    set a new PATH set before a run
    """
    def inner(logpath,*args, **kwargs):
        Set_Environment_Variables(logpath)
        func(*args, **kwargs)
    return inner


def Set_Environment_Variables(logpath = r"C:\\Users\\ZJin\\Desktop\\demo\\transCAD_bug\\numpyerror\\log_set.txt"):
    import os
    uname = os.environ.get('USERNAME')
    ls = [ "C:\Program Files\TransCAD 9.0\\",
           r"C:\Users\%s\anaconda3\envs\tdm23_env_1"%uname,
           r"C:\Users\%s\anaconda3\envs\tdm23_env_1\Library\bin"%uname,]
    ls = [ "C:\Program Files\TransCAD 9.0\\",]
    envpath = ";".join(ls)
    
    os.environ["PATH"] = envpath
    with open(logpath,"w") as file:
        print("PYTHONPATH:", os.environ.get('PYTHONPATH'),file=file)
        for path in os.environ.get('PATH').split(";"):
            print("PATH:", path,file=file)

def Add_Environment_Variable(logpath = r"C:\\Users\\ZJin\\Desktop\\demo\\transCAD_bug\\numpyerror\\log_add.txt"):
    import os
    uname = os.environ.get('USERNAME')
    old_path = os.environ.get('PATH')
    new_path = r"C:\Users\%s\anaconda3\envs\tdm23_env_1"%uname + ";" + old_path
    os.environ["PATH"] = new_path

    with open(logpath,"w") as file:
        print("PYTHONPATH:", os.environ.get('PYTHONPATH'),file=file)
        for path in os.environ.get('PATH').split(";"):
            print("PATH:", path,file=file)

def Check_Environment_Variables(logpath = r"C:\\Users\\ZJin\\Desktop\\demo\\transCAD_bug\\numpyerror\\log.txt"):
    import os
    with open(logpath,"w") as file:
        print("PYTHONPATH:", os.environ.get('PYTHONPATH'),file=file)
        for path in os.environ.get('PATH').split(";"):
            print("PATH:", path,file=file)