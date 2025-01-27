from pprint import pprint
# from . import dk
dk = None
import pandas as pd
import numpy as np
import ast

class MatrixHandle:

    def __init__(self,mtx_hdl,__dk):


        self.obj = mtx_hdl
        self.MatrixIndex    = __dk.GetMatrixIndex(mtx_hdl)
        # self.MatrixName           = self.__dk.GetMatrixName(mtx_hdl)
        self.BaseIndex      = __dk.GetMatrixBaseIndex(mtx_hdl)

        self.mc             = CreateMatrixCurrency(mtx_hdl,conn=__dk)
        self.RowLabels      = __dk.GetMatrixRowLabels(self.mc)
        self.ColumnLabels   = __dk.GetMatrixColumnLabels(self.mc)
        # self.EditorLabels   = self.__dk.GetMatrixEditorLabels(self.mc)

class MatrixClass:
    """ a python class wrapping a transcad  MatrixClass
    
    file:///C:/Program%20Files/TransCAD%209.0/Help/GISDK/api/MatrixClass.htm
    """
    

    def __init__(self,mtx_mobj,conn):
        print("-"*60,dk)
        if dk is None:
            self.__dk = conn
            print("-"*60,"connection",self.__dk)
        else:
            self.__dk = dk

        self.obj = mtx_mobj
        self.GetFileName   = self.obj.GetFileName()
        self.CoreNames     = self.obj.GetCoreNames()
        self.MatrixName    = self.obj.GetMatrixName()
        self.defaultCore   = self.CoreNames[0]
        self.mh            = self.obj.GetMatrixHandle() 
        self.MatrixHandle  = MatrixHandle(self.mh,self.__dk)

    def __getitem__(self, index):
        # print (index)
        if isinstance(index,int) == 1:
            row = index
            opts = {"Core": "hb",  "Row" :  row+1,}
            value = self.GetVector(opts)
        else:
            row, col = index
            if isinstance(row,slice) and isinstance(col,int):
                opts = {"Core": "hb",  "Column"  :  col+1,}
                value = self.GetVector(opts)
            elif isinstance(row,int) and isinstance(col,slice):
                opts = {"Core": "hb",  "Row"  :  row+1,}
                value = self.GetVector(opts)
            elif isinstance(row,int) and isinstance(col,int): 
                opts = {"Core": "hb",  "Row"  :  row+1,}
                arr = self.GetVector(opts)
                value = arr[col+1]
            else:
                value = np.NaN 
        return value

    # def __setitem__(self, index, value):
    #     row, col = index
    #     self.values[row][col] = value

    def MatrixStatistics(self,core=None):
        if core is None:
            core = self.CoreNames[0]
            print ("summarizing core [%s]"%core)
        tup  = self.obj.GetMatrixStatistics(core)
        df   = pd.DataFrame.from_records(tup,columns=[core,"num"])
        return df

    def GetVector(self, opts = {"Core": "hb",  "Marginal": "Row Sum"}):
        """ 
        Returns a vector of values from a matrix. The options are:
        
        Core:        name or index of the core to be filled. (string/int)
        Row:         ID of the row to get data from. (integer)
        Column:      ID of the column to get data from. (integer)
        Diagonal:    either "Row" or "Column" to get a row or column-based vector of diagonal elements. (string)
        Marginal:    e.g. "Row Sum" to get the sum of the rows, 
                          "Column Maximum" to get the max value in each column. 
                          Possible summaries: "Sum", "Minimum", "Maximum", 
                                              "Mean", "MinID", MaxID", "Count". (string)
        Index:       either "Row" or "Column" to get the row/column IDs. (string)
                     Only one of 'Row', 'Column', 'Diagonal', 'Marginal', or 'Index' should be included.
        """
        
        opts["Core"] = self.defaultCore if opts["Core"] is None else opts["Core"]
        v = self.obj.GetVector(opts)
        # self.__dk.ShowArray has a  maximum length of 1024?
        # vls = ast.literal_eval(self.__dk.ShowArray(v))
        # vector = np.array(vls[0])
        vtup = self.__dk.VectorToArray(v)
        vector = np.array(vtup)
        if "Diagonal" in opts:
            if opts["Diagonal"] == "Column":
                # reshape the array into a column with -1 
                # specifying that the number of rows should be inferred from 
                # the size of the array and the number of columns.
                vector = vector.reshape(-1, 1)

        return vector


def CreateMatrixCurrency(m,
                        core:str=None,
                        rowindex:str=None,
                        colindex:str=None,
                        options = None,
                        conn = None
                        ):      
    """
    CreateMatrixCurrency: Creates a matrix currency from a matrix handle.

    Arguments:
    - m (matrix): The matrix handle.
    - core (string): The name of the matrix core.
    - rowindex (string): The index to use for the matrix row (optional).
    - colindex (string): The index to use for the matrix column (optional).

    Returns:
    - matrix_currency: A matrix currency for accessing a matrix file, matrix cores, and a set of matrix indices.

    Note:
    - No options are currently supported.

    """
    if conn is not None:
        dk = conn
    obj = dk.CreateMatrixCurrency(m,core,rowindex,colindex,options)
    return obj


def OpenMatrix(file_name,file_based="Auto"):      
    """
    Summary
    Opens a matrix file.

    Syntax
    matrix = OpenMatrix(string file_name, string file_based)

    Argument
    file_name: The path and name of the matrix file
    file_based: A string indicating whether the matrix should be opened as file-based or memory-based, regardless of the mode stated in the matrix file. 
                "True" forces matrix to be file-based, 
                "False" forces matrix to be memory-based, 
                "Auto" (the default) uses the mode stored in the matrix file.

    Returns
    The matrix handle.
    """
    obj = dk.OpenMatrix(file_name,file_based)
    return obj

def CopyMatrix(*args):
    """
    Summary: Copies a matrix to a new matrix file.
    Changes: Added Memory Only and OMX options in Version 6.0.
    Syntax:  new_matrix = CopyMatrix(matcurrency currency, array options)

    Returns: The matrix handle of the new matrix.
    Ref: /Help/GISDK/dk/CopyMatrix.htm
    """ 
    obj = dk.CopyMatrix(*args)
    return obj

def ExportMatrix(*args) -> None:
    """
    ExportMatrix: Exports data from a matrix into a new table with one record per row or column.

    Arguments:
    - currency (matcurrency): A matrix currency.
    - items (array): An array of either row IDs (if dimension is "Columns") or column IDs (if dimension is "Rows").
    - dimension (string): "Rows" or "Columns".
    - class (string): The class of the resulting table: "dBASE", "FFA", "FFB", or "CSV".
    - file_name (string): The path and file name of the resulting table.
    - options (array): Additional options for the export.

    Options:
    - Marginal (string): The name of the marginal to be calculated: "Sum", "Mean", "Minimum", "Maximum", "MinID", "MaxID", or "Count".

    Changes:
    - In version 7.0, the "Marginal" option was added with the "MinID" and "MaxID" options.
    
    """ 
    obj = dk.ExportMatrix(*args)
    return obj

def ExportMatricesToExcelx(*args) -> None:
    """
    ExportMatricesToExcelx: Exports an array of matrix currencies to sheets in an xlsx format Excel file.

    Arguments:
    - specs (array): An array of specifications for each sheet to be created.
    - dimension (string): "Rows" or "Columns".
    - file_name (string): The path and file name of the resulting Excel file.

    Specification:
    - SheetName (string): The name of the data sheet.
    - Currency (matcurrency): The matrix currency.
    - Items (array): An array of either row IDs (if dimension is "Columns") or column IDs (if dimension is "Rows").
    - Options (array): Additional options for the export (optional).

    Options:
    - Marginal (string): The name of the marginal to be calculated: "Sum", "Mean", "Minimum", "Maximum", "MinID", "MaxID", or "Count".

    Changes:
    - Added in version 9.0.

    """ 
    obj = dk.ExportMatricesToExcelx(*args)
    return obj


