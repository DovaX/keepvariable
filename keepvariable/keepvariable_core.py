import inspect
import ast
import redis
import json
import pandas as pd
import numpy as np
import datetime

def get_definition(jump_frames,*args,**kwargs):
    """Returns the definition of a function or a class from inside"""
    frame = inspect.currentframe()
    frame = inspect.getouterframes(frame)[jump_frames]
    try:
        string = inspect.getframeinfo(frame[0]).code_context[0].strip()
    except TypeError as e:
        print("Warning: Keepvariable was not correctly executed",e)
        string=""
    return(string)
    
def analyze_definition(string):    
    args = string[string.find('(') + 1:-1].split(',')
    inputs = []
    for i in args:
        if i.find('=') != -1:
            inputs.append(i.split('=')[1].strip())
        else:
            inputs.append(i)
    try:
        keyword=string.split("=")[1].split("(")[0].strip()
        varname=string.split("=")[0].strip()
    except:
        keyword=""
        varname=""
    return(varname,keyword,inputs)
 
kept_variables={}

class Var:
    def __new__(cls,var):
        definition=get_definition(2,var)
        varname,keyword,inputs=analyze_definition(definition)
        joined_inputs=",".join(inputs)
        try:
            kept_variables[varname]=eval(joined_inputs) #not use ast.literal_eval -> wrong handling of strings for this use case
        except NameError:
            kept_variables[varname]=var
        return(var)
    
    
class VarSafe:
    def __new__(cls,var,varname,inputs):
        """
        var=variable
        varname=string of var.__name__
        inputs=parameters in bracket
        
        Example - difference to kv.Var:
        db_details_list=kv.Var(db_details_list)    
        db_details_list=kv.VarSafe(db_details_list,"db_details_list","db_details_list")
        """
        # definition=get_definition(2,var)
        # varname,keyword,inputs=analyze_definition(definition)
        joined_inputs=",".join(inputs)
        try:
            kept_variables[varname]=eval(joined_inputs) #not use ast.literal_eval -> wrong handling of strings for this use case
        except NameError:
            kept_variables[varname]=var
        return(var)

def save_variables(variables,filename="vars.kpv"):
    with open(filename,"w+", encoding="utf8",errors='ignore') as file: #errors ignore dirty way - might be improved
        #try:
        file.write(str(variables)) #.encode("utf-8")
        #except UnicodeEncodeError:
            #print("")
            #pass
   
        

def load_variable_safe(filename="vars.kpv",varname="varname"):
    with open(filename,"r", encoding="utf8",errors='ignore') as file:  #errors ignore dirty way - might be improved
        rows=file.readlines()        
    variable_dict=ast.literal_eval(rows[0])
    this_variable=variable_dict[varname]
    return(this_variable)

    
def load_variable(filename="vars.kpv"):    
    definition=get_definition(2)
    varname,keyword,inputs=analyze_definition(definition) 
    this_variable=load_variable_safe(filename=filename,varname=varname)
    return(this_variable)


def load_variables(filename="vars.kpv"):    
    with open(filename,"r", encoding="utf8",errors='ignore') as file:  #errors ignore dirty way - might be improved
        rows=file.readlines()        
    variable_dict=ast.literal_eval(rows[0])
    return(variable_dict)


class RefList:
    """This object type serves for enabling grouping lists of objects (e.g. visible/draggable) with common attribute in one list which is always up to date"""
    def __init__(self,elements=[],referenced_lists=None):
        self.elements=elements
        self.referenced_lists=referenced_lists
        self.embedded_in_lists=[]
        if self.referenced_lists is not None:
            self.elements=[]
            for i,magic_list in enumerate(self.referenced_lists):
                for item in magic_list.elements:
                    self.elements.append(item)
                self.referenced_lists[i].embedded_in_lists.append(self)
                                
    def append(self,obj):
        self.elements.append(obj)
        for i,list1 in enumerate(self.embedded_in_lists):
           
            self.embedded_in_lists[i].elements=[]
            for j in range(len(list1.referenced_lists)):
                self.embedded_in_lists[i].elements+=list1.referenced_lists[j].elements
        
    def pop(self,index):
        self.elements.pop(index)
        for i,item in enumerate(self.embedded_in_lists):
            item.elements.pop(index)
              
            
    def __str__(self):
        return(str(self.elements))
   
    
class KeepVariableDummyRedisServer:
    def __init__(self,host="localhost"):
        self.host=host
        self.storage={}
        
    def parse_saved_value(self, value, additional_params: dict = {}):
        """Parses enterted value to json format. Certain special type values are serialized (DFs, datetimes, functions, classes).

        Args:
            value (Any): Entered value of any type (not all types can get serialized and stored however!)
            additional_params (dict, optional): Additional parameters used for serialization, e.g. for a function variable it's 
            code must be passed somehow --> additional_params = {'code': <function_code>}. Defaults to {}.
        """        
        if isinstance(value,list) or isinstance(value,bool) or isinstance(value,dict):
            value=json.dumps(value)
        elif isinstance(value,pd.DataFrame):
            data=value.values.tolist()
            columns=list(value.columns)
            final_data={"columns":columns,"data":data,"object_type":"pd.DataFrame"}
            print(final_data)
            value=json.dumps(final_data)
        elif isinstance(value,np.ndarray):
            data=value.tolist()
            final_data={"data":data,"object_type":"np.ndarray"}
            value=json.dumps(final_data)
        elif isinstance(value, datetime.datetime):
            data=value.strftime("%Y-%m-%d %H:%M:%S")
            final_data = {"data": data, "object_type": "datetime.datetime"}
            value = json.dumps(final_data)
        elif inspect.isfunction(value):
            code = additional_params.get("code")
            value = {"code": code, "object_type": "function"}
            value = json.dumps(code)
        elif inspect.isclass(value):
            code = additional_params.get("code")
            value = {"code": code, "object_type": "class"}
            value = json.dumps(code)
            
        return(value)
    
    def decode_loaded_value(self,value):
        """Decodes value stored in redis into it's initial value.
        For functions and classes only their code is returned --> they need to be evaluated afterwards!!!

        Args:
            value (Any): Variable value from redis

        Returns:
            Any: Parsed variable value
        """        
        try:
            value=json.loads(value)
            if "object_type" in value and isinstance(value,dict):
                if value["object_type"]=="pd.DataFrame":
                    df=pd.DataFrame(value["data"],columns=value["columns"])
                    return(df)
                elif value["object_type"]=="np.ndarray":
                    array=pd.DataFrame(value["data"]).values #to ensure 64bit values in array
                    return(array)
                elif value["object_type"] == "datetime.datetime":
                    datetime_value = datetime.datetime.strptime(value["data"],"%Y-%m-%d %H:%M:%S")
                    return datetime_value
                elif value["object_type"] == "function" or value["object_type"] == "class":
                    return value["code"]
            return(value)
        except json.JSONDecodeError: #if type is str, it fails to decode
            return(value)
        
        
    def set(self, key, value, additional_params: dict = {}):
        value=self.parse_saved_value(value, additional_params)
        self.storage[key]=value
        return({key:value})

    def get(self,key):
        value=self.storage.get(key)
        decoded_value=self.decode_loaded_value(value)
        return(decoded_value)
    
class KeepVariableRedisServer(KeepVariableDummyRedisServer):
    def __init__(self,host="localhost",port=6379,password=None):
        self.host=host
        self.port=port
        self.password=password
        
        self.redis = redis.Redis(host=self.host, port=self.port, db=0,password=self.password,decode_responses=True,charset="utf-8")
    
    @property
    def kept_variables(self):
        return(self._kept_variables)
    
    @kept_variables.setter
    def kept_variables(self,kept_variables):
        return(self._kept_variables)
    
    def set(self,key,value):
        value=self.parse_saved_value(value)
        result=self.redis.set(key,value)
        return(result)
        
    def get(self,key):
        value=self.redis.get(key)
        decoded_value=self.decode_loaded_value(value)
        return(decoded_value)
        

