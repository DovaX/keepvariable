import inspect
import ast

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
        db_details_list=kv.VarSafe(db_details_list,"db_details_list",str(db_details_list)) #safe for packaging
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
   