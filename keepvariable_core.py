import inspect
import ast

def get_definition(jump_frames,*args,**kwargs):
    """Returns the definition of a function or a class from inside"""
    frame = inspect.currentframe()
    frame = inspect.getouterframes(frame)[jump_frames]
    string = inspect.getframeinfo(frame[0]).code_context[0].strip()
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
        keyword=string.split("=")[1].split("(")[0]
        varname=string.split("=")[0]
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

def save_variables(variables):
    with open("vars.kpv","w+") as file:
        file.write(str(variables))
        
def load_variable():    
    definition=get_definition(2)
    varname,keyword,inputs=analyze_definition(definition)    
    with open("vars.kpv","r") as file:
        rows=file.readlines()        
    variable_dict=ast.literal_eval(rows[0])
    this_variable=variable_dict[varname]
    return(this_variable)