from keepvariable_core import Var,kept_variables,save_variables,load_variable

a=Var("b")
b=Var("c")

c=a+b

print(c)

dict1=Var({1,2,3,4,5})

list1=Var(a)

print(kept_variables)
save_variables(kept_variables)

list1=load_variable()

b=load_variable()

c=Var(c)
print(list1)