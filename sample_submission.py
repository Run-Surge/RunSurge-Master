##########################################################################################################
#! User defined variables

FILE_NAME = 'test.csv'  # Name of the file to read data from

##########################################################################################################







##########################################################################################################
#! User Code is written below this line (Note: code is written in the form of fucntions and entry point is a function named 'main')
#! all functions must have a return statement
#! the returned value is not assumed to be by reference, but a copy of it
#! no nested user defined functions are allowed
#! functions return value must be 1 value and a variable or a slice of it not an operation (ex: return x / return x[0] not return x + 1)
#! available aggregates are c --> concatenate, a --> average, s --> sum, m --> max, n --> min, l --> length, i --> multiply, empty string for means don't parallelize
#! format aggregation = "type:list"
#! each function must have an aggregation variable just before the return statement
def add1(data):
    header = data[0][0]
    rows = data[1:]
    new_data = []
    for row in data:
        new_row = []
        for value in row:
            new_value = int(value) + 1  
            new_row.append(new_value)
        new_data.append(new_row)
    new_data.insert(0, header) 
    aggregation = "s:new_data"  
    return new_data            
def hello(z):
    z = z + 1
    aggregation = ""
    return z
def hello2():
    v  = 4 + 3
    aggregation = ""
    return v
def hello3(x,y,z):
    x = x+1
    x += y + z + 5
    aggregation = ""
    return x 
def calc(x,y,z,a,b,c,d):
    # x =  [['Output', str(x), str(y), str(z), str(a), str(b), str(c), str(d)]] to be handled later
    x = [['Output', x, y, z, a, b, c,d]] 
    aggregation = ""
    return x
##########################################################################################################


##########################################################################################################
if __name__ == '__main__':
#---------------------------------------------------------------------------------------------------------
#! This block handles data loading please don't edit it (Note:The data is loaded into a list of name data)
    try:
        with open(FILE_NAME, 'r') as file:
            lines = file.readlines()
            data = [line.strip().split(',') for line in lines]
    except FileNotFoundError:
        print("File not found. Please ensure 'test.csv' exists in the current directory.")
#---------------------------------------------------------------------------------------------------------

#! User main function is defined here
#! Note for boosting performance if list is modified inside the function (each function call is independent) then return pass a 
#! copy of the list instead of the same list for performing different operations in parallel
    data = add1(data) 
    z = add1(data)
    x = hello2()
    y = z.copy() 
    z = hello(z)
    a = hello(z) 
    k = hello(a)
    a = hello(x) 
    z = hello(a)
    b = hello3(x,y,z)
    c = hello(z)
    d = hello(x) 
    output = calc(x,y,z,a,b,c,d)
    
    
#---------------------------------------------------------------------------------------------------------
#! Saving the output to a file please don't edit this block
#! output name should be a list named output
    with open('output.csv', 'w') as file:
        for row in output:
            file.write(','.join(row) + '\n')
#---------------------------------------------------------------------------------------------------------




##########################################################################################################






