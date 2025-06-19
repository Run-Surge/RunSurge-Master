
import json
import time

# --- Communication & Execution Stubs ---
# In a real system, these would interact with a message queue (e.g., RabbitMQ, ZeroMQ)
def wait_for_data(variable_name, from_node):
    print(f"--> [WAIT] Waiting for '{variable_name}' from {from_node}...")
    time.sleep(1) # Simulate network latency
    print(f"<-- [RECV] Received '{variable_name}'.")
    # In a real system, this would block until data is received.
    # Here, we return a placeholder.
    return f"data_for_{variable_name}"

def send_data(variable_name, data, consumers):
    print(f"--> [SEND] Sending '{variable_name}' to {consumers}...")
    time.sleep(0.5) # Simulate network latency
    print(f"<-- [SENT] '{variable_name}'.")


# ==================================================
#          RECONSTRUCTED FUNCTION DEFINITIONS          
# ==================================================

def add1(data):
    header = data[0]
    rows = data[1:]
    new_data = []
    for row in data:
        new_row = []
        for value in row:
            new_value = int(value) + 1
            new_row.append(new_value)
            new_data.append(new_row)
            new_data.insert(0, header)
            return new_data

def hello2():
    v = 4 + 3
    return v

def hello(z):
    z = z + 1
    return z

def hello3(x, y, z):
    x = x + 1
    x = x + (y + z + 5)
    return x

def calc(x, y, z, a, b, c, d):
    x = [['Output', x, y, z, a, b, c, d]]
    return x

# ==================================================
#               MAIN EXECUTION LOGIC                 
# ==================================================

def main(node_name):
    print(f"\n*** Starting execution on {node_name} ***\n")
    try:
        with open(f'{node_name}_data.json', 'r') as f:
            initial_data = json.load(f)
        print("--- Loaded initial data. ---")
    except FileNotFoundError:
        initial_data = {}
        print("--- No initial data file found. ---")
    
    # --- SCHEDULED TASKS ---

    # --- Task: Execute Block 0 ---
    print('--- Loading initial data ---')
    data = initial_data['data']
    print(f'EXECUTING: data = add1(data)')
    data = add1(data)
    print(f'EXECUTING: z = add1(data)')
    z = add1(data)
    print(f'EXECUTING: x = hello2()')
    x = hello2()
    print(f'EXECUTING: y = z.copy()')
    y = z.copy()
    print(f'EXECUTING: z = hello(z)')
    z = hello(z)
    print(f'EXECUTING: a = hello(z)')
    a = hello(z)
    print(f'EXECUTING: k = hello(a)')
    k = hello(a)
    print(f'EXECUTING: a = hello(x)')
    a = hello(x)
    print(f'EXECUTING: d = hello(x)')
    d = hello(x)
    print(f'EXECUTING: z = hello(a)')
    z = hello(a)
    print(f'EXECUTING: b = hello3(x, y, z)')
    b = hello3(x, y, z)
    print(f'EXECUTING: c = hello(z)')
    c = hello(z)
    print(f'EXECUTING: output = calc(x, y, z, a, b, c, d)')
    output = calc(x, y, z, a, b, c, d)
    
    print(f"\n*** Execution on {node_name} complete. ***\n")

if __name__ == "__main__":
    main("nodeA")
