# Original Python examples from Distributed_Systems_Python_Codes_with_CSV.pdf
# No CSV generation. Create the CSV files yourself using the PDF.
# Set EXPERIMENT to the experiment number (1-10).
# Code statements match the PDF; indentation glitches in experiments 3 and 6 corrected.
EXPERIMENT = 8


# 1. RMI / RPC — Remote Procedure Call Simulation
if EXPERIMENT == 1:
    import csv

    def server(method, value):
        print("[Server] Request received:", method, value)
        if method == "double":
            return value * 2
        elif method == "square":
            return value * value
        elif method == "negate":
            return -value
        return "Invalid method"

    with open("rpc_calls_49.csv", "r", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            method = row["method_name"]
            value = int(row["param_value"])
            print("\n[Client]", row["client_name"], "sends call", row["call_id"])
            result = server(method, value)
            print("[Client] Result:", result)

# 2. Multithreading — Concurrent Processing and Locks
if EXPERIMENT == 2:
    import csv
    import threading

    completed = 0
    lock = threading.Lock()

    def process_task(row):
        global completed
        print(threading.current_thread().name, "processing",
              row["task_id"], "burst:", row["burst_ms"], "ms")
        with lock:
            completed += 1

    with open("tasks_50.csv", "r", newline="") as file:
        tasks = list(csv.DictReader(file))

    threads = []
    for task in tasks:
        t = threading.Thread(target=process_task, args=(task,),
                             name=task["thread_name"])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("Total completed tasks:", completed)

# 3. Lamport's Logical Clocks
if EXPERIMENT == 3:
    import csv

    clocks = {}
    messages = {}

    with open("events_61.csv", "r", newline="") as file:
        for row in csv.DictReader(file):
            process = row["process_id"]
            action = row["event_type"].strip().lower()
            target = row["message_to"].strip()
            clocks.setdefault(process, int(row["initial_clock"]))

            if action == "receive":
                message_time = messages.get(process, 0)
                clocks[process] = max(clocks[process], message_time) + 1
            else:
                clocks[process] += 1

            print("Event", row["event_id"], process, action,
                  "| Clock =", clocks[process])

            if action == "send" and target:
                messages[target] = clocks[process]

# 4. Cristian's Algorithm — Physical Clock Synchronization
if EXPERIMENT == 4:
    import csv

    with open("cristian_clocks_105.csv", "r", newline="") as file:
        for row in csv.DictReader(file):
            t0 = int(row["t0_send_ms"])
            server_time = int(row["t_server_ms"])
            t1 = int(row["t1_recv_ms"])
            rtt = t1 - t0
            delay = rtt / 2
            estimated = server_time + delay
            print("Client:", row["client_id"])
            print("RTT:", rtt, "ms")
            print("Estimated delay:", delay, "ms")
            print("Synchronized time:", estimated, "ms")
            print()

# 5. Berkeley's Algorithm — Clock Averaging
if EXPERIMENT == 5:
    import csv

    nodes = []
    with open("node_clocks_119.csv", "r", newline="") as file:
        for row in csv.DictReader(file):
            faulty = row["is_faulty"].strip().lower() == "true"
            if not faulty:
                nodes.append((row["node_id"], float(row["clock_time_sec"])))

    if nodes:
        average = sum(node[1] for node in nodes) / len(nodes)
        print("Average clock time:", round(average, 2))
        for node_id, current in nodes:
            print(node_id, "adjustment:", round(average - current, 2), "seconds")
    else:
        print("No non-faulty clocks found.")

# 6. Vector Clocks — Causal Ordering
if EXPERIMENT == 6:
    import csv

    vectors = {"P1": [0, 0], "P2": [0, 0]}
    messages = {}

    with open("vector_events_60.csv", "r", newline="") as file:
        for row in csv.DictReader(file):
            process = row["process_id"]
            action = row["event_action"].strip().lower()
            target = row["target_process"].strip()
            index = int(process[1:]) - 1

            if action == "receive":
                message = messages.get(process, [0, 0])
                for i in range(2):
                    vectors[process][i] = max(vectors[process][i], message[i])
                vectors[process][index] += 1
            else:
                vectors[process][index] += 1

            print(row["event_id"], process, action, ":", vectors[process])
            if action == "send" and target:
                messages[target] = vectors[process].copy()

# 7. Ring Algorithm — Leader Election
if EXPERIMENT == 7:
    import csv

    nodes = {}
    with open("ring_nodes_63.csv", "r", newline="") as file:
        for row in csv.DictReader(file):
            node = int(row["node_id"])
            nodes[node] = {
                "next": int(row["next_node_id"]),
                "active": row["is_active"].strip().lower() == "true",
                "rank": int(row["priority_rank"])
            }

    initiator = 2
    current = initiator
    visited = set()
    participants = []
    print("Election started by node", initiator)

    while current not in visited:
        visited.add(current)
        if nodes[current]["active"]:
            participants.append(current)
            print("Active node participates:", current)
        current = nodes[current]["next"]

    if participants:
        leader = max(participants, key=lambda n: nodes[n]["rank"])
        print("New leader elected:", leader)
    else:
        print("No active node found.")

# 8. Bully Algorithm — Leader Election
if EXPERIMENT == 8:
    import csv

    alive = []
    with open("bully_cluster_64.csv", "r", newline="") as file:
        for row in csv.DictReader(file):
            if row["is_alive"].strip().lower() == "true":
                alive.append(int(row["node_id"]))

    initiator = 2
    print("Process", initiator, "starts election")
    for node_id in alive:
        if node_id > initiator:
            print("Election message sent to process", node_id)

    if alive:
        leader = max(alive)
        print("New coordinator:", leader)
        print("Coordinator announcement sent")
    else:
        print("No active process is available.")

# 9. Dynamic Load Balancing — Round-Robin and Least-Connections
if EXPERIMENT == 9:
    import csv

    with open("workload_51.csv", "r", newline="") as file:
        jobs = list(csv.DictReader(file))

    servers = ["Server1", "Server2", "Server3"]
    print("Round-Robin Scheduling")
    for i, job in enumerate(jobs):
        print(job["job_id"], "assigned to", servers[i % len(servers)])

    print("\nLeast-Connections Scheduling")
    connections = [0, 0, 0]
    for job in jobs:
        min_index = 0
        for i in range(1, len(servers)):
            if connections[i] < connections[min_index]:
                min_index = i
        print(job["job_id"], "assigned to", servers[min_index])
        connections[min_index] += 1
    print("Final connection counts:", connections)

# 10. MPI Matrix Multiplication — Scatter-Gather Simulation
if EXPERIMENT == 10:
    import csv

    A = []
    B = []

    # Each CSV row stores an A row, then the matching B row.
    with open("matrix_62.csv", "r", newline="") as file:
        for row in csv.DictReader(file):
            A.append([int(row["col_0"]), int(row["col_1"])])
            B.append([int(row["col_2"]), int(row["col_3"])])

    n = len(A)
    result = []

    # Scatter: each simulated process receives one row of A.
    for i in range(n):
        print("Process", i, "receives row:", A[i])

    # Compute each row of A multiplied by matrix B.
    for i in range(n):
        result_row = []
        for j in range(n):
            total = 0
            for k in range(n):
                total += A[i][k] * B[k][j]
            result_row.append(total)
        result.append(result_row)

    # Gather all calculated rows.
    print("Final matrix:")
    for row in result:
        print(row)
