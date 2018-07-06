import time
from neo4j.v1 import GraphDatabase, basic_auth

_driver = GraphDatabase.driver("bolt://localhost")

# Provide an interface to get a session.
def get_session():
    # Open a session to the DB.
    NEO4J_HOST = 'localhost'
    NEO4J_PASSWORD = 'scilo'
    NEO4J_PORT = '7687'
    NEO4J_USERNAME = 'neo4j'
    #driver = GraphDatabase.driver("bolt://{0}:{1}".format(NEO4J_HOST, NEO4J_PORT), auth=basic_auth(NEO4J_USERNAME, NEO4J_PASSWORD))
    #driver = GraphDatabase.driver('bolt://{0}:{1}'.format(NEO4J_HOST, NEO4J_PORT))
    
    return _driver.session()


# Provide an interface to run commands.
def run_commands(session, command_string_list, track_time_flag, debug_flag, display_results_flag):
    # Combine command strings.
    command = "\n".join(command_string_list)
        
    # Run the command, handling debug and time operations as desired.
    if debug_flag == True:
        print("NEO4J - RUNNING")
        print(command)
        
    if track_time_flag == True:
        start_time = time.time()
        print("Command count:", len(command_string_list), "Start time:", start_time)
    
    command_result_list = session.run(command)
    if display_results_flag == True:
        for command_result in command_result_list:
            print(command_result)
            
    if track_time_flag == True:
        end_time = time.time()
        print("Command completed.  End time:", end_time, "Total time:", end_time-start_time)


# Create a command to clear the current database.
def create_clear_database_command():
    return "MATCH (n)\nOPTIONAL MATCH (n)-[r]-()\nDELETE n,r"


# Create a command to setup indexing on host nodes.
def create_host_index_command():
    return "CREATE INDEX ON :Host(name)"


# Create a command to setup indexing on socket nodes.
def create_socket_index_command():
    return "CREATE INDEX ON :Socket(name)"


# Create a command to setup indexing on host nodes.
def create_warm_cache_command():
    property = "name"
    return "MATCH (n)\nOPTIONAL MATCH (n)-[r]->()\nRETURN count(n.{0}) + count(r.{0})".format(property)


# Create host node commands: create/match X nodes as separate or a single command.
def create_host_node_commands(host_start_index, host_count, match_flag, as_single_command):
    command_list = []
    for hid in range(host_count):
        hid += host_start_index
        command_list.append("(hid_{0}:Host {{name:'{0}'}})".format(hid))

    if as_single_command == True:
        command_list = [",".join(command_list)]

    if match_flag == False:
        command_list = ["CREATE {0}".format(command_string) for command_string in command_list]
    else:
        command_list = ["MATCH {0}".format(command_string) for command_string in command_list]

    return command_list


# Create a return command for the given host counts.
def create_return_host_node_command(host_start_index, host_count):
    command_list = []
    for hid in range(host_count):
        hid += host_start_index
        command_list.append("hid_{0}".format(hid))

    return "RETURN " + ",".join(command_list)


# Create socket node commands: create/match X nodes as separate or a single command.
def create_socket_node_commands(socket_start_index, socket_count, match_flag, as_single_command, match_with_index_flag):
    command_list = []
    for socket_id in range(socket_count):
        socket_id += socket_start_index
        if match_with_index_flag == True:
            command_list.append("(socket_10_7_7_{0}_{0}:Socket {{name:'10_7_7_{0}_{0}'}}) USING INDEX socket_10_7_7_{0}_{0}:Socket(name)".format(socket_id))
        else:
            command_list.append("(socket_10_7_7_{0}_{0}:Socket {{name:'10_7_7_{0}_{0}'}})".format(socket_id))

    if as_single_command == True:
        command_list = [",".join(command_list)]

    if match_flag == False:
        command_list = ["CREATE {0}".format(command_string) for command_string in command_list]
    else:
        command_list = ["MATCH {0}".format(command_string) for command_string in command_list]

    return command_list


# Create a return command for the given host counts.
def create_return_socket_node_command(socket_start_index, socket_count):
    command_list = []
    for socket_id in range(socket_count):
        socket_id += socket_start_index
        command_list.append("socket_10_7_7_{0}_{0}".format(socket_id))

    return "RETURN " + ",".join(command_list)


# Create relationship commands: create/match X nodes as separate or a single command.
def create_relationship_commands(host_start_index, host_count, socket_start_index, socket_count, time_stamp, match_flag, as_single_command):
    command_list = []
    for hid in range(host_count):
        for socket_id in range(socket_count):
            loop_hid = hid + host_start_index
            socket_id += socket_start_index
            rel_name = "hs_{0}_10_7_7_{1}_{1}".format(loop_hid, socket_id)
            command_list.append("(hid_{0})-[:LOCAL_SOCKET {{name:'{1}', time:{2}}}]->(socket_10_7_7_{3}_{3})".format(loop_hid, rel_name, time_stamp, socket_id))

    if as_single_command == True:
        command_list = [",".join(command_list)]

    if match_flag == False:
        command_list = ["CREATE {0}".format(command_string) for command_string in command_list]
    else:
        command_list = ["MATCH {0}".format(command_string) for command_string in command_list]

    return command_list


# Runs the commands, tracking time taken.
def run_test_commands(session, command_string_list, run_as_separate_commands, display_results_flag):
    track_time_flag = False
    debug_flag = True
    
    import time
    start_time = time.time()
    if run_as_separate_commands == False:
        run_commands(session, command_string_list, track_time_flag, debug_flag, display_results_flag)
    else:
        for command_string in command_string_list:
            run_commands(session, [command_string], track_time_flag, debug_flag, display_results_flag)
            
    return time.time() - start_time


def run_test_1(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, types_as_single_command_flag):
    # TEST #1: 
    # Create nodes and relationships, sent together as one call. 
    
    # Prepare for test.
    run_test_commands(get_session(), [create_clear_database_command(), create_host_index_command(), create_socket_index_command()], True, True)
    
    # Set command flags.
    host_as_single_command_flag = types_as_single_command_flag
    socket_as_single_command_flag = types_as_single_command_flag
    rel_as_single_command_flag = types_as_single_command_flag    
    run_as_separate_commands = False
    
    # Set match flags.
    host_match_flag = False
    socket_match_flag = False
    socket_match_with_index_flag = False
    rel_match_flag = False
    
    # Run commands, tracking total time.
    command_string_list = []
    command_string_list.extend(create_host_node_commands(host_start_index, host_count, host_match_flag, host_as_single_command_flag))
    command_string_list.extend(create_socket_node_commands(socket_start_index, socket_count, socket_match_flag, socket_as_single_command_flag, socket_match_with_index_flag))
    command_string_list.extend(create_relationship_commands(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, rel_match_flag, rel_as_single_command_flag))
    total_time = run_test_commands(get_session(), command_string_list, run_as_separate_commands, True)
    
    # Report.
    print("Test #1: Create nodes and relationships together in one call.")
    print("- Test parameters:", host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp)
    print("- Types as a single command flag: {}".format(types_as_single_command_flag))
    print("- Total Time:", total_time)


def run_test_2(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, types_as_single_command_flag):
    # TEST #2: 
    # Create nodes as one bulk command.  
    # Match nodes, create relationships as a second bulk command.. 
    
    # Prepare for test.
    run_test_commands(get_session(), [create_clear_database_command(), create_host_index_command(), create_socket_index_command()], True, True)
    
    # Set command flags.
    host_as_single_command_flag = types_as_single_command_flag
    socket_as_single_command_flag = types_as_single_command_flag
    rel_as_single_command_flag = types_as_single_command_flag    
    run_as_separate_commands = False
    
    # Setup and run node commands.
    host_match_flag = False
    socket_match_flag = False
    socket_match_with_index_flag = False
    
    command_string_list = []
    command_string_list.extend(create_host_node_commands(host_start_index, host_count, host_match_flag, host_as_single_command_flag))
    command_string_list.extend(create_socket_node_commands(socket_start_index, socket_count, socket_match_flag, socket_as_single_command_flag, socket_match_with_index_flag))
    node_time = run_test_commands(get_session(), command_string_list, run_as_separate_commands, True)
    
    # Setup and run relationship commands.
    host_match_flag = True
    socket_match_flag = True
    rel_match_flag = False
    
    command_string_list = []
    command_string_list.extend(create_host_node_commands(host_start_index, host_count, host_match_flag, host_as_single_command_flag))
    command_string_list.extend(create_socket_node_commands(socket_start_index, socket_count, socket_match_flag, socket_as_single_command_flag, socket_match_with_index_flag))
    command_string_list.extend(create_relationship_commands(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, rel_match_flag, rel_as_single_command_flag))
    rel_time = run_test_commands(get_session(), command_string_list, run_as_separate_commands, True)
    
    total_time = node_time + rel_time
    
    print("Test #2: Create nodes in one call.  Match nodes and create relationships in a second call.")
    print("- Test parameters:", host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp)
    print("- Types as a single command flag: {}".format(types_as_single_command_flag))
    print("- Node time:", node_time)
    print("- Relationship time:", rel_time)
    print("- Total Time:", total_time)


def run_test_3(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, types_as_single_command_flag):
    # TEST #3: 
    # Create nodes as one bulk command.
    # Create each relationship as a separate bulk command (match nodes, create relationship).
    
    # Prepare for test.
    run_test_commands(get_session(), [create_clear_database_command(), create_host_index_command(), create_socket_index_command()], True, True)
    
    # Set command flags.
    host_as_single_command_flag = types_as_single_command_flag
    socket_as_single_command_flag = types_as_single_command_flag
    rel_as_single_command_flag = types_as_single_command_flag    
    run_as_separate_commands = False
    
    # Setup and run node commands.
    host_match_flag = False
    socket_match_flag = False
    socket_match_with_index_flag = False
    
    command_string_list = []
    command_string_list.extend(create_host_node_commands(host_start_index, host_count, host_match_flag, host_as_single_command_flag))
    command_string_list.extend(create_socket_node_commands(socket_start_index, socket_count, socket_match_flag, socket_as_single_command_flag, socket_match_with_index_flag))
    node_time = run_test_commands(get_session(), command_string_list, run_as_separate_commands, True)
    
    # Setup and run relationship commands.
    host_match_flag = True
    socket_match_flag = True
    rel_match_flag = False
    
    # Pull commands.
    host_command_list = create_host_node_commands(host_start_index, host_count, host_match_flag, False)
    socket_command_list = create_socket_node_commands(socket_start_index, socket_count, socket_match_flag, False, socket_match_with_index_flag)
    rel_command_list = create_relationship_commands(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, rel_match_flag, False)
    
    # Build the commands - match one node, one socket, and create the relationship between them.
    command_string_list = []
    index = 0
    for host_command in host_command_list:
        for socket_command in socket_command_list:
            rel_command = rel_command_list[index]
            full_command = "\n".join([host_command, socket_command, rel_command])
            command_string_list.append(full_command)
            index += 1
    
    # Force each relationship command to be ran separately.
    run_as_separate_commands = True
    
    # Run.
    rel_time = run_test_commands(get_session(), command_string_list, run_as_separate_commands, True)
     
    total_time = node_time + rel_time
    
    print("Test #3: Create nodes in one call.  Create relationships separately (match required nodes, create relationship) in successive calls.")
    print("- Test parameters:", host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp)
    print("- Types as a single command flag: {}".format(types_as_single_command_flag))
    print("- Node time:", node_time)
    print("- Relationship time:", rel_time)
    print("- Total Time:", total_time)


# Test setup.
rel_time_stamp = 1482273300
types_as_single_command_flag = False

# Call test.
import sys
try:
    host_start_index = int(sys.argv[1])
    host_count = int(sys.argv[2])
    socket_start_index = int(sys.argv[3])
    socket_count = int(sys.argv[4])
    test_number = int(sys.argv[5])
except:
    print("Failed to parse parameters.  Format should be: <host_count> <socket_count> <test_id>")
    raise

if test_number == 1:
    run_test_1(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, types_as_single_command_flag)
    
elif test_number == 2:
    run_test_2(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, types_as_single_command_flag)
    
elif test_number == 3:
    run_test_3(host_start_index, host_count, socket_start_index, socket_count, rel_time_stamp, types_as_single_command_flag)
    
else:
    raise Exception("Invalid test number.")
