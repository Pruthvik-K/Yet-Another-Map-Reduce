import os
import sys
import json 
import datetime

def create_DFS(config_file_path) :
    config_file = open(config_file_path)
    configuration = json.load(config_file)
    config_file.close()

    worker = os.path.expandvars(configuration["worker_path"])
    master = os.path.expandvars(configuration['master_path'])
    
    if os.path.isdir(worker) or os.path.isdir(master) :
        print('DFS exists !!')
        quit()
    

    os.makedirs(worker)
    os.makedirs(master)

    log_file_path = os.path.expandvars(configuration['log_path'])
    log_file = open(log_file_path,'a+')
    log_file.write(str(datetime.datetime.now()) + " : created master log file \n")

    file_list_path = os.path.join(master,'file_list.json')
    file_list = open(file_list_path, 'w')
    file_list.write(json.dumps({"files":[]}, indent=3))
    file_list.close()
    log_file.write(str(datetime.datetime.now()) + " : created master file list \n")
    configuration['file_list_path'] = file_list_path

    
    location_file_path = os.path.join(master,'location_file.json')
    location_file = open(location_file_path, 'w')
    location_file.write(json.dumps({}, indent=3))
    location_file.close()
    log_file.write(str(datetime.datetime.now()) + " : created master location file \n")
    configuration['location_file_path'] = location_file_path

   
    worker_tracker_file_path = os.path.join(master,'worker_tracker.json')
    
    worker_tracker_file = open(worker_tracker_file_path, 'w')
    log_file.write(str(datetime.datetime.now()) + " : created worker tracker file \n")
    configuration['worker_tracker_file_path'] = worker_tracker_file_path
    

    worker_data = {}
    worker_data['Next_worker']=1

    worker_size = configuration['worker_size']
    blocks = [0 for i in range(worker_size)]
    
    
    N = configuration['N']
    configuration['paths_to_workers'] = []
    for i in range(N):
        node_name = 'worker' + str(i+1)
        path = os.path.join(worker, node_name)
        os.mkdir(path)

        log_file.write(str(datetime.datetime.now()) + ": created worker "+node_name+ "\n")
        worker_data[node_name] = blocks
        configuration['paths_to_workers'].append(path)
    
    worker_tracker_file.write(json.dumps(worker_data, indent=3))
    
    

    worker_tracker_file.close()
    log_file.close()


    config_file = open(config_file_path,'w')
    config_file.write(json.dumps(configuration, indent=3))
    config_file.close()


config_file_path = sys.argv[1]
create_DFS(config_file_path)