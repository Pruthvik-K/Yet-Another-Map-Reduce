import  os, json
import helper
import datetime


home = os.path.expandvars('/home/$USER')
configuration_file = open('configuration_file.json','r')
configuration = json.load(configuration_file)

N = configuration['N']
split_size = configuration['split_size']


master = os.path.expandvars(configuration['master_path'])
worker = os.path.expandvars(configuration['worker_path'])



def mapReducejob(input_file,mapper_path,reducer_path, output_name):
    '''print('\n'+input_file)
    print(mapper_path)
    print(reducer_path)
    print(output_name + '\n') '''

    if not os.path.exists(mapper_path):
        raise Exception('Mapper not found')
    if not os.path.exists(reducer_path):
        raise Exception('Reducer not found')
    out_path = os.path.join(home,output_name)
    if os.path.exists(out_path):
        raise Exception('File exists',out_path)

    file_list = open(configuration['file_list_path'],'r+')
    file_names = json.load(file_list)
    
    location_file = open(configuration["location_file_path"],"r+")
    location_data = json.load(location_file)

    logfile_path = os.path.expandvars(configuration['log_path'])
    log_file = open(logfile_path,'a+')


    worker_tracker = open(configuration['worker_tracker_file_path'], 'r+')
    worker_details = json.load(worker_tracker)

    if input_file not in location_data:
        location_file.close()
        raise Exception(input_file, "File does not exist")

    save_path = "/home/$USER"
    save_path = os.path.expandvars(save_path)
    file_name = "temp"
    complete_path = os.path.join(save_path,file_name)
    os.chmod(mapper_path, 0o777)
    
    location_data[file_name] = []
    next_worker = worker_details['Next_worker']

    for file_blk in location_data[input_file]:
        block_path = os.path.join(worker, file_blk)
        index = 0
        curr = 0
		
        while curr < N:
            DN_str = 'worker' + str(next_worker)
            try:
                index = worker_details[DN_str].index(0)
				# print(index)
                break
            except Exception:
                curr += 1
                next_worker = (next_worker % N) + 1
			
        if curr == N:
            raise Exception("All workers are full")
        block = 'block' + str(index)
		
        worker_details[DN_str][index] = 1
        next_worker = (next_worker % N) + 1
        
        store_path = DN_str + '/' + block

        os.chmod(worker, 0o777)
        file_path = os.path.join(worker, store_path)
		# print(file_path)
        
        os.system("cat {} | python3 {} >> {}".format(block_path,mapper_path,file_path))
        

        location_data[file_name].append(store_path)
        
    worker_details['Next_worker'] = next_worker


    worker_tracker.seek(0)
    worker_tracker.truncate(0)
    json.dump(worker_details, worker_tracker, indent=3)

    log_file.write(str(datetime.datetime.now()) + " : worker_tracker file updated \n")

    location_file.seek(0)
    location_file.truncate(0)
    json.dump(location_data, location_file, indent=3)
    log_file.write(str(datetime.datetime.now()) + " : location file updated \n")


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------    
    
    fileArr =[]
    file_name_arr =[]
    for file_blk in location_data[input_file]:
        block_path = os.path.join(worker, file_blk)
        index = 0
        curr = 0
		
        while curr < N:
            DN_str = 'worker' + str(next_worker)
            try:
                index = worker_details[DN_str].index(0)
				# print(index)
                break
            except Exception:
                curr += 1
                next_worker = (next_worker % N) + 1
			
        if curr == N:
            raise Exception("All workers are full")
        block = 'block' + str(index)
		
        worker_details[DN_str][index] = 1
        next_worker = (next_worker % N) + 1
        
        store_path = DN_str + '/' + block

        os.chmod(worker, 0o777)
        file_path = os.path.join(worker, store_path)
		# print(file_path)
        
        f=open(file_path,'w+')
        fileArr.append(f)
        file_name_arr.append(file_path)

        location_data[file_name].append(store_path)
        
    worker_details['Next_worker'] = next_worker


    worker_tracker.seek(0)
    worker_tracker.truncate(0)
    json.dump(worker_details, worker_tracker, indent=3)

    log_file.write(str(datetime.datetime.now()) + " : worker_tracker file updated \n")

    location_file.seek(0)
    location_file.truncate(0)
    json.dump(location_data, location_file, indent=3)
    log_file.write(str(datetime.datetime.now()) + " : location file updated \n")


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------    
    
    




    for file_blk in location_data[file_name]:
        block_path = os.path.join(worker, file_blk)
        if os.path.getsize(block_path)!=0:
            content = open(block_path, 'r')
            for line in content:
                val = helper.hashFunc(line)
                fileArr[val%N].write(line+'\n')
            content.close()
    
 
    for file in fileArr:
        file.seek(0)
        data=file.readlines()
        file.seek(0)
        file.truncate(0)
        data.sort()
        file.write(''.join(data))
        file.close()



    for file in file_name_arr:
        if not os.path.getsize(file) ==0:
        # os.system("cat {} | python3 {} ".format(home+"/file"+str(i),reducer_path, "/home/$USER/Desktop/output"))
            os.system("cat {} | python3 {} >> /home/$USER/{}".format(file,reducer_path,output_name))
       
    for file in file_name_arr:
        os.remove(file)
    

   

    file_list.close()
    location_file.close()
    worker_tracker.close()

    
  

     
''' 
    
'''