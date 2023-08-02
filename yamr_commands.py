import json
import helper
import os
import datetime
import helper

configuration_file = open('configuration_file.json','r')
configuration = json.load(configuration_file)
configuration_file.close()



master = os.path.expandvars(configuration['master_path'])
worker = os.path.expandvars(configuration['worker_path'])
N = configuration['N']
split_size = configuration['split_size']


logfile_path = os.path.expandvars(configuration['log_path'])
log_file = open(logfile_path,'a+')

	

def write(source):	


	file_list = open(configuration['file_list_path'],'r+')
	file_names = json.load(file_list)

	location_file = open(configuration["location_file_path"],"r+")
	location_data = json.load(location_file)

	worker_tracker = open(configuration['worker_tracker_file_path'], 'r+')
	worker_details = json.load(worker_tracker)


	file_name = source.split('/')[-1]
	if file_name in file_names["files"]:
		raise Exception("File already exists " ,file_name)

	file_names["files"].append(file_name)

	file_list.seek(0)
	file_list.truncate(0)
	json.dump(file_names,file_list, indent =3)
	log_file.write(str(datetime.datetime.now()) + source + "wtitten into DFS"+"\n")

	#file to keep track of where file blocks are stored
	location_data[file_name] = []

	
	
	next_worker = worker_details['Next_worker']
	for split in helper.splitByLine(source,split_size):
		# print(split)
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


		file_path = os.path.join(worker, store_path)
		# print(file_path)
		file = open(file_path,'w')
		file.write(split)
		file.close()

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


	file_list.close()
	location_file.close()
	worker_tracker.close()

def read(path):
	print(path)
	file_list = open(configuration['file_list_path'],'r+')
	file_names = json.load(file_list)

	location_file = open(configuration["location_file_path"],"r+")
	location_data = json.load(location_file)


	if path not in file_names["files"]:
		raise FileNotFoundError
		
	for file_blk in location_data[path]:

			# print('entered')
			block_path = os.path.join(worker, file_blk)
			if os.path.isfile(block_path):
				content = open(block_path, 'r').read()
				print(content, end='')
			
	
	file_list.close()
	location_file.close()





	





