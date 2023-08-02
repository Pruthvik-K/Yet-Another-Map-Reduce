import yamr_commands
import mapreduce

inp = input("\nSyntax--\n write : write <path to file> \n read : read : read <file name> \n mapreduce : mareduce <input file> <path to mapper> <path to reducer> <name of output file>\n >>  ")

inp=inp.split()

for ele in inp:
    ele = ele.strip()

command = inp[0]

if command == 'write':
    source = inp[1]
    yamr_commands.write(source)

elif command == 'read':
    file_name = inp[1]
    yamr_commands.read(file_name)


elif command == 'mapreduce':

    file = inp[1]
    mapper_path = inp[2]
    reducer_path = inp[3]
    output_name = inp[4]

    mapreduce.mapReducejob(file, mapper_path, reducer_path, output_name)

else:
    raise Exception("Invalid command")



