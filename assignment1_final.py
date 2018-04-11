import json
import re
from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size

#---------------------------------Declaring the neccessary data structures--------------------------------------------------#

coordinates = []
melb_Grid = {}
block_result = {}
row_count = {}
column_count = {}
block_count = {}
coord = []

#---------------------------------------------------------------------------------------------------------------------------#

start = time.clock()

#-----------------------------Reading the files and fetching the different grids--------------------------------------------#

file_size = -2
f = open('bigInstagram.json', 'r')
line = f.readline()

while line:
    file_size += 1
    line = f.readline()
f.close()
f = open('melbGrid.json', 'r')
data_str = f.read()
data = json.loads(data_str)
for block in data['features']:
    grid_data = block['properties']
    melb_Grid[grid_data['id']] = (grid_data['xmin'], grid_data['xmax'], grid_data['ymin'], grid_data['ymax'])
    block_result[grid_data['id']] = 0

#---------------------------Partitinoning the instagram file and sending it across nodes------------------------------------#

partition_size = file_size / size
if rank == size-1:
    start = int(partition_size * rank)
    end = file_size
else:
    start = int(partition_size * rank)
    end = int(partition_size + partition_size * rank)
with open('bigInstagram.json') as f:
    for i in range(0, start):
        f.readline()
    for i in range(start, end):
        coord = re.search(',"coordinates":{"type":"Point","coordinates":\[(.*?)\]}', f.readline())
        if coord:
            sm = coord.group(1).split(",")
            if sm[0] != 'null' and sm[1] != 'null':
                coordinates.append([float(sm[0]), float(sm[1])])
for point in coordinates:
    for key, value in melb_Grid.items():
        if value[0] < point[1] <= value[1] and value[2] < point[0] <= value[3]:
            block_result[key] = block_result[key] + 1

final_result = comm.gather(block_result, root=0)

#-------------------------Printing the final results------------------------------------------------------------------------#

if rank == 0:
    for i in range(0, size):
        for key, value in final_result[i].items():
            if key not in block_count:
                block_count[key] = 0
            block_count[key] = block_count[key] + value
            if key not in row_count:
                row_count.setdefault(key[0], 0)
            row_count[key[0]] = row_count[key[0]] + value
            if key not in column_count:
                column_count.setdefault(key[1:], 0)
            column_count[key[1:]] = column_count[key[1:]] + value
    block_final_counts = sorted([(key, value) for key, value in block_count.items()], key=lambda x: x[1], reverse=True)
    row_final_counts = sorted([(key, value) for key, value in row_count.items()], key=lambda x: x[1], reverse=True)
    column_final_counts = sorted([(key, value) for key, value in column_count.items()], key=lambda x: x[1], reverse=True)
    print('----------Instagram posts in each block----------\n')
    for block in block_final_counts:
        print('%s: %d Instagram posts\n' % (block[0], block[1]))
    print('----------Instagram posts in each row------------\n')
    for row in row_final_counts:
        print('%s: %d Instagram posts\n' % (row[0], row[1]))
    print('----------Instagram posts in each column---------\n')
    for col in column_final_counts:
        print('%s: %d Instagram posts\n' % (col[0], col[1]))
    print('----------Time elapsed---------------------------\n')
    print('Time elapsed: %f ' % (time.clock() - start))