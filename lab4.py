from mpi4py import MPI
from datetime import datetime
import itertools
import fnmatch
import os

TM = 1
TC = 2
TR = 3

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank=comm.Get_rank()
status = MPI.Status()

mapPath = '/map/'
combinerPath = '/reduce/';

root = 0;
limit = 5
successM = 0
successC = 0
successR = 0

def storeSubsets(data): 
    for i in range(len(data)):
        subset = list(itertools.combinations(set(data), i+1))
        for j in subset:
            filename = str(j) + '_' + str(1) + '_' + str(rank) + '_' + str(datetime.timestamp(datetime.now())) + '.txt'
            f = open('map/' + filename, "x")

def doCombiner():
    p = '*1_'+ str(rank) + '*'
    filesCountMap = {}
    for file in os.listdir(mapPath):
        if fnmatch.fnmatch(file, p):
            fileName = file.rsplit('_',1)[0]
            if fileName in filesCountMap:
                filesCountMap[fileName] = filesCountMap[fileName] + 1
            else:
                filesCountMap[fileName] = 1
    for file, count in filesCountMap.items():
        newFile = file.replace('_1_', '_'+str(count)+'_')
        f = open('/combiner/' + newFile, "x")
    

if rank == root:
    lines = []
    with open('retail.dat.txt') as my_file:
        for line in my_file:
            lines.append(line)
            
    chunks = [[] for _ in range(size)]
    for i, chunk in enumerate(lines):
        chunks[i % size].append(chunk)
else:
    chunks = None
    data = None

receivedData = comm.scatter(chunks, root=0)

for line in receivedData:
    data = line.split()
    storeSubsets(data)
if (rank != root):
    req = comm.isend(rank, dest=root, tag=TM)
    req.wait()
    print('Process ' + str(rank) + ' sent finished mapping phase meesage to root')

if rank == root:
    while True:
        r = comm.irecv(source=MPI.ANY_SOURCE, tag=TM)
        msg = r.wait(status=status)
        source = status.Get_source()
        print('Root received finished mapping phase message from ' + str(source))
        successM = successM + 1
        if (successM == size-1):
            break
    for i in range(size):
        if i != root:
            req = comm.isend('combiner', dest=i, tag=TC)
            req.wait()
            print('Process ' + str(rank) + ' sent starting combiner phase to '.format(i))
    doCombiner()
    
    subsets = []
    while True:
        r = comm.irecv(source=MPI.ANY_SOURCE, tag=TC)
        msg = r.wait(status=status)
        source = status.Get_source()
        print('Root received finished combiner phase from ' + str(source))
        successC = successC + 1
        if (successC == size-1):
            break
    subsets = []
    for file in os.listdir(combinerPath):
        subset = file.split('_')[0]
        if subset not in subsets:
            subsets.append(subset)
    chunks = [[] for _ in range(size)]
    for i, chunk in enumerate(lines):
        chunks[i % size].append(chunk)
else:
    subsets = None
    chunks = None
    
    r = comm.irecv(source=root, tag=MPI.ANY_TAG)
    msg = r.wait(status=status)
    if status.Get_tag() == TC:
        print('Process '+str(rank)+' received starting combiner phase msg')
        doCombiner()
        if rank != root:
            req = comm.isend(rank, dest=root, tag=TC)
            req.wait()
            print('Process ' + str(rank) + ' sent finished combiner to root')

subsets = comm.scatter(chunks, root=0)

reduceMap = {}
for subset in subsets:
    files = [f for f in os.listdir(combinerPath) if subset == f.split('_')[0]]
    count = 0
    for file in files:
        count += file.split('_')[1]
    reduceMap[subset] = count
    
if (rank != root):
    req = comm.isend(reduceMap, dest=root, tag=TR)
    req.wait()
    print('Process ' + str(rank) + ' sent reduce phase to root')
else:
    for key, value in reduceMap.items():
        if value >= limit:
            print('R: ' + str(key))

if rank == root:
    while True:
        r = comm.irecv(source=MPI.ANY_SOURCE, tag=TR)
        reduceMap = r.wait(status=status)
        source = status.Get_source()
        print('Root received reduce phase from ' + str(source))
        successR = successR + 1
        for key, value in reduceMap.items():
            if value >= limit:
                print('R: ' + str(key))
        if (successR == size-1):
            break