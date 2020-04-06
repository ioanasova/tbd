from mpi4py import MPI
import json
import base64
import os, fnmatch
import time
import random

def map(parentLink, insideLinks):
	for insideLink in insideLinks:
		encodedNode =base64.b64encode(bytes(insideLink, 'utf-8'))
		encodedSource = base64.b64encode(bytes(parentLink, 'utf-8'))
		encodedNodeString = str(encodedNode, 'utf-8').replace('/','.')
		encodedSourceString = str(encodedParent, 'utf-8').replace('/','.')
		file = 'mapDirectory/'+  encodedNodeString + '_' + encodedSourceString
		f = open(file, 'w')
		f.close()

def reduce(insideLink):
	encodedTarget = base64.b64encode(bytes(insideLink, 'utf-8'))
	encodedTargetString = str(encodedTarget,'utf-8')
	filteredFiles = fnmatch.filter(os.listdir('mapDirectory'), encodedTargetStr + '_*')
	result = []
	for file in filteredFiles:
		encodedSourceId = file.split('_')[1]
		sourceId = base64.b64decode(bytes(encodedSourceId.replace('.','/'), 'utf-8'))
		result.append(str(sourceId,'utf-8'))

	return result

class Process:
	def __init__(self, status, rank):
		self.status = status
		self.rank = rank
	def set_time(self, beginTime):
		self.beginTime = beginTime

class Msg:
	def __init__(self, srcId, adjList=None):
		self.srcId = srcId
		if adjList is not None:
			self.adjList = adjList
		else:
			self.adjList = []

comm = MPI.COMM_WORLD
rank=comm.Get_rank()
size = comm.Get_size()
status = MPI.Status()

processes = []
coordonator = 0
dt = 120

TM = 10
TR = 20
TE = 30

targetUrls = ["https://docs.oracle.com/javase/7/docs/api/java/util/Collection.html"]

if rank == coordonator:
	#map
	for i in range(0, size-1):
		processes.append(Process("free",i+1)) 
	with open('file.json', 'r') as f:
		links = json.loads(f.read())
	unprocessedLinks = list(links.keys())
	while len(unprocessedLinks) > 0:
		freeProcesses = [process for process in processes if process.status == 'free']
		if freeProcesses is not None:
			process = random.choice(freeProcesses)
			insideLink = unprocessedLinks.pop(0)
			msg = Msg(insideLink, links)
			process.beginTime = int(round(time.time()))
			process.status = 'busy'
			req = comm.isend(msg, dest=process.rank, tag=TM)
			req.wait()
			print('Process {} sent msg to worker'+str(process.rank)+''.format(rank), msg)
		else:
			for process in processes:
				currentTimeMillisec = int(round(time.time()))
				if(process.status == 'busy' and ((currentTimeMillisec - process.beginTime) > dt)):
					process.status = 'free'
		r = comm.irecv(source=MPI.ANY_SOURCE, tag=TM)
		rank = r.wait(status=status)
		for process in processes:
			if process.rank == rank:
				process.status = 'free'
		source = status.Get_source
		print('Process {} received complete msg from worker' + str(source) + ':'.format(rank))
	#reduce
	while len(targetUrls) > 0:
		freeProcesses = [process for process in processes if process.status == 'free']
		if freeProcesses is not None:
			process = random.choice(freeProcesses)
			targetUrl = targetUrls.pop(0)
			msg = Msg(targetUrl)
			process.beginTime = int(round(time.time()))
			process.status = 'busy'
			req = comm.isend(msg, dest = process.rank, tag = TR)
			req.wait()
			print('Process {} sent meesage to workers '+str(process.rank)+' (reduce)'.format(rank), msg)
		else:
			for process in processes:
				currentTimeMillisec = int(round(time.time()))
				if(process.status == 'busy' and ((currentTimeMillisec - process.beginTime) > dt)):
					process.status = 'free'
		r = comm.irecv(source=MPI.ANY_SOURCE, tag=TR)
		completeRank = r.wait(status=status)
		for process in processes:
			if process.rank == completeRank:
				process.status = 'free'
		source = status.Get_source
		print('Process {} received complete msg from worker '+str(source)+' (reduce):'.format(rank))

	for i in range(1, size):
		req = comm.isend('end', dest = i, tag = TE)
		req.wait()
		print('Process {} sent ending msg to workers'.format(rank))

else:
	while True:
		r = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
		msg = r.wait(status=status)
		print('Process {} received msg from coordonator:'.format(rank), msg)
		if status.Get_tag() == TR:
			break
		parentLink = msg.srcId
		map(parentLink, msg.adjList[parentLink])
		req = comm.isend(rank, dest=coordonator, tag=TM)
		req.wait()
		print('Process {} sent complete msg to coordonator'.format(rank))

	while True:
		r = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
		msg = r.wait(status=status)
		print('Process {} received msg from coordonator (reduce):'.format(rank), msg)
		if status.Get_tag() == TE:
			break
		result = reduce(msg.srcId);
		for link in result:
			print (link)
		req = comm.isend(rank, dest=coordonator, tag=TR)
		req.wait()
		print('Process {} sent complete msg to coordonator (reduce)'.format(rank))

