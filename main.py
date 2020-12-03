from concurrent import futures
import sys 
import socket  
import grpc
import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc
g=0
k = 0
hosttable = None
fflag = True
currentnode = None
keymap = None
c=1
f=0
compvalue=0
#compare function using the xor
def comarefunc(element):
	global compvalue
	return element.id ^ compvalue

def updatevalue(self,a,b,x):
	if(self.tb[a][b] == self.rtb[a][b] or (x<self.tb[a][b])):
		self.rtb[a][b]=x
		self.tb[a][b]=x
		return True
	else:
		self.rtb[a][b]=x
		return False
	return True

#server code
class KadImpl(csci4220_hw3_pb2_grpc.KadImplServicer):
	def FindValue(self, request, context):
		global currentnode
		global k
		global fflag
		global keymap
		global f
		global hosttable
		global c
		global compvalue
		
		print('Serving FindKey({0}) request for {1}'.format(request.idkey, request.node.id))
		l=None
		for i in range(0,pow(2,k)):
			if (currentnode.id ^ request.node.id >= pow(2, i) and currentnode.id ^ request.node.id < pow(2, i + 1)):
				l=i
				c=c+1
		if (l != None):
			for i in range(0,len(hosttable[l])):
				if (request.node.id == hosttable[l][i].id):
					c=pow(c,2)
					hosttable[l].pop(i)
					break
			if (k==len(hosttable[l])):
				hosttable[l].pop(0)
			hosttable[l].append(request.node)
		value = keymap.get(request.idkey, None)
		if (value != None):
			# print("findsomething")
			f=f+1
			if (not fflag):
				fflag=True
			return csci4220_hw3_pb2.KV_Node_Wrapper(
				responding_node=currentnode, 
				mode_kv=True, 
				kv=csci4220_hw3_pb2.KeyValue(node=currentnode, key=request.idkey, value=value),
				nodes=[]
			)
			
		elif(fflag == True):
			# print("findnothing")
			# print(keymap)
			fflag = True
			node_list = []
			for i in range(0,len(hosttable)):
				for j in range(0,len(hosttable[i])):
					node_list.append(hosttable[i][j])
			compvalue = request.idkey
			node_list.sort(key=comarefunc)
			results=[]
			for i in range(0,min(k,len(node_list))):
				results.append(node_list[i])
			return csci4220_hw3_pb2.KV_Node_Wrapper(
				responding_node=currentnode,
				mode_kv=False,
				kv=csci4220_hw3_pb2.KeyValue(node=currentnode, key=None, value=None),
				nodes=results
			)
	def FindNode(self, request, context):
		global fflag
		global currentnode
		global hosttable
		global k
		global c
		global f
		global compvalue
		node_list = []
		for i in range(0,len(hosttable)):
			for node in hosttable[i]:
				node_list.append(node)
		compvalue = request.idkey
		node_list.sort(key=comarefunc)
		results = []
		for i in range(0,min(k,len(node_list))):
			results.append(node_list[i])
		fflag = True
		f=f+1
		l=None
		#print(results)
		for i in range(0,pow(2,k)):
			if ((currentnode.id ^ request.node.id >= pow(2, i)) and (currentnode.id ^ request.node.id < pow(2, i + 1))):
				l=i
				c=c+1
		if (l != None):
			for i in range(0,len(hosttable[l])):
				if (request.node.id == hosttable[l][i].id):
					c=pow(c,2)
					hosttable[l].pop(i)
					break
			if (k==len(hosttable[l])):
				hosttable[l].pop(0)
			hosttable[l].append(request.node)
		print('Serving FindNode({}) request for {}'.format(request.idkey, request.node.id))
		return csci4220_hw3_pb2.NodeList(responding_node=currentnode, nodes=results)

	def Store(self, request, context):
		global keymap
		global currentnode
		global fflag
		global c
		global f
		keymap[request.key] = request.value
		l=None
		for i in range(0,pow(2,k)):
			if (currentnode.id ^ request.node.id >= pow(2, i) and currentnode.id ^ request.node.id < pow(2, i + 1)):
				l=i
				c=c+1
		if (l != None):
			for i in range(0,len(hosttable[l])):
				if (request.node.id == hosttable[l][i].id):
					c=pow(c,2)
					hosttable[l].pop(i)
					break
			if (k==len(hosttable[l])):
				hosttable[l].pop(0)
			hosttable[l].append(request.node)
		if fflag == False:
			fflag = True
		print('Storing key {} value "{}"'.format(request.key, request.value))
		return csci4220_hw3_pb2.IDKey(idkey=currentnode.id, node=currentnode)
	def Quit(self, request, context):
		global hosttable
		global currentnode
		global fflag
		global c
		global f
		fflag = True
		found = False
		for i in range(len(hosttable)):
			for n in range(len(hosttable[i])):
				if (request.idkey==hosttable[i][n].id and fflag == True):
					found = True			
					hosttable[i].pop(n)
					print('Evicting quitting node {} from bucket {}'.format(request.idkey, i))
					c=c-1
					f=f-1
					break
		if not found:
			if(not fflag):
				fflag = True
				c=c-1
				f=f-1
			print('No record of quitting node {} in k-buckets.'.foramt(request.idkey))
		
		return csci4220_hw3_pb2.IDKey(node=currentnode, idkey=currentnode.id)
def updatetable(self,otb,oname):
	changes = False
	up = False
	global fflag
	for i in otb[oname].keys():
		if(self.tb[oname][i]!=otb[oname][i] and fflag):
			self.tb[oname][i]=otb[oname][i]
			changes = True    
	for i in self.tb[self.name].keys():
		m=[]
		m.append(int(self.rtb[self.name][i]))
		for j in otb.keys():
			if(j!=i and j!=self.name and fflag):
				m.append((int(self.rtb[self.name][j])+int(self.tb[j][i])))
		
		res = min(m)
		if(self.tb[self.name][i] != str(res)):
			up = True
			self.tb[self.name][i] = str(res)
	return (up,changes)
#main code
def run():
	if (len(sys.argv) != 4):
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)

	global keymap
	global k
	global hosttable
	global currentnode
	global c
	global f
	global fflag
	global compvalue
	local_id = int(sys.argv[1])
	my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
	k = int(sys.argv[3])
	my_hostname = socket.gethostname() # Gets my host name
	my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname

	''' Use the following code to convert a hostname to an IP and start a channel
	Note that every stub needs a channel attached to it
	When you are done with a channel you should call .close() on the channel.
	Submitty may kill your program if you have too many file descriptors open
	at the same time. '''
	
	#remote_addr = socket.gethostbyname(remote_addr_string)
	#remote_port = int(remote_port_string)
	#channel = grpc.insecure_channel(remote_addr + ':' + str(remote_port))


	currentnode = csci4220_hw3_pb2.Node(id=local_id, port=int(my_port), address=my_address)
	N = 4
	hosttable=[]
	for i in range(0,4):
		hosttable.append([])
	keymap = {}

	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	csci4220_hw3_pb2_grpc.add_KadImplServicer_to_server(KadImpl(), server)
	server.add_insecure_port('[::]:' + my_port)
	server.start()

	cmd = 'dddddd'
	while (cmd != 'QUIT'):
		words = input()
		line = words.split(' ')
		cmd = line[0].upper()
		if (cmd == 'BOOTSTRAP'):#bootstrap
			oipname = line[1]
			oport = line[2]
			remote_addr = socket.gethostbyname(oipname)
			oport = int(oport)
			channel= grpc.insecure_channel(remote_addr + ':' + str(oport))
			stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)
			nodelist = stub.FindNode(csci4220_hw3_pb2.IDKey(idkey=currentnode.id, node=currentnode))
			print('After BOOTSTRAP({}), k_buckets now look like:'.format(nodelist.responding_node.id))
			channel.close()
			fflag = True
			l=None
			for i in range(0,pow(2,k)):#find bucket
				if (currentnode.id ^ nodelist.responding_node.id >= pow(2, i) and currentnode.id ^ nodelist.responding_node.id < pow(2, i + 1)):
					l=i
					c=c+1
			if (l != None):
				for i in range(0,len(hosttable[l])):
					if (nodelist.responding_node.id == hosttable[l][i].id):
						c=pow(c,2)
						hosttable[l].pop(i)
						break
				if (k==len(hosttable[l])):
					hosttable[l].pop(0)
				hosttable[l].append(nodelist.responding_node)
			for j in nodelist.nodes:
				l=None
				for i in range(0,pow(2,k)):
					if (currentnode.id ^ j.id >= pow(2, i) and currentnode.id ^ j.id < pow(2, i + 1)):
						l=i
						c=c+1
				if (l != None):
					for i in range(0,len(hosttable[l])):
						if (j.id == hosttable[l][i].id):
							c=pow(c,2)
							hosttable[l].pop(i)
							break
					if (k==len(hosttable[l])):
						hosttable[l].pop(0)
					hosttable[l].append(j)

			for i in range(0,len(hosttable)):#print table
				print(str(i)+":",end="")
				for j in range(0,len(hosttable[i])):
					print(" "+str(hosttable[i][j].id)+":"+str(hosttable[i][j].port),end="")
				print("")
		elif (cmd == 'FIND_NODE'):#find node
			a=0
			tmpbool=False
			nodeid = int(line[1])
			print('Before FIND_NODE command, k-buckets are:')
			for i in range(0,len(hosttable)):#print table
				print(str(i)+":",end="")
				for j in range(0,len(hosttable[i])):
					print(" "+str(hosttable[i][j].id)+":"+str(hosttable[i][j].port),end="")
				print("")
			if (nodeid == currentnode.id and fflag):
				print('Found destination id {}'.format(nodeid))
				print('After FIND_NODE command, k-buckets are:')
				if(fflag == False):
					fflag = True

				for i in range(0,len(hosttable)):#find the ode
					print(str(i)+":",end="")
					for j in range(0,len(hosttable[i])):
						print(" "+str(hosttable[i][j].id)+":"+str(hosttable[i][j].port),end="")
						a=a+1
					print("")
				continue

			foundflag = False
			tmpl = {currentnode.id}
			if (fflag == False):
				updatevalue(hosttable,f,c)
			while (not foundflag):
				#print(tmpl)
				nodelist = []
				for i in range(0,len(hosttable)):
					for node in hosttable[i]:
						nodelist.append(node)
				compvalue = nodeid
				nodelist.sort(key=comarefunc)
				S = []
				for i in range(0,min(k,len(nodelist))):
					S.append(nodelist[i])
				flag = True
				for i in S:
					if i.id not in tmpl:
						flag = False
				if (flag):
					break
				for j in S:
					if j.id in tmpl:
						continue
					tmpl.add(j.id)
					l=None
					for i in range(0,pow(2,k)):
						if (currentnode.id ^ j.id >= pow(2, i) and currentnode.id ^ j.id < pow(2, i + 1)):
							l=i
							c=c+1
					if (l != None):
						for i in range(0,len(hosttable[l])):
							if (j.id == hosttable[l][i].id):
								c=pow(c,2)
								hosttable[l].pop(i)
								break
						if (k==len(hosttable[l])):#add the node
							hosttable[l].pop(0)
						hosttable[l].append(j)
					if (j.id == nodeid):
						foundflag = True
						break
					remote_addr = socket.gethostbyname(j.address)
					oport = int(j.port)#link to grpc server
					channel= grpc.insecure_channel(remote_addr + ':' + str(oport))
					stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)
					R = stub.FindNode(csci4220_hw3_pb2.IDKey(idkey=nodeid, node=currentnode)).nodes
					channel.close()
					for m in R:
						bb=None
						#print(currentnode.id,m.id)
						for q in range(0,pow(2,k)):
							if (currentnode.id ^ m.id >= pow(2, q) and currentnode.id ^ m.id < pow(2, q + 1)):
								bb=q

						if (not bb == None):
							if (sum(x.id == m.id for x in hosttable[bb]) == 0):
								l=None
								for i in range(0,4):
									if (currentnode.id ^ m.id >= pow(2, i) and currentnode.id ^ m.id < pow(2, i + 1)):
										l=i
										c=c+1
								if (l != None):
									#print("append to table {} : {}".format(l,m))
									for i in range(0,len(hosttable[l])):
										if (m.id == hosttable[l][i].id):
											c=pow(c,2)
											hosttable[l].pop(i)
											break
									if (k==len(hosttable[l])):
										hosttable[l].pop(0)
									hosttable[l].append(m)
									if(len(hosttable[l])>1):
										tmpbool=True
										f=l
			if (foundflag):
				print('Found destination id {}'.format(nodeid))
			else:
				print('Could not find destination id {}'.format(nodeid))

			print('After FIND_NODE command, k-buckets are:')
			for i in range(0,len(hosttable)):#print table
				for j in (hosttable[i]):
					if (j.id == nodeid):
						hosttable[i].reverse()
						break
			for i in range(0,len(hosttable)):
				print(str(i)+":",end="")
				for j in range(0,len(hosttable[i])):
					print(" "+str(hosttable[i][j].id)+":"+str(hosttable[i][j].port),end="")
				print("")
		elif (cmd == 'FIND_VALUE'):#find value
			key = int(line[1])
			print('Before FIND_VALUE command, k-buckets are:')
			for i in range(0,len(hosttable)):
				print(str(i)+":",end="")#print table
				for j in range(0,len(hosttable[i])):
					print(" "+str(hosttable[i][j].id)+":"+str(hosttable[i][j].port),end="")
				print("")
			res = keymap.get(key, None)
			if (res != None and fflag):#found
				print('Found data "{}" for key {}'.format(res, key))
				fflag = True
			else:
				fflag = True
				tmpl = {currentnode.id}
				foundflag = False
				while not foundflag:#finding
					nodelist = []
					for i in range(0,len(hosttable)):
						for node in hosttable[i]:
							nodelist.append(node)
					compvalue = key
					nodelist.sort(key=comarefunc)
					S = []
					for i in range(0,min(k,len(nodelist))):
						S.append(nodelist[i])
					flag = True
					for i in S:
						if (i.id not in tmpl):
							flag = False
					if (flag and fflag):
						break
					fflag = True
					for j in S:
						if (j.id in tmpl and fflag):
							c = c+1
							continue
						tmpl.add(j.id)#link to grpc server
						remote_addr = socket.gethostbyname(j.address)
						oport = int(j.port)
						channel= grpc.insecure_channel(remote_addr + ':' + str(oport))
						stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)
						response = stub.FindValue(csci4220_hw3_pb2.IDKey(idkey=key, node=currentnode))
						channel.close()
						l=None
						for i in range(0,pow(2,k)):
							if (currentnode.id ^ j.id >= pow(2, i) and currentnode.id ^ j.id < pow(2, i + 1)):
								l=i
								c=c+1
						if (l != None):
							for i in range(0,len(hosttable[l])):
								if (j.id == hosttable[l][i].id):
									c=pow(c,2)
									hosttable[l].pop(i)
									break
							if (k==len(hosttable[l])):
								hosttable[l].pop(0)
							hosttable[l].append(j)
						if (response.mode_kv == True and fflag):
							foundflag = True
							fflag = True
							f=f+1
							print('Found value "{}" for key {}'.format(response.kv.value, response.kv.key))
							break
						for m in response.nodes:
							bb=None
							for q in range(0,pow(2,k)):
								if (currentnode.id ^ m.id >= pow(2, q) and currentnode.id ^ m.id < pow(2, q + 1)and fflag):
									bb=q
							if (bb != None and fflag):
								if (sum(x.id == m.id for x in hosttable[bb]) == 0):
									l=None
									for i in range(0,pow(2,k)):
										if (currentnode.id ^ m.id >= pow(2, i) and currentnode.id ^ m.id < pow(2, i + 1)):
											l=i
											c=c+1
									if (l != None):
										for i in range(0,len(hosttable[l])):
											if (m.id == hosttable[l][i].id):
												c=pow(c,2)
												hosttable[l].pop(i)
												break
										if (k==len(hosttable[l])):
											hosttable[l].pop(0)
										hosttable[l].append(m)

				if (not foundflag and fflag):
					print('Could not find key {}'.format(key))

			print('After FIND_VALUE command, k-buckets are:')
			for i in range(0,len(hosttable)):#print table
				print(str(i)+":",end="")
				for j in range(0,len(hosttable[i])):
					print(" "+str(hosttable[i][j].id)+":"+str(hosttable[i][j].port),end="")
				print("")
		elif (cmd == 'STORE'):#store
			fflag = True
			key = int(line[1])
			nodelistt = []
			for i in range(0,len(hosttable)):
				for node in hosttable[i]:
					nodelistt.append(node)
			compvalue = key
			nodelistt.sort(key=comarefunc)
			nodelist = nodelistt[0:k]
			tmpnode = None
			value = line[2]
			if (len(nodelist) != 0):
				tmpnode = nodelist[0]

			if (len(nodelist) ==0) or ((key ^ currentnode.id) < (key ^ tmpnode.id) and fflag):
				keymap[key] = value
				tmpnode = currentnode
			else:
				#keymap[key] = value
				remote_addr = socket.gethostbyname(tmpnode.address)
				oport = int(tmpnode.port)
				fflag = True #link to server
				channel= grpc.insecure_channel(remote_addr + ':' + str(oport))
				stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)
				stub.Store(csci4220_hw3_pb2.KeyValue(node=currentnode, key=key, value=value))
				channel.close()
			fflag = True
			print('Storing key {} at node {}'.format(key, tmpnode.id))
		elif (cmd == 'QUIT'):#quit
			for bb in hosttable:
				for j in bb:
					print('Letting {} know I\'m quitting.'.format(j.id))
					remote_addr = socket.gethostbyname(j.address)
					oport = int(j.port)
					channel= grpc.insecure_channel(remote_addr + ':' + str(oport))
					stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)
					try:
						stub.Quit(csci4220_hw3_pb2.IDKey(idkey=currentnode.id, node=currentnode))
					except Exception:
						pass
					channel.close()

			print('Shut down node {}'.format(currentnode.id))
		else:
			print('commandnotrecognize')
	
	server.stop(None)
def testprinttable(self):
	for i in self.nl:
		for j in self.tb[i]:
			print(self.tb[i][j],end="")
			self.ft.write(self.tb[i][j])
		print("")
		self.ft.write("\n")

if (__name__ == '__main__'):
	run()
