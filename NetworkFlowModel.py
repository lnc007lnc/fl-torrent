from scipy.optimize import linprog
import numpy as np
import networkx as nx


class NetworkFlowModel:
    def __init__(self, neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector):
        self.neighborhood_matrix = neighborhood_matrix
        self.chunks_matrix = chunks_matrix
        self.uplink_vector = uplink_vector
        self.downlink_vector = downlink_vector
        self.network = nx.DiGraph()        
        self.nodes = len(neighborhood_matrix)
        self.chunks = len(chunks_matrix[0])
        self.sumUplink=0
        self.sumDownlink=0
        

        self.stages = {
            'Source': 1,
            'Intermediate_1': 2,
            'Intermediate_2': 3,
            'Intermediate_3': 4,
            'Sink': 5
        }

    def build_network_flow_model(self):
        # Initialize lists to store nodes and edges
        #nodes = []
        #edges = []
        
        # Stage 1: Source
        self.network.add_node("Source")
        # Stage 5: Sink
        self.network.add_node("Sink")

        # Stage 2 and 6 and links to source and sink
        for i in range(self.nodes):
            self.network.add_node(f"N{i}")
            self.network.add_node(f"dN{i}")
            
            self.network.add_edge("Source", f"N{i}", capacity=self.downlink_vector[i])
            self.sumDownlink+=int(self.downlink_vector[i])
            self.network.add_edge( f"dN{i}", "Sink", capacity=self.uplink_vector[i])
            self.sumUplink+=int(self.uplink_vector[i])
        
        # Stage 3 and 4: Intermediate_2 (chunk nodes)
        for i in range(self.nodes):   
            for j in range(self.chunks):
                if self.chunks_matrix[i][j] == 0:
                        self.network.add_node(f"N{i}C{j}")                        
                        self.network.add_edge(f"N{i}",f"N{i}C{j}",capacity=1) #edges between stages 2,3
                        for k in range(self.nodes):
                            if self.chunks_matrix[k][j] == 1 and k!=i and self.neighborhood_matrix[i][k]==1:
                                self.network.add_node(f"N{i}C{j}N{k}")
                                self.network.add_edge(f"N{i}C{j}",f"N{i}C{j}N{k}",capacity=1)
                                self.network.add_edge(f"N{i}C{j}N{k}",f"dN{k}",capacity=1)                          
                            
        return self.sumDownlink,self.sumUplink
    
    def solve_max_flow(self, source, sink):
        flow_value, flow_dict = nx.maximum_flow(self.network, 'Source', 'Sink')
        G=self.network.copy()
        return G,flow_value, flow_dict 
    
    def printFlow(self,G,maxFlow, flow_dict):
        print("Max_flow:", maxFlow)
        for u, v in G.edges:
            flow = flow_dict[u][v] if u in flow_dict and v in flow_dict[u] else 0
            capacity = G[u][v]['capacity']
            print(f"{u}->{v}: {flow}/{capacity}")

    def printnetwork(self):
        for u, v in self.network.edges:
            capacity = self.network[u][v]['capacity']
            print(f"{u}->{v}: {capacity}")

    def DecreasePathFlowbyOne(self,receiver,chunk,sender,flow_dict):
        flow_dict["Source"]['N'+str(receiver)]-=1
        flow_dict['N'+str(receiver)]['C'+str(chunk)]-=1
        flow_dict['C'+str(chunk)]['dN'+str(sender)]-=1
        flow_dict['dN'+str(sender)]["Sink"]-=1
        
    def DecreaseUpDoWnLinkbyOne(self,receiver,chunk,sender,flow_dict):
        self.downlink_vector[receiver]-=1
        self.uplink_vector[sender]-=1
                
    def add_triple(self,mydict,key, triple):
        if key in mydict:
            mydict[key].append(triple)
        else:
            mydict[key] = [triple]
            
    def Update_Dict(self,round_index,receiver,sender,chunk,mydict):
        self.add_triple(mydict,round_index,(receiver,sender,chunk))
        
    
    
  
  
    def ChunkUpdate(self,G,flow_dict,round_index,mydict):
        
        transmittion_count=0

        for i in range(self.nodes):
            chunks_requested=flow_dict[f"N{i}"]
            #print(f"requested for {i} is {chunks_requested}")
            chunks_received = [int(key[key.rfind('C')+1:]) for key, value in chunks_requested.items() if value == 1]
            #print(f"received for {i} is {chunks_received}")
            for chunk in chunks_received:
                owners=flow_dict[f"N{i}C{chunk}"]
                senders = [int(key[key.rfind('N')+1:]) for key, value in owners.items() if value == 1]# key[2:] because start with dN
                #print(f"node {i} have input of chunk{chunk} from {senders}")
                for k in senders:
                    if self.neighborhood_matrix[i][k]==1:
                        self.Update_Dict(round_index,i,k,chunk,mydict)
                        self.chunks_matrix[i][chunk]+=1
                        transmittion_count+=1
                        break

                    #else:
                        #print(f"node {i} can not receive chunk{chunk} from node {k}")
                #print("\n")
                    
        return self.chunks_matrix.copy(),self.downlink_vector.copy(), self.uplink_vector.copy(), transmittion_count
    


                
def count_ones(matrix):
    count = 0
    for row in matrix:
        for element in row:
            if element == 1:
                count += 1
    return count           
            

    
