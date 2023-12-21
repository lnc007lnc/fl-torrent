import numpy as np
import os
import random
import numpy as np


class Random_FIFO:
    
    def __init__(self,neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector, P_value, K_value):
        self.neighborhood_matrix=neighborhood_matrix
        self.chunks_matrix=chunks_matrix
        self.uplink_vector=uplink_vector
        self.downlink_vector=downlink_vector
        self.P_value=P_value
        self.K_value=K_value      
        
   
    def add_triple(self,mydict,key, triple):
        if key in mydict:
            mydict[key].append(triple)
        else:
            mydict[key] = [triple]
            
    def Update_Dict(self,round_index,receiver,sender,chunk,mydict):
        self.add_triple(mydict,round_index,(receiver,sender,chunk))
        
            
    def Random_FIFO_Execution(self,round_index,mydict):
        #neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector=read_matrices_from_file()
        nodes_Number=len( self.neighborhood_matrix)
        chunks_Number=len(self.chunks_matrix)
        downlink_vector_copy=self.downlink_vector.copy()
        uplink_vector_copy=self.uplink_vector.copy()
        partialtransmission=0
        Chunk_requests = [[] for _ in range(chunks_Number)]
        for i in range (nodes_Number):
            #missed_chunks=[index for index, value in enumerate(self.chunks_matrix [i]) if value == 0 ]
            missed_chunks = np.where(self.chunks_matrix[i] == 0)[0].tolist()
            for chunk in missed_chunks:
                #chunk_owners= [row for row in range(nodes_Number) if self.chunks_matrix[row][chunk] == 1 and self.neighborhood_matrix[i][row]==1]
                chunk_owners = np.where((self.chunks_matrix[:, chunk] == 1) & (self.neighborhood_matrix[i, :] == 1))[0].tolist()
                #print("i:",i, " chunk:",chunk, chunk_owners)
                if len(chunk_owners)>0:
                    random_sender = random.choice(chunk_owners)
                    Chunk_requests[random_sender].append((i,chunk))
        #print(Chunk_requests)
        #sending------------------------------
        for i in range (nodes_Number):
            for j in range (min(len(Chunk_requests[i]),self.K_value)):# i is sender
                receiver=Chunk_requests[i][j][0]
                chunk=Chunk_requests[i][j][1]
                if self.chunks_matrix[receiver][chunk]==0:
                    if downlink_vector_copy[receiver]>0 and uplink_vector_copy[i]>0:
                        self.chunks_matrix[receiver][chunk]+=1   #sending chunk to sender
                        self.Update_Dict(round_index,receiver,i,chunk,mydict)
                        
                        partialtransmission+=1
                        downlink_vector_copy[receiver]-=1
                        uplink_vector_copy[i]-=1
    
        return self.chunks_matrix.copy(),partialtransmission

    def Operation(self):
        mydict={}
        Totaltransmission=0
        round_index=1
        Continue=True
        while(Continue):
            print("---------time slot-Random-Fifo:",round_index," ---------" )
            Nodes_Number=len(self.neighborhood_matrix)
            chunks_matrix,partialtransmission=self.Random_FIFO_Execution(round_index,mydict)
            count_ones_per_row = [sum(row) for row in chunks_matrix]        
            count_values_greater_than_p = sum(1 for value in count_ones_per_row if value >= self.P_value)
            round_index+=1
            print("Partial Communications:",partialtransmission)
            Totaltransmission+=partialtransmission
            if count_values_greater_than_p>=Nodes_Number or partialtransmission==0:
                Continue=False
            print("count_values_greater_than_p", count_values_greater_than_p)
            #print("Ones", count_ones(chunks_matrix))
                
        print("Total Communications:",Totaltransmission)
        #print("Ones", count_ones(chunks_matrix))
        return mydict
    
    
