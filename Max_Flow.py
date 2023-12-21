
from NetworkFlowModel import NetworkFlowModel
import numpy as np
import random


class Max_Flow:
    
    def __init__(self,neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector, P_value ):
        self.neighborhood_matrix=neighborhood_matrix
        self.chunks_matrix=chunks_matrix
        self.uplink_vector=uplink_vector
        self.downlink_vector=downlink_vector
        self.P_value=P_value
        #self.owner_matrix=owner_matrix
    
    def maxflow_run(self,round_index,mydict):
        #neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector,owner_matrix=read_matrices_from_file()    
        #round_index=0
        sumdownlink=0
        sumuplink=0
        partialtransmission=0
        Continue=True
    
        network_flow_model = NetworkFlowModel(self.neighborhood_matrix, self.chunks_matrix, self.uplink_vector, self.downlink_vector)
    
            #print("ones", count_ones(network_generator.chunks_matrix))
            # Build the network flow model and solve the linear programming problem
        down,up=network_flow_model.build_network_flow_model()         
            
        G,maxFlow, flow_dict=network_flow_model.solve_max_flow("Source", "Sink")
        chunks_matrix,downlink_vector,uplink_vector,transmittion_count=network_flow_model.ChunkUpdate(G, flow_dict,round_index,mydict)
        partialtransmission+=transmittion_count
        
        if transmittion_count==0:
            Continue=False
        
        return chunks_matrix,partialtransmission
    
    def operation(self):
        mydict={}
        Totaltransmission=0
        round_index=1
        Continue=True
        while(Continue):
            print("-------time slot-maxflow:",round_index," --------" )
            Nodes_Number=len(self.neighborhood_matrix)
            self.chunks_matrix,partialtransmission=self.maxflow_run(round_index,mydict)
            count_ones_per_row = [sum(row) for row in self.chunks_matrix]        
            count_values_greater_than_p = sum(1 for value in count_ones_per_row if value >= self.P_value)
            round_index+=1
            print("Communications:",partialtransmission)
            Totaltransmission+=partialtransmission
            if count_values_greater_than_p>=Nodes_Number or partialtransmission==0:
                Continue=False
               
        print("Total Communications:",Totaltransmission)
        return mydict