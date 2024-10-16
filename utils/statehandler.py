from pyspark.sql.streaming.state import GroupState

class statehandler():
    def __init__(self,input,state: GroupState):
        self.input = input
        self.state = state.get

    def updatestate(self,output_dict):
        self.state.update(tuple(output_dict.values()))
    def removestate(self):
        self.state.remove()
    def state_to_dict(self,colnames):
        return dict(zip(self.state,colnames))

    


    
