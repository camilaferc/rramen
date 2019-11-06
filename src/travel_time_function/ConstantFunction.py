'''
Created on Oct 2, 2019

@author: camila
'''
from travel_time_function.TravelTimeFunction import TravelTimeFunction

class ConstantFunction(TravelTimeFunction):
    def __init__(self, travel_time):
        self.travel_time = travel_time
        
    def getTravelTime(self, arrival_time):
        return self.travel_time
    
    def __str__(self):
        return "travel time:" + str(self.travel_time)
    
    def __repr__(self):
        return "travel time:" + str(self.travel_time)
    
    def splitFunctionRatio(self, ratio):
        left_time = round(self.travel_time*ratio)
        leftFunction = ConstantFunction(left_time)
        rightFunction = ConstantFunction(self.travel_time - left_time)
        return leftFunction, rightFunction