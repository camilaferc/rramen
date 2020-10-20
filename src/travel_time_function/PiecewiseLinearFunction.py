'''
Created on Oct 2, 2019

@author: camila

table contains (departure_time, travel_time) pairs for a given edge
'''
from ..util import TimeUtil
from copy import copy
from ..travel_time_function.TravelTimeFunction import TravelTimeFunction

class PiecewiseLinearFunction(TravelTimeFunction):
    
    def __init__(self, x, y):
        self.list_functions = self.buildFunctions(x, y)
        self.interval_length = x[1] - x[0]
        
    def __str__(self):
        return "functions:" + str(self.list_functions)
    
    def __repr__(self):
        return "functions:" + str(self.list_functions)
    
    def comp(self, function):
        value_a = self.list_functions[0].y1
        value_b = function.list_functions[0].y1
        if value_a > value_b:
            return 1
        elif value_a > value_b:
            return 0
        else:
            return -1
            
    def buildFunctions(self, x, y):
        functions = []
        x_prev = x[0]
        y_prev = y[0]
        for i in range(1, len(x)):
            x_cur = x[i]
            y_cur = y[i] 
            functions.append(LinearFunction(x_prev, y_prev, x_cur, y_cur))
            x_prev = x_cur
            y_prev = y_cur
        functions.append(LinearFunction(x_prev, y_prev, x[0], y[0]))
        return functions
    
    def splitFunctionRatio(self, ratio):
        leftFunction = copy(self)
        rightFunction = copy(self)
        
        leftFunction.list_functions = []
        rightFunction.list_functions = []
        
        for i in range(len(self.list_functions)):
            linear_function = copy(self.list_functions[i])
            
            y1_left = round(linear_function.y1*ratio)
            y2_left = round(linear_function.y2*ratio)
            left_function = LinearFunction(linear_function.x1, y1_left, linear_function.x2, y2_left)
            right_function = LinearFunction(linear_function.x1, linear_function.y1 - y1_left, linear_function.x2, linear_function.y2 - y2_left)
            
            leftFunction.list_functions.append(left_function)
            rightFunction.list_functions.append(right_function)
            
        return leftFunction, rightFunction        
    
    def getTravelTime(self, arrival_time):
        total_sec = TimeUtil.getTotalSeconds(arrival_time)
        pos = int(total_sec/self.interval_length)
        if pos >= len(self.list_functions):
            pos = (pos % len(self.list_functions))
        linear_function = self.list_functions[pos]
        return linear_function.getValue(total_sec)
        
        
    def getInterval(self, arrival_time):
        first = 0
        last = len(self.table) - 1

        while(first <= last):
            mid = (first + last) // 2
            if self.table[mid][0] >= arrival_time and (mid == 0 or self.table[mid-1][0] <= arrival_time):
                if mid == 0 or mid == len(self.table):
                    return [self.table[-1], self.table[0]]
                else:
                    return [self.table[mid-1], self.table[mid]]
                 
            else:
                if arrival_time < self.table[mid][0]:
                    last = mid - 1
                else:
                    first = mid + 1    
        return [self.table[-1], self.table[0]]
    
    def getIntervalFixedLength(self, arrival_time):
        pos = int(arrival_time/self.interval_length)
        if pos < len(self.table) - 1:
            return [self.table[pos], self.table[pos+1]]
        else:
            return [self.table[-1], self.table[0]]
        
    
class LinearFunction:
    def __init__(self, x1, y1, x2, y2):
        self.x1 = x1
        self.y1 = y1
        self.x2 = x2
        self.y2 = y2
        self.slope = self.getSlope()
        self.b = self.getB()
        
    def getSlope(self):
        return (self.y2-self.y1)/(self.x2 - self.x1)
    
    def getB(self):
        #b = y-mx
        return self.y1 - self.slope*self.x1
    
    def getValue(self, x):
        #y = mx + b
        return self.slope*x + self.b
    
    def __str__(self):
        return "f:" + str(self.x1) + "," +  str(self.y1) + "," + str(self.x2) + "," + str(self.y2)
    
    def __repr__(self):
        return "f:" + str(self.x1) + "," +  str(self.y1) + "," + str(self.x2) + "," + str(self.y2)
