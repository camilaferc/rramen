'''
Created on Oct 2, 2019

@author: camila

table contains (departure_time, arrival_time) pairs for a given edge
'''
import bisect
from datetime import timedelta
import sys

from travel_time_function.TravelTimeFunction import TravelTimeFunction
from util import TimeUtil


class TimeTable(TravelTimeFunction):
    
    def __init__(self, table):
        self.table = table
        
    def __str__(self):
        return "table:" + str(self.table)
    
    def __repr__(self):
        return "table:" + str(self.table)
    
    def getTravelTime(self, arrival_time):
        next_departure = self.getNextDeparture(arrival_time)
        #print(arrival_time, next_departure)
        waiting_time = next_departure[0] - arrival_time
        travel_time = next_departure[1] - next_departure[0]
        return (waiting_time + travel_time).total_seconds()
        
    def getNextDeparture(self, arrival_time):
        day = TimeUtil.getDayOfTheWeekNumber(arrival_time)
        time = [arrival_time.time()]
        #print(time)
        timetable = self.table.get(day)
        pos = sys.maxsize
        if timetable:
            pos = bisect.bisect_left(timetable, time)
        if timetable and pos < len(timetable):
            dep_arrival = timetable[pos]
            departure = arrival_time
            arrival = arrival_time
            departure = departure.replace(hour=dep_arrival[0].hour, minute=dep_arrival[0].minute, second = dep_arrival[0].second)
            arrival = arrival.replace(hour=dep_arrival[1].hour, minute=dep_arrival[1].minute, second = dep_arrival[1].second)
            
            if arrival < departure:
                arrival = arrival + timedelta(days=1)
            return [departure, arrival]
        else:
            #departure can not happen on the same day
            day = (day + 1)%6
            extra_days = 1
            while day not in self.table and extra_days <= 7:
                day = (day + 1)%6
                extra_days += 1
            
            if day not in self.table:
                departure = arrival_time + timedelta(days=extra_days)
                arrival = departure
                return [departure, arrival]
            
            dep_arrival = self.table[day][0]
            departure = arrival_time + timedelta(days=extra_days)
            arrival = departure
            departure = departure.replace(hour=dep_arrival[0].hour, minute=dep_arrival[0].minute, second = dep_arrival[0].second)
            arrival = arrival.replace(hour=dep_arrival[1].hour, minute=dep_arrival[1].minute, second = dep_arrival[1].second)
            
            if arrival < departure:
                arrival = arrival + timedelta(days=1)
            return [departure, arrival]
        '''
        first = 0
        last = len(self.table) - 1
        while(first <= last):
            mid = (first + last) // 2
            if self.table[mid][0] >= arrival_time and (mid == 0 or self.table[mid-1][0] < arrival_time):
                return self.table[mid]
            else:
                if arrival_time < self.table[mid][0]:
                    last = mid - 1
                else:
                    first = mid + 1    
        return self.table[0]
        '''
        
    def getTable(self):
        return self.table

