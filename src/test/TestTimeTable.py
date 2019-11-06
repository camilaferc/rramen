'''
Created on Oct 23, 2019

@author: camila
'''
from datetime import datetime
from datetime import timedelta
from travel_time_function.TimeTable import TimeTable

table = {2:[[datetime.strptime("01:00:00", '%H:%M:%S').time(), datetime.strptime("01:30:00", '%H:%M:%S').time()], 
            [datetime.strptime("05:00:00", '%H:%M:%S').time(), datetime.strptime("05:30:00", '%H:%M:%S').time()], 
            [datetime.strptime("08:00:00", '%H:%M:%S').time(), datetime.strptime("08:30:00", '%H:%M:%S').time()],
            [datetime.strptime("23:00:00", '%H:%M:%S').time(), datetime.strptime("00:30:00", '%H:%M:%S').time()]]}
#table = {2:[[datetime.strptime("23:00:00", '%H:%M:%S').time(), datetime.strptime("00:30:00", '%H:%M:%S').time()]]}
tb = TimeTable(table)
print (tb.getNextDeparture(datetime.today()))
print(tb.getTravelTime(datetime.today()))
time = datetime.today() - timedelta(hours=3)

print (tb.getNextDeparture(time))
print(tb.getTravelTime(time))

time = datetime.today() - timedelta(hours=6)
print (tb.getNextDeparture(time))
print(tb.getTravelTime(time))

time2 = datetime.today() + timedelta(hours=14)
print (tb.getNextDeparture(time2))
print(tb.getTravelTime(time2))

