'''
Created on Oct 21, 2019

@author: camila
'''
import calendar

def getTotalSeconds(datetime):
    total = datetime.hour * 3600
    total += datetime.minute * 60
    total += datetime.second
    return total

def getDay(datetime):
    return datetime.day

def getDayOfTheWeekNumber(datetime):
    return datetime.weekday()

def getDayOfTheWeek(datetime):
    return calendar.day_name[datetime.weekday()]
