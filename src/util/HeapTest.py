'''
Created on Oct 4, 2019

@author: camila
'''

from heapq import *
from FibonacciHeap import FibonacciHeap

f = FibonacciHeap()

node = f.insert(10, 'a:32')
f.insert(2, 'b')
f.insert(15, 'c')
f.insert(6, 'd')

f.decrease_key(node, 5, 'a:31')

m = f.find_min()
print m.key, m.value # 2

q = f.extract_min()
print q.key, q.value

q = f.extract_min()
print q.value # 6

f2 = FibonacciHeap()
f2.insert(100)
f2.insert(56)

f3 = f.merge(f2)
x = f3.root_list.right # pointer to random node
f3.decrease_key(x, 1, x.value)

# print the root list using the iterate class method
print [x.value for x in f3.iterate(f3.root_list)] # [10, 1, 56]

q = f3.extract_min()
print q.value # 1