'''
Created on Feb 13, 2020

@author: camila
'''


from multiprocessing import Manager
import multiprocessing
import time


# bar
def bar(name, data):
    print(name)
    for i in range(3):
        data[i] = i
        print ("Tick")
        time.sleep(1)
    print(data)

if __name__ == '__main__':
    # Start bar as a process
    manager = Manager()
    data = manager.dict()
    p = multiprocessing.Process(target=bar, args=["test", data])
    p.start()

    # Wait for 10 seconds or until process finishes
    p.join(5)

    # If thread is still active
    if p.is_alive():
        print ("running... let's kill it...")

        # Terminate
        p.terminate()
        p.join()
    print(data)
