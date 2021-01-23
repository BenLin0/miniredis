# Python program to explain the
# use of wait() method in Event() class
import threading
import time


def helper_function(event_obj, timeout, i):
    # Thread has started, but it will wait 10 seconds for the event
    print("Thread started, for the event to set")

    flag = event_obj.wait(timeout)
    if flag:
        print("Event was set to true() earlier, moving ahead with the thread")
    else:
        print("Time out occured, event internal flag still false. Executing thread without waiting for event")
        print("Value to be printed=", i)


if __name__ == '__main__':
    # Initialising an event object
    event_obj = threading.Event()

    # starting the thread who will wait for the event
    thread1 = threading.Thread(target=helper_function, args=(event_obj, 10, 27))
    thread1.start()
    # sleeping the current thread for 5 seconds
    time.sleep(5)

    # generating the event
    event_obj.set()
    print("Event is set to true. Now threads can be released.")
    print()