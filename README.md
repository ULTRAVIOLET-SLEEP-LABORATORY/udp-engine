# udp-engine
Manage sockets and queues for network communications with biosignal sensors.


### Structure
The basic structure of the code is as follows.
The UDPEngine consists of two ports: the data port and control port.
The UDPEngine also consists of four queues, two for each port.
These are useful for communicating with biosignal sensors among other purposes.

Finally, there are four threads whose jobs are something like this:
1) Wait around listening for data
2) As soon as data is received, run a list of operators/functions on the data.

An example of this would be a thread that listens to one of the sockets.
As soon as the socket receives data, it awakens the thread, which pushes the
data to a queue. It's also possible to hook other functions to be called by
the thread, but these functions should be made to run as fast as possible.

There are two threads listening (one for each socket) that copy received data
over to queues to be processed, and two threads that do the opposite. 
They listen to the transmit queues and copy the data over to the sockets.


### Future work
I believe it is possible to run all of this on one thread with python's 
asyncio library. This may possibly improve performance during high throughput 
due to reduced context switching, but I believe the gains are minimal since 
threads and processes are constantly being context switched anyway on most
operating systems. 