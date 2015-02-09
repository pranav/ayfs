# ayFS
Are you fucking serious

This is a filesystem where all the files exist on the network between machines so disk is not used.

Requires an etcd server and at least 3 servers to form a chain of servers sending each other packets. 

I'll polish up the code and README later but the setup code is pretty straight forward.

I made this at HackBeanpot 2015 so its a bit of a mess right now.

And no, its not POSIX compliant.


On the servers, just run `python start_server.py` after changing __main__ to point to the right etcd server.

On your local client, run `python client/ayfs.py <location of etcd server> <mountpoint>`
