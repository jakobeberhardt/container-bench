This is the object which is resposible for managing the probing. It exposes an interface to the scheduler which looks like this
Probe(container (the container config, e.g. the name, the index, the command etc), the dataframes of the containers, ProbeKernel, time, cores, socket, isolated, abortable)

and in addition it gets a docker client from main.go which allows it to start a dockercontaienr which will run the probekernel which will return the aggrgated data which is then returned to the scheduler.

