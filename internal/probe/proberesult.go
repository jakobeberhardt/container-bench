Returned to the schedueer and looks like this:
benchmarkId
containerId
containerName
containerIndex
containerCores
containerSocket
containerImage
containerCommand # These we get from the container config which should be passed from the scheduler 
probingContainerId
probingContainerId
probingContainerName
probingContainerCores
probingContainerSocket
probeTime
isolated = false
aborted = false
started # Timestamp time.Time  
finished # Timestamp time.Time  
aborted # Timestamp time.Time  
firstDataframe # first dataframe sampling step used by the prober (SamplingStep in Dataframe.go)
lastDataframe
float cpuInteger
float cpuFloat
float llc
float memRead
float memWrite
float storeBuffer
float scoreboard
float networkRead
float networkWrite
float sysCall