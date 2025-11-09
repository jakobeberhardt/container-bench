# TODO
- ~~Cache info in meta data, e.g. 3MB LCC, 1KB L1~~
	- Write it to DB
- Env variables in .yml file
- ~~Parallel start up and tear down~~
- ~~Bulk write to DBs~~
- ~~RDT Memory/s Field: Use Delta~~
- ~~Add relative time~~
- Handle docker timeouts using context
- Preallocation of Dataframes according to sice, e.g. if sampling rate is different
- ~~Start collectors faster and earlier (we want to see the cache warming up)~~
- Add support for container exit (e.g. when benchmark finished)
- ~~Make perf counters count all cores~~
- ~~Integrate perf stat -a   -e cycles,instructions,inst_retired.any,cycle_activity.stalls_total,cycle_activity.stalls_l3_miss,cycle_activity.stalls_l2_miss,cycle_activity.stalls_l1d_miss,cycle_activity.stalls_mem_any,resource_stalls.sb,resource_stalls.scoreboard   --cgroup=/system.slice/docker-e6733f3b68b810da8bffa4b6c983126db03da6cc69ececc48faddf862513d8c0.scope   sleep 10~~
- ~~Make a Probe() function which return float vector with impact on IPC/Stalls/ApplicationMetric per stressor, e.g. matrixprod, stream, io~~
- Implement probe kernel with minimal time and amount of stressors
- Intel RDT Interface  for scheduler
- Add Scheduler interface for docker (for affinity, resource allocation). Is this possible during runtime? 
- Add tracking of execution core(s) per dockercontainer for timeline
  