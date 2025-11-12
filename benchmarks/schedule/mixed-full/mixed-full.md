## Relocate the worst-case placement after e.g. 30 seconds
- Global Mean IPC: 
- Total Instructions: 

```sh
sudo docker update --cpuset-cpus 16 cache-l3-0 && \
sudo docker update --cpuset-cpus 0 matrixprod-0 && \
sudo docker update --cpuset-cpus 18 cache-l3-2 && \
sudo docker update --cpuset-cpus 2 matrixprod-2 && \
sudo docker update --cpuset-cpus 20 matrix-3d-0 && \
sudo docker update --cpuset-cpus 4 heapsort-0 && \
sudo docker update --cpuset-cpus 22 matrix-3d-2 && \
sudo docker update --cpuset-cpus 6 heapsort-2 && \
sudo docker update --cpuset-cpus 24 stream-0 && \
sudo docker update --cpuset-cpus 8 qsort-0 && \
sudo docker update --cpuset-cpus 26 stream-2 && \
sudo docker update --cpuset-cpus 10 qsort-2 && \
sudo docker update --cpuset-cpus 28 tsearch-0 && \
sudo docker update --cpuset-cpus 12 cache-l1-0 && \
sudo docker update --cpuset-cpus 30 tsearch-2 && \
sudo docker update --cpuset-cpus 14 cache-l1-2
```