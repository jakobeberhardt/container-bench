# Mixed-8 Benchmark Results

## worst-case.yml
All high-sensitivity on socket 0, all low-sensitivity on socket 1

- Global Mean IPC: 
- Total Instructions: 

## Relocate the worst-case placement after 30 seconds

Runtime optimization via docker update to achieve balanced placement:

```sh
sudo docker update --cpuset-cpus 17 matrix-3d-0 && \
sudo docker update --cpuset-cpus 1 heapsort-0 && \
sudo docker update --cpuset-cpus 19 tsearch-0 && \
sudo docker update --cpuset-cpus 3 cache-l1-0
```

## Move zip
```bash
sudo docker update --cpuset-cpus 17 7z && sudo docker update --cpuset-cpus 0 heapsort-0
```