# Neighbor Workload Container

This container provides a configurable memory workload generator that can be used as a "noisy neighbor" in container benchmarking scenarios.

## Features

- **Configurable Duration**: Set how long the workload runs
- **Configurable Buffer Size**: Control memory usage (supports KB, MB, GB)
- **Multiple Access Patterns**: 
  - `random`: Random memory access
  - `stride`: Strided memory access (4KB stride)
  - `sequential`: Sequential memory access

```shell
/neighbor --duration 3600 --buffer-size 1000MB --pattern random" container=random
```
