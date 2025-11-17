# allocator
Start a test container
```sh
sudo docker run -d --name test nginx
```

```sh
sudo runc --root /run/docker/runtime-runc/moby update --l3-cache-schema "L3:0=7" $(sudo docker inspect test | jq .[0].Id)
```