# Page Size Setup

reserve 50 GB of 1 GB pages
```sh
sudo sed -i 's/GRUB_CMDLINE_LINUX="/GRUB_CMDLINE_LINUX="default_hugepagesz=1G hugepagesz=1G hugepages=50 /' /etc/default/grub
sudo update-grub
sudo reboot
```
```sh
grep -E 'HugePages_(Total|Free|Rsvd|Surp)|Hugepagesize' /proc/meminfo
```

2 MB transparent
```sh
echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

```sh
grep -E 'Anon|THP' /proc/meminfo
```