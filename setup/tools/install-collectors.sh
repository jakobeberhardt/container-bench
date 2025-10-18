sudo apt -y install intel-cmt-cat

sudo apt -y install linux-tools-common linux-tools-generic linux-tools-`uname -r`
sudo sysctl kernel.perf_event_paranoid=-1

modprobe msr
umount /sys/fs/resctrl 2>/dev/null
mount -t resctrl resctrl /sys/fs/resctrl

export RDT_IFACE=OS

mount | grep '/var/lock\|/run/lock'
sudo rm -f /var/lock/libpqos* /run/lock/libpqos* 2>/dev/null || true

for g in /sys/fs/resctrl/*; do
  case "$g" in
    */info|*/mon_groups|*/mon_data) continue ;;
  esac
  [ ! -s "$g/tasks" ] && rmdir "$g" 2>/dev/null
done


pqos -I -V -s
perf -v
for g in /sys/fs/resctrl/*; do
  case "$g" in
    */info|*/mon_groups|*/mon_data) continue ;;
  esac
  [ ! -s "$g/tasks" ] && rmdir "$g" 2>/dev/null
done