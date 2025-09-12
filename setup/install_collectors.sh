sudo apt -y install intel-cmt-cat

sudo apt -y install linux-tools-common linux-tools-generic linux-tools-`uname -r`

modprobe msr
umount /sys/fs/resctrl 2>/dev/null
mount -t resctrl resctrl /sys/fs/resctrl
export RDT_IFACE=OS


for g in /sys/fs/resctrl/*; do
  case "$g" in
    */info|*/mon_groups|*/mon_data) continue ;;
  esac
  [ ! -s "$g/tasks" ] && rmdir "$g" 2>/dev/null
done


pqos -R
pqos -V
pqos -R

for g in /sys/fs/resctrl/*; do
  case "$g" in
    */info|*/mon_groups|*/mon_data) continue ;;
  esac
  [ ! -s "$g/tasks" ] && rmdir "$g" 2>/dev/null
done