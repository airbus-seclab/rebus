#!/bin/sh
# similar to examples provided as README for rebus_demo agent set
rebus_master dbus &
for agent in web_interface unarchive link_finder hasher stringer
  do rebus_agent -m rebus_demo.agents --bus dbus $agent &
done

rebus_agent --bus dbus inject /bin/ls

sleep 1
rebus_agent --bus dbus -m rebus_demo.agents grep Software

pkill -9 -f rebus
