#!/bin/sh
# similar to examples provided as README for rebus_demo agent set
rebus_agent -m rebus_demo.agents hasher unarchive inject /bin/ls -- return --short md5_hash
