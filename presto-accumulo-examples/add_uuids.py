#!/usr/bin/env python

from uuid import uuid4
import os, sys

LINEITEM_TBL = "tpch/lineitem.tbl"
LINEITEM_WITH_UUID_TBL = "tpch/lineitem_with_uuid.tbl"
PARTSUPP_TBL = "tpch/partsupp.tbl"
PARTSUPP_WITH_UUID_TBL = "tpch/partsupp_with_uuid.tbl"

if not os.path.isfile(LINEITEM_TBL):
    print LINEITEM_TBL + " does not exist or is not a file"
    sys.exit(1)

if not os.path.isfile(PARTSUPP_TBL):
    print PARTSUPP_TBL + " does not exist or is not a file"
    sys.exit(1)

l = open(LINEITEM_TBL, 'r')
l_uuid = open(LINEITEM_WITH_UUID_TBL, 'w')

print "Re-writing %s with a UUID column to file %s" % (LINEITEM_TBL, LINEITEM_WITH_UUID_TBL)
for line in l.readlines():
    l_uuid.write(str(uuid4()) + "|" + line)

l_uuid.close()

print "Re-writing %s with a UUID column to file %s" % (PARTSUPP_TBL, PARTSUPP_WITH_UUID_TBL)
p = open(PARTSUPP_TBL, 'r')
p_uuid = open(PARTSUPP_WITH_UUID_TBL, 'w')

for line in p.readlines():
    p_uuid.write(str(uuid4()) + "|" + line)

p_uuid.close()
