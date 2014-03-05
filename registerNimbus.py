# !/usr/bin/env python


# ** boto version 2.5.2 required

import os
import sys

import boto
from boto.ec2.regioninfo import RegionInfo


access_id = "h6X3iTOHQdoRJnDmTy68j"
access_secret = "rcdkVgjanPoAiXV8jIeak0RpDMC5dzGB0TUWR2M8f4"
host = 'nimbus-dev0.oceanobservatories.org'

keytext = open('/Users/jachen/.ssh/ooi-west.pub').read()
keyname = "ooi"

print "creating key %s||%s" % (keyname, keytext)

region = RegionInfo(name="nimbus", endpoint=host)
ec2conn = boto.connect_ec2(access_id, access_secret, region=region,
        port=8444)

ec2conn.create_key_pair('%s||%s' % (keyname, keytext))
