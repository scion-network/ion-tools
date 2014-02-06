#!/usr/bin/env python

import sys
import argparse
import os
import re

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsException, NoNodeException

# Sysname pattern
pat = re.compile(r'^/(.*?)/')

# This function can be expanded to handle non-null username and password if needed
def get_kazoo_kwargs(username=None, password=None, retry_backoff=1.1):
    kwargs = {"retry_backoff": retry_backoff}
    kwargs['auth_data'] = None
    kwargs['default_acl'] = None
    return kwargs

def get_kazoo(zk_config):
    hosts = zk_config.get('hosts')
    if not hosts:
        raise CliError("ZK config missing hosts")
    username = zk_config.get('username')
    password = zk_config.get('password')
    if (username or password) and not (username and password):
        raise CliError("Both username and password must be specified, or neither")
    kwargs = get_kazoo_kwargs(username, password)
    return KazooClient(hosts, **kwargs)

def main(args=None):
    parser = argparse.ArgumentParser(description='CEI ZooKeeper tree utility')

    subparsers = parser.add_subparsers()
    destroy_parser = subparsers.add_parser("destroy",
        description=destroy.__doc__, help="delete path")
    destroy_parser.add_argument("zk_host", help="zookeeper host")
    destroy_parser.add_argument("path", help="zookeeper base path")
    destroy_parser.set_defaults(func=destroy)
    
    export_parser = subparsers.add_parser("export",
        description=export_db.__doc__, help="Export data from a cei zookeeper host")
    export_parser.add_argument("zk_host", help="zookeeper host")
    export_parser.add_argument("sysname", help="sysname")
    export_parser.add_argument("-out_file", help="output file, default is /tmp/ceizk.txt", default='/tmp/ceizk.txt')

    export_parser.set_defaults(func=export_db)

    import_parser = subparsers.add_parser("import",
        description=import_db.__doc__, help="Import data to a cei zookeeper host")
    import_parser.add_argument("zk_host", help="zookeeper host")
    import_parser.add_argument("sysname", help="sysname")
    import_parser.add_argument("-in_file", help="input file, default is /tmp/ceizk.txt", default='/tmp/ceizk.txt')
    import_parser.set_defaults(func=import_db)

    args = parser.parse_args(args=args)
    kazoo = get_kazoo({'hosts': args.zk_host})
    print "Zookeeper is starting"
    kazoo.start()
    try:
        args.func(kazoo, args)
    except Exception, e:
        import traceback,sys
        traceback.print_exc(file=sys.stdout)
        parser.error(str(e))
    print "Zookeeper is stopping"
    kazoo.stop()

def destroy(kazoo, args):
    kazoo.delete(args.path, recursive=True)

def walk(kazoo, f, path):
    data, stat = kazoo.get(path)
    if data != '':
        line= '%s %s' % (path, data)
        old_sys = pat.match(path).group(1)
        line = re.sub(old_sys, 'SYSNAME', line)
        print line
        f.write(line + '\n')

    children = kazoo.get_children(path)
    if len(children) == 0:
        return
    for child in children:
        walk(kazoo, f, path + '/' + child)

def export_db(kazoo, args):
    """Export pd and dtrs subtrees""" 
    f=open(args.out_file, 'w')
    walk(kazoo, f, '/' + args.sysname + '/pd')
    walk(kazoo, f, '/' + args.sysname + '/dtrs')
    f.close()

def import_db(kazoo, args):
    """Import pd and dtrs subtrees""" 
    for line in open(args.in_file).xreadlines():
        # Finds and replaces all occurrences of old sysnames with new sysnames
        old_sys = pat.match(line).group(1)
        line = re.sub(old_sys, args.sysname, line)
        path,sep,data = line.partition(' ')
        short_path, node = path.rsplit('/', 1)
        print path
        kazoo.ensure_path(short_path)
        try:
            kazoo.create(path, data)
        except NodeExistsException:
            kazoo.set(path, data)

if __name__ == '__main__':
    main()
