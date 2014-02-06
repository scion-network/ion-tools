#!/usr/bin/env python

from rabbitmqadmin import Management, make_parser, parser, assert_usage

def main():
    make_parser()
    parser.add_option('-n', '--import_sysname', dest='sysname', help='sysname required for import')
    (options, args) = parser.parse_args()
    assert_usage(len(args)>0, 'Action not specified')
    method = args[0]

    if method == 'export':
        export_rabbit(options, args)
    elif method == 'import':
        import_rabbit(options, args)
    else:
        mgmt = Management(options, args[1:])
        mode = "invoke_" + args[0]
        assert_usage(hasattr(mgmt, mode), 'Action {0} not understood'.format(args[0]))
        method = getattr(mgmt, "invoke_%s" % args[0])
        method()

def export_rabbit(options, args):
    mgmt = Management(options, args[1:])
    mgmt.invoke_export()

    json_data = open(args[1]).read()
    import json
    data = json.loads(json_data)

    # Find all durable exchanges
    exes = [exchange for exchange in data['exchanges'] if exchange['durable']==True]

    ex_names = [exchange['name'] for exchange in exes]
    # Find all durable queues
    queues =  [queue for queue in data['queues'] if queue['durable']==True]
    q_names = [queue['name'] for queue in queues]

    # Find bindings all durable exchanges and queues
    bindings = [binding for binding in data['bindings'] if binding['source'] in ex_names and binding['destination'] in q_names]

    newdata = {}
    newdata['users'] = []
    newdata['vhosts'] = []
    newdata['rabbit_version'] = ''
    newdata['permissions'] = []
    newdata['exchanges'] = exes
    newdata['queues'] = queues
    newdata['bindings'] = bindings
    newstr = json.dumps(newdata)

    import re
    pattern = re.compile(r'(.*?)[.]ion[.]xs[.]ioncore[.]xp[.]science_data')
    matched_sysname = pattern.match(','.join(ex_names)).group(1).split(',')[-1]

    import string
    transstr = string.replace(newstr, matched_sysname, 'SYSNAME')
    f = open(args[1], 'w')
    f.write(transstr)
    f.close()

def import_rabbit(options, args):
    assert_usage(options.sysname, 'Sysname required for import')
    data = open(args[1], 'r').read()
    import string
    transstr = string.replace(data, 'SYSNAME', options.sysname)
    open('/tmp/rabbitin', 'w').write(transstr)
    mgmt = Management(options, ['/tmp/rabbitin'])
    mgmt.invoke_import()

if __name__ == '__main__':
    main()
