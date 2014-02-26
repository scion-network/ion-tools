#!/usr/bin/python

from rabbitmqadmin import Management, LISTABLE, DELETABLE
import shlex
import simplejson

class RabbitManagementHelper(object):
    def __init__(self, options):
        self.options = options

    def list_names(self, listable_type):
        list_str = '%s name' % listable_type
        mgmt = Management(self.options, shlex.split(list_str))
        uri = mgmt.list_show_uri(LISTABLE, 'list', mgmt.args[1:])
        output_json = mgmt.get(uri)
        listables = simplejson.loads(output_json)
        return listables

    def list_names_with_prefix(self, listables, name_prefix):
        return [l['name'] for l in listables if l['name'].startswith(name_prefix)]

    # This function works on exchange, queue, vhost, user
    def delete_names_with_prefix(self, deletable_type, deleteable,  name_prefix):
        deleted = []
        for d in deleteable:
            if d['name'] != '' and d['name'].startswith(name_prefix):
                # Do not delete builtin exchanges
                if deletable_type == 'exchange' and d['name'].startswith('amq.'):
                        continue
                delete_cmd = '%s name="%s"' % (deletable_type, d['name'])
                mgmt = Management(self.options, shlex.split(delete_cmd))
                mgmt.invoke_delete()
                deleted.append(d['name'])
        return deleted

def clean_by_sysname(options, sysname):
    """
    Utility method to clean sysname-prefixed exchanges and queues on a broker.

    @param  connect_string  The connect string to use with the RabbitManagementHelper.
                            Form is similar to "-H localhost -P 55672 -u guest -p guest -V /"
    @param  sysname         The sysname to use to select exchanges and queues to delete.
                            Must be the prefix to the exchange or queue or this will not be
                            deleted.
    @returns                A 2-tuple of (list of exchanges deleted, list of queues deleted).
    """

    rmh               = RabbitManagementHelper(options)

    exchanges         = rmh.list_names('exchanges')
    deleted_exchanges = rmh.delete_names_with_prefix('exchange', exchanges, sysname)

    queues            = rmh.list_names('queues')
    deleted_queues    = rmh.delete_names_with_prefix('queue', queues, sysname)

    return (deleted_exchanges, deleted_queues)

