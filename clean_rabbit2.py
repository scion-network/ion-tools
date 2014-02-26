from rabbithelper import clean_by_sysname
from rabbitmqadmin import parser, make_parser, assert_usage
import sys

make_parser()
options, args = parser.parse_args()
assert_usage(len(args)==0, 'Please do not specify any action args')
#connect_string = ' '.join(sys.argv[1:])
deleted_exchanges, deleted_queues = clean_by_sysname(options, '')

print('Deleted exchanges:\n%s \n' % '\n'.join(deleted_exchanges))
print('Deleted queues:\n%s \n' % '\n'.join(deleted_queues))
