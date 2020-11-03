from common.constants import SECOND

# BROKER
# connection
CONNECTION_RETRY_COUNT = 5
RECONNECT_DELAY = 5 * SECOND
# exchange
EXCHANGE_NAME = 'default'
EXCHANGE_TYPE = 'direct'


# routing
class RoutingKeys:
    AGENT_LITE = 'lite'
    AGENT_ON_BE = 'backend'
    DISPATCHER = 'dispatcher'
    RESULTS = 'results'
    TASK = 'task'

    ALL_QUEUES = [AGENT_LITE, AGENT_ON_BE, DISPATCHER, RESULTS, TASK]


# DISPATCHER
DISPATCHER_PORT = 9999
INIT_AGENT_ID = 1001
