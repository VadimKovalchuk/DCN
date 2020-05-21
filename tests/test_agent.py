import logging
from tests.conftest import polling_expiration
from functools import partial

logger = logging.getLogger(__name__)


def test_agent_registration(dispatcher, agent):
    name = 'agent_test_name'
    logger.info('Starting')
    interrupt = partial(dispatcher.listen, 1)
    agent.register(interrupt)
    # dispatcher.listen(1, polling_expiration)
    assert agent.id, 'Agent ID was not set'
