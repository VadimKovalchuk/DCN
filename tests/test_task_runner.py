from copy import deepcopy
import json
import logging

from dcn.agent.agent import TaskRunner
from dcn.common.data_structures import task_body

logger = logging.getLogger(__name__)


def test_task_runner_flow():
    test_task = deepcopy(task_body)
    test_task['arguments'] = {'arg': 'agent_task_test'}
    runner = TaskRunner(test_task)
    assert runner.run(), 'Error occur during task execution'
    report = runner.report
    assert test_task['arguments'] == report['result'], \
        'Wrong task is received from task queue for Agent'
