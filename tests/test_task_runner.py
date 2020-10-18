from copy import deepcopy
import json
import logging

from agent.agent import TaskRunner
from common.broker import Task
from common.data_structures import task_body

logger = logging.getLogger(__name__)


def test_happy_path():
    test_task_body = deepcopy(task_body)
    test_task_body['client'] = 'test'
    test_task_body['arguments'] = {'arg': 'agent_task_test'}
    test_task = Task(None, None, json.dumps(test_task_body))
    runner = TaskRunner(test_task)
    assert runner.run(), 'Error occur during task execution'
    report = runner.report
    assert test_task_body['arguments'] == report['result'], \
        'Wrong task is received from task queue for Agent'
