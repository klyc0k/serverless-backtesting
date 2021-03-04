from workflow.trigger import Trigger
from workflow.deploy import Deploy, DeployItem
from workflow.reduce import Reducer
import json
import sys
import os

with open('deploy_config.json', 'r') as f:
    deploy_config = json.load(f)
with open('task_config.json', 'r') as f:
    task_config = json.load(f)

task_workspace = os.path.join(deploy_config['workspace_path'], task_config['task_id'])


def run_deploy():
    deploy = Deploy(deploy_config, task_config)
    deploy.run()


def run_trigger():
    with open(os.path.join(task_workspace, 'historical_data_ids.txt'), 'r') as f:
        data = f.read()
        data_ids = data.split('\n')
        if '' in data_ids:
            data_ids.remove('')
    with open(os.path.join(task_workspace, 'deployed_list.json'), 'r') as f:
        import json
        deployed = json.load(f)
        sqs_queue_urls = [v for k, v in deployed if k == DeployItem.SQS_QUEUE and v.find('_completion') < 0]

    trigger = Trigger()
    trigger.run(data_ids, sqs_queue_urls)


def run_reduce():
    with open(os.path.join(task_workspace, 'historical_data_ids.txt'), 'r') as f:
        data = f.read()
        data_ids = data.split('\n')
        if '' in data_ids:
            data_ids.remove('')
    with open(os.path.join(task_workspace, 'deployed_list.json'), 'r') as f:
        deployed = json.load(f)
        sqs_queue_urls = [v for k, v in deployed if k == DeployItem.SQS_QUEUE and v.find('_completion') > 0]

    reducer = Reducer(deploy_config, task_config['task_id'], sqs_queue_urls[0], data_ids)
    reducer.run()


def run_cleanup():
    deploy = Deploy(deploy_config, task_config)
    deploy.clean_up()


if __name__ == '__main__':
    if len(sys.argv) < 1:
        print("Unknown arguments. Valid arguments are 'deploy', 'trigger', 'reduce', 'cleanup' and 'all'")
        exit(1)

    arg = sys.argv[1]
    if arg == 'deploy':
        run_deploy()
    elif arg == 'trigger':
        run_trigger()
    elif arg == 'reduce':
        run_reduce()
    elif arg == 'cleanup':
        run_cleanup()
    elif arg == 'all':
        run_deploy()
        run_trigger()
        run_reduce()
        run_cleanup()
    else:
        print("Unknown arguments. Valid arguments are 'deploy', 'trigger', 'reduce', 'cleanup' and 'all'")
