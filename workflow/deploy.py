import git
import requests
import os
import subprocess
import shutil
import jinja2
import boto3
import json


class Deploy:
    DEPLOY_PACKAGE_FILE_NAME = 'deployment-package.zip'

    def __init__(self, deploy_config, task_config):
        for k, v in deploy_config.items():
            setattr(self, k, v)
        self.task_workspace = os.path.join(self.workspace_path, task_config['task_id'])
        self.task_config = task_config
        self.deployed_list = []
        self.context = {}

    def run(self):
        try:
            os.makedirs(self.task_workspace, exist_ok=True)
            # create sqs queue for completion signal
            print('Creating SQS completion queue...')
            self.completion_queue_url, _ = self.create_sqs(self.task_config['task_id'] + '_completion')
            print('Deploying benchmark processes...')
            self.sub_pipeline('benchmark')
            print('Deploying test processes...')
            self.sub_pipeline('test')
        finally:
            list_path = os.path.join(self.task_workspace, 'deployed_list.json')
            with open(list_path, 'w') as f:
                json.dump(self.deployed_list, f, indent=2)

    def sub_pipeline(self, exec_type='benchmark'):
        exec_location = os.path.join(self.task_workspace, exec_type)
        os.makedirs(exec_location, exist_ok=True)

        self.fetch_source(exec_type, os.path.join(exec_location, 'package'))
        self.fetch_files(exec_type, os.path.join(exec_location, 'package'))
        self.generate_handler(exec_type, os.path.join(exec_location, 'package'))
        self.package_source(exec_location)

        exec_efs_lib_location = os.path.join(self.ec2_efs_mount_path, self.task_config['task_id'], exec_type)
        os.makedirs(exec_efs_lib_location, exist_ok=True)
        self.deploy_libraries(self.task_config[exec_type]['python'], exec_efs_lib_location)
        sqs_url, sqs_arn = self.create_sqs(self.task_config['task_id'] + '_' + exec_type)

        function_name = self.task_config['task_id'] + '_' + exec_type
        self.deploy_lambda(function_name, sqs_arn, os.path.join(exec_location, Deploy.DEPLOY_PACKAGE_FILE_NAME))

    def clean_up(self):
        list_path = os.path.join(self.task_workspace, 'deployed_list.json')
        if len(self.deployed_list) == 0:
            print("Loading %s for deployed component list..." % list_path)
            with open(list_path, 'r') as f:
                self.deployed_list = json.load(f)
        try:
            while len(self.deployed_list) > 0:
                t = self.deployed_list.pop(0)
                if t[0] == DeployItem.LAMBDA:
                    print('Deleting Lambda function: %s' % t[1])
                    conn = boto3.client('lambda')
                    conn.delete_function(FunctionName=t[1])
                elif t[0] == DeployItem.SQS_QUEUE:
                    print('Deleting SQS queue: %s' % t[1])
                    conn = boto3.client('sqs')
                    conn.delete_queue(QueueUrl=t[1])
                elif t[0] == DeployItem.EFS_MOUNT:
                    continue
                elif t[0] == DeployItem.DYNAMODB:
                    continue
                elif t[0] == DeployItem.LOCAL_FILES:
                    print('Deleting local file: %s' % t[1])
                    if os.path.exists(t[1]):
                        shutil.rmtree(t[1])
                elif t[0] == DeployItem.LAMBDA_SQS_MAPPING:
                    print('Deleting Lambda-SQS mapping: %s' % t[1])
                    conn = boto3.client('lambda')
                    conn.delete_event_source_mapping(UUID=t[1])
        finally:
            with open(list_path, 'w') as f:
                json.dump(self.deployed_list, f, indent=2)

    def register_deployed(self, object_type, identifier):
        self.deployed_list.append((object_type, identifier))

    def template_dict(self, exec_type):
        if self.completion_queue_url is None:
            raise RuntimeError("Completion queue has to be created first")
        return {
            'lib_location': os.path.join(self.lambda_efs_mount_path, self.task_config['task_id'], exec_type),
            'completion_queue': self.completion_queue_url,
            'exec_type': exec_type,
            'task_id': self.task_config['task_id']
        }

    def fetch_source(self, exec_type, save_path):
        print('Pulling source code from git repo...')
        source_code_repo = self.task_config[exec_type]['git']
        branch = self.task_config[exec_type]['branch']
        if os.path.exists(save_path):
            shutil.rmtree(save_path)
        os.makedirs(save_path, exist_ok=True)
        git.Repo.clone_from(
            source_code_repo,
            save_path,
            branch=branch
        )
        self.register_deployed(DeployItem.LOCAL_FILES, save_path)

    def fetch_files(self, exec_type, save_path):
        print('Fetching support files...')
        os.makedirs(save_path, exist_ok=True)
        urls_to_be_fetched = self.task_config[exec_type]['files']
        for u in urls_to_be_fetched:
            r = requests.get(u, allow_redirects=True)
            file_name = u.rsplit('/', 1)[1]
            with open(os.path.join(save_path, file_name), 'wb') as f:
                f.write(r.content)
        self.register_deployed(DeployItem.LOCAL_FILES, save_path)

    def generate_handler(self, exec_type, save_path):
        print('Generating lambda function handler...')
        cur_dir = os.path.dirname(os.path.realpath(__file__))
        template_dict = self.template_dict(exec_type)
        with open(os.path.join(cur_dir, 'lambda_function.py.j2')) as f:
            template = jinja2.Template(f.read())
        with open(os.path.join(save_path, 'lambda_function.py'), 'w') as f:
            f.write(template.render(template_dict))
        self.register_deployed(DeployItem.LOCAL_FILES, save_path)

    def package_source(self, exec_location):
        print('Creating deployment package...')
        subprocess.run(['zip', '-r', '../%s' % Deploy.DEPLOY_PACKAGE_FILE_NAME, './', '-x', '*.git*'], cwd=os.path.join(exec_location, 'package'))
        self.register_deployed(DeployItem.LOCAL_FILES, exec_location)

    def deploy_libraries(self, python_command, lib_location):
        """ deploy libraries to EFS (this has to be executed on a Lambda matching EC2 instance,
            otherwise compiled libraries will not be able to load) """
        print('Deploying dependencies to EFS...')
        os.makedirs(lib_location, exist_ok=True)
        subprocess.run([python_command, '-m', 'pip', 'install', '-q', '-r', 'requirements.txt', '-t', lib_location])
        self.register_deployed(DeployItem.EFS_MOUNT, lib_location)

    def create_sqs(self, queue_name, fifo=False):
        print('Creating SQS queue "%s"...' % queue_name)
        conn = boto3.client('sqs')
        get_response = conn.list_queues(QueueNamePrefix=queue_name)
        if 'QueueUrls' not in get_response or len(get_response['QueueUrls']) == 0:
            attr = {
                'VisibilityTimeout': '30'
            }
            if fifo:
                queue_name += '.fifo'
                attr['FifoQueue'] = 'true'
                attr['ContentBasedDeduplication'] = 'true'

            create_response = conn.create_queue(
                QueueName=queue_name,
                Attributes=attr
            )
            queue_url = create_response['QueueUrl']
        else:
            # if the queue already exists, purge it
            if len(get_response['QueueUrls']) > 1:
                raise RuntimeError('Multiple existing SQS queues matches search criteria: %s' % ','.join(get_response['QueueUrls']))
            print("SQS queue %s exists, purging..." % queue_name)
            conn.purge_queue(QueueUrl=get_response['QueueUrls'][0])
            queue_url = get_response['QueueUrls'][0]

        attr_response = conn.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
        self.register_deployed(DeployItem.SQS_QUEUE, queue_url)
        return queue_url, attr_response['Attributes']['QueueArn']

    def deploy_lambda(self, function_name, sqs_queue_arn, zip_file_path):
        print('Deploying Lambda function "%s"...' % function_name)
        with open(zip_file_path, 'rb') as f:
            zip_file = f.read()

        # create
        conn = boto3.client('lambda')
        response = conn.create_function(
            FunctionName=function_name,
            Runtime='python3.8',
            Role=self.aws_role_lambda_arn,
            Handler='lambda_function.lambda_handler',
            Timeout=30,
            MemorySize=2048,
            Code={
                'ZipFile': zip_file
            },
            FileSystemConfigs=[
                {
                    'Arn': self.efs_ap_arn,
                    'LocalMountPath': self.lambda_efs_mount_path
                },
            ],
            VpcConfig={
                'SubnetIds': self.lambda_vpc_subnet_ids,
                'SecurityGroupIds': [
                    self.lambda_security_group_id,
                ]
            }
        )
        self.register_deployed(DeployItem.LAMBDA, response['FunctionArn'])

        # set trigger
        response = conn.create_event_source_mapping(
            BatchSize=10,
            EventSourceArn=sqs_queue_arn,
            FunctionName=function_name,
        )
        self.register_deployed(DeployItem.LAMBDA_SQS_MAPPING, response['UUID'])


class DeployItem:
    LOCAL_FILES = 'local_files'
    LAMBDA = 'lambda_func'
    SQS_QUEUE = 'sqs_queue'
    LAMBDA_SQS_MAPPING = 'lambda_sqs_mapping'
    DYNAMODB = 'dynamodb'
    EFS_MOUNT = 'efs_mount'
