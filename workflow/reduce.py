import boto3
import time


class Reducer:
    def __init__(self, deploy_config, task_id, completion_queue_url, ids):
        for k, v in deploy_config.items():
            setattr(self, k, v)
        self.completion_queue_url = completion_queue_url
        self.task_id = task_id
        self.ids = set(ids)
        if '' in self.ids:
            self.ids.remove('')
        self.idle_threshold = 60

    def run(self):
        start = time.time()
        comparator = Comparator()
        try:
            sqs = boto3.client('sqs')
            dynamo = boto3.resource('dynamodb')
            compare_map = {}
            n = len(self.ids)
            time_slept = 0
            while len(self.ids) > 0:
                response = sqs.receive_message(
                    QueueUrl=self.completion_queue_url,
                    AttributeNames=['SentTimestamp'],
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=['All'],
                    VisibilityTimeout=30,
                    WaitTimeSeconds=0
                )
                if 'Messages' not in response:
                    if time_slept > self.idle_threshold:
                        print("No new messages come in within %d seconds, terminating..." % self.idle_threshold)
                        break
                    time_slept += 1
                    time.sleep(1)
                    continue
                time_slept = 0
                messages = response['Messages']
                messages_to_delete = []
                rids = []
                for m in messages:
                    messages_to_delete.append((m['MessageId'], m['ReceiptHandle']))
                    rids.append(m['Body'])

                db_response = dynamo.meta.client.batch_get_item(
                    RequestItems={
                        'backtesting-result': {
                            'Keys': [{'result_id': rid} for rid in rids]
                        }
                    }
                )
                for item in db_response['Responses']['backtesting-result']:
                    if item['data_id'] not in self.ids:
                        continue
                    if item['data_id'] not in compare_map:
                        compare_map[item['data_id']] = {}
                    compare_map[item['data_id']][item['exec_type']] = item['data']
                    if 'test' in compare_map[item['data_id']] and 'benchmark' in compare_map[item['data_id']]:
                        comparator.compare(compare_map[item['data_id']]['benchmark'], compare_map[item['data_id']]['test'])
                        self.ids.remove(item['data_id'])

                sqs.delete_message_batch(
                    Entries=[{'Id': i, 'ReceiptHandle': rh} for i, rh in messages_to_delete],
                    QueueUrl=self.completion_queue_url
                )
                print("%d / %d tasks processed" % (n-len(self.ids), n), end='\r')
        finally:
            print("")
            end = time.time()
            print("Process time: " + time.strftime("%H:%M:%S", time.gmtime(end - start)))
            comparator.aggregate_and_print()


class Comparator:
    def __init__(self):
        self.total_test_cases = 0
        self.total_test_cases_with_diffs = 0
        self.field_diffs = {}

    def compare(self, a, b):
        """This is a simplified comparing function and can only handle numerical values"""
        a_map = Comparator.flatten(a)
        b_map = Comparator.flatten(b)
        a_only = set(a_map) - set(b_map)
        b_only = set(b_map) - set(a_map)

        result = {}
        for k in set(a_map).intersection(set(b_map)):
            if a_map[k] != b_map[k]:
                result[k] = b_map[k] - a_map[k]
        for k in a_only:
            result[k] = 'deleted'
        for k in b_only:
            result[k] = 'added'

        for k, v in result.items():
            if k not in self.field_diffs:
                self.field_diffs[k] = []
            self.field_diffs[k].append(v)

        self.total_test_cases += 1
        if len(result) > 0:
            self.total_test_cases_with_diffs += 1

    @staticmethod
    def flatten(target):
        """This is a simplified flatten method that only handles nested dicts"""
        results = {}

        def visit(obj, curr_str):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_str = k if curr_str == '' else curr_str + '.' + k
                    visit(v, new_str)
            else:
                results[curr_str] = obj
        visit(target, '')
        return results

    def aggregate_and_print(self):
        print("Total test cases: %d" % self.total_test_cases)
        print("Total test cases with differences: %d" % self.total_test_cases_with_diffs)
        if self.total_test_cases_with_diffs > 0:
            print("Changes in output:")
            print("{0:>30} {1:>10} {2:>10}".format("name", "diff_mean", "count"))
            str_format = "{0:>30} {1:>10.2f} {2:>10}"
            import numpy as np
            for k, v in self.field_diffs.items():
                if isinstance(v[0], str):
                    diff_mean = ''
                else:
                    diff_mean = np.mean(v)
                print(str_format.format(k, diff_mean, len(v)))
