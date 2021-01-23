import boto3
import uuid


class Trigger:
    def run(self, data_ids, queue_urls):
        sqs = boto3.client('sqs')

        # each request can only take 10 in a batch
        n = len(data_ids)
        for x in range(0, n, 10):
            if x >= len(data_ids):
                break
            for qu in queue_urls:
                if qu.endswith('.fifo'):
                    entries = [{'MessageGroupId': 'task', 'MessageBody': i, 'Id': str(uuid.uuid4())} for i in data_ids[x:x + 10]]
                else:
                    entries = [{'MessageBody': i, 'Id': str(uuid.uuid4())} for i in data_ids[x:x + 10]]
                response = sqs.send_message_batch(
                    QueueUrl=qu,
                    Entries=entries
                )
                # print("Sent %d events to %s" % (len(entries), qu))
                # print(response)
            print("%d / %d events pushed" % (x + 10, n), end='\r')
