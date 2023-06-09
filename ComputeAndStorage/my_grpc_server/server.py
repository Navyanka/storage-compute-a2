import boto3
import grpc
from concurrent import futures
import computeandstorage_pb2
import computeandstorage_pb2_grpc


class EC2OperationsServicer(computeandstorage_pb2_grpc.EC2OperationsServicer):
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id='ASIA4T77KOA4D7AEMW4O',
            aws_secret_access_key='NZBLW1RBYdKXJG6Ktd9TJlS1YfYBFo1SsxK3FwbY',
            aws_session_token='FwoGZXIvYXdzEFgaDCCRAmN80dUasaXN0yLAAW1Vyesg2rbMLaguBCJyor76VHuM29vwQ4pD0/c2VmYjOiedY+Sj8VHOgAbJMu5eQyOIMPRFYDJhx/hI4Jj3SHPeuJV2y+LiS5qLyhyTVOlXmoinbmvngeIGF3dTC1SOSX1ELSlM+NqvfuR/RZlrBKSIDC03i1iDa5F5RTrcRkfsoWfKmkhJkws8EFInrZCv//6NnAI0yIWLjBGBXfgQt7MntJ8Z8gkxGpmIIuDW2d9BfYjCW75CZmU1jXnx/26eViiq7oykBjItgmvzmj3zargsnmWPTqMTKlbCwXhnd8c2pScqSlTJVYB3raAX+QJ5FFCt80YY',
            region_name='us-east-1'
        )

    def StoreData(self, request, context):
        # Assuming 'mybucket' is your bucket name and 'myfile.txt' is the file you want to create
        self.s3.put_object(Body=request.data, Bucket='mybucketcomputestorage', Key='myfile.txt')
        s3_uri = f"https://mybucketcomputestorage.s3.amazonaws.com/myfile.txt"
        return computeandstorage_pb2.StoreReply(s3uri=s3_uri)

    def AppendData(self, request, context):
        obj = self.s3.get_object(Bucket='mybucketcomputestorage', Key='myfile.txt')
        original = obj['Body'].read().decode()
        appended = original + request.data
        self.s3.put_object(Body=appended, Bucket='mybucketcomputestorage', Key='myfile.txt')
        return computeandstorage_pb2.AppendReply()

    def DeleteFile(self, request, context):
        self.s3.delete_object(Bucket='mybucketcomputestorage', Key='myfile.txt')
        return computeandstorage_pb2.DeleteReply()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    computeandstorage_pb2_grpc.add_EC2OperationsServicer_to_server(EC2OperationsServicer(), server)
    server.add_insecure_port('0.0.0.0:8080')
    server.start()
    print("Server started. Listening on 0.0.0.0:8080.")
    server.wait_for_termination()



if __name__ == '__main__':
    serve()
