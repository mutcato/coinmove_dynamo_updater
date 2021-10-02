from datetime import datetime
import boto3
import botocore
from botocore.config import Config
from decimal import Decimal

TTL = {"5m": 120*24*60*60, "15m": 120*24*60*60*3, "1h": 120*24*60*60*12, "4h": 120*24*60*60*12*4, "8h": 120*24*60*60*12*8, "1d": 120*24*60*60*12*24}

class Metrics:
    def __init__(self):
        resource = boto3.resource(
            "dynamodb", 
            config=Config(read_timeout=585, connect_timeout=585)
        )
        self.table_name = "metrics_test"
        self.table = resource.Table(self.table_name)
        self.sqs_client = boto3.client('sqs')

    def insert(self, event):
        records = event["Records"]
        for record in records:
            timestamp = int(record["messageAttributes"]["time"]["stringValue"])
            response = self.table.put_item(
                Item={
                    'ticker_interval': record["body"],
                    'time': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                    'open': Decimal(record["messageAttributes"]["open"]["stringValue"]),
                    'high': Decimal(record["messageAttributes"]["high"]["stringValue"]),
                    'low': Decimal(record["messageAttributes"]["low"]["stringValue"]),
                    'close': Decimal(record["messageAttributes"]["close"]["stringValue"]),
                    'volume': Decimal(record["messageAttributes"]["volume"]["stringValue"]),
                    'number_of_trades': int(record["messageAttributes"]["number_of_trades"]["stringValue"]),
                    'TTL': timestamp + TTL[record["messageAttributes"]["interval"]["stringValue"]]
                }
            )
            print(f"RECORD------->: {record}")
            print(f"INSERTED:  {response}")
            deletion = self.delete_from_queue(record["receiptHandle"])
            print(f"DELETED:  {deletion}")

        
    def delete_from_queue(self, receipt_handle):
        queue_url = self.sqs_client.get_queue_url(QueueName="ticker-ohlcv")["QueueUrl"]
        response = self.sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        return response


class Summary:
    def __init__(self, event):
        self.table_name = "metrics_summary"
        resource = boto3.resource("dynamodb", config=Config(read_timeout=585, connect_timeout=585))
        self.table = resource.Table(self.table_name)
        self.event = event

    @staticmethod
    def get_ticker_interval(record):
        message_body = record["body"]
        ticker, interval = message_body.rsplit("_", 1)
        """
        Todo: Add a loop to insert other metric types (open, high, low, volume, number_of_trades)
        """
        interval_metric = interval + "_" + "close"
        return ticker, interval_metric
        
    @staticmethod
    def get_volume_in_usdt(record):
        volume_in_usdt = Decimal(record["messageAttributes"]["close"]["stringValue"]) * Decimal(record["messageAttributes"]["volume"]["stringValue"])
        return volume_in_usdt

    def insert_single(self, record):
        volume_in_usdt = self.get_volume_in_usdt(record)
        ticker, interval_metric = self.get_ticker_interval(record)
        try:
            self.table.put_item(
                Item={"ticker": ticker, "interval_metric": interval_metric, "volume_in_usdt": volume_in_usdt},
                ConditionExpression="attribute_not_exists(ticker) AND attribute_not_exists(interval_metric)"
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up
            # other exceptions.
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

    def insert(self):
        for record in self.event["Records"]:
            self.insert_single(record)

def lambda_handler(event, context):
    print("EVENT: ")
    print(event)
    summary = Summary(event)
    summary.insert()
        
    metrics = Metrics()
    metrics.insert(event)
