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
        self.table_name = "metrics"
        self.table = resource.Table(self.table_name)

    def insert(self, event):    
        timestamp = int(event["Records"][0]["messageAttributes"]["time"]["stringValue"])
        response = self.table.put_item(
            Item={
                'ticker_interval': event["Records"][0]["body"],
                'time': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                'open': Decimal(event["Records"][0]["messageAttributes"]["open"]["stringValue"]),
                'high': Decimal(event["Records"][0]["messageAttributes"]["high"]["stringValue"]),
                'low': Decimal(event["Records"][0]["messageAttributes"]["low"]["stringValue"]),
                'close': Decimal(event["Records"][0]["messageAttributes"]["close"]["stringValue"]),
                'volume': Decimal(event["Records"][0]["messageAttributes"]["volume"]["stringValue"]),
                'number_of_trades': int(event["Records"][0]["messageAttributes"]["number_of_trades"]["stringValue"]),
                'TTL': timestamp + TTL[event["Records"][0]["messageAttributes"]["interval"]["stringValue"]]
            }
        )
        return response


class Summary:
    def __init__(self, event):
        self.table_name = "metrics_summary"
        resource = boto3.resource("dynamodb", config=Config(read_timeout=585, connect_timeout=585))
        self.table = resource.Table(self.table_name)
        self.event = event
        self.ticker, self.interval_metric = self.get_ticker_interval()
        self.volume_in_usdt = self.get_volume_in_usdt()

    def get_ticker_interval(self):
        message_body = self.event["Records"][0]["body"]
        ticker, interval = message_body.rsplit("_", 1)
        """
        Todo: Add a loop to insert other metric types (open, high, low, volume, number_of_trades)
        """
        interval_metric = interval + "_" + "close"
        return ticker, interval_metric
        
    def get_volume_in_usdt(self):
        volume_in_usdt = Decimal(self.event["Records"][0]["messageAttributes"]["close"]["stringValue"]) * Decimal(self.event["Records"][0]["messageAttributes"]["volume"]["stringValue"])
        return volume_in_usdt

    def insert(self):
        try:
            self.table.put_item(
                Item={"ticker": self.ticker, "interval_metric": self.interval_metric, "volume_in_usdt": self.volume_in_usdt},
                ConditionExpression="attribute_not_exists(ticker) AND attribute_not_exists(interval_metric)"
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up
            # other exceptions.
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

def lambda_handler(event, context):
    print("EVENT: ")
    print(event)
    metrics = Metrics()
    summary = Summary(event)
    metrics.insert(event)
    summary.insert()
