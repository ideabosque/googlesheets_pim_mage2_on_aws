#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import sys
sys.path.append('/opt')

import boto3, os, traceback, json
from boto3.dynamodb.conditions import Key, Attr
from aws_mage2connector import Mage2Connector
from sshtunnel import SSHTunnelForwarder
from time import sleep, time
from decimal import Decimal
from datetime import datetime, date

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))
lastRequestId = None


# Helper class to convert an entity to JSON.
class JSONEncoder(json.JSONEncoder):
    def default(self, o):   # pylint: disable=E0202
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        elif isinstance(o, (datetime, date)):
            return o.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(o, (bytes, bytearray)):
            return str(o)
        else:
            return super(JSONEncoder, self).default(o)


class ProductsDataSyncMage2(object):
    def __init__(self, **params):
        self.dynamodb = boto3.resource('dynamodb')
        self.mage2Connector = Mage2Connector(
            setting=params.get("mage2_setting"), logger=logger
        )

    @classmethod
    def invoke(cls, functionName, payload, invocationType="Event"):
        response = boto3.client('lambda').invoke(
            FunctionName=functionName,
            InvocationType=invocationType,
            Payload=json.dumps(payload),
        )
        if "FunctionError" in response.keys():
            log = json.loads(response['Payload'].read())
            logger.exception(log)
            raise Exception(log)
        if invocationType == "RequestResponse":
            return json.loads(response['Payload'].read())

    def getItems(self, tableName, source):
        response = self.dynamodb.Table(tableName).query(
            IndexName="updated_at_index",
            KeyConditionExpression=Key('source').eq(source),
            FilterExpression=Attr('tx_status').eq('N'),
            ProjectionExpression="id,sku,#data",
            ExpressionAttributeNames={"#data": "data"}
        )
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = self.dynamodb.Table(tableName).query(
                IndexName="updated_at_index",
                KeyConditionExpression=Key('source').eq(source),
                FilterExpression=Attr('tx_status').eq('N'),
                ProjectionExpression="id,sku,#data",
                ExpressionAttributeNames={"#data": "data"},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])
        items = [item for item in sorted(items, key=lambda item: item["sku"])]
        return items

    def updateItem(self, tableName, productData):
        response = self.dynamodb.Table(tableName).update_item(
            Key={
                'id': productData['id']
            },
            UpdateExpression="set updated_at=:val0, tx_status=:val1, tx_note=:val2, product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productData['tx_status'],
                ':val2': productData['tx_note'],
                ':val3': productData['product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return response

    @classmethod
    def dataSync(cls, **params):
        productsDataSync = cls(**params)
        dataType = params.get("data_type")
        dataType = dataType.strip(
            "products-") if dataType != "products" else dataType
        productsData = productsDataSync.getItems(
            params.get("table_name"),
            params.get("source")
        )
        offset = 0
        for productData in productsData:
            try:
                stimems = int(round(time()*1000))
                try:    
                    if dataType == "products":
                        sku = productData["sku"]
                        typeId = productData["data"].pop("type_id", "simple")
                        storeId = productData["data"].pop("store_id", "0")
                        attributeSet = params.get("attribute_set", "Default")
                        txmap = params.get("txmap")
                        data = dict (
                            (k, v) for k, v in dict(
                                (
                                    k,
                                    productData["data"].get(v.get("key")) if productData["data"].get(v.get("key")) is not None else v.get("default")
                                ) for k, v in txmap.items()
                            ).items() if v is not None
                        )
                        productData['product_id'] = productsDataSync.mage2Connector.syncProduct(sku, attributeSet, data, typeId, storeId)
                    else:
                        sku = productData["sku"]
                        data = productData["data"]
                        productData['product_id'] = productsDataSync.mage2Connector.syncProductExtData(sku, dataType, data)
                    # productData['product_id'] = '####'
                    productData['tx_status'] = 'S'
                    productData['tx_note'] = 'STG->MAGE2'
                    logger.info(json.dumps(productData, indent=4, cls=JSONEncoder, ensure_ascii=False))
                except Exception:
                    productData['product_id'] = '####'
                    productData['tx_status'] = 'F'
                    productData['tx_note'] = traceback.format_exc()
                productsDataSync.updateItem(params.get("table_name"), productData)
                logger.info(json.dumps(
                        productData, indent=4, cls=JSONEncoder, ensure_ascii=False
                    )
                )
            
                offset += 1
                spendms = int(round(time()*1000)) - stimems
                yield offset, len(productsData), spendms
            except Exception:
                del productsDataSync.mage2Connector
                break

def handler(event, context):
    # TODO implement
    global lastRequestId
    if lastRequestId == context.aws_request_id:
        return # abort
    else:
        lastRequestId = context.aws_request_id # keep request id for next invokation
    
    mage2Setting = event.get("mage2_setting")
    with SSHTunnelForwarder(
        (mage2Setting['SSHSERVER'], mage2Setting['SSHSERVERPORT']),
        ssh_username=mage2Setting['SSHUSERNAME'],
        ssh_pkey=mage2Setting.get('SSHPKEY'),
        ssh_password=mage2Setting.get('SSHPASSWORD'),
        remote_bind_address=(
            mage2Setting['REMOTEBINDSERVER'], mage2Setting['REMOTEBINDSERVERPORT']
        ),
        local_bind_address=(
            mage2Setting['LOCALBINDSERVER'], mage2Setting['LOCALBINDSERVERPORT']
        )
    ) as server:
        try:
            maxspendms = 0
            logger.info("Allow MS/Loop: {allowms}/{loop}".format(
                allowms=context.get_remaining_time_in_millis(), 
                loop=int(event.get("loop", "0")))
            )
            nextLoop = lambda offset, total, maxspendms: \
                context.get_remaining_time_in_millis() - maxspendms*5 <= 0 and total - offset > 0
            
            dataSync = ProductsDataSyncMage2.dataSync(**event)
            for offset, total, spendms in dataSync:
                maxspendms = spendms if spendms > maxspendms else maxspendms
                if nextLoop(offset, total, maxspendms):
                    loop = int(event.get("loop", "0")) + 1
                    assert  loop <= 100, "Over the limit of loops."
                    payload = dict(event, **{"loop": str(loop)})
                    logger.info("payload: {payload}".format(
                            payload=json.dumps(payload, indent=4, cls=JSONEncoder, ensure_ascii=False)
                        )
                    )
                    ProductsDataSyncMage2.invoke(context.invoked_function_arn, payload)
                    dataSync.throw(Exception("Database connection is closed."))
                    break
        except Exception:
            log = traceback.format_exc()
            logger.exception(log)
            boto3.client("sns").publish(
                TopicArn=os.environ["SNSTOPICARN"],
                Subject=context.invoked_function_arn,
                MessageStructure="json",
                Message=json.dumps({"default": log})
            )
        server.stop()
        server.close()