#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import sys
sys.path.append('/opt')

import boto3, requests, os, traceback, json, csv, uuid
from io import StringIO
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta, date
from time import sleep, time
from decimal import Decimal

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))
lastRequestId = None


# Helper class to convert a DynamoDB item to JSON.
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


class ProductsDataSync(object):
    def __init__(self, **params):
        self.dynamodb = boto3.resource('dynamodb')

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

    def setProducts(self, rows):
        try:
            # Retrieve value 'attribute_value_x' by 'attribute_name_x'.
            txFunct = lambda src: src.get(
                list(
                    filter(lambda key: key.find('attribute_name') != -1 and src[key]==src['code'], src.keys())
                ).pop().replace('name', 'value')
            ) if any((k.find('attribute_name') != -1 and v == src['code'] for k,v in src.items())) else None

            products = []
            for row in rows:
                product = {
                    'sku': row.pop('sku'),
                    'data': {}
                }
                for k,v in row.items():
                    if k.find('attribute_value') != -1:
                        continue
                    elif k.find('attribute_name') != -1:
                        product['data'][
                            v.lower().strip().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                        ] = txFunct({'code': v, **row})
                    else:
                        product['data'][k] = v
                product['data'] = dict(
                    (k, v) for k, v in product['data'].items() if v is not None
                )
                products.append(product)
            return products
        except Exception:
            log = traceback.format_exc()
            logger.exception(log)
            raise

    def setInventory(self, rows):
        try:
            productsExtData = []
            metaData = dict((k, list(set(map(lambda d: d[k], rows)))) for k in ['sku', 'warehouse'])
            for sku in metaData["sku"]:
                inventories = []
                for warehouse in metaData["warehouse"]:
                    _rows = list(filter(lambda t: (t["sku"]==sku and t["warehouse"]==warehouse), rows))
                    row = _rows.pop()
                    storeId = row.get("store_id", 0)
                    full = True if row.get("full") == "TRUE" else False

                    try:
                        qty = Decimal(row.get("qty"))
                    except Exception:
                        qty = Decimal(0)

                    inventory = {
                        "store_id": storeId,
                        "warehouse": warehouse,
                        "on_hand": qty if full else 0,
                        "past_on_hand": 0,
                        "qty": qty,
                        "full": full,
                        "in_stock": True if qty > 0 else False
                    }
                    inventories.append(inventory)
                productExtData = {'sku': sku, 'data': inventories}
                productsExtData.append(productExtData)
            return productsExtData
        except Exception:
            log = traceback.format_exc()
            logger.exception(log)
            raise

    def setImageGallery(self, rows):
        try:
            productsExtData = []
            metaData = dict((k, list(set(map(lambda d: d[k], rows)))) for k in ['sku'])
            for sku in metaData["sku"]:
                _rows = list(filter(lambda t: (t["sku"]==sku), rows))
                imageGallery = {
                    "media_gallery": [
                        dict(
                            (k, v) for k,v in row.items() if v is not None
                        ) for row in [
                            {
                                "value": row.get("value"),
                                "store_id": row.get("store_id", "0"),
                                "position": row.get("position", "1"),
                                "label": row.get("label"),
                                "media_source": row.get("media_source", 'CSV'),
                                "media_type": row.get("media_type", "image")
                            } for row in _rows
                        ]
                    ]
                }
                for imageType in ["image", "small_image", "thumbnail", "swatch_image"]:
                    rs = list(filter(lambda r: r["type"]==imageType, _rows))
                    if len(rs) > 0:
                        imageGallery[imageType] = rs[0]["value"]
                    else:
                        imageGallery[imageType] = imageGallery["media_gallery"][0]["value"]

                productExtData = {'sku': sku, 'data': imageGallery}
                productsExtData.append(productExtData)
            return productsExtData
        except Exception:
            log = traceback.format_exc()
            logger.exception(log)
            raise

    def setVariants(self, rows):
        try:
            productsExtData = []
            metaData = dict((k, list(set(map(lambda d: d[k], rows)))) for k in ['sku'])
            for sku in metaData["sku"]:
                data = {
                    "variants": []
                }
                _rows = list(filter(lambda t: (t["sku"]==sku), rows))
                
                while _rows:
                    row = _rows.pop()
                    sku = row.pop("sku")
                    storeId = row.pop("store_id", None)
                    variantVisibility = row.pop("variant_visibility", None)
                    variantSku = row.pop("variant_sku")
                    if storeId is not None and data.get("store_id") is None:
                        data["store_id"] = storeId
                    if variantVisibility is not None and data.get("variant_visibility") is None:
                        data["variant_visibility"] = True if variantVisibility == "TRUE" else False
                    data["variants"].append(
                        {
                            "variant_sku": variantSku,
                            "attributes": dict(
                                (key, value) for key, value in row.items() if value is not None and value != ""
                            )
                        }
                    )
                productExtData = {'sku': sku, 'data': data}
                productsExtData.append(productExtData)
            return productsExtData
        except Exception:
            log = traceback.format_exc()
            logger.exception(log)
            raise

    def setCustomOption(self, rows):
        try:
            productsExtData = []
            metaData = dict((k, list(set(map(lambda d: d[k], rows)))) for k in ['sku', 'title'])
            for sku in metaData["sku"]:
                options = []
                for title in metaData["title"]:
                    _rows = list(filter(lambda t: t.get("sku")==sku and t.get("title")==title, rows))
                    option = {}

                    while _rows:
                        row = _rows.pop()
                        title = row.pop("title", None)
                        titleAlt = row.pop("title_alt", None)
                        storeId = row.pop("store_id", "0")
                        _type = row.pop("type", None)
                        isRequire = row.pop("is_require", "0")
                        sortOrder = row.pop("sort_order", "1")
                        optionSku = row.pop("option_sku", None)
                        optionPrice = row.pop("option_price", None)
                        optionPriceType = row.pop("option_price_type", None)
                        if title is not None and option.get("title") is None:
                            option["title"] = title
                        if titleAlt is not None and option.get("title_alt") is None:
                            option["title_alt"] = titleAlt
                        if storeId is not None and option.get("store_id") is None:
                            option["store_id"] = storeId
                        if _type is not None and option.get("type") is None:
                            option["type"] = _type
                        if isRequire is not None and option.get("is_require") is None:
                            option["is_require"] = isRequire
                        if sortOrder is not None and option.get("sort_order") is None:
                            option["sort_order"] = sortOrder 
                        if optionSku is not None and option.get("option_sku") is None:
                            option["option_sku"] = optionSku
                        if optionPrice is not None and option.get("option_price") is None:
                            option["option_price"] = optionPrice
                        if optionPriceType is not None and option.get("option_price_type") is None:
                            option["option_price_type"] = optionPriceType
                        if option.get("option_values") is None:
                            option["option_values"] = []

                        option["option_values"].append(
                            {
                                "option_value_title": row.get("option_value_title"),
                                "option_value_title_alt": row.get("option_value_title_alt"),
                                "store_id": row.get("store_id", 0),
                                "option_value_sku": row.get("option_value_sku"),
                                "option_value_price": row.get("option_value_price"),
                                "option_value_price_type": row.get("option_value_price_type"),
                                "option_value_sort_order": row.get("option_value_sort_order", 1)
                            }
                        )
                    
                    if len(option.keys()):
                        option = dict(
                            (
                                k, [
                                    dict(
                                        (_k, str(_v)) for _k, _v in r.items() if _v is not None
                                    ) for r in v
                                ] if type(v) is list else str(v)
                            ) for k, v in option.items() if v is not None
                        )
                        options.append(option)
                productExtData = {'sku': sku, 'data': options}
                productsExtData.append(productExtData)
            return productsExtData
        except Exception:
            log = traceback.format_exc()
            logger.exception(log)
            raise

    def getItems(self, tableName, source):
        response = self.dynamodb.Table(tableName).query(
            IndexName="updated_at_index",
            KeyConditionExpression=Key('source').eq(source),
            ProjectionExpression="id,sku,#data",
            ExpressionAttributeNames={"#data": "data"}
        )
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = self.dynamodb.Table(tableName).query(
                IndexName="updated_at_index",
                KeyConditionExpression=Key('source').eq(source),
                ProjectionExpression="id,sku,#data",
                ExpressionAttributeNames={"#data": "data"},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])
        items = [item for item in sorted(items, key=lambda item: item["sku"])]
        return items

    def putItem(self, tableName, source=None, sku=None, data=None):
        table = self.dynamodb.Table(tableName)
        createdAt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        updatedAt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        _id = uuid.uuid1().int>>64

        response = table.query(
            IndexName="sku_index",
            KeyConditionExpression=Key("sku").eq(sku),
            FilterExpression=Attr('source').eq(source)
        )

        if response['Count'] != 0:
            item = response["Items"][0]
            createdAt = item["created_at"]
            _id = item["id"]

        entity = {
            "id": _id,
            "source": source,
            "sku": sku,
            "data": data,
            "created_at": createdAt,
            "updated_at": updatedAt,
            "tx_status": "N",
            "tx_note": "GOOGLESHEETS->STG"
        }
        try:
            table.put_item(Item=entity)
            return entity
        except Exception:
            entity["data"] = {}
            entity["tx_status"] = "F"
            entity["tx_note"] = traceback.format_exc()
            table.put_item(Item=entity)
            raise

    @classmethod
    def dataSync(cls, **params):
        productsDataSync = cls(**params)

        offset = int(params.get("offset", "0"))
        if offset != 0:
            sleep(int(params.get("time_interval", 0)))

        rows, total = productsDataSync.getDataSet(**params)
        items = productsDataSync.getItems(params.get("table_name"), params.get("source"))

        for row in rows:
            stimems = int(round(time()*1000))
            updateRequire = False
            # validate data
            txFunct = lambda x: json.loads(json.dumps(x, cls=JSONEncoder), parse_float=Decimal)
            logger.info("validate data for {sku}".format(sku=row["sku"]))
            _items = list(filter(lambda item: item["sku"] == row["sku"], items))
            if len(_items) == 0:
                updateRequire = True
            else:
                if params.get("data_type") == "inventory":
                    if len(row["data"]) != len(_items[0]["data"]):
                        updateRequire = True
                    else:
                        for i in row["data"]:
                            k = list(filter(lambda j: txFunct(i)["store_id"]==txFunct(j)["store_id"] and \
                                txFunct(i)["warehouse"]==txFunct(j)["warehouse"] and \
                                txFunct(i)["qty"]==txFunct(j)["qty"],
                                _items[0]["data"])
                            )
                            if len(k) == 0:
                                updateRequire = True
                                break
                elif params.get("data_type") in ("imagegallery", "variants"):
                    if txFunct(row["data"]) != txFunct(_items[0]["data"]):
                        updateRequire = True
                elif params.get("data_type") in ("customoption"):
                    if len(row["data"]) != len(_items[0]["data"]):
                        updateRequire = True
                    else:
                        for i in row["data"]:
                            k = list(filter(lambda j: txFunct(i)==txFunct(j), _items[0]["data"]))
                            if len(k) == 0:
                                updateRequire = True
                                break
                else:
                    if txFunct(row["data"]) != txFunct(_items[0]["data"]):
                        updateRequire = True
            
            if updateRequire:
                try:
                    # process data
                    logger.info("process data for {sku}".format(sku=row["sku"]))
                    entity = productsDataSync.putItem(
                        params.get("table_name"),
                        source=params.get("source"),
                        sku=row["sku"],
                        data=row["data"]
                    )
                    logger.info("Data: {data}".format(
                            data=json.dumps(entity.get("data"), indent=4, cls=JSONEncoder, ensure_ascii=False)
                        )
                    )
                except Exception:
                    log = traceback.format_exc()
                    logger.exception(log)

            offset += 1
            spendms = int(round(time()*1000)) - stimems
            yield offset, total, spendms

    def getDataFeedUrl(self, **params):
        googleSheetId = params.get("google_sheet_id")
        gid = params.get("gid")

        if googleSheetId is not None and gid is not None:
            dataFeedUrl = "https://docs.google.com/spreadsheets/d/{id}/export?format=csv&id={id}&gid={gid}".format(
                id=googleSheetId,
                gid=gid
            )
        else:
            dataFeedUrl = None

        assert dataFeedUrl, "Data Feed URL is None.  Please check the parameters."
        return dataFeedUrl

    def getDataSet(self, **params):
        dataFeedUrl = params.get("data_feed_url")
        if dataFeedUrl is None:
            dataFeedUrl = self.getDataFeedUrl(**params)
        logger.info("Data Feed URL: {datafeedurl}".format(datafeedurl=dataFeedUrl))

        s = requests.get(dataFeedUrl).content

        offset = int(params.get("offset", "0"))

        rows = []
        for row in csv.DictReader(StringIO(s.decode(params.get("decode")))):
            row = dict(
                (
                    k.lower().strip().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", ""), 
                    v.strip().split("|") if len(v.split("|")) > 1 else v.strip()
                ) for k, v in row.items() if k is not None and (v!="" and v is not None)
            )
            if "sku" in row.keys() and row["sku"] != "---":
                if params.get("data_type") == "inventory":
                    if "warehouse" not in row.keys():
                        row["warehouse"] = "admin"
                    if "stock" in row.keys():
                        row["qty"] = row.get("stock")
                    rows.append(row)
                elif params.get("data_type") == "imagegallery":
                    for k, v in row.items():
                        if k.find('image') == 0:
                            rows.append({
                                'sku': row['sku'],
                                'type': 'media_gallery',
                                'value': v
                            })
                else:
                    rows.append(row)

        if params.get("data_type") == 'products':
            rows = self.setProducts(rows)
        elif params.get("data_type") == 'inventory':
            rows = self.setInventory(rows)
        elif params.get("data_type") == 'imagegallery':
            rows = self.setImageGallery(rows)
        elif params.get("data_type") == 'variants':
            rows = self.setVariants(rows)
        elif params.get("data_type") == 'customoption':
            rows = self.setCustomOption(rows)

        return sorted(rows[offset:], key=lambda row: row["sku"]), len(rows)


def handler(event, context):
    # TODO implement
    global lastRequestId
    if lastRequestId == context.aws_request_id:
        return # abort
    else:
        lastRequestId = context.aws_request_id # keep request id for next invokation

    params = dict(event, **{
        "time_interval": int(os.environ["TIMEINTERVAL"])
    })
    
    try:
        maxspendms = 0
        logger.info("Allow MS/Loop: {allowms}/{loop}".format(
            allowms=context.get_remaining_time_in_millis(), 
            loop=int(params.get("loop", "0")))
        )
        nextLoop = lambda offset, total, maxspendms: \
            context.get_remaining_time_in_millis() - maxspendms*5 <= 0 and total - offset > 0
        for offset, total, spendms in ProductsDataSync.dataSync(**params):
            if total - offset <= 0:
                ProductsDataSync.invoke(os.environ["SYNCPRODUCTSDATAMAGE2TASKARN"], event)

            maxspendms = spendms if spendms > maxspendms else maxspendms
            if nextLoop(offset, total, maxspendms):
                loop = int(event.get("loop", "0")) + 1
                assert  loop <= 100, "Over the limit of loops."
                payload = dict(event, **{"offset": str(offset), "loop": str(loop)})
                logger.info("payload: {payload}".format(
                        payload=json.dumps(payload, indent=4, cls=JSONEncoder, ensure_ascii=False)
                    )
                )
                ProductsDataSync.invoke(context.invoked_function_arn, payload)
                break

        logger.info("Remain MS: {allowms}".format(allowms=context.get_remaining_time_in_millis()))
        logger.info("Offset/Total/Spend MS: {offset}/{total}/{spendms}".format(offset=offset, total=total, spendms=spendms))
    except Exception:
        log = traceback.format_exc()
        logger.exception(log)
        boto3.client("sns").publish(
            TopicArn=os.environ["SNSTOPICARN"],
            Subject=context.invoked_function_arn,
            MessageStructure="json",
            Message=json.dumps({"default": log})
        )
