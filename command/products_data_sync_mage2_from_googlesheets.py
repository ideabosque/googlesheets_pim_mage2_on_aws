import requests, csv, sys, traceback, json, os, dotenv
from io import StringIO
from datetime import datetime, date
from decimal import Decimal
from aws_mage2connector import Mage2Connector
from sshtunnel import SSHTunnelForwarder
from time import sleep
from txmap import txmap

import logging
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("products_data_sync.log"),
        logging.StreamHandler(sys.stdout)
    ]
)   
logger = logging.getLogger()


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


# Mage2 From Google Sheet
class ProductsDataSync(object):
    def __init__(self, **params):
        self.mage2Connector = Mage2Connector(
            setting=params.get("mage2_setting"), 
            logger=logger
        )

    def getRows(self, googleSheetId, gid, decode):
        dataFeedUrl = "https://docs.google.com/spreadsheets/d/{id}/export?format=csv&id={id}&gid={gid}".format(
            id=googleSheetId,
            gid=gid
        )

        s = requests.get(dataFeedUrl).content

        rows = []
        for row in csv.DictReader(StringIO(s.decode(decode))):
            row = dict(
                (
                    k.lower().strip().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", ""), 
                    v.strip().split("|") if len(v.split("|")) > 1 else v.strip()
                ) for k, v in row.items() if k is not None and (v!="" and v is not None)
            )
            if "sku" in row.keys() and row["sku"] != "---":
                row = {
                    'sku': row.pop('sku'),
                    'data': row
                }
                rows.append(row)
        return rows

    def syncProduct(self, mage2Setting, attributeSet, products):
        with SSHTunnelForwarder(
            (mage2Setting['SSHSERVER'], mage2Setting['SSHSERVERPORT']),
            ssh_username=mage2Setting['SSHUSERNAME'],
            ssh_pkey=mage2Setting.get('SSHPKEY'),
            ssh_password=mage2Setting.get('SSHPASSWORD'),
            remote_bind_address=(mage2Setting['REMOTEBINDSERVER'], mage2Setting['REMOTEBINDSERVERPORT']),
            local_bind_address=(mage2Setting['LOCALBINDSERVER'], mage2Setting['LOCALBINDSERVERPORT'])
        ) as server:
            sleep(2)
            for product in products:
                sku = product["sku"]
                typeId = product["data"].pop("type_id", "simple")
                storeId = product["data"].pop("store_id", "0")
                product["data"] = dict (
                    (k, v) for k, v in dict(
                        (
                            k,
                            product["data"].get(v.get("key")) if product["data"].get(v.get("key")) is not None else v.get("default")
                        ) for k, v in txmap.items()
                    ).items() if v is not None
                )
                try:
                    product['product_id'] = self.mage2Connector.syncProduct(sku, attributeSet, product["data"], typeId, storeId)
                    product['sync_status'] = 'S'
                except Exception:
                    product['sync_status'] = 'F'
                    product['log'] = traceback.format_exc()
                
                logger.info(json.dumps(
                        product, indent=4, cls=JSONEncoder, ensure_ascii=False
                    )
                )
            del self.mage2Connector
            server.stop()
            server.close()

    @classmethod
    def dataSync(cls, **params):
        productsDataSync = cls(**params)
        mage2Setting = params.get('mage2_setting')
        googleSheetId = params.get('google_sheet_id')
        gid = params.get('gid')
        decode = params.get('decode')
        attributeSet = params.get('attribute_set')

        # Retrieve value 'attribute_value_x' by 'attribute_name_x'.
        txFunct = lambda src: src.get(
            list(
                filter(lambda key: key.find('attribute_name') != -1 and src[key]==src['code'], src.keys())
            ).pop().replace('name', 'value')
        ) if any((k.find('attribute_name') != -1 and v == src['code'] for k,v in src.items())) else None

        products = []
        for row in productsDataSync.getRows(googleSheetId, gid, decode):
            product = {
                'sku': row['sku'],
                'data': {}
            }
            for k,v in row['data'].items():
                if k.find('attribute') != -1:
                    product['data'][
                        v.lower().strip().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                    ] = txFunct({'code': v, **row['data']})
                else:
                    product['data'][k] = v
            products.append(product)

        productsDataSync.syncProduct(mage2Setting, attributeSet, products)


if __name__ == '__main__':
    # Look for a .env file
    if os.path.exists('.env'):
        dotenv.load_dotenv('.env')

    ## Parameters
    params = {
        'decode': os.getenv("DECODE"),
        'google_sheet_id': os.getenv("GOOGLESHEETID"),
        'gid': os.getenv("GID"),
        'attribute_set': os.getenv("ATTRIBUTESET"),
        'mage2_setting': {
            "SSHSERVER": os.getenv("SSHSERVER"),
            "SSHSERVERPORT": int(os.getenv("SSHSERVERPORT")),
            "SSHUSERNAME": os.getenv("SSHUSERNAME"),
            "SSHPKEY": os.getenv("SSHPKEY"),
            "SSHPASSWORD": os.getenv("SSHPASSWORD"),
            "REMOTEBINDSERVER": os.getenv("REMOTEBINDSERVER"),
            "REMOTEBINDSERVERPORT": int(os.getenv("REMOTEBINDSERVERPORT")),
            "LOCALBINDSERVER": os.getenv("LOCALBINDSERVER"),
            "LOCALBINDSERVERPORT": int(os.getenv("LOCALBINDSERVERPORT")),
            "MAGE2DBSERVER": os.getenv("MAGE2DBSERVER"),
            "MAGE2DBUSERNAME": os.getenv("MAGE2DBUSERNAME"),
            "MAGE2DBPASSWORD": os.getenv("MAGE2DBPASSWORD"),
            "MAGE2DB": os.getenv("MAGE2DB"),
            "MAGE2DBPORT": int(os.getenv("MAGE2DBPORT")),
            "VERSION": os.getenv("VERSION")
        }
    }
    
    ProductsDataSync.dataSync(**params)