# Google Sheets as PIM for Magento 2 on AWS

## Synopsis
These event-driven microservices that are built top on AWS SAM ([Serverless Application Model](https://aws.amazon.com/serverless/sam/)) with [Lambda](https://aws.amazon.com/lambda/) functions and [DynamoDB](https://aws.amazon.com/dynamodb/) tables to perform product data management as a PIM (Product Information Management) system with Google Sheets and Magento 2.  All of the application resources will be modeled and deployed by Cloudformation as a stack.  The stack can be as the trade service (product data) for contractors, dealers, distributors, and manufacturers.  The following steps describe the detail of the process.

### Step 1: Pull the product data from Google Sheets into a staging table ([DynamoDB](https://aws.amazon.com/dynamodb/)).
The [Lambda](https://aws.amazon.com/lambda/) function (**syncproductsdata_task**) pulls down the data in a google sheet and places it into a [DynamoDB](https://aws.amazon.com/dynamodb/) table.  Initially, each record will be marked as **N** (New) on column **tx_status** (transaction Status) with a brief log on column **tx_note** (transaction note).
![Pull the product data from Google Sheets into staging tables](/images/2019-12-24_21-28-00.png)

### Step 2: Send the product data from the staging table ([DynamoDB](https://aws.amazon.com/dynamodb/)) to Magento 2.
The 2nd [Lambda](https://aws.amazon.com/lambda/) function (**syncproductsdatamage2_task**) fetches the product data with **N** on column **tx_status** and pushes to Magento 2 to create new products or update products.  If a product is inserted or updated without any issue, the column **tx_status** will be changed to **S**; otherwise, **F** with the error log on column **tx_note**.
![Send the product data from the staging table to Magento 2](/images/2019-12-24_21-28-19.png)

## Prerequisites
The following prerequisites are required to deploy the [Cloudformation](https://aws.amazon.com/cloudformation/) stack.
1. OS: Linux.
2. Python: 3.7

## AWS Services Applied
* [Lambda](https://aws.amazon.com/lambda/).
* [DynamoDB](https://aws.amazon.com/dynamodb/).
* [CloudWatch](https://aws.amazon.com/cloudwatch/).
* [SNS](https://aws.amazon.com/sns/).
* [IAM](https://aws.amazon.com/iam/).
* [Cloudformation](https://aws.amazon.com/cloudformation/).

## Python Modules Applied in [Lambda Layers](https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html).
* [requests](https://github.com/psf/requests)
* [AWS-Mage2Connector](https://github.com/ideabosque/AWS-Mage2Connector)
* [pymysql](https://github.com/PyMySQL/PyMySQL)
* [sshtunnel](https://github.com/pahaz/sshtunnel)

## Cloudformation Stack Deployment

The stack is assembled with [Lambda](https://aws.amazon.com/lambda/) functions and [DynamoDB](https://aws.amazon.com/dynamodb/) tables by [Cloudformation](https://aws.amazon.com/cloudformation/) to perform the product data management with Google Sheets.

Utilize the following steps to deploy the stack.
1. Create a S3 bucket with the version enabled.
![Create a S3 bucket 1](/images/2019-12-25_18-02-48.jpg)
![Create a S3 bucket 2](/images/2019-12-25_18-04-58.jpg)
![Enable the version of the S3 bucket](/images/2019-12-25_18-06-47.jpg)
2. Pull down the project from git repository.
```bash
git clone https://github.com/ideabosque/googlesheets_pim_mage2_on_aws.git
```
3. Goto the folder './deployment' and set up virtual env with python 3.7.
```bash
cd ./deployment
virtualenv --python=python3.7 ./env
```
4. Activate the virtaul env and install the required modules.
```bash
cd ./deployment
source ./env/bin/activate
pip install -r requirements.txt
```
5. Configure './deployment/.env' by the reference of './deployment/.env.example'.
```
## Setting for the stack deployment
bucket=XXXXX                                                # The S3 bucket to store the zip packages. 
region_name=us-west-2                                       # The AWS region.
aws_access_key_id=XXXXXXXXXXXXXXXXXXXX                      # AWS ACCESS KEY ID.
aws_secret_access_key=XXXXXXXXXXXXXXXXXXX                   # AWS SECRET ACCESS KEY.
root_path=/opt/googlesheets_pim_mage2_on_aws                # The root path of the stack.
site_packages=env/lib/python3.7/site-packages               # The path of the python packages.
stack_name=GoogleSheetsasPIMMagento2onAWS                   # The stack name.
TIMEINTERVAL=0                                              # The time interval between each loop.
## Versions for Lambda functions and layers in S3 bucket (optional)
syncproductsdata_task_version=XXXXXXXXXXXXXXXXXXX           # The version of the syncproductsdata_task. 
syncproductsdatamage2_task_version=XXXXXXXXXXXXXXXXXXX      # The version of the syncproductsdatamage2_task.
googlesheets_pim_mage2_layer_version=XXXXXXXXXXXXXXXXXXX    # The version of the googlesheets_pim_mage2_layer.
```
6. Run 'cloudformation_stack.py' to deploy the stack.
```bash
cd ./deployment
source ./env/bin/activate
python cloudformation_stack
```

## Configuration and Scheduling

### Create a sheet in Google Sheets with the following template. 

sku | name | type_id | attribute_name_1 | attribute_name_2 | attribute_value_1 | attribute_value_2
--- | --- | --- | --- | --- | --- | ---
abc-111 | XXXXXXXXXX | simple | description | color | XXXXXXXXXX | White

* sku: Required for identity of each product.
* attribute_name_{x}: An attribute code such as 'description', 'color'.
* attribute_value_{x}: The value associated with attribute_name_{x}.
* Rest of columns will be treated as attributes.

### Share the sheet and get the id and gid.

1. Make the sheet shared for everyone by a link.
2. With the sheet url (https://docs.google.com/spreadsheets/d/{id}/edit#gid={gid}), you will be able to retrieve the **id** and **gid**.

### Event parameters.

* google_sheet_id: The google sheet id.
* gid: A gid of the google sheet.
* data_type: The data type of data sync.
* table_name: The staging table for the data type.
* source: The name of the source.
* decode: The decode of the data.
* mage2_setting: The Magento connection setting.
```
"SSHSERVER": "XXX.XXX.XXX.XXX",     # Remote Magento 2 server by IP or full domain address.
"SSHSERVERPORT": 22,                # SSH Port.
"SSHUSERNAME": "XXXXXXXXXX",        # SSH Username.
"SSHPKEY": "id_rsa",                # SSH Key (Either use SSHKEY or SSHPASSWORD).
"SSHPASSWORD": "XXXXXXXXXX",        # SSH Password (Either use SSHKEY or SSHPASSWORD).
"REMOTEBINDSERVER": "localhost",    # The MySQL server IP address.
"REMOTEBINDSERVERPORT": 3306,       # The MySQL server port.
"LOCALBINDSERVER": "0.0.0.0",       # Allow binding server.
"LOCALBINDSERVERPORT": 10022,       # Local binding port.
"MAGE2DBSERVER": "127.0.0.1",       # Using local binding IP address for the remote MySQL server.
"MAGE2DBUSERNAME": "root",          # MySQL username.
"MAGE2DBPASSWORD": "12345abc",      # MySQL password.
"MAGE2DB": "mage23ee",              # MySQL database.
"MAGE2DBPORT": 10022,               # Using local binding port for the remote MySQL server.
"VERSION": "EE"                     # Magento 2 version either EE or CE.
```
* txmap: The transaction mapping from the source data to the Magento product data.
```
{
    "description": {                # The Magento attribute code.
      "key": "long_description",    # The key to retrieve the value from the source.
      "default": null               # The default value if there is no value in the source.
    },
    ......
}
```

### Example of the test event.
```
{
  "google_sheet_id": "XXXXXXXXXXXXXXXXXXX",
  "gid": "XXXXXXXXXX",
  "data_type": "products",
  "table_name": "stg_products",
  "source": "TradeSrv",
  "decode": "utf-8",
  "mage2_setting": {
    "SSHSERVER": "0.tcp.ngrok.io",
    "SSHSERVERPORT": 11003,
    "SSHUSERNAME": "magento_user",
    "SSHPKEY": "id_rsa",
    "REMOTEBINDSERVER": "localhost",
    "REMOTEBINDSERVERPORT": 3306,
    "LOCALBINDSERVER": "0.0.0.0",
    "LOCALBINDSERVERPORT": 10022,
    "MAGE2DBSERVER": "127.0.0.1",
    "MAGE2DBUSERNAME": "root",
    "MAGE2DBPASSWORD": "12345abc",
    "MAGE2DB": "mage23ee",
    "MAGE2DBPORT": 10022,
    "VERSION": "EE"
  },
  "txmap": {
    "short_description": {
      "key": "short_description",
      "default": null
    },
    "color": {
      "key": "color",
      "default": null
    },
    "description": {
      "key": "long_description",
      "default": null
    },
    "price": {
      "key": "price",
      "default": "0"
    },
    "msrp": {
      "key": "msrp",
      "default": "0"
    },
    "name": {
      "key": "product_name",
      "default": null
    },
    "status": {
      "key": "status",
      "default": "1"
    }
  }
}
```
### Configure test events.

* Configure test event for the AWS [Lambda](https://aws.amazon.com/lambda/) function **syncproductsdata_task**.

![Configure test event 0](/images/2019-12-25_20-52-35.jpg)
![Configure test event 1](/images/2019-12-25_18-59-52.jpg)
![Configure test event 2](/images/2019-12-25_19-02-23.jpg)

* Test event.

![Configure test event 3](/images/2019-12-25_19-29-34.jpg)

### Schedule the event.

We could apply a rule in [CloudWatch](https://aws.amazon.com/cloudwatch/) to schedule an event to load the data from the Google Sheet to Magento 2 periodically.

* Create a rule in [CloudWatch](https://aws.amazon.com/cloudwatch/).

![Schedule the event 1](/images/2019-12-29_17-43-57.png)

* Schedule a rule with the event.

1. Create rule. 
![Schedule the event 2](/images/2019-12-29_17-45-37.png)
2. Configure rule details.
![Schedule the event 3](/images/2019-12-29_17-54-36.png)

## Run command directly
If you would like to skip the staging table to publish product data from google sheets to Magento 2 directly for troubleshooting, you could use the script **./command/products_data_sync_mage2_from_googlesheets.py** in the package.  The following steps describe how to configure the parameters for the script.

1. Configure './command/.env' by the reference of './command/.env.example'.
```
DECODE=utf-8                        # Decode for data importing.
ATTRIBUTESET=Default                # Magento 2 attribute set.
GOOGLESHEETID=XXXXXXXXXXXXXXXX      # Google sheet id.
GID=XXXXXXXX                        # Grid id of the Google sheet.
SSHSERVER=XXX.XXX.XXX.XXX           # Remote Magento 2 server by IP or full domain address.
SSHSERVERPORT=22                    # SSH Port.
SSHUSERNAME=XXXXXXXXXX              # SSH Username.
SSHPKEY=id_rsa                      # SSH Key (Either use SSHKEY or SSHPASSWORD).
SSHPASSWORD=XXXXXXXXXX              # SSH Password (Either use SSHKEY or SSHPASSWORD).
REMOTEBINDSERVER=localhost          # The MySQL server IP address.
REMOTEBINDSERVERPORT=3306           # The MySQL server port.
LOCALBINDSERVER=0.0.0.0             # Allow binding server.
LOCALBINDSERVERPORT=10022           # Local binding port.
MAGE2DBSERVER=127.0.0.1             # Using local binding IP address for the remote MySQL server.
MAGE2DBUSERNAME=root                # MySQL username.
MAGE2DBPASSWORD=12345abc            # MySQL password.
MAGE2DB=magento232                  # MySQL database.
MAGE2DBPORT=10022                   # Using local binding port for the remote MySQL server.
VERSION=EE                          # Magento 2 version either EE or CE.
```
2. Configure './command/txmap.py' that is the transaction mapping from the source data to the Magento product data.
```
txmap = {
    "description": {                # The Magento attribute code.
      "key": "long_description",    # The key to retrieve the value from the source.
      "default": null               # The default value if there is no value in the source.
    },
    ......
}
```
3. Run the **./command/products_data_sync_mage2_from_googlesheets.py** to load the products data.
```bash
python ./command/products_data_sync_mage2_from_googlesheets.py
```
