import boto3, json, sys, os, dotenv, zipfile
from datetime import datetime, date
from decimal import Decimal
from time import sleep
from lambda_config import lambdaConfig


# Look for a .env file
if os.path.exists('.env'):
    dotenv.load_dotenv('.env')


import logging
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        # logging.FileHandler("cloudformation_stack.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()


# Helper class to convert a DynamoDB item to JSON.
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
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


class CloudformationStack(object):
    def __init__(self):
        self.awsCloudformation = boto3.client(
            'cloudformation',
            region_name=os.getenv("region_name"),
            aws_access_key_id=os.getenv('aws_access_key_id'),
            aws_secret_access_key=os.getenv('aws_secret_access_key')
        )
        self.awsS3 = boto3.resource(
            's3',
            aws_access_key_id=os.getenv('aws_access_key_id'),
            aws_secret_access_key=os.getenv('aws_secret_access_key')
        )

    @staticmethod
    def zip_dir(dirpath, fzip, isPackage=True):
        basedir = os.path.dirname(dirpath) + '/'
        for root, dirs, files in os.walk(dirpath):
            # if os.path.basename(root)[0] == '.':
                # continue  # skip hidden directories
            dirname = root.replace(basedir, '')
            for f in files:
                # if f[-1] == '~' or (f[0] == '.' and f != '.htaccess'):
                    # skip backup files and all hidden files except .htaccess
                    # continue
                if not isPackage:
                    dirname = ''
                fzip.write(root + '/' + f, dirname + '/' + f)

    def packAWSLambda(self, lambdaFile, base, packages, packageFiles=[], files={}):
        fzip = zipfile.ZipFile(lambdaFile, 'w', zipfile.ZIP_DEFLATED)
        base = "{root_path}/{base}".format(
            root_path=os.getenv('root_path'),
            base=base
        )
        self.zip_dir(base, fzip, isPackage=False)
        for package in packages:
            self.zip_dir(
                "{site_packages}/{package}".format(
                    site_packages=os.getenv('site_packages'),
                    package=package
                ),
                fzip
            )
        for f in packageFiles:
            fzip.write(
                "{root_path}/deployment/{site_packages}/{file}".format(
                    root_path=os.getenv('root_path'),
                    site_packages=os.getenv('site_packages'),
                    file=f
                ),
                f
            )
        for f, path in files.items():
            fzip.write(
                "{path}/{file}".format(path=path, file=f), f
            )
        fzip.close()

    def packAWSLambdaLayer(self, layerFile, packages, packageFiles=[], files={}):
        fzip = zipfile.ZipFile(layerFile, 'w', zipfile.ZIP_DEFLATED)
        for package in packages:
            self.zip_dir(
                "{site_packages}/{package}".format(
                    site_packages=os.getenv('site_packages'),
                    package=package
                ),
                fzip
            )
        for f in packageFiles:
            fzip.write(
                "{root_path}/deployment/{site_packages}/{file}".format(
                    root_path=os.getenv('root_path'),
                    site_packages=os.getenv('site_packages'),
                    file=f
                ),
                f
            )
        for f, path in files.items():
            fzip.write(
                "{path}/{file}".format(path=path, file=f), f
            )
        fzip.close()

    def uploadAWSS3Bucket(self, lambdaFile, bucket):
        f = open(lambdaFile, 'rb')
        self.awsS3.Bucket(bucket).put_object(Key=lambdaFile, Body=f)

    # Check if the stack exists.
    def _stackExists(self, stackName):
        stacks = self.awsCloudformation.list_stacks()['StackSummaries']
        for stack in stacks:
            if stack['StackStatus'] == 'DELETE_COMPLETE':
                continue
            if stackName == stack['StackName']:
                return True
        return False

    # Retrieve the last version of the object in a S3 bucket.
    def _getObjectLastVersion(self, s3Key):
        objectSummary = self.awsS3.ObjectSummary(os.getenv('bucket'), s3Key)
        return objectSummary.get()['VersionId']

    @classmethod
    def deploy(cls):
        cf = cls()
        # Package and upload the code.
        for name, funct in lambdaConfig["functions"].items():
            if funct["update"]:
                lambdaFile = "{function_name}.zip".format(function_name=name)
                cf.packAWSLambda(
                    lambdaFile,
                    funct["base"],
                    funct["packages"],
                    packageFiles=funct["package_files"],
                    files=funct["files"]
                )
                cf.uploadAWSS3Bucket(lambdaFile, os.getenv("bucket"))
                logger.info("Upload the lambda package.")

        for name, layer in lambdaConfig["layers"].items():
            if layer["update"]:
                layerFile = "{layer_name}.zip".format(layer_name=name)
                cf.packAWSLambdaLayer(
                    layerFile,
                    layer["packages"],
                    packageFiles=layer["package_files"],
                    files=layer["files"]
                )
                cf.uploadAWSS3Bucket(layerFile, os.getenv("bucket"))
                logger.info("Upload the lambda layer package.")

        # Update the cloudformation stack.
        stackName = os.getenv("stack_name")
        template = open("{stack_name}.json".format(stack_name=stackName), "r")
        template = json.load(template)

        for key, value in template["Resources"].items():
            if value["Type"] == "AWS::Lambda::Function":
                functionName = value["Properties"]["FunctionName"]
                functionFile = "{function_name}.zip".format(function_name=functionName)
                functionVersion = "{function_name}_version".format(function_name=functionName)
                template["Resources"][key]["Properties"]["Code"] = {
                    "S3Bucket": os.getenv("bucket"),
                    "S3ObjectVersion": os.getenv(functionVersion, cf._getObjectLastVersion(functionFile)),
                    "S3Key": functionFile
                }
                template["Resources"][key]["Properties"]["Environment"]["Variables"] = dict(
                    (
                        k,
                        os.getenv(k, v)
                    ) for k, v in template["Resources"][key]["Properties"]["Environment"]["Variables"].items()
                )
            elif value["Type"] == "AWS::Lambda::LayerVersion":
                layerName = value["Properties"]["LayerName"]
                layerFile = "{layer_name}.zip".format(layer_name=layerName)
                layerVersion = "{layer_name}_version".format(layer_name=layerName)
                template["Resources"][key]["Properties"]["Content"] = {
                    "S3Bucket": os.getenv("bucket"),
                    "S3ObjectVersion": os.getenv(layerVersion, cf._getObjectLastVersion(layerFile)),
                    "S3Key": layerFile
                }

        params = {
            "StackName": stackName,
            "TemplateBody": json.dumps(template, indent=4),
            "Capabilities": ['CAPABILITY_NAMED_IAM'],
            "Tags": [
                {
                    "Key": 'autostack',
                    "Value": 'true'
                }
            ],
            "Parameters": []
        }
        if cf._stackExists(stackName):
            response = cf.awsCloudformation.update_stack(**params)
        else:
            response = cf.awsCloudformation.create_stack(**params)
        logger.info(
            json.dumps(response, indent=4, cls=JSONEncoder, ensure_ascii=False)
        )

        stack = cf.awsCloudformation.describe_stacks(StackName=stackName)["Stacks"][0]
        while (stack["StackStatus"].find("IN_PROGRESS") != -1):
            logger.info(
                json.dumps(stack["StackStatus"], indent=4, cls=JSONEncoder, ensure_ascii=False)
            )
            sleep(5)
            stack = cf.awsCloudformation.describe_stacks(
                StackName=stackName
            )["Stacks"][0]

        if stack["StackStatus"] == "CREATE_COMPLETE":
            logger.info(
                json.dumps(stack["StackStatus"], indent=4, cls=JSONEncoder, ensure_ascii=False)
            )
        else:
            logger.info(
                json.dumps(stack["StackStatus"], indent=4, cls=JSONEncoder, ensure_ascii=False)
            )


if __name__ == "__main__":
    CloudformationStack.deploy()

