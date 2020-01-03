lambdaConfig = {
    "functions": {
        "syncproductsdata_task": {
            "update": True,
            "base": "/taskqueue/syncproductsdata",
            "packages": [],
            "package_files": [],
            "files": {}
        },
        "syncproductsdatamage2_task": {
            "update": True,
            "base": "/taskqueue/syncproductsdatamage2",
            "packages": [],
            "package_files": [],
            "files": {}
        }
    },
    "layers": {
        "googlesheets_pim_mage2_layer": {
            "update": True,
            "packages": [
                "requests",
                "urllib3",
                "chardet",
                "certifi",
                "idna",
                "aws_mage2connector",
                "pymysql",
                "asn1crypto",
                "bcrypt",
                "cffi",
                "cryptography",
                "nacl",
                "paramiko",
                "pyasn1",
                "pycparser",
                ".libs_cffi_backend"
            ],
            "package_files": [
                "six.py",
                "sshtunnel.py",
                "_cffi_backend.cpython-37m-x86_64-linux-gnu.so"
            ],
            "files": {}
        }
    }
}
