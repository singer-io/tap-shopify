#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-shopify",
<<<<<<< HEAD
    version="1.2.6.7",
=======
    version="1.2.7",
>>>>>>> c7579c5 (Bump to v1.2.7 and update changelog (#85))
    description="Singer.io tap for extracting Shopify data",
    author="Stitch",
    url="http://github.com/singer-io/tap-shopify",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_shopify"],
    install_requires=[
        "ShopifyAPI==9.0.0",
        "singer-python==5.12.1",
    ],
    extras_require={
        'dev': [
            'pylint',
            'ipdb',
            'requests==2.20.0',
            'nose',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-shopify=tap_shopify:main
    """,
    packages=["tap_shopify"],
    package_data = {
        "schemas": ["tap_shopify/schemas/*.json"]
    },
    include_package_data=True,
)
