#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-shopify",
    version="1.1.17",
    description="Singer.io tap for extracting Shopify data",
    author="Stitch",
    url="http://github.com/singer-io/tap-shopify",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_shopify"],
    install_requires=[
        "ShopifyAPI==3.1.0",
        "singer-python==5.4.1",
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
