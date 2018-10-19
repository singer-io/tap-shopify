#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-shopify",
    version="0.2.2",
    description="Singer.io tap for extracting Shopify data",
    author="Stitch",
    url="http://github.com/singer-io/tap-shopify",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_shopify"],
    install_requires=[
        "ShopifyAPI==3.1.0",
        "requests==2.19.1",
        "singer-python==5.2.2",
    ],
    extras_require={
        'dev': [
            'pylint',
            'ipdb',
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
