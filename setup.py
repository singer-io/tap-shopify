#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-shopify",
    version="1.10.0",
    description="Singer.io tap for extracting Shopify data",
    author="Stitch",
    url="http://github.com/singer-io/tap-shopify",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    python_requires='>=3.5.2',
    py_modules=["tap_shopify"],
    install_requires=[
        "ShopifyAPI==12.4.0",
        "singer-python==6.0.0",
    ],
    extras_require={
        'dev': [
            'pylint==3.0.3',
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
