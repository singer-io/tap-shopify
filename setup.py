#!/usr/bin/env python
from setuptools import setup
from setuptools import find_packages

setup(
    name="tap-shopify",
    version="3.0.0",
    description="Singer.io tap for extracting Shopify data",
    author="Stitch",
    url="http://github.com/singer-io/tap-shopify",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    python_requires='>=3.5.2',
    py_modules=["tap_shopify"],
    install_requires=[
        # Important: review the monkey-patched method in the GraphQL client when upgrading this dependency.
        "ShopifyAPI==12.7.0",
        "singer-python==6.1.1",
    ],
    extras_require={
        'dev': [
            'pylint==3.3.6',
            'ipdb',
            'requests==2.32.3',
            'nose',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-shopify=tap_shopify:main
    """,
    packages=find_packages(),
    package_data = {
        "schemas": ["tap_shopify/schemas/*.json"]
    },
    include_package_data=True,
)
