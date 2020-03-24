.DEFAULT_GOAL := test

test:
	pylint tap_shopify -d missing-docstring
	nosetests tests/unittests
