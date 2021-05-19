.DEFAULT_GOAL := test

test:
	pylint tap_shopify -d missing-docstring,too-many-branches
	nosetests tests/unittests
