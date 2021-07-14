.DEFAULT_GOAL := test

test:
	pylint tap_shopify -d missing-docstring,too-many-branches,duplicate-code
	nosetests tests/unittests
