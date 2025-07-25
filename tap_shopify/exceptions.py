class ShopifyError(Exception):
    def __init__(self, error, msg=''):
        super().__init__('{}\n{}'.format(error.__class__.__name__, msg))

class ShopifyAPIError(Exception):
    """Raised for any unexpected api error without a valid status code"""
