class ShopifyError(Exception):
    def __init__(self, error, msg=''):
        super().__init__('{}: {}'.format(error.__class__.__name__, msg))
