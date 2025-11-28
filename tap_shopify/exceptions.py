class ShopifyError(Exception):
    def __init__(self, error, msg=''):
        super().__init__('{}\n{}'.format(error.__class__.__name__, msg))

class ShopifyAPIError(Exception):
    """Raised for any unexpected api error without a valid status code"""

class BulkOperationInProgressError(Exception):
    """Raised when a bulk operation is already in progress"""
    def __init__(self, message, bulk_op_id=None):
        super().__init__(message)
        self.bulk_op_id = bulk_op_id
