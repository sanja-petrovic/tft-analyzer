class NotFoundError(Exception):
    """Exception raised when a resource is not found."""

    def __init__(self, message="Not found"):
        self.message = message
        super().__init__(self.message)
