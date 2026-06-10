from src.protocols import DEFAULT_CONTRACTS_DIR


class DataContractNotFoundError(Exception):
    """
    Exception raised when client try to write to table without an associated data contract.
    """
    def __init__(self, table):
        self.message = f"Table {table} has no associated data contract! Try running `fw init <table>` and check contract directory: {DEFAULT_CONTRACTS_DIR}."
        super().__init__(self.message)


class DataQualityError(Exception):
    """
    Exception raised when dome data quality constraint was not met.
    """
    def __init__(self, rule):
        self.message = f"Rule not met: {rule}"
        super().__init__(self.message)
