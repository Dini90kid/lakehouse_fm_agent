
class Context:
    """Execution context placeholder.
    In Databricks, this would hold the SparkSession, current_df, config accessors,
    read_uc/write/merge helpers, and logging.
    """
    def __init__(self, spark=None):
        self.spark = spark
        self.current_df = None
