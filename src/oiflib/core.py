def are_dfs_equal(df1, df2):
    """Returns true if the schema and data of two dataframes are equal"""
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True
