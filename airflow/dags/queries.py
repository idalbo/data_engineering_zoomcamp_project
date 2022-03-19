def create_bq_table_from_external(dataset, dest_table, origin_table, partition_col=None, cluster_col=None, partition=False, cluster=False):
    if partition==True and cluster==True:
        return f"CREATE OR REPLACE TABLE {dataset}.{dest_table} \
                PARTITION BY DATE({partition_col}) \
                CLUSTER BY {cluster_col} \
                AS \
                SELECT * FROM {dataset}.{origin_table};"
    elif partition==True and cluster==False:
        return f"CREATE OR REPLACE TABLE {dataset}.{dest_table} \
            PARTITION BY DATE({partition_col}) \
            AS \
            SELECT * FROM {dataset}.{origin_table};"
    elif partition==False and cluster==True:
        return f"CREATE OR REPLACE TABLE {dataset}.{dest_table} \
                CLUSTER BY {cluster_col} \
                AS \
                SELECT * FROM {dataset}.{origin_table};"
    else:
        return f"CREATE OR REPLACE TABLE {dataset}.{dest_table} \
            AS \
            SELECT * FROM {dataset}.{origin_table};"
