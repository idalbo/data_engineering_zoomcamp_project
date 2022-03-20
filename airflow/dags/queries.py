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


def create_weekly_bq_table_prod_province():
    return """
        drop table if exists `astute-lyceum-338516.dtc_de_project_prod.covid_province_weekly`; 

        create table `astute-lyceum-338516.dtc_de_project_prod.covid_province_weekly` 
        partition by weeks
        cluster by province
        as
        select 
            date(date_trunc(data, week(monday)))    as weeks,
            denominazione_provincia         as province,
            sum(totale_casi)                as total_cases
        from 
            `astute-lyceum-338516.dtc_de_project_dev.covid_province`
        group by 
            date(date_trunc(data, week(monday))), 
            denominazione_provincia;
    """
