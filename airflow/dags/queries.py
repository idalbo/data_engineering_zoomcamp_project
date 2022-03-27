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


def insert_bq_table_from_external(dest_table, origin_table, date_col, dates):
    return"""
        delete from {0} where date({1}) = {2};

        insert into {0}
        select 
            *
        from 
            {3}
        where 
            date({1}) = {2}
    """.format(
        dest_table,
        date_col,
        "\'"+dates+"\'",
        origin_table
        )


def create_weekly_bq_table_prod_region():
    return """
        drop table if exists `astute-lyceum-338516.dtc_de_project_prod.dim_covid_region_weekly`; 

        create table `astute-lyceum-338516.dtc_de_project_prod.dim_covid_region_weekly` 
        partition by weeks
        cluster by region
        as
        select 
            date(date_trunc(data, week(monday)))    as weeks,
            denominazione_regione                   as region,
            sum(totale_casi)                        as total_cases,
            sum(ricoverati_con_sintomi)             as total_symptomatic,
            sum(terapia_intensiva)                  as total_icu,
            sum(totale_ospedalizzati)               as total_hospitalized,
            sum(totale_positivi)                    as total_positive,
            sum(variazione_totale_positivi)         as total_delta_positive,
            sum(nuovi_positivi)                     as total_new_positive,
            sum(deceduti)                           as total_deceased,
            sum(tamponi)                            as total_swabs,
            sum(casi_testati)                       as total_tested
        from 
            `astute-lyceum-338516.dtc_de_project_dev.covid_regional`
        group by 
            date(date_trunc(data, week(monday))), 
            denominazione_regione;
    """


def append_weekly_bq_table_prod_region(dates):
    return """
        delete from `astute-lyceum-338516.dtc_de_project_prod.dim_covid_region_weekly` where weeks = {0};

        insert into `astute-lyceum-338516.dtc_de_project_prod.dim_covid_region_weekly` 
        select 
            date(date_trunc(data, week(monday)))    as weeks,
            denominazione_regione                   as region,
            sum(totale_casi)                        as total_cases,
            sum(ricoverati_con_sintomi)             as total_symptomatic,
            sum(terapia_intensiva)                  as total_icu,
            sum(totale_ospedalizzati)               as total_hospitalized,
            sum(totale_positivi)                    as total_positive,
            sum(variazione_totale_positivi)         as total_delta_positive,
            sum(nuovi_positivi)                     as total_new_positive,
            sum(deceduti)                           as total_deceased,
            sum(tamponi)                            as total_swabs,
            sum(casi_testati)                       as total_tested
        from 
            `astute-lyceum-338516.dtc_de_project_dev.covid_regional`
        where 
            date(date_trunc(data, week(monday))) = {0}
        group by 
            date(date_trunc(data, week(monday))), 
            denominazione_regione;
    """.format("\'"+dates+"\'")


def create_weekly_bq_table_prod_province():
    return """
        drop table if exists `astute-lyceum-338516.dtc_de_project_prod.dim_covid_province_weekly`; 

        create table `astute-lyceum-338516.dtc_de_project_prod.dim_covid_province_weekly` 
        partition by weeks
        cluster by province
        as
        select 
            date(date_trunc(data, week(monday)))    as weeks,
            denominazione_provincia                 as province,
            sum(totale_casi)                        as total_cases,
            avg(totale_casi)                        as average_weekly_cases
        from 
            `astute-lyceum-338516.dtc_de_project_dev.covid_province`
        group by 
            date(date_trunc(data, week(monday))), 
            denominazione_provincia;
    """


def append_weekly_bq_table_prod_province(dates):
    return """
        delete from `astute-lyceum-338516.dtc_de_project_prod.dim_covid_province_weekly` where weeks = {0};

        insert into `astute-lyceum-338516.dtc_de_project_prod.dim_covid_province_weekly` 
        select 
            date(date_trunc(data, week(monday)))    as weeks,
            denominazione_provincia                 as province,
            sum(totale_casi)                        as total_cases,
            avg(totale_casi)                        as average_weekly_cases
        from 
            `astute-lyceum-338516.dtc_de_project_dev.covid_province`
        where 
            date(date_trunc(data, week(monday))) = {0}
        group by 
            date(date_trunc(data, week(monday))), 
            denominazione_provincia;
    """.format("\'"+dates+"\'")


def create_bq_table_fact_population():
    return """
        drop table if exists `astute-lyceum-338516.dtc_de_project_prod.fact_population`; 

        create table `astute-lyceum-338516.dtc_de_project_prod.fact_population` as
        select * from
        (
            select 
                denominazione_regione                                   as region,
                replace(replace(range_eta, "-", "_"), "+", "_")         as age_range,
                totale_generale                                         as total
            from 
                `astute-lyceum-338516.dtc_de_project_dev.population_overview`
        )
        pivot
        (
            sum(total)            as total
            for age_range in ('0_15', '16_19', '20_29', '30_39', '40_49', '50_59', '60_69', '70_79', '80_89', '90_')
        );
    """


def create_bq_table_moving_average_region_prod_task():
    return """
    drop table if exists `astute-lyceum-338516.dtc_de_project_prod.dim_covid_region_moving_average`; 

    create table `astute-lyceum-338516.dtc_de_project_prod.dim_covid_region_moving_average`
    partition by date 
    cluster by region
    as
    with italy_cases_region_daily AS (
    select
        date(data)              as date,
        denominazione_regione   as region,
        sum(nuovi_positivi)     as new_cases
    from
        `astute-lyceum-338516.dtc_de_project_dev.covid_regional` 
    group by
        date(data),
        denominazione_regione
    )
    select 
        *,
        avg(new_cases) over(partition by region order by unix_date(date) 
            range between 6 preceding and current row)                      as seven_days_ma
    from 
        italy_cases_region_daily;
    """