import polars as pl
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import OrdinalEncoder
import os
import shutil

def impute(df: pl.LazyFrame, target_columns=['category_code', 'brand']):
    df_imp = df

    df_imp = df_imp.with_columns(
        pl.col('event_time').str.strip_suffix(" UTC").str.to_datetime().alias('event_time_dt')
    ).with_columns([
        pl.col('price').log1p().alias('log_price'),
        pl.col('event_time_dt').dt.day().alias('day'),
        pl.col('event_time_dt').dt.month().alias('month'),
        pl.col('event_time_dt').dt.year().alias('year'),
        pl.col('event_time_dt').dt.weekday().alias('weekday'),
        pl.col('event_time_dt').dt.ordinal_day().alias('day_of_year')
    ]).drop('event_time')
    
    cat_cols = ['event_type', 'user_session'] + target_columns

    df_imp = df_imp.with_columns(
        [pl.col(c).fill_null('MISSING') for c in cat_cols]
    )

    df_imp = df_imp.with_columns(pl.col('category_code').str.split('.').list.first().alias('catalog'))

    return df_imp


def process_data(file_path):
    print(f'Сканиорвание файла {file_path}')

    q = pl.scan_csv(file_path).select(['product_id', 'category_id', 'category_code', 'brand'])

    category_to_brand = (
        q.filter(
            pl.col('category_code').is_null())
            .group_by('brand')
            .agg(pl.col('category_code').mode().first().alias('common_category')
        )).collect()
    brand_to_category = (
        q.filter(pl.col('brand').is_null())
        .group_by('category_code')
        .agg(pl.col('brand').mode().first().alias('common_brand'))
    ).collect()

    prod_to_category_and_brand = q.group_by('product_id').agg(
        pl.col('category_id').max().alias('lookup_id'),
        pl.col('category_code').mode().first().alias('lookup_category'),
        pl.col('brand').mode().first().alias('lookup_brand')
    ).collect()

    category_id_to_code = (
        q.filter(pl.col('category_code').is_not_null())
        .select(['category_id', 'category_code'])
        .unique()
        .group_by('category_id')
        .first()
        .rename({'category_code': 'lookup_code_from_id'})
    ).collect()

    print(f'Чтение файла {file_path}')

    df = pl.scan_csv(file_path)

    df = df.join(prod_to_category_and_brand.lazy(), how='left', on='product_id')
    df = df.with_columns([
        pl.coalesce(['category_code', 'lookup_category']).alias('category_code'),
        pl.coalesce(['brand', 'lookup_brand']).alias('brand'),
        pl.coalesce(['product_id', 'lookup_id']).alias('product_id')
    ])

    df = df.join(brand_to_category.lazy(), how='left', on='category_code')
    df = df.with_columns([
        pl.coalesce(['brand', 'common_brand']).alias('brand')
    ])

    df = df.join(category_to_brand.lazy(), how='left', on='brand')
    df = df.with_columns(pl.coalesce(['category_code', 'common_category']).alias('category_code'))

    df = df.join(category_id_to_code.lazy(), how='left', on='category_id')
    df = df.with_columns(pl.coalesce(['category_code', 'lookup_code_from_id']).alias('category_code'))

    df = df.drop(['lookup_category', 'lookup_brand', 'lookup_id', 'common_category', 'common_brand', 'lookup_code_from_id'])

    df = impute(df)

    return df

def make_new_tables(data):
    session_df = data.group_by(pl.col('user_session')).agg(
    event_number=pl.col('event_type').count(),
    session_duration=(pl.col('event_time_dt').max() - pl.col('event_time_dt').min()).dt.total_seconds().cast(pl.Int64),
    carted=(pl.col('event_type') == 'cart').any().cast(pl.Int8),
    purchased=(pl.col('event_type') == 'purchase').any().cast(pl.Int8),
    most_searched_catalog=pl.col('catalog').mode().first(),
    most_searched_brand=pl.col('brand').mode().first(),
    mean_price_searched=pl.col('price').mean()
    )

    group_df = data.group_by('product_id').agg(
    category_id=pl.col('category_id').max(),
    category_code=pl.col('category_code').mode().first(),
    brand=pl.col('brand').mode().first(),
    mean_price=pl.col('price').mean(),
    popularity=pl.col('user_session').n_unique(),
    times_purchased=(pl.col('event_type') == 'purchase').sum(),
    times_carted=(pl.col('event_type') == 'cart').sum()
    )
    #group_df = group_df.with_columns((pl.col('popularity') - pl.col('popularity').min()) / (pl.col('popularity').max() - pl.col('popularity').min()))
    
    max_date = data.select('event_time_dt').max().collect().item()
    users_df = data.group_by('user_id').agg(
    number_of_sessions=pl.col('user_session').count(),
    most_viewed_category_code=pl.col('category_code').mode().first(),
    most_viewed_brand=pl.col('brand').mode().first(),
    times_purchased=(pl.col('event_type') == 'purchase').sum().cast(pl.Int32),
    times_carted=(pl.col('event_type') == 'cart').sum().cast(pl.Int32),
    time_since_last_event=(pl.lit(max_date) - (pl.col('event_time_dt')).max()).dt.total_days()
    )
    spend = data.filter(pl.col('event_type') == 'purchase').group_by('user_id').agg(pl.col('price').sum().alias('total_spend'))
    users_df = users_df.join(spend, on='user_id', how='left').fill_null(0)

    return session_df, group_df, users_df


def process_and_save(file_path, month_name):
   # data = pl.scan_csv(file_path)
    #data_oct = pl.scan_csv('raw_csv/2019-Oct.csv')
    imputed = process_data(file_path)
    print('Успех')

    print('Создание новых таблиц')
    session_df, group_df, users_df = make_new_tables(imputed)
    print('Успех')

    print('Сохранение')
    os.makedirs('temp_tables', exist_ok=True)
    print(f"Сохранение sessions_{month_name}...")
    session_df.sink_parquet(f'temp_tables/sessions_{month_name}.parquet')
    
    print(f"Сохранение groups_{month_name}...")
    group_df.sink_parquet(f'temp_tables/groups_{month_name}.parquet')
    
    print(f"Сохранение users_{month_name}...")
    users_df.sink_parquet(f'temp_tables/users_{month_name}.parquet')
    del session_df, group_df, users_df
    print('Успех')

def combine_results():
    print('Объединение таблиц')
    os.makedirs('new_tables', exist_ok=True)
    tables = ['sessions', 'groups', 'users']
    months = ['Oct', 'Nov']

    raw_dfs = []

    for table in tables:
        print(f'Сборка {table}.parquet')

        dfs = []

        for month in months:
            path = f'temp_tables/{table}_{month}.parquet'
            if os.path.exists(path):
                dfs.append(pl.scan_parquet(path))
        
        if dfs:
            full_df = pl.concat(dfs)
            full_df.sink_parquet(f'new_tables/{table}.parquet')
        print(f'Таблица {table} объединена')
    

    for month in months:
        path = f'raw_csv/2019-{month}.csv'
        if os.path.exists(path):
            raw_dfs.append(pl.scan_csv(path))
    if raw_dfs:
        full_raw = pl.concat(raw_dfs)
        full_raw.sink_parquet(f'new_tables/raw.parquet')
    
    shutil.rmtree('./temp_tables', ignore_errors=True)

process_and_save('raw_csv/2019-Oct.csv', 'Oct')

process_and_save('raw_csv/2019-Nov.csv', 'Nov')

combine_results()