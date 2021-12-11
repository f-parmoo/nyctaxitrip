import pandas as pd
import os
from sqlalchemy import create_engine
import time

engine = create_engine('postgresql://postgres:postgres@localhost:5432/taxitrip')


def import_csv_in_db(file_name, table_name):
    if not os.path.exists(file_name):
        raise Exception(f'File {file_name} does not exist')
    start_time = time.perf_counter()
    dframe = pd.read_csv(file_name, low_memory=False)
    print('Record Count:', dframe.shape[0])
    dframe['tpep_pickup_hour'] = dframe.apply(lambda row: int(row['tpep_pickup_datetime'][11:13]), axis=1)
    dframe.columns = [item.lower() for item in list(dframe.columns)]
    try:
        engine.execute(f"drop table {table_name}")
    except:
        pass

    # Because File is huge and inserting this file to database at once may occur Memory Error, I inserted data in a batch
    batch_size = 100000
    for item in range((dframe.shape[0] // batch_size) + 1):
        start = 0 if item == 0 else item * batch_size + 1
        end = (item + 1) * batch_size
        sub_dframe = dframe[start:min(end + 1, dframe.shape[0] + 1)]
        sub_dframe.to_sql(name=table_name, con=engine, if_exists='append')
    end_time = time.perf_counter()
    print(f'Execution Time:', end_time - start_time)


def create_large_table(table_name, large_table_name, partition_column, partition_values, index_list):
    start_time = time.perf_counter()
    try:
        engine.execute(f'drop table {large_table_name} ')
    except:
        pass
    col_definition = ''
    col_names = ''
    result = engine.execute(
        f"select column_name, udt_name  from information_schema.columns where table_name='{table_name}'")
    for item in result:
        col_definition += item[0] + ' ' + item[1] + ','
        col_names += item[0] + ','
    engine.execute(f"""create table {large_table_name}  ({col_definition.replace('varchar', 'varchar(300)').strip(',')})
                            partition by list({partition_column})""")

    for item in partition_values:
        engine.execute(
            f""" create table {large_table_name}_p_{item}
                        partition of {large_table_name}
                        for values in ({item}); """)

    for index, item in enumerate(index_list):
        engine.execute(f'create INDEX {large_table_name}_{index} ON  {large_table_name}({item}) ')

    for item in range(10):
        print(item)
        engine.execute(
            f"insert into {large_table_name}({col_names.strip(',')}) (select {col_names.strip(',')} from  {table_name})")

    end_time = time.perf_counter()
    print(f'Execution Time:', end_time - start_time)


def run_queries(sql):
    start_time = time.perf_counter()
    result = engine.execute(sql).fetchall()
    end_time = time.perf_counter()
    print('Execution Time: ', end_time - start_time, '      Result: ', result[0][0])
    return result


if __name__ == '__main__':
    # import_csv_in_db(file_name='yellow_tripdata_2020-01.csv',
    #                  table_name='nyc_taxitrip')
    # create_large_table(table_name='nyc_taxitrip',
    #                    large_table_name='nyc_taxitrip_big',
    #                    partition_column='tpep_pickup_hour',
    #                    partition_values=list(range(24)),
    #                    index_list=['tip_amount', 'total_amount','tpep_pickup_hour'])
    # table_names = ('nyc_taxitrip', 'nyc_taxitrip_big')
    table_names = ('nyc_taxitrip',)
    start_time = time.perf_counter()
    for index, item in enumerate(table_names):
        print(f'--------------------- {item} -----------------------')
        result1 = run_queries(f'''select  sum(1) from {item} where total_amount<5 ''')
        result2 = run_queries(
            f'''select  tpep_pickup_hour , sum(1)  from {item} group by tpep_pickup_hour order by 2 desc  ''')
        result3 = run_queries(f'''select avg(tip_amount) from {item}  ''')
