import snowflake.connector


conn=snowflake.connector.connect(
      user='coolkeonjhar',
                password='Amrita@123',
                account='zu02863.ap-south-1.aws',
                warehouse='SNOW_LARGE_VWH',
                database='NIMBUS_MAPS' ,
                schema='PRODUCTION'
                )



curs=conn.cursor()
sql = 'CREATE TEMPORARY TABLE NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES_BKP AS SELECT * FROM NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES;'
curs.execute(sql)

sql_truncate_target = 'TRUNCATE TABLE NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES;'
curs.execute(sql_truncate_target)

sql_insert_data_to_target = 'insert into NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES select * from NIMBUS_MAPS.PRESENTATION.DECISION_DATA_PRES;'
curs.execute(sql_insert_data_to_target)

target_count=curs.execute('select count(*) from NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES;')
print(target_count)
count_tar=[]
for i in target_count:
    count_tar.append(i[0])
count_target_table = count_tar[0]
print(count_target_table)
source_count=curs.execute('select count(*) from NIMBUS_MAPS.PRESENTATION.DECISION_DATA_PRES;')
print(source_count)
count_sou=[]
for j in source_count:
    count_sou.append(j[0]) 
count_source_table = count_sou[0]
print(count_source_table)

if count_target_table == count_source_table:
    print('data successfully copied to production')
    sql_drop_temp_table='DROP TABLE NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES_BKP;'
    curs.execute(sql_drop_temp_table)
    print('droppped temporary table')
else:
    sql_insert_data_back='insert into NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES  select * from NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES_BKP;'
    print('data load failed, rolling back the changes')
