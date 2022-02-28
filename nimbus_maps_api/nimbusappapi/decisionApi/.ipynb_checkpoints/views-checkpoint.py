from django.http import JsonResponse
from faker import Faker
import random
from django.shortcuts import render, redirect
import snowflake.connector


def home(request):
    return render(request, 'home.html')

def show_design_diagram(request):
    return render(request, 'design.html')

def show_dataflow(request):
    return render(request, 'dataflow.html')

def get_record_count_snowflake(request):
    conn=snowflake.connector.connect(
      user='coolkeonjhar',
                password='Amrita@123',
                account='zu02863.ap-south-1.aws',
                warehouse='SNOW_LARGE_VWH',
                database='NIMBUS_MAPS' ,
                schema='PRODUCTION'
                )
    
    curs=conn.cursor()
    sql = 'select (select count(*) from NIMBUS_MAPS.PRODUCTION.DECISION_DATA_PRES) AS PROD_COUNT,(SELECT count(*) from NIMBUS_MAPS.PRESENTATION.DECISION_DATA_PRES) AS PRESE_COUNT,(select count(*) from NIMBUS_MAPS.RAW_LAYER.DECISION_DATA_RAW) AS RAW_COUNT,(select count(*) from NIMBUS_MAPS.STAGING_DATA_LAKE.DECISION_DATA_STG ) AS STAGE_LAKE_COUNT,(select count(*) from nimbus_maps.raw_layer.log_duplicates) AS duplicates_COUNT;'
    GET_COUNTS=curs.execute(sql)
    for i in GET_COUNTS:
        list =[]
        list.append(i[0])
        list.append(i[1])
        list.append(i[2])
        list.append(i[3])
        list.append(i[4])
    Prod_count = list[0] 
    pres_count = list[1]
    raw_count  = list[2] 
    stage_lake_count = list[3]
    duplicates_count = list[4]
    
    count_data = {"STAGE_LAKE_ROW_COUNT" : stage_lake_count,
                   "RAW_TABLE_ROW_COUNT"   : raw_count,
                  "PRES_TABLE_ROW_COUNT": pres_count,
                  "PRODUCTION_ROW_COUNT" : Prod_count,
                   "DUPLICATES_ROW_COUNT" : duplicates_count
                 }
    
    
    
    return render(request, 'row_counts.html',count_data)
    

def get_data(request):
    fake = Faker()
    LIST_DECISION = ['A', 'B', 'C']
    UID = fake.random_number(digits=7)
    ADDRESS = fake.street_address()
    CITY = Faker('it_IT').city()
    ZIPCODE=fake.zipcode()
    DECISION=random.choice(LIST_DECISION)


    data = {
        'uid': UID,
        'address': ADDRESS,
        'city':CITY,
        'zipcode':ZIPCODE,
        'decision':DECISION
    }
    return JsonResponse(data)
