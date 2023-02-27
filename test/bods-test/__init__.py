import logging
import azure.functions as func
import pandas as pd
import xmltodict
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import psycopg2
import time
import glob
import schedule
import os
import requests
from datetime import datetime
from datetime import datetime as dt
from pandas.errors import ParserError


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
   # Azure Blob Storage credentials
    blob_connection_string = 'DefaultEndpointsProtocol=https;AccountName=rawstoress;AccountKey=Q0pliofo1R/wvuIRpBXhWXq42BrlNfTRD4fNGLgeWk7DXPgyxQaf0mBiJl6HcJVn0Ee70Hc31HYJ+AStHLLcHw==;EndpointSuffix=core.windows.net'
    container_name = 'test-azure-function-container'

    # Azure PostgreSQL credentials
    server = 'bods-location-data.postgres.database.azure.com'
    database = 'bods_data_midds'
    username = 'azure_bods'
    password = 'Bods@2023'
    port = 5432
    sslmode = 'require'

    #Dataframe Column Names
    pd.set_option('display.max_columns', None)

    COLUMN_NAMES = ['recorded_at_time', 'item_identifier', 'valid_until_time', 'line_ref', 'direction_ref',
                        'dated_vehicle_journey_ref', 'data_frame_ref', 'published_line_name', 'operator_ref',
                        'origin_ref', 'origin_name', 'destination_ref', 'destination_name', 'longitude', 'latitude',
                        'vehicle_ref', 'tckt_machine_service_code', 'journey_code',
                        'seated_capacity', 'wheelchair_capacity', 'occupancy_thresholds']

    #API Authentication
    api_key = '1ac6ae31503a139fa219d2925e845c9e38f90df7'

    # API endpoint
    url = f'https://data.bus-data.dft.gov.uk/api/v1/datafeed/706' + '/?api_key=' + api_key

    # Create blob client
    blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Connect to PGSQL Database

    conn = psycopg2.connect(host=server, port=port, database=database, user=username, password=password, sslmode=sslmode)
    cursor = conn.cursor()


    def job():
    # Read data from API
        response = requests.get(url)

        all_df = {}
        if response.status_code == 200:
            dt_string = datetime.now().strftime("%Y%m%d_%H%M%S%f")
            try:
                        data_dict = xmltodict.parse(response.content)
                        data_dict2 = data_dict['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']['VehicleActivity']
                        df = pd.DataFrame(data_dict2)
                        df = df.drop('Extensions', axis=1).join(pd.DataFrame(df['Extensions'].values.tolist()))
                        df = df.drop('VehicleJourney', axis=1).join(pd.DataFrame(df['VehicleJourney'].values.tolist()))
                        df = df.drop('MonitoredVehicleJourney', axis=1).join(
                            pd.DataFrame(df['MonitoredVehicleJourney'].values.tolist()))
                        df = df.drop('VehicleLocation', axis=1).join(pd.DataFrame(df['VehicleLocation'].values.tolist()))
                        all_df[dt_string] = df
            except ParserError as pe:
                        print(f"Parse error: {pe} ")
            if len(all_df) > 0:
                df = pd.concat(all_df).reset_index(drop=True)
                df = df.drop('FramedVehicleJourneyRef', axis=1).join(pd.DataFrame(df['FramedVehicleJourneyRef'].values.tolist()))
                if 'TicketMachineServiceCode' or 'JourneyCode' or 'SeatedCapacity' or 'WheelchairCapacity' or 'OccupancyThresholds' or 'DriverRef' or 'VehicleUniqueId' or 'DriverRef' not in df:
                    df['TicketMachineServiceCode'] = 0
                    df['JourneyCode'] = 0
                    df['SeatedCapacity'] = 0
                    df['WheelchairCapacity'] = 0
                    df['OccupancyThresholds'] = 0
                    df['BlockRef'] = 0
                    df['VehicleUniqueId'] = 0
                    df['DriverRef'] = 0

                df = df.rename(
                    {'RecordedAtTime': 'recorded_at_time',
                    'ItemIdentifier': 'item_identifier',
                    'ValidUntilTime': 'valid_until_time',
                    'LineRef': 'line_ref',
                    'DirectionRef': 'direction_ref',
                    'DatedVehicleJourneyRef': 'dated_vehicle_journey_ref',
                    'PublishedLineName': 'published_line_name',
                    'DataFrameRef': 'data_frame_ref',
                    'OperatorRef': 'operator_ref',
                    'OriginRef': 'origin_ref',
                    'OriginName': 'origin_name',
                    'DestinationRef': 'destination_ref',
                    'DestinationName': 'destination_name',
                    'Longitude': 'longitude',
                    'Latitude': 'latitude',
                    'VehicleRef': 'vehicle_ref',
                    'TicketMachineServiceCode': 'tckt_machine_service_code',
                    'JourneyCode': 'journey_code',
                    'SeatedCapacity': 'seated_capacity',
                    'WheelchairCapacity': 'wheelchair_capacity',
                    'OccupancyThresholds': 'occupancy_thresholds',
                    },
                    axis=1
                )


            # Convert data to DataFrame
                df = df[COLUMN_NAMES]

            # Set up your folder structure
                today = datetime.now()
                year = today.strftime("%Y")
                month = today.strftime("%m")
                day = today.strftime("%d")
                output = year + "/" + month + "/" + day
                
            # Store data in Azure Blob Storage
                blob_name = "middlesbrough_{}.csv".format(today.strftime("%Y%m%d_%H:%M:%S"))
                blob_client = container_client.get_blob_client(output + "/" + blob_name)

                blob_name = blob_client.url
                blob_client.upload_blob(df.to_csv (index=False))

                # Create SQL table based on the DataFrame schema
                # table_name = 'test'
                # columns = ', '.join([f'"{col}" varchar' for col in df.columns])
                # create_table_query = f"CREATE TABLE {table_name} ({columns})"
                # cursor.execute(create_table_query)
                # conn.commit()

                insert_query = f"INSERT INTO test VALUES ({','.join(['%s' for _ in range(len(df.columns))])})"
                cursor.executemany(insert_query, df.values.tolist())
                conn.commit()

    schedule.every(10).seconds.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)

    
