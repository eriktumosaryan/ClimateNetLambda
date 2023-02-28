import mysql
from mysql.connector import Error
from datetime import datetime

config = {
    'host': 'database-3.c8nb4zcoufs1.us-east-1.rds.amazonaws.com',
    'database': 'ClimateNet',
    'user': 'admin',
    'password': 'lovetumolabs',
}


def connect_to_db(config):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        status = connection.is_connected()
    except Error as err:
        print(err)
        status = err
    return connection, cursor, status


def write_info_db(connection, cursor, status, event):
    if 'queryStringParameters' in event:
        try:
            insert_query = "INSERT INTO Data (temperature, humidity, pressure, jolt, light, CO2, data_and_time) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            data = {
                "temperature": event['queryStringParameters']['temperature'],
                "humidity": event['queryStringParameters']['humidity'],
                "pressure": event['queryStringParameters']['pressure'],
                "jolt": event['queryStringParameters']['jolt'],
                "light": event['queryStringParameters']['light'],
                "CO2": event['queryStringParameters']['CO2'],
                "data_and_time": datetime.now()
            }
            insert_values = tuple(data.values())
            cursor.execute(insert_query, insert_values)
            connection.commit()
            cursor.close()
            connection.close()
            status = "Ok"
        except Error as err:
            status = err
    else:
        pass
    return status


def lambda_handler(event, context):
    connection, cursor, status = connect_to_db(config)
    status = write_info_db(connection, cursor, status, event)
    return status

# if __name__ == '__main__':
#     print("i have start ")
#     lambda_handler(1, 1)
