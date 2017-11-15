import csv
import pymysql.cursors
from datetime import datetime, timedelta


# Object for connecting to a MySQL database and writing to that database using the given 'sample_dataset.csv'
class MySQLCreate(object):
    def __init__(self, host, user, password):
        self.connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

    def fill_db(self, out_file):
        self.create_table()
        self.write_to_db()
        self.write_table_to_csv(out_file)

    """"Create the database and table"""
    def create_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute("create database IF NOT EXISTS sensors;")
            cursor.execute("use sensors;")
            cursor.execute("drop table if exists  `SensorTable`")
            sqlQuery = "CREATE TABLE IF NOT EXISTS `SensorTable` (" \
                       "`Time` datetime NOT NULL," \
                       "`DeviceID` int(11) NOT NULL, " \
                       "`SensorID` int(11) NOT NULL, " \
                       "`Reading` int(11) NOT NULL, " \
                       "PRIMARY KEY (`Time`, `DeviceID`, `SensorID`)" \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

        self.connection.commit()

    """Uncompress the sample dataset and write the contents to the table"""
    def write_to_db(self):
        with self.connection.cursor() as cursor:

            with open('sample_dataset.csv', 'r') as raw_file:
                raw_data = csv.reader(raw_file, delimiter=',', quoting=csv.QUOTE_NONE)
                data_list = list(raw_data)[1:]

            for row in data_list:
                # Use the count to know how long prior to this time the sensor reading has been the same
                count = int(row[4])
                for i in range(count):
                    date = datetime.strptime(row[0], '%Y-%m-%d %H:%M')
                    # Subtract the current time with i minutes to get the sensor data that was removed in the
                    # compression
                    date = date - timedelta(minutes=i)
                    date_string = date.strftime('%Y-%m-%d %H:%M')
                    device_id = int(row[1])
                    sensor_id = int(row[2])
                    reading = int(row[3])
                    sql_query = "INSERT INTO `SensorTable` (`Time`, `DeviceID`, `SensorID`, `Reading`) " \
                                "values ('{}', '{}', '{}', '{}');".format(date_string, device_id, sensor_id, reading)
                    cursor.execute(sql_query)

        self.connection.commit()

    """Write the contents of the table to a csv for the luigi task output"""
    def write_table_to_csv(self, out_file):
        with self.connection.cursor() as cursor:
            cursor.execute("select * from `SensorTable`;")
            rows = cursor.fetchall()

            with open(out_file, "w") as out:
                out.write("Time, DeviceID, SensorID, Reading")
                for row in rows:
                    out.write("\n")
                    time = row['Time'].strftime('%Y-%m-%d %H:%M')
                    device_id = row['DeviceID']
                    sensor_id = row['SensorID']
                    reading = row['Reading']
                    out.write("{}, {}, {}, {}".format(time, device_id, sensor_id, reading))
        self.connection.commit()


if __name__ == '__main__':
    sqlCreate = MySQLCreate(host='localhost', user='root', password='')
    sqlCreate.fill_db('create-sql-db.csv')

