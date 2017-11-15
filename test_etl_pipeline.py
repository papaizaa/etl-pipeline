import csv
import unittest
import os
from etl_pipeline import FlattenSqlDB, CreateDbFromCsv
from create_sensor_db import MySQLCreate


class MySqlCreateTest(unittest.TestCase):

    def setUp(self):
        self.mysql = MySQLCreate(host='localhost',
                                 user='root',
                                 password='')

    def tearDown(self):
        self.mysql = None

    def test_create_table(self):
        self.mysql.create_table()

        with self.mysql.connection.cursor() as cursor:
            # Check that table is created and empty
            self.assertEqual(0, cursor.execute("select * from `SensorTable`;"))

    def test_write_to_db(self):
        self.mysql.create_table()
        self.mysql.write_to_db()

        with self.mysql.connection.cursor() as cursor:
            # Check that table is created and filled
            self.assertEqual(4320, cursor.execute("select * from `SensorTable`;"))

            # Chose a time and sensor reading that was compressed in the initial csv input
            cursor.execute("select S.Reading from `SensorTable` S "
                                 "where S.Time = '2017-01-01 0:06' and S.SensorId = 1 ;")

            rows = cursor.fetchall()
            self.assertEqual(rows[0]["Reading"], 303)
            self.assertEqual(len(rows), 1)

            # Chose a time and sensor reading that was not compressed in the initial csv input
            cursor.execute("select S.Reading from `SensorTable` S "
                           "where S.Time = '2017-01-01 0:10' and S.SensorId = 2 ;")

            rows = cursor.fetchall()
            self.assertEqual(rows[0]["Reading"], 304)
            self.assertEqual(len(rows), 1)

    def test_write_table_to_csv(self):
        filepath = 'test.csv'
        if os.path.isfile(filepath):
            os.remove(filepath)

        self.mysql.fill_db(filepath)
        with open(filepath, 'r') as raw_file:
            raw_data = csv.reader(raw_file, delimiter=',', quoting=csv.QUOTE_NONE)
            data_list = list(raw_data)[1:]

        row = data_list[0]
        self.assertEqual(row[0], '2017-01-01 00:00')
        self.assertEqual(int(row[1]), 1)
        self.assertEqual(int(row[2]), 1)
        self.assertEqual(int(row[3]), 305)


class EtlPipelineTest(unittest.TestCase):
    def setUp(self):
        self.mysql = MySQLCreate(host='localhost',
                                 user='root',
                                 password='')

    def tearDown(self):
        self.mysql = None

    def test_generate_flat_sensor_table(self):
        self.mysql.create_table()
        self.mysql.write_to_db()

        FlattenSqlDB.generate_flat_sensor_table(self.mysql.connection)

        with self.mysql.connection.cursor() as cursor:

            # Check whether there is data in the new table, SensorsData
            self.assertTrue(cursor.execute("select * from `SensorsData`;") > 0)
            self.assertTrue(len(cursor.fetchall()) > 0)

            # Fetch sensor readings for time 0 and use the sample dataset to validate whether the values are correct
            cursor.execute("select S.Sensor1Reading, S.Sensor2Reading, S.Sensor3Reading from `SensorsData` S "
                            "where S.Time = '2017-01-01 0:00' ;")
            rows = cursor.fetchall()

            self.assertEqual(1, len(rows))
            self.assertEqual(int(rows[0]["Sensor1Reading"]), 305)
            self.assertEqual(int(rows[0]["Sensor2Reading"]), 301)
            self.assertEqual(int(rows[0]["Sensor3Reading"]), 304)

    def test_write_table_to_csv(self):
        self.mysql.create_table()
        self.mysql.write_to_db()

        FlattenSqlDB.generate_flat_sensor_table(self.mysql.connection)

        filepath = 'test.csv'
        if os.path.isfile(filepath):
            os.remove(filepath)
        FlattenSqlDB.write_flat_table_to_csv(self.mysql.connection, filepath)

        with open(filepath, 'r') as raw_file:
            raw_data = csv.reader(raw_file, delimiter=',', quoting=csv.QUOTE_NONE)
            data_list = list(raw_data)[1:]

        row = data_list[0]
        self.assertEqual(row[0], '2017-01-01 00:00')
        self.assertEqual(int(row[1]), 1)
        self.assertEqual(int(row[2]), 305)
        self.assertEqual(int(row[3]), 301)
        self.assertEqual(int(row[4]), 304)
