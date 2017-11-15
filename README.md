# ETL Pipeline

The purpose of this code base is to create an ETL pipeline from a given dataset. A csv file is provided with readings from
three sensors for various amounts of time. The idea is to create an ETL pipeline, that will read in the compressed data
provided in the csv file, create a database of the uncompressed data, then transform the database into a flat
representation of the data. The final outcome is to write the flat database into a csv file for easier viewing.

To accomplish all this I decided on using MySQL as the datastore because it is fast, reliable and easy to start working 
with. In order to get the pipeline infrastructure up and running, I decided to use Luigi. Luigi is an open source python 
library that helps you build pipelines. It works by letting you chain tasks by creating dependencies and running 
certain tasks only when their dependencies are finished running. Luigi relies on intermediate outputs in between 
tasks in the graph, meaning these outputs will be persisted indefinitely. The good thing about this is if the 2nd task 
fails, we won't have to rebuild the first, but this also means that there will be a lot of intermediate results in your
file system. An automatic garbage collection system should be used to keep the size of your file system reasonable.

The process in this pipeline involves the following components:
1) Create a Luigi Task, CreateDbFromCsv, that creates a new database and fills it with data from the sample dataset 
provided. The sample dataset is compressed to not include time readings where the sensor reading did not change. Hence,
I first uncompressed these readings and create a table entry for the three sensors and all their readings from time 0 to
the end of recording. 
2) The Luigi task then outputs the result into a local csv file called ```sql-db-{datetime}.csv``` as an intermediary
file. This is specified as a LocalTarget in the output function. 
3) The next Luigi task waits for the previous to finish and then it performs a transformation on the table to create a 
new table. The result is then outputted into a csv file called ````output-{datetime}.csv````.

## To run the code:
1) Make sure to have MySQL installed on your machine. If not follow here: https://dev.mysql.com/doc/refman/5.7/en/installing.html
If you have a mac, you can simply run 
````
    brew install mysql
```` 
2) Make sure to have Python 2.7 on your computer. Follow here: https://www.python.org/downloads/
3) Have pip installed on your computer (Comes with versions greater than Python 2.7.9). If not follow here: https://pip.pypa.io/en/stable/installing/#do-i-need-to-install-pip
4) From the terminal navigate to the etl_pipeline folder on your machine. Run the following command to install all the 
necessary pip packages to run the program: 
```
    sudo pip install -r requirements.txt
```
5) To run the Luigi Pipeline, run the following command: 
```
    python etl_pipeline.py
```
6) To view the results of the ETL transformation, find the newly created file: ```output-{datetime}.csv``` to view the 
results.
7) To run the unit tests, simply run:
```
    python -m unittest discover
```