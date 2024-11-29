from mtspark import get_spark
from onetl.connection import Postgres, Greenplum, SparkHDFS
from onetl.db import DBReader, DBWriter
from py4j.protocol import Py4JJavaError
from Internal import Internal
from onetl.log import setup_logging
from onetl.file import FileDFReader, FileDownloader
from onetl.file.format import Excel

class ETL:

    def __init__(self, source_table: str, target_table: str):
        
        setup_logging()
        
        self.spark = self.spark_config()

        self.__extra = {
            'tcpKeepAlive': 'true',
            'server.port': '49152-65535',
            'pool.maxSize': 10
        }

        self.__source_table = source_table

        self.__target_table = target_table

        self.__source_conn = None

        self.__target_conn = None

        self.__df_from_outer_db = None

        self.__internal = Internal(source_table)

        
    def __str__(self):

        return f'source table: {self.__source_table}\ntarget table: {self.__target_table}'

    def spark_config(self):

        """
            The Method creates spark configuration considering the number of cores used for calculations, the amount of                 allocated memory 

            :return: customized spark configuration

        """

        try:
            spark = get_spark(
                config=
                {
                    "appName": "mysessionname",  # название сессии
                    "spark.jars.packages": Postgres.get_packages() + Greenplum.get_packages(spark_version="3.2.0"),
                    'spark.ui.showConsoleProgress': True,
                    'spark.sql.execution.arrow.pyspark.enabled': True,
                    "spark.driver.memory": "2g",
                    "spark.driver.cores": 2,  # Задаем только в Cluster Mode
                    "spark.executor.cores": 6,
                    "spark.executor.memory": "20g",
                    "spark.submit.deploymode": "client",
                    "spark.dynamicAllocation.enabled": True,
                    "spark.dynamicAllocation.initialExecutors": 1,
                    "spark.dynamicAllocation.minExecutors": 0,
                    "spark.dynamicAllocation.maxExecutors": 8,
                    "spark.dynamicAllocation.executorIdleTimeout": "60s",
                },
                spark_version="local",  # версия Spark, он должен быть установлен на edge ноду в /opt/spark
            )

        except Py4JJavaError as e:
            spark = None
            print(f"An error occured:{e}")

        self.spark = spark
        
        print('Hello spark')
        
        return spark

    def source_connection(self):

        """
            The method returns the connection to the source database 

            :return: the connection to the source database

        """

        try:
            connection_source = Postgres(
                host="smth",
                user="user",
                password="password",
                database="dwh",
                spark=self.spark,
                extra=self.__extra
            ).check()

        except Exception as e:
            connection_source = None
            print(f"An error occured: {e}")

        self.__source_conn = connection_source
        return connection_source

    def target_connection(self):

        """
            The method returns the connection to the destination database

            :return: connection to the destination database

        """

        try:
            target_conn = Greenplum(
                host="smth",
                user="user",
                password="password",
                database="core",
                spark=self.spark,
                extra=self.__extra
            ).check()

        except Exception as e:
            target_conn = None
            print(f"an error occured: {e}")

        self.__target_conn = target_conn
        return target_conn

    def query_reader(self, query: str, part_column: str = None):

        """
            The method accepts a query body and optionally a partition column to create a query and write to the output of             a dataframe with output data

            :param query: SQL Query

            :param part_column: (optional) Partition column

            :return: DataFrame with output data

        """

        try:
            if part_column == None:
                
                df_from_outer_db = self.__source_conn.sql(f"{query}")
            else:
                upper_bound =self.__internal.max_upper_bound(self.source_connection(), part_column)
                
                df_from_outer_db = self.__source_conn.sql(f"{query}",
                                               options=Postgres.SQLOptions(
                                               partition_column=part_column,
                                               num_partitions=8,
                                               lower_bound=0,
                                               upper_bound=upper_bound
                                           ))
        except Py4JJavaError as e:
            df_from_outer_db = None
            # print(f"an error occured: {e}")

        self.__df_from_outer_db = df_from_outer_db
        return df_from_outer_db

    def write_to_DB(self):

        """
            The method writes the data that is stored in the data frame to a table

            :return: Nothing        
        """
        try:
            
            writer = DBWriter(
                    connection=self.__target_conn,
                    target=self.__target_table,
                    options=Greenplum.WriteOptions(
                    if_exists="append",
                    truncate="false",
                    # distributedBy="regid"
                    ))
            
            writer.run(self.__df_from_outer_db)
            
            self.spark.stop()
                
        except Py4JJavaError as e:
            print(f"an error occured: {e}")
            return None
        
    
    def transfer_table(self, transfer_table_name : str, part_column : str = None):

        """
            The method transfers table from source database to target database

            :param transfer_table_name: Name of the desired transfer table

            :param part_column: (optional) Partition column

            :return: Nothing        
        """

        try:
            if part_column == None:
                reader_transfer_table = DBReader(
                    connection=self.__source_conn,
                    source=transfer_table_name,
                    columns=["*"],
                    )
            else :
                upper_bound =self.__internal.max_upper_bound(self.source_connection(), part_column)

                reader_transfer_table = DBReader(
                    connection=self.__source_conn,
                    source=self.__source_table,
                    columns=["*"],
                    options=Postgres.ReadOptions(
                        partition_column=part_column,
                        num_partitions=8,
                        lower_bound=0,
                        upper_bound=upper_bound
                    )
                )

            table_transfer_table = reader_transfer_table.run()

        except Exception as e:
            print(f"an error occured: {e}")
            return None  
            
            self.__df_from_outer_db=reader_transfer_table

            return reader_transfer_table

    def load_file_toDB(self, file_link:str):

        """
            The method transfers local file to target database

            :param file_link: Link to the file to be transferred to the database

            :return: Nothing
        """

        try:
            hdfs = SparkHDFS(
                host="host",
                cluster="rnd-dwh",
                spark=self.spark,
            ).check()

            reader = FileDFReader(
                connection = hdfs,
                header= True,
                source_path=file_link,
                options=FileDFReader.Options(recursive=False),
            )

            file_df = reader.run()

            self.__df_from_outer_db=df
        
        except Py4JJavaError as e:
                print(f"an error occured: {e}")



