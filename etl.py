import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, regexp_replace


# read var names from dl.cfg
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Description: Create a Spark Session.
    
    Arguments:
        None
        
    Return:
        None
    """         
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_cotas_data(spark, input_path, output_path, input_file):
    
    """
    Description: This function is responsible for reading the "Cotas" 
    file in raw json format from the Staging layer and transforming 
    it into a columnar Parquet file, saving it in the SOR layer
    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        input_file: input file name.
        
    Return:
        None
    """        
    
    # read cotas file
    df = spark.read.option("multiLine", "true").json(input_path + input_file)

    # format cotas file
    df = df.withColumn("dados_explode",explode("dados")) \
        .withColumn("ano",col("dados_explode.ano")) \
        .withColumn("cnpjCPF",col("dados_explode.cnpjCPF")) \
        .withColumn("codigoLegislatura",col("dados_explode.codigoLegislatura")) \
        .withColumn("cpf",col("dados_explode.cpf")) \
        .withColumn("dataEmissao",col("dados_explode.dataEmissao")) \
        .withColumn("descricao",col("dados_explode.descricao")) \
        .withColumn("descricaoEspecificacao",col("dados_explode.descricaoEspecificacao")) \
        .withColumn("fornecedor",col("dados_explode.fornecedor")) \
        .withColumn("idDeputado",col("dados_explode.idDeputado")) \
        .withColumn("idDocumento",col("dados_explode.idDocumento")) \
        .withColumn("legislatura",col("dados_explode.legislatura")) \
        .withColumn("lote",col("dados_explode.lote")) \
        .withColumn("mes",col("dados_explode.mes")) \
        .withColumn("nomeParlamentar",col("dados_explode.nomeParlamentar")) \
        .withColumn("numero",col("dados_explode.numero")) \
        .withColumn("numeroCarteiraParlamentar",col("dados_explode.numeroCarteiraParlamentar")) \
        .withColumn("numeroDeputadoID",col("dados_explode.numeroDeputadoID")) \
        .withColumn("numeroEspecificacaoSubCota",col("dados_explode.numeroEspecificacaoSubCota")) \
        .withColumn("numeroSubCota",col("dados_explode.numeroSubCota")) \
        .withColumn("parcela",col("dados_explode.parcela")) \
        .withColumn("passageiro",col("dados_explode.passageiro")) \
        .withColumn("ressarcimento",col("dados_explode.ressarcimento")) \
        .withColumn("restituicao",col("dados_explode.restituicao")) \
        .withColumn("siglaPartido",col("dados_explode.siglaPartido")) \
        .withColumn("siglaUF",col("dados_explode.siglaUF")) \
        .withColumn("tipoDocumento",col("dados_explode.tipoDocumento")) \
        .withColumn("trecho",col("dados_explode.trecho")) \
        .withColumn("urlDocumento",col("dados_explode.urlDocumento")) \
        .withColumn("valorDocumento",col("dados_explode.valorDocumento")) \
        .withColumn("valorGlosa",col("dados_explode.valorGlosa")) \
        .withColumn("valorLiquido",col("dados_explode.valorLiquido")) \
        .drop("dados", "dados_explode")
    
    # write cotas to parquet files partitioned by year
    df.write.partitionBy("ano").mode('overwrite').parquet(os.path.join(output_path, 'tab_cotas'))


def process_empresas_data(spark, input_path, output_path, input_file):
    
    """
    Description: This function is responsible for reading the "Empresas" 
    file in raw csv format from the Staging layer and transforming 
    it into a columnar Parquet file, saving it in the SOR layer
    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        input_file: input file name.
        
    Return:
        None
    """        
    
    # read empresas file
    df = spark.read.options(delimiter=';',inferSchema=True).csv(input_path + input_file)

    # format empresas file
    df = df.withColumn("cnpjBasico", col("_c0")) \
       .withColumn("razaoSocial", col("_c1")) \
       .withColumn("codigoNatureza", col("_c2")) \
       .withColumn("qualificacaoResponsavel", col("_c3")) \
       .withColumn("capitalSocial", regexp_replace("_c4", ',', '.').cast("double")) \
       .withColumn("porte", col("_c5")) \
       .withColumn("enteFederativoRsponsavel", col("_c6")) \
       .drop("_c0","_c1","_c2","_c3","_c4","_c5","_c6")
    
    # write empresas to parquet files 
    df.write.mode('overwrite').parquet(os.path.join(output_path, 'tab_empresas'))


def process_naturezas_data(spark, input_path, output_path, input_file):
    
    """
    Description: This function is responsible for reading the "Naturezas" 
    file in raw csv format from the Staging layer and transforming 
    it into a columnar Parquet file, saving it in the SOR layer

    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        input_file: input file name.
        
    Return:
        None
    """        
    
    # read naturezas file
    df = spark.read.options(delimiter=';',inferSchema=True).option("encoding", "windows-1252").csv(input_path + input_file)

    # format naturezas file
    df = df.withColumn("codigoNatureza", col("_c0")) \
       .withColumn("descricaoNatureza", col("_c1")) \
       .drop("_c0","_c1")
    
    # write naturezas to parquet files 
    df.write.mode('overwrite').parquet(os.path.join(output_path, 'tab_naturezas'))
    
    
def load_expense_table(spark, input_path, output_path):
    
    """
    Description: This function is responsible for reading 
    file(s) from the SOR layer to create the "tab_expense" 
    fact table, saving it in the SOT layer.
    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        
    Return:
        None
    """        
    
    # read tab_cotas file
    df = spark.read.parquet(input_path + 'tab_cotas')

    # create temporary view
    df.createOrReplaceTempView("tab_cotas")
    
    # load tab_expense 
    tab_expense = spark.sql('''
                            SELECT idDocumento                      AS receipt_id,
                                   numeroDeputadoID                 AS deputy_id,
                                   CASE
                                       WHEN SUBSTRING(cnpjCPF,15,1) = " " THEN 
                                           CONCAT("F", SUBSTRING(cnpjCPF,1,3), SUBSTRING(cnpjCPF,5,3), SUBSTRING(cnpjCPF,9,3))
                                       ELSE 
                                           CONCAT("J0", SUBSTRING(cnpjCPF,1,3), SUBSTRING(cnpjCPF,5,3), SUBSTRING(cnpjCPF,9,2))  
                                   END                              AS supplier_id,
                                   CAST(dataEmissao   AS TIMESTAMP) AS receipt_time,
                                   CAST(tipoDocumento AS INT)       AS expense_type,
                                   descricao                        AS expense_description,
                                   urlDocumento                     AS receipt_url,
                                   sum(valorDocumento)              AS receipt_amount,
                                   sum(valorGlosa)                  AS canceled_amount,
                                   sum(valorLiquido)                AS net_amount,
                                   ano                              AS accrual_year
                            FROM tab_cotas 
                            WHERE idDocumento != 0
                            GROUP BY idDocumento,
                                     numeroDeputadoID,
                                     cnpjCPF,
                                     dataEmissao,
                                     tipoDocumento,
                                     descricao,
                                     urlDocumento,
                                     ano
                            ''' )
    
    # write tab_expense to parquet files partitioned by year
    tab_expense.write.partitionBy("accrual_year").mode('overwrite').parquet(os.path.join(output_path, 'tab_expense'))

    
def load_deputy_table(spark, input_path, output_path):
    
    """
    Description: This function is responsible for reading 
    file(s) from the SOR layer to create the "tab_deputy" 
    dimension table, saving it in the SOT layer.
    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        
    Return:
        None
    """        
    
    # read tab_cotas file
    df = spark.read.parquet(input_path + 'tab_cotas')

    # create temporary view
    df.createOrReplaceTempView("tab_cotas")
    
    # load tab_deputy 
    tab_deputy = spark.sql('''
                           SELECT DISTINCT numeroDeputadoID    AS deputy_id,
                                           CAST(cpf AS BIGINT) AS person_id,
                                           nomeParlamentar     AS deputy_name,
                                           siglaPartido        AS deputy_party,
                                           siglaUF             AS deputy_state,
                                           legislatura         AS legislature_year
                           FROM tab_cotas 
                           ''')
    
    # write tab_deputy to parquet files partitioned by legislature year
    tab_deputy.write.partitionBy("legislature_year").mode('overwrite').parquet(os.path.join(output_path, 'tab_deputy'))

    
def load_time_table(spark, input_path, output_path):
    
    """
    Description: This function is responsible for reading 
    file(s) from the SOR layer to create the "tab_time" 
    dimension table, saving it in the SOT layer.
    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        
    Return:
        None
    """        
    
    # read tab_cotas file
    df = spark.read.parquet(input_path + 'tab_cotas')

    # create temporary view
    df.createOrReplaceTempView("tab_cotas")
    
    # load tab_time 
    tab_time = spark.sql('''
                         SELECT CAST(dataEmissao AS TIMESTAMP)             AS full_datetime,
                                HOUR(CAST(dataEmissao AS TIMESTAMP))       AS hour,
                                DAY(CAST(dataEmissao AS TIMESTAMP))        AS day,
                                WEEKOFYEAR(CAST(dataEmissao AS TIMESTAMP)) AS week,
                                MONTH(CAST(dataEmissao AS TIMESTAMP))      AS month,
                                YEAR(CAST(dataEmissao AS TIMESTAMP))       AS year,
                                WEEKDAY(CAST(dataEmissao AS TIMESTAMP))    AS weekday
                         FROM tab_cotas 
                         WHERE idDocumento != 0
                         GROUP BY dataEmissao
                         ORDER BY dataEmissao
                         ''' )
    
    # write tab_time to parquet files partitioned by year
    tab_time.write.partitionBy("year").mode('overwrite').parquet(os.path.join(output_path, 'tab_time'))
    

def load_company_table(spark, input_path, output_path):
    
    """
    Description: This function is responsible for reading 
    file(s) from the SOR layer to create the "tab_company" 
    dimension table, saving it in the SOT layer.
    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        
    Return:
        None
    """        
    
    # read input files
    df_empresas = spark.read.parquet(input_path + 'tab_empresas')
    df_naturezas = spark.read.parquet(input_path + 'tab_naturezas')

    # create temporary views
    df_empresas.createOrReplaceTempView("tab_empresas")
    df_naturezas.createOrReplaceTempView("tab_naturezas")
    
    # load tab_company 
    tab_company = spark.sql('''
                            SELECT DISTINCT CONCAT("J0", LPAD(te.cnpjBasico, 8, '0')) AS company_id,
                                            te.razaoSocial                            AS company_name,
                                            tn.descricaoNatureza                      AS company_classification
                            FROM tab_empresas AS te
                            LEFT JOIN tab_naturezas AS tn
                                   ON te.codigoNatureza = tn.codigoNatureza 
                            ORDER BY company_id
                            ''' )
    
    # write tab_company to parquet files
    tab_company.write.mode('overwrite').parquet(os.path.join(output_path, 'tab_company'))
    
    
def quality_check_rows(spark, input_path, output_path, tables):
    
    """
    Description: This function is responsible for 
    counting the number of rows in each table and 
    checking whether it is greater than zero. 
    At the end, a log file is saved.
    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        tables: list of tables.
        
    Return:
        None
    """        
    
    for table in tables:    

        # read input file      
        df = spark.read.parquet(input_path + table)

        # count rows from table      
        table_records = df.count()

        # do the check      
        if table_records > 0 :           
            text = f"Data quality succeeded. {table} with {table_records} records"
        else:
            text = f"Data quality check failed. {table} with {table_records} records"

        # create a dataframe to store text result      
        log = spark.createDataFrame([text], "string").toDF("log_message")

        # create temporary view
        log.createOrReplaceTempView("log")

        # load tab_log
        tab_log = spark.sql('''
                            SELECT 'Row'                AS check_type,
                                    log_message         AS log_message,
                                    current_timestamp() AS log_time
                            FROM log
                            ''' )    

        # write tab_log to parquet files (append if already exists)
        tab_log.write.mode('append').parquet(os.path.join(output_path, 'tab_log'))
    

def quality_check_null_keys(spark, input_path, output_path, tables):
    
    """
    Description: This function is responsible for 
    counting the number of rows in each table and 
    checking whether it is greater than zero. 
    At the end, a log file is saved.
    
    Arguments:
        spark: the spark session object.
        input_path: input file path.
        output_path: output file path.
        tables: list of tables.
        
    Return:
        None
    """        
    
    for table in tables:    

        # read input file      
        df = spark.read.parquet(input_path + table)

        # retrieve primary key name from the table              
        primary_key_name = df.columns[0]
    
        # count key null values rows from table      
        table_records = df.filter(df[0].isNull()).count()
                
        # do the check      
        if table_records == 0 :  
            text = f"Data quality succeeded. {table} with {table_records} null values on primary key {primary_key_name}"
        else:
            text = f"Data quality check failed. {table} with {table_records} null values on primary key {primary_key_name}"

        # create a dataframe to store text result      
        log = spark.createDataFrame([text], "string").toDF("log_message")

        # create temporary view
        log.createOrReplaceTempView("log")

        # load tab_log
        tab_log = spark.sql('''
                            SELECT 'Null Key'           AS check_type,
                                    log_message         AS log_message,
                                    current_timestamp() AS log_time
                            FROM log
                            ''' )    

        # write tab_log to parquet files (append if already exists)
        tab_log.write.mode('append').parquet(os.path.join(output_path, 'tab_log'))    
    
    
def main():
    
    """
    Description: This function is responsible for call the function for create
    spark session, set the input and output file paths, call the functions for 
    process song and log data.
    
    Arguments:
        None
        
    Return:
        None
    """            
    
    spark = create_spark_session()
    
    staging = 's3a://frans-udacity-studies/staging/'
    sor = 's3a://frans-udacity-studies/sor/'
    sot = 's3a://frans-udacity-studies/sot/'
    
    cotas_file = 'Ano-2017.json'
    empresas_file = 'K3241.K03200Y*.D21217.EMPRECSV'
    naturezas_file = 'F.K03200$Z.D21217.NATJUCSV'
    
    final_tables = ['tab_expense', 'tab_deputy', 'tab_time', 'tab_company']
    
    process_cotas_data(spark, staging, sor, cotas_file)    
    process_empresas_data(spark, staging, sor, empresas_file)
    process_naturezas_data(spark, staging, sor, naturezas_file)    

    load_expense_table(spark, sor, sot)    
    load_deputy_table(spark, sor, sot)
    load_time_table(spark, sor, sot)        
    load_company_table(spark, sor, sot) 
    
    quality_check_rows(spark, sot, sot, final_tables)
    quality_check_null_keys(spark, sot, sot, final_tables)

if __name__ == "__main__":
    main()
