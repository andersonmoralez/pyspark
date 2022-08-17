#!/usr/bin/env python3
# Copyright © 2021, Oracle and/or its affiliates. 
# The Universal Permissive License (UPL), Version 1.0 as shown at https://oss.oracle.com/licenses/upl.

import argparse
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, concat, lit

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--input-2-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    # Set up Spark.
    spark_session = get_dataflow_spark_session()
    #sql_context = SQLContext(spark_session)
    spark = SparkSession(spark_session)

    # Path Empresas
    path_empresas = args.input_path
    
    # Path Estabelecimentos
    path_estabelecimentos = args.input_2_path
    
    # Read Empresas
    df_empresas = spark\
        .read.format('csv')\
        .options(inferSchema=False, Header=False)\
        .option('quote', '"')\
        .option('delimiter', ';')\
        .option('encoding', 'iso-8859-1')\
        .csv(path_empresas)

    # Read Estabelecimentos
    df_estabelecimentos = spark\
        .read.format('csv')\
        .options(inferSchema=False, Header=False)\
        .option('quote', '"')\
        .option('delimiter', ';')\
        .option('encoding', 'iso-8859-1')\
        .csv(path_estabelecimentos)

    # Join
    df_razao_social = df_empresas.alias('emp').join(df_estabelecimentos.alias('est'), col('emp._c0') == col('est._c0'), 'inner')\
        .select(
            concat(col('est._c0'), col('est._c1'), col('est._c2')).alias('cnpj'),
            col('emp._c1').alias('nm_razao_social'),
            col('est._c4').alias('nm_fantasia'),
            lit('999').alias('cd_origem'),
            lit('dd/mm/aaaa').alias('dt_inclusao'),
            lit('dd/mm/aaaa').alias('dt_atualizacao'),
            lit('999').alias('cd_score')  
        )

    # Save the results as Parquet.
    #df_empresas.write.mode("overwrite").parquet(args.output_path)
    df_razao_social.coalesce(1).write.mode("overwrite").format("csv").save(args.output_path)

    # Show on the console that something happened.
    print("Successfully converted {} rows to Parquet and wrote to {}.".format(df_razao_social.count(), args.output_path))

def get_dataflow_spark_session(
    app_name="DataFlow", file_location=None, profile_name=None, spark_config={}
):
    """
    Get a Spark session in a way that supports running locally or in Data Flow.
    """
    if in_dataflow():
        spark_builder = SparkSession.builder.appName(app_name)
    else:
        # Import OCI.
        try:
            import oci
        except:
            raise Exception(
                "You need to install the OCI python library to test locally"
            )

        # Use defaults for anything unset.
        if file_location is None:
            file_location = oci.config.DEFAULT_LOCATION
        if profile_name is None:
            profile_name = oci.config.DEFAULT_PROFILE

        # Load the config file.
        try:
            oci_config = oci.config.from_file(
                file_location=file_location, profile_name=profile_name
            )
        except Exception as e:
            print("You need to set up your OCI config properly to run locally")
            raise e
        conf = SparkConf()
        conf.set("fs.oci.client.auth.tenantId", oci_config["tenancy"])
        conf.set("fs.oci.client.auth.userId", oci_config["user"])
        conf.set("fs.oci.client.auth.fingerprint", oci_config["fingerprint"])
        conf.set("fs.oci.client.auth.pemfilepath", oci_config["key_file"])
        conf.set(
            "fs.oci.client.hostname",
            "https://objectstorage.{0}.oraclecloud.com".format(oci_config["region"]),
        )
        spark_builder = SparkSession.builder.appName(app_name).config(conf=conf)

    # Add in extra configuration.
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # Create the Spark session.
    session = spark_builder.getOrCreate()
    return session


def in_dataflow():
    """
    Determine if we are running in OCI Data Flow by checking the environment.
    """
    if os.environ.get("HOME") == "/home/dataflow":
        return True
    return False


if __name__ == "__main__":
    main()