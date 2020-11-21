export SPARK_MAJOR_VERSION=2
PYSPARK_DRIVER_PYTHON=ipython pyspark \
--executor-memory 8G \
--executor-cores 4 \
--num-executors 4 \
--driver-memory 8G
---------------------------------------
import pyspark.sql
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import datetime

sconf = SparkConf().setAppName("ETL_l_report_gp001_visa") 
sc = SparkContext.getOrCreate(conf=sconf)
sqlContext = HiveContext(sc)

now1 = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

sqlContext.setConf("hive.metastore.uris","thrift://tfbdadrnode04.carte.local:9083,thrift://tfbdadrnode05.carte.local:9083,thrift://tfbdadrnode06.carte.local:9083")
sqlContext.setConf("hive.zookeeper.quorum","tfbdadrnode01.carte.local,tfbdadrnode02.carte.local,tfbdadrnode03.carte.local")
sqlContext.setConf("hbase.zookeeper.quorum","tfbdadrnode01.carte.local,tfbdadrnode02.carte.local,tfbdadrnode03.carte.local")
sqlContext.setConf("zookeeper.znode.parent","/hbase")
sqlContext.setConf("spark.hbase.host","tfbdadrnode05.carte.local")
sqlContext.setConf("spark.sql.sources.partitionOverwriteMode","DYNAMIC")


def stripBlanks(field_to_be_stripped):
    if (field_to_be_stripped is None):
        resultField = ''
    else:
        resultField = field_to_be_stripped.strip()
    return (resultField)

stpBlanks = udf(stripBlanks, StringType())


## CREATE MAIN DATAFRAMES & FILTER

l_issuer = sqlContext.table("anagrafe.l_issuer").cache()

l_wallet_provider = sqlContext.table("anagrafe.l_wallet_provider").where( col("token_requestor_name") == 'Google Pay' ).cache()

l_issuer_group_per_report = sqlContext.table("anagrafe.l_issuer_group_per_report").where( col("id") == 'GP001' ).cache()

l_issuer_per_wallet_provider = sqlContext.table("anagrafe.l_issuer_per_wallet_provider").cache()

fp_message = sqlContext.table("vtsmdes.l_clearing_transaction_visa")\
                       .where( col("message_type_identifier") == '1240')\
					   .select( col("transaction_date"),
                                col("issuer_code"),
                                col("bin_code"),
                                col("token_requestor_id"),
                                col("c_dv_rcnc"),
                                col("p_exp_rcnc"),
                                col("funding_pan"),
                                col("arn"),
                                col("trace_id"),                                                                         
                                col("v_rcnc")).cache()

fp_message = fp_message.alias('fp_message')

cb_message = sqlContext.table("vtsmdes.l_clearing_transaction_visa")\
                       .where( (col("message_type_identifier") == '1442') & (col("fraud_flag") == 'Y') & (col("mdes_flag") == 'Y') ).cache()

cb_message = cb_message.alias('cb_message')

## TROVIAMO IL VALORE MASSIMO ATTUALE INSERITO IN TABELLA REPORT FINALE

#l_report_gp001 = sqlContext.table("vtsmdes.l_report_gp001")

#try:
#    TMP_maxdate_l_report_gp001 = l_report_gp001\
#    .select(lit(l_report_gp001["year_of_last_elaboration_date"] * 10000 + l_report_gp001["month_of_last_elaboration_date"] * 100 + l_report_gp001["day_of_last_elaboration_date"]).alias("base_maxdate"))\
#    .agg(max("base_maxdate").alias("base_maxdate"))
#
#    maxDate = TMP_maxdate_l_report_gp001.first()
#except:
#    maxDate = None

#if (maxDate[0] == None):
#    max_insert = 0
#else:
#    max_insert = maxDate[0]


# JOIN 

l_issuer = l_issuer.join( l_issuer_group_per_report, col("l_issuer.group_code") == col("l_issuer_group_per_report.group_code"), how = "inner" )
l_issuer = l_issuer.alias("l_issuer")

join_condition = [ (fp_message.funding_pan == cb_message.funding_pan) & \
                   (fp_message.arn == cb_message.arn) & \
                   (fp_message.trace_id == cb_message.trace_id) & \
                   (fp_message.v_rcnc == cb_message.v_rcnc)]

l_report_gp001_new = fp_message.join( cb_message, join_condition , how="left_outer" )\
                               .join( l_issuer, col("fp_message.issuer_code") == col("l_issuer.issuer_code") , how="inner" )\
                               .join( l_issuer_per_wallet_provider, col("fp_message.issuer_code") == col("l_issuer_per_wallet_provider.issuer_code") ,how="inner" )\
                               .join( l_wallet_provider, col("fp_message.token_requestor_id").cast("bigint") == col("l_wallet_provider.token_requestor_id") , how="inner" )\
                               .where( (col("fp_message.transaction_date") >= col("l_issuer_per_wallet_provider.start_attivation")) & \
                                       (col("fp_message.transaction_date") <= col("l_issuer_per_wallet_provider.end_attivation")) )\
                               .groupby( date_format( col("fp_message.transaction_date"), "yyyyMM" ).alias("aggr_transaction_date"), \
                                        col("fp_message.issuer_code"), \
                                        col("fp_message.bin_code"), \
                                        col("l_issuer.issuer_name"), \
                                        col("fp_message.token_requestor_id"), \
                                        col("l_wallet_provider.token_requestor_name"), \
                                        col("fp_message.c_dv_rcnc"), \
                                        col("fp_message.p_exp_rcnc"))\
                               .agg( (sum(coalesce(col("cb_message.v_rcnc"),lit(0))) / pow(lit(10), col("fp_message.p_exp_rcnc") ) ).alias("reconciliation_amount"),\
                                      coalesce(count(col("cb_message.v_rcnc")),lit(0)).alias("total_transaction"),\
                                      max(col("fp_message.transaction_date")).alias("last_elaboration_date"))\
                               .select( col("aggr_transaction_date").alias("month_of_report"),\
                                        col("fp_message.issuer_code"),\
                                        col("fp_message.bin_code"),\
                                        col("l_issuer.issuer_name").alias("bin_name"),\
                                        col("fp_message.token_requestor_id"),\
                                        col("l_wallet_provider.token_requestor_name"),\
                                        col("fp_message.c_dv_rcnc").alias("reconciliation_currency_code"),\
                                        col("fp_message.p_exp_rcnc").alias("reconciliation_currency_code_exponents"),\
                                        col("reconciliation_amount"),\
                                        col("total_transaction"),\
                                        col("last_elaboration_date"))\
                               .withColumn( "year_of_last_elaboration_date" ,  year(col("last_elaboration_date")).cast("int"))\
                               .withColumn( "month_of_last_elaboration_date",  month(col("last_elaboration_date")).cast("int"))\
                               .withColumn( "day_of_last_elaboration_date"  ,  substring(col("last_elaboration_date"),9,10).cast("int"))

#l_report_gp001_new = l_report_gp001_new.where(                   \
#lit(l_report_gp001_new['year_of_last_elaboration_date'] * 10000 + \
#l_report_gp001_new['month_of_last_elaboration_date'] * 100 + \
#l_report_gp001_new['day_of_last_elaboration_date']) > max_insert )


l_report_gp001_new = l_report_gp001_new.select(\
col("month_of_report"),                         \
col("issuer_code"),                              \
col("bin_code"),                                  \
col("bin_name"),                                   \
col("token_requestor_id"),                          \
col("token_requestor_name"),                         \
col("reconciliation_currency_code"),                  \
col("reconciliation_currency_code_exponents"),         \
col("reconciliation_amount"),                           \
col("total_transaction"),                                \
col("year_of_last_elaboration_date"),                     \
col("month_of_last_elaboration_date"),                     \
col("day_of_last_elaboration_date"))


#APPEND FINALE

l_report_gp001_new.write.mode("overwrite").format("orc").saveAsTable("vtsmdes.l_report_gp001_visa")

now2 = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
hours_elapsed=int(now2[8:10])-int(now1[8:10])
minutes_elapsed=int(now2[10:12])-int(now1[10:12])
seconds_elapsed=int(now2[12:14])-int(now1[12:14])
total_time_elapsed= hours_elapsed*3600 + minutes_elapsed*60 + seconds_elapsed
print( 'Time taken: ' + str(total_time_elapsed) + ' seconds' )


