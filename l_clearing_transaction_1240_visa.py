
import pyspark.sql
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import datetime

sconf = SparkConf().setAppName("ETL_l_clearing_transaction_visa") 
sc = SparkContext.getOrCreate(conf=sconf)
sqlContext = HiveContext(sc)


# parametri connessione per il test
sqlContext.setConf("hive.metastore.uris","thrift://tfbdadrnode04.carte.local:9083,thrift://tfbdadrnode05.carte.local:9083,thrift://tfbdadrnode06.carte.local:9083")
sqlContext.setConf("hive.zookeeper.quorum","tfbdadrnode01.carte.local,tfbdadrnode02.carte.local,tfbdadrnode03.carte.local")
sqlContext.setConf("hbase.zookeeper.quorum","tfbdadrnode01.carte.local,tfbdadrnode02.carte.local,tfbdadrnode03.carte.local")
sqlContext.setConf("zookeeper.znode.parent","/hbase")
sqlContext.setConf("spark.hbase.host","tfbdadrnode05.carte.local")

# parametri connessione per la prod
#sqlContext.setConf("hive.metastore.uris","thrift://tfbdaprnode04.carte.local:9083,thrift://tfbdaprnode05.carte.local:9083,thrift://tfbdaprnode06.carte.local:9083")
#sqlContext.setConf("hive.zookeeper.quorum","tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local")
#sqlContext.setConf("hbase.zookeeper.quorum","tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local")
#sqlContext.setConf("zookeeper.znode.parent","/hbase")
#sqlContext.setConf("spark.hbase.host","tfbdaprnode05.carte.local")


def decodeClearingTable1(field):
    if (field in ('ALL SPACE','LOW-VALUE','ZEROE')):
        decode = 'N'
    else:
        decode = 'Y'
    return (decode)

decClearing1 = udf(decodeClearingTable1, StringType())

def decodeClearingTable2(field):
    if (field == '978'):
        decode = '2'
    else:
        decode = None
    return (decode)

decClearing2 = udf(decodeClearingTable2, StringType())

def decodeClearingTable3(field):
    if (field in ('5','6','7','8')):
        decode = 'Y'
    else:
        decode = 'N'
    return (decode)

decClearing3 = udf(decodeClearingTable3, StringType())


def decodeClearingTable4(L00255):
    if (L00255 in ('ALL SPACE' ,'LOW-VALUE','ZEROE')):
        decode = 'ALL-SPACE'
    else:
        decode = L00255
    return (decode)

decClearing4 = udf(decodeClearingTable4, StringType())


def decodeClearingTable5(field):
    if (field == '07'):
        decode = 'Y'
    else:
        decode = 'N'
    return (decode)

decClearing5 = udf(decodeClearingTable5, StringType())

def stripBlanks(field_to_be_stripped):
    if (field_to_be_stripped is None):
        resultField = ''
    else:
        resultField = field_to_be_stripped.strip()
    return (resultField)

stpBlanks = udf(stripBlanks, StringType())

## CREATE MAIN DATAFRAMES & FILTER

e05p_visa_clearing_incoming = sqlContext.table("clearing.e05p_visa_clearing_incoming")\
                                        .where( (col("day") <> 'day') & (col("l00045") == '1') & (col("l00016").isin('05','06','07')) & \
                                                (col("l00255").isNotNull()) & (col("l00196").isNotNull()))\
                                        .cache()

l_issuer = sqlContext.table("anagrafe.l_issuer").cache()

l_bin = sqlContext.table("anagrafe.l_bin_exploded_visa")

l_merchant_category_group = sqlContext.table("anagrafe.l_merchant_category_group").where(col("circuit") == 'VISA' ).cache()

l_merchant_category = sqlContext.table("anagrafe.l_merchant_category").cache()

## TROVIAMO IL VALORE MASSIMO ATTUALE INSERITO IN TABELLA REPORT FINALE

#l_clearing_transaction = sqlContext.table("vtsmdes.l_clearing_transaction")\
#.where( (col("message_type_identifier") == '1240') & (col("circuit") == 'MAST') )\
#.withColumn("year_of_transaction_date",year(col("transaction_date")).cast("int"))\
#.withColumn("month_of_transaction_date",month(col("transaction_date")).cast("int"))\
#.withColumn("day_of_transaction_date",substring(col("transaction_date"),9,10).cast("int"))\
#.cache()

#try:
#    TMP_maxdate_l_clearing_transaction = l_clearing_transaction\
#    .select(lit(l_clearing_transaction["year_of_transaction_date"] * 10000 + l_clearing_transaction["month_of_transaction_date"] * 100 + l_clearing_transaction["day_of_transaction_date"]).alias("base_maxdate"))\
#    .agg(max("base_maxdate").alias("base_maxdate"))
#
#    maxDate = TMP_maxdate_l_clearing_transaction.first()
#except:
#    maxDate = None


#if (maxDate[0] == None):
#    max_insert = 0
#else:
#    max_insert = maxDate[0]


#JOIN 

l_merchant_category = l_merchant_category.join( l_merchant_category_group, col("visa_mc_group_code") == col("mc_group_code") , how ="inner")


##col("l00196").alias("token_requestor_id"),\
l_clearing_transaction_new = e05p_visa_clearing_incoming.join(   l_merchant_category, col("l00040") == col("mc_code") , how="inner" )\
                                                        .join(   l_issuer, col("l00004") == col("issuer_code") , how="inner" )\
                                                        .join(   l_bin, substring(rpad(col("l00019"),19,'0'),1,8).cast("bigint") ==  col("c_pan_visa").cast("bigint")  , how="inner" )\
                                                        .select( lit('VISA').alias("circuit"),\
                                                                 substring(col("circuito"),1,2).alias("source_platform"),\
                                                                 lit('1240').alias("message_type_identifier"),\
                                                                 lit(None).cast("string").alias("abi_code"),\
                                                                 col("l00004").alias("issuer_code"),\
                                                                 substring(col("l00019"),1,6).alias("bin_code"),\
                                                                 stpBlanks(col("l00019")).alias("funding_pan"),\
                                                                 col("c_tp_addbt").alias("card_type"),\
                                                                 stpBlanks(concat(col("l00026"),col("l00027"),col("l00028"),col("l00029"),col("l00030"))).alias("arn"),\
                                                                 stpBlanks(col("l00328")).alias("trace_id"),\
                                                                 col("l00046").alias("reason_code"),\
                                                                 lit('N').alias("fraud_flag"),\
                                                                 decClearing1(col("l00255")).alias("mdes_flag"),\
                                                                 decClearing4(col("l00255")).alias("dpan"),\
                                                                 col("l00062").alias("token_assur_lev"),\
                                                                 lit(50120834693).alias("token_requestor_id"),\
                                                                 decClearing3(col("l00074")).alias("remote_trx_flag"),\
                                                                 decClearing5(col("l00054")).alias("nfc_technology_flag"),\
                                                                 col("l00054").alias("pos_entry_mode"),\
                                                                 col("l00072").alias("cod_terminale"),\
                                                                 col("l00071").alias("cod_esercente"),\
                                                                 col("l00037").alias("merchant_name"),\
                                                                 lit(None).cast("string").alias("merchant_address"),\
                                                                 col("l00038").alias("merchant_city"),\
                                                                 col("l00039").alias("merchant_country"),\
                                                                 col("l00042").alias("merchant_type"),\
                                                                 col("mastercard_mc_group_code").alias("mc_group_code"),\
                                                                 col("mc_description").alias("mc_description"),\
                                                                 col("l00033").alias("v_rcnc"),\
                                                                 col("l00034").alias("c_dv_rcnc"),\
                                                                 decClearing2(col("l00034")).alias("p_exp_rcnc"),\
                                                                 lit('I').alias("c_activity"),\
                                                                 concat(col("circuito"),col("l00032"),col("ticket")).alias("clearing_file_name"),\
                                                                 concat(substring(col("import_timestamp"),1,5),substring(col("l00032"),1,2),lit('-'),substring(col("l00032"),3,2)).alias("transaction_date"))\
                                                                 .withColumn("year_of_transaction_date",year(col("transaction_date")).cast("int"))\
                                                                 .withColumn("month_of_transaction_date",month(col("transaction_date")).cast("int"))\
                                                                 .withColumn("day_of_transaction_date",substring(col("transaction_date"),9,10).cast("int"))\
                                                                 .cache()


#l_clearing_transaction_new = l_clearing_transaction_new.where( \
#lit(l_clearing_transaction_new['year_of_transaction_date'].cast('int') * 10000 + \
#l_clearing_transaction_new['month_of_transaction_date'].cast('int') * 100 + \
#l_clearing_transaction_new['day_of_transaction_date'].cast('int')) > max_insert )

l_clearing_transaction_new = l_clearing_transaction_new.select( col('source_platform'),\
                                                                col('message_type_identifier'),\
                                                                col('abi_code'),\
                                                                col('issuer_code'),\
                                                                col('bin_code'),\
                                                                col('funding_pan'),\
                                                                col('card_type'),\
                                                                col('arn'),\
                                                                col('trace_id'),\
                                                                col('reason_code'),\
                                                                col('fraud_flag'),\
                                                                col('mdes_flag'),\
                                                                col('dpan'),\
                                                                col('token_assur_lev'),\
                                                                col('token_requestor_id'),\
                                                                col('remote_trx_flag'),\
                                                                col('nfc_technology_flag'),\
                                                                col('pos_entry_mode'),\
                                                                col('cod_terminale'),\
                                                                col('cod_esercente'),\
                                                                col('merchant_name'),\
                                                                col('merchant_address'),\
                                                                col('merchant_city'),\
                                                                col('merchant_country'),\
                                                                col('merchant_type'),\
                                                                col('mc_group_code'),\
                                                                col('mc_description'),\
                                                                col('v_rcnc'),\
                                                                col('c_dv_rcnc'),\
                                                                col('p_exp_rcnc'),\
                                                                col('c_activity'),\
                                                                col('circuit'),\
                                                                col('clearing_file_name'),\
                                                                col('transaction_date'))

l_clearing_transaction_new.sortWithinPartitions( col('transaction_date'))

l_clearing_transaction_new.write.mode("overwrite").format("orc").saveAsTable("vtsmdes.l_clearing_transaction_visa")

now2 = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
hours_elapsed=int(now2[8:10])-int(now1[8:10])
minutes_elapsed=int(now2[10:12])-int(now1[10:12])
seconds_elapsed=int(now2[12:14])-int(now1[12:14])
total_time_elapsed= hours_elapsed*3600 + minutes_elapsed*60 + seconds_elapsed
print( 'Time taken: ' + str(total_time_elapsed) + ' seconds' )


