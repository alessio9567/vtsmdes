export SPARK_MAJOR_VERSION=2
PYSPARK_DRIVER_PYTHON=ipython pyspark \
--executor-memory 8G \
--executor-cores 4 \
--num-executors 4 \
--driver-memory 8G
#---------------------------------------
import pyspark.sql
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import datetime

sconf = SparkConf().setAppName("ETL_l_clearing_transaction") 
sc = SparkContext.getOrCreate(conf=sconf)
sqlContext = HiveContext(sc)

now1 = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

sqlContext.setConf("hive.metastore.uris","thrift://tfbdadrnode04.carte.local:9083,thrift://tfbdadrnode05.carte.local:9083,thrift://tfbdadrnode06.carte.local:9083")
sqlContext.setConf("hive.zookeeper.quorum","tfbdadrnode01.carte.local,tfbdadrnode02.carte.local,tfbdadrnode03.carte.local")
sqlContext.setConf("hbase.zookeeper.quorum","tfbdadrnode01.carte.local,tfbdadrnode02.carte.local,tfbdadrnode03.carte.local")
sqlContext.setConf("zookeeper.znode.parent","/hbase")
sqlContext.setConf("spark.hbase.host","tfbdadrnode05.carte.local")
sqlContext.setConf("spark.sql.sources.partitionOverwriteMode","DYNAMIC")


def decodeClearingTable1(field):
    if (field in ('4837','4840','4849','4863','4870','4871')):
        decode = 'Y'
    else:
        decode = 'N'
    return (decode)

decClearing1 = udf(decodeClearingTable1, StringType())


def decodeClearingTable2(field):
    if (field in ('5','6','7','8')):
        decode = 'Y'
    else:
        decode = 'N'
    return (decode)

decClearing2 = udf(decodeClearingTable2, StringType())

def decodeClearingTable3(field):
    if (field == '07'):
        decode = 'Y'
    else:
        decode = 'N'
    return (decode)

decClearing3 = udf(decodeClearingTable3, StringType())


def decodeClearingTable4(L00393):
    if (L00393 in ('ALL SPACE','LOW-VALUE','ZEROE')):
        decode = 'N'
    else:
        decode = 'Y'
    return (decode)

decClearing4 = udf(decodeClearingTable4, StringType())

def decodeClearingTable5(L00393):
    if (L00393 in ('ALL SPACE' ,'LOW-VALUE','ZEROE')):
        decode = 'ALL-SPACE'
    else:
        decode = L00393
    return (decode)

decClearing5 = udf(decodeClearingTable5, StringType())

def decodeClearingTable6(field):
    if (field == '978'):
        decode = '2'
    else:
        decode = None
    return (decode)

decClearing6 = udf(decodeClearingTable6, StringType())

def stripBlanks(field_to_be_stripped):
    if (field_to_be_stripped is None):
        resultField = ''
    else:
        resultField = field_to_be_stripped.strip()
    return (resultField)

stpBlanks = udf(stripBlanks, StringType())



## CREATE MAIN DATAFRAMES & FILTER

yesterday = sqlContext.sql("select date_add(current_date(),-1)").collect()[0][0].strftime("%Y-%m-%d")

cb_message = sqlContext.table("clearing.e05_visa_clearing_incoming")\
                       .where( (col("l00045") == '1') & (col("l00016").isin('15','16','17')) )\
                       .cache()
                   #   .where( (col("year") == yesterday[0:4] ) & \
                   #           (col("month") == yesterday[5:7] ) & \
                   #           (col("day") == yesterday[8:10] ) )

cb_message = cb_message.alias("cb_message")

#aggiungere where sui ultimi 5 giorni della transaction_date
fp_message = sqlContext.table("vtsmdes.l_clearing_transaction_visa")\
                       .where( (col("message_type_identifier") == '1240'))\
                       .select( col("dpan"),\
                                col("trace_id"),\
								col("mdes_flag"),\
                                col("token_assur_lev"),\
                                col("token_requestor_id"),\
                                col("v_rcnc"),\
                                col("c_dv_rcnc"),\
                                col("bin_code"),\
                                col("p_exp_rcnc"),\
                                col("arn"))\
                       .cache()                                         

fp_message = fp_message.alias("fp_message")

l_bin = sqlContext.table("anagrafe.l_bin_exploded_visa")

l_merchant_category_group = sqlContext.table("anagrafe.l_merchant_category_group")\
                                      .where(col("circuit") == 'VISA' )\
                                      .cache()

l_merchant_category = sqlContext.table("anagrafe.l_merchant_category")\
                                .cache()



# JOIN 

# da aggiungere le altre 2 condizioni
join_condition = [ (concat(cb_message.l00026,cb_message.l00027,cb_message.l00028,cb_message.l00029,cb_message.l00030 ) == \
                    fp_message.arn)] 

l_merchant_category = l_merchant_category.join( l_merchant_category_group, col("mastercard_mc_group_code") == col("mc_group_code") , how ="inner")

##col("l00196").alias("token_requestor_id"),\
l_clearing_transaction_new = cb_message.join( l_merchant_category, col("l00040") == col("mc_code"),how="inner")\
                                       .join( l_bin, substring(rpad(col("l00019"),19,'0'),1,8).cast("bigint") ==  col("c_pan_visa").cast("bigint")  , how="inner")\
                                       .join( fp_message, join_condition, how="inner")\
                                       .select( lit('VISA').alias("circuit"),\
                                                substring(col("cb_message.circuito"),1,2).alias("source_platform"),\
                                                lit('1442').alias("message_type_identifier"),\
                                                lit(None).alias("abi_code"),\
                                                col("cb_message.l00004").alias("issuer_code"),\
                                                substring(col("cb_message.l00019"),1,6).alias("bin_code"),\
                                                stpBlanks(col("cb_message.l00019")).alias("funding_pan"),\
                                                col("c_tp_addbt").alias("card_type"),\
                                                stpBlanks(concat(col("cb_message.l00026"),col("cb_message.l00027"),col("cb_message.l00028"),col("cb_message.l00029"),col("cb_message.l00030"))).alias("arn"),\
                                                stpBlanks(col("cb_message.l00328")).alias("trace_id"),\
                                                col("cb_message.l00046").alias("reason_code"),\
                                                stpBlanks(decClearing1(col("cb_message.l00046"))).alias("fraud_flag"),\
                                                col("fp_message.mdes_flag").alias("mdes_flag"),\
                                                col("fp_message.dpan").alias("dpan"),\
                                                col("fp_message.token_assur_lev").alias("token_assur_lev"),\
                                                lit(50120834693).alias("token_requestor_id"),\
                                                decClearing2(col("cb_message.l00074")).alias("remote_trx_flag"),\
                                                decClearing3(col("cb_message.l00054")).alias("nfc_technology_flag"),\
                                                col("cb_message.l00054").alias("pos_entry_mode"),\
                                                col("cb_message.l00072").alias("cod_terminale"),\
                                                col("cb_message.l00071").alias("cod_esercente"),\
                                                col("cb_message.l00037").alias("merchant_name"),\
                                                lit(None).cast("string").alias("merchant_address"),\
                                                col("cb_message.l00038").alias("merchant_city"),\
                                                col("cb_message.l00039").alias("merchant_country"),\
                                                col("cb_message.l00042").alias("merchant_type"),\
                                                col("mastercard_mc_group_code").alias("mc_group_code"),\
                                                col("mc_description"),\
                                                col("fp_message.v_rcnc").alias("v_rcnc"),\
                                                col("fp_message.c_dv_rcnc").alias("c_dv_rcnc"),\
                                                col("fp_message.p_exp_rcnc").alias("p_exp_rcnc"),\
                                                lit('I').alias("c_activity"),\
                                                concat(col("cb_message.circuito"),col("cb_message.l00022"),col("cb_message.ticket")).alias("clearing_file_name"),\
                                                concat(substring(col("cb_message.import_timestamp"),1,5),substring(col("cb_message.l00032"),1,2),lit('-'),substring(col("cb_message.l00032"),3,2)).alias("transaction_date"))\

						
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


l_clearing_transaction_new.sortWithinPartitions( col('clearing_file_name'),\
                                                 col('transaction_date'))
#da modificare per tutti e 4 i clearing
l_clearing_transaction_new.write\
                          .mode("append")\
						  .format("orc")\
						  .partitionBy("transaction_date")\
						  .saveAsTable("vtsmdes.LV_l_clearing_transaction_visa")

now2 = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
hours_elapsed=int(now2[8:10])-int(now1[8:10])
minutes_elapsed=int(now2[10:12])-int(now1[10:12])
seconds_elapsed=int(now2[12:14])-int(now1[12:14])
total_time_elapsed= hours_elapsed*3600 + minutes_elapsed*60 + seconds_elapsed
print( 'Time taken: ' + str(total_time_elapsed) + ' seconds' )


