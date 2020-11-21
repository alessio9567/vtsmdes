
import pyspark.sql
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import datetime

sconf = SparkConf().setAppName("Autorizzativo_json_to_hive_visa")
sc = SparkContext.getOrCreate(conf=sconf)
sqlContext = HiveContext(sc)


# parametri connessione per la prod
sqlContext.setConf("hive.metastore.uris","thrift://tfbdaprnode04.carte.local:9083,thrift://tfbdaprnode05.carte.local:9083,thrift://tfbdaprnode06.carte.local:9083")
sqlContext.setConf("hive.zookeeper.quorum","tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local")
sqlContext.setConf("hbase.zookeeper.quorum","tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local")
sqlContext.setConf("zookeeper.znode.parent","/hbase")
sqlContext.setConf("spark.hbase.host","tfbdaprnode05.carte.local")

yesterday = sqlContext.sql("select date_add(current_date(),-1)").collect()[0][0].strftime("%Y-%m-%d")

trace_i_visatrace_orc_DF = sqlContext.table("autorizzativoprod.i_visatrace_orc")\
                                     .where( (col("time_stamp") == yesterday ) )

#DEFINISCO LO SCHEMA DELLA RIGA JSON

schema = StructType().\
    add("id", StringType()).\
    add("DE0_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE2_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE3_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE4_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE6_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE7_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE11_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE14_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE15_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE18_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE22_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE32_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE37_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE39_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE42_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE43_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE49_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE51_ss", StructType().\
        add("field_id", StringType()).\
        add("field_name", StringType()).\
        add("field_value", StringType())).\
    add("DE63_ss", StructType().\
        add("subfield", ArrayType(StructType().\
            add("field_id", StringType()).\
            add("field_name",StringType()).\
            add("field_value",StringType())))).\
    add("DE90_ss", StructType().\
        add("subfield", ArrayType(StructType().\
            add("field_id", StringType()).\
            add("field_name",StringType()).\
            add("field_value",StringType())))).\
    add("DE123_ss", StructType().\
        add("subfield", ArrayType(StructType().\
            add("field_id", StringType()).\
            add("field_name", StringType()).\
            add("field_value", StringType()).\
            add("field_tag", StringType())))).\
    add("DE125_ss", StructType().\
        add("subfield", ArrayType(StructType().\
            add("field_id", StringType()).\
            add("field_name", StringType()).\
            add("field_value", StringType()).\
            add("field_tag", StringType()))))

trace_i_visatrace_orc_DF = trace_i_visatrace_orc_DF.withColumn("id", monotonically_increasing_id())\
                                                   .withColumn("Year", substring(regexp_extract(from_json(col("json_field"),schema).id,"(.)(\d{2}-\d{2}-\d{2})(.)",2),1,2))\
                                                   .withColumn("subfield0", from_json(col("json_field"),schema).DE123_ss.subfield)\
                                                   .withColumn("subfield1", from_json(col("json_field"),schema).DE125_ss.subfield)\
                                                   .withColumn("subfield2", from_json(col("json_field"),schema).DE63_ss.subfield)\
                                                   .withColumn("subfield3", from_json(col("json_field"),schema).DE90_ss.subfield)\
                                                   .withColumn("Message_Type", from_json(col("json_field"),schema).DE0_ss.field_value)\
                                                   .withColumn("Primary_Account_Number", from_json(col("json_field"),schema).DE2_ss.field_value)\
                                                   .withColumn("Fpan_Date_Expiration", from_json(col("json_field"),schema).DE14_ss.field_value)\
                                                   .withColumn("Date_Settlement", from_json(col("json_field"),schema).DE15_ss.field_value)\
                                                   .withColumn("Month_Day_Hour_Minute_Seconds", from_json(col("json_field"),schema).DE7_ss.field_value)\
                                                   .withColumn("Response_Code", from_json(col("json_field"),schema).DE39_ss.field_value)\
                                                   .withColumn("Card_Acceptor_ID_Code", from_json(col("json_field"),schema).DE42_ss.field_value)\
                                                   .withColumn("Issuer_Amount_Currency", from_json(col("json_field"),schema).DE51_ss.field_value)\
                                                   .withColumn("Operation_Type", from_json(col("json_field"),schema).DE3_ss.field_value)\
                                                   .withColumn("PAN_Entry_Mode", from_json(col("json_field"),schema).DE22_ss.field_value)\
                                                   .withColumn("Issuer_Amount", from_json(col("json_field"),schema).DE6_ss.field_value)\
                                                   .withColumn("Acquirer_Amount_Currency", from_json(col("json_field"),schema).DE49_ss.field_value)\
                                                   .withColumn("Acquirer_Amount", from_json(col("json_field"),schema).DE4_ss.field_value)\
                                                   .withColumn("Retrieval_Reference_Number", from_json(col("json_field"),schema).DE37_ss.field_value)\
                                                   .withColumn("System_Trace_Audit_Number", from_json(col("json_field"),schema).DE11_ss.field_value)\
                                                   .withColumn("Merchant_Category_Code", from_json(col("json_field"),schema).DE18_ss.field_value)\
                                                   .withColumn("Acquiring_Institution_ID_Code", from_json(col("json_field"),schema).DE32_ss.field_value)\
                                                   .withColumn("Card_Acceptor_Name", from_json(col("json_field"),schema).DE43_ss.field_value)

trace_i_visatrace_orc_DF.createOrReplaceTempView("view1")

trace_i_visatrace_orc_DF = sqlContext.sql("with view2 as ( SELECT id,\
                                                                  Year,\
                                                                  Primary_Account_Number,\
                                                                  Fpan_Date_Expiration,\
                                                                  Date_Settlement,\
                                                                  Month_Day_Hour_Minute_Seconds,\
                                                                  Response_Code,\
                                                                  Card_Acceptor_ID_Code,\
                                                                  Issuer_Amount_Currency,\
                                                                  Operation_Type,\
                                                                  PAN_Entry_Mode,\
                                                                  Issuer_Amount,\
                                                                  Message_Type,\
                                                                  Acquirer_Amount_Currency,\
                                                                  Acquirer_Amount,\
                                                                  Retrieval_Reference_Number,\
                                                                  System_Trace_Audit_Number,\
                                                                  Merchant_Category_Code,\
                                                                  Acquiring_Institution_ID_Code,\
                                                                  Card_Acceptor_Name,\
                                                                  b.field_id as field_id0,b.field_name as field_name0,b.field_value as field_value0,b.field_tag as field_tag0,\
                                                                  c.field_id as field_id1,c.field_name as field_name1,c.field_value as field_value1,c.field_tag as field_tag1,\
                                                                  d.field_id as field_id2,d.field_name as field_name2,d.field_value as field_value2 \
                                                           FROM view1 a \
                                                           LATERAL VIEW OUTER inline(a.subfield0) b \
                                                           LATERAL VIEW OUTER inline(a.subfield1) c \
                                                           LATERAL VIEW OUTER inline(a.subfield2) d ),\
                                          view3 as ( SELECT id,\
                                                            Year,\
                                                            Primary_Account_Number,\
                                                            Fpan_Date_Expiration,\
                                                            Date_Settlement,\
                                                            Month_Day_Hour_Minute_Seconds,\
                                                            Response_Code,\
                                                            Card_Acceptor_ID_Code,\
                                                            Issuer_Amount_Currency,\
                                                            Operation_Type,\
                                                            Message_Type,\
                                                            PAN_Entry_Mode,\
                                                            Issuer_Amount,\
                                                            Acquirer_Amount_Currency,\
                                                            Acquirer_Amount,\
                                                            Retrieval_Reference_Number,\
                                                            System_Trace_Audit_Number,\
                                                            Merchant_Category_Code,\
                                                            Acquiring_Institution_ID_Code,\
                                                            Card_Acceptor_Name,\
                                                            CASE WHEN field_tag0='01' THEN field_value0 end as Token,\
                                                            CASE WHEN field_tag0='03' THEN field_value0 end as Token_Requestor_id,\
                                                            CASE WHEN field_tag0='05' THEN field_value0 end as Token_Id,\
                                                            CASE WHEN field_tag0='06' THEN field_value0 end as Token_Expiry_Date,\
                                                            CASE WHEN field_tag0='07' THEN field_value0 end as Token_Type,\
                                                            CASE WHEN field_tag0='08' THEN field_value0 end as Token_State,\
                                                            CASE WHEN field_tag1='01' THEN field_value1 end as Wallet_Provider_Risk_Assessment,\
                                                            CASE WHEN field_tag1='03' THEN field_value1 end as Device_Id,\
                                                            CASE WHEN field_id2='1'   THEN field_value2 end as Network_Data,\
                                                            CASE WHEN field_id2='3'   THEN field_value2 end as Message_Reason_Code\
                                                     FROM view2)\
                                          SELECT 'VISA' as Circuit,\
                                                  max(Token_Id) as Token_Id,\
                                                  max(Token_Expiry_Date) as Token_Expiry_Date,\
                                                  max(Primary_Account_Number) as Primary_Account_Number,\
                                                  max(Fpan_Date_Expiration) as Fpan_Date_Expiration,\
                                                  max(Token_Requestor_id) as Token_Requestor_id,\
                                                  max(Token_Type) as Token_Type,\
                                                  max(Token_State) as Token_State,\
                                                  max(concat('20',Year,'-',concat(substr(Month_Day_Hour_Minute_Seconds,1,2),'-',\
                                                                                  substr(Month_Day_Hour_Minute_Seconds,3,2),' ',\
                                                                                  substr(Month_Day_Hour_Minute_Seconds,5,2),':',\
                                                                                  substr(Month_Day_Hour_Minute_Seconds,7,2),':',\
                                                                                  substr(Month_Day_Hour_Minute_Seconds,9,2),'.','000'))) as Token_Timestamp,\
                                                  max(Response_Code) as Response_Code,\
                                                  max(Wallet_Provider_Risk_Assessment) as Wallet_Provider_Risk_Assessment,\
                                                  max(Device_Id) as Device_Id,\
                                                  concat(max(Token_Id),max(Token_Requestor_id)) as Correlation_Id,\
                                                  max(Operation_Type) as Operation_Type,\
                                                  max(PAN_Entry_Mode) as PAN_Entry_Mode,\
                                                  max(Token) as Token,\
                                                  max(Card_Acceptor_ID_Code) as Card_Acceptor_ID_Code,\
                                                  max(Card_Acceptor_Name) as Card_Acceptor_Name,\
                                                  max(Merchant_Category_Code) as Merchant_Category_Code,\
                                                  max(Acquiring_Institution_ID_Code) as Acquiring_Institution_ID_Code,\
                                                  max(System_Trace_Audit_Number) as System_Trace_Audit_Number,\
                                                  max(Retrieval_Reference_Number) as Retrieval_Reference_Number,\
                                                  max(Acquirer_Amount) as Acquirer_Amount,\
                                                  max(Acquirer_Amount_Currency) as Acquirer_Amount_Currency,\
                                                  max(Issuer_Amount) as Issuer_Amount,\
                                                  max(Issuer_Amount_Currency) as Issuer_Amount_Currency,\
                                                  max(Message_Reason_Code) as Message_Reason_Code,\
                                                  max(Network_Data) as Network_Data,\
                                                  max(Date_Settlement) as Date_Settlement,\
                                                  max(Message_Type) as Message_Type,\
                                                  concat(substring(current_date(),1,4),substring(current_date(),6,2),substring(current_date(),9,2)) as Elaboration_Date \
                                                  FROM view3 \
                                                  GROUP BY id")

trace_i_visatrace_orc_DF = trace_i_visatrace_orc_DF.withColumn( "Token_Date" , substring(col("Token_Timestamp"),1,10) )

trace_i_visatrace_orc_DF.write\
                        .mode("append")\
                        .partitionBy( "Token_Date" )\
                        .format("orc")\
                        .saveAsTable("autorizzativoprod.trace_k1_visatrace_orc")

