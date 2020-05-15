#pip install pyspark, pandas, xlrd, openpyxl
from pyspark.sql import SparkSession
import pandas, sys, logging, configparser, os, json
from datetime import datetime
from openpyxl import load_workbook

def readfromexcel(path, sheet_name):
    df = pandas.read_excel(path,sheet_name=sheet_name,inferSchema='').astype(str)
    return spark.createDataFrame(df)

def createspark(app_name):
    spark = SparkSession.builder.master("local").appName(app_name).getOrCreate()
    return spark

def loadconfig(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def writetoexcel(df, full_path, sheet_name):
    writer = pandas.ExcelWriter(full_path, engine = 'openpyxl') 
    if os.path.exists(full_path):
        book = load_workbook(full_path)
        writer.book = book
    df.to_excel(writer, sheet_name=sheet_name)
    writer.save()
    writer.close()

if __name__ == '__main__':
    ##### Initializing logger and variables
    config_file = sys.argv[1]
    #config_file = 'config.txt'
    config = loadconfig(config_file)
    vars = config._sections['GLOBAL']
    vars['input_file_1_name'] = vars['input_file_1'][:-5]
    vars['input_file_2_name'] = vars['input_file_2'][:-5]
    vars['logfilename'] = vars['root_path'] + 'logs/log_'+datetime.now().strftime("%Y%m%d%H%M%S")+'.log'
    os.makedirs(os.path.dirname(vars['logfilename']), exist_ok=True)
    logging.basicConfig(filename=vars['logfilename'],filemode='a',format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',level=logging.INFO)
    logger = logging.getLogger('main')
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.info("Logger Initialized and Config file loaded")
    logger.info("All variables initialized:")
    logger.info(json.dumps(vars, indent=4))
    
    #### Creating spark session
    spark = createspark('Master')
    logger.info("Spark session initialized")

    ### Reading data from input_1
    logger.info("Reading from {}".format(vars['input_file_1']))
    df_input_1 = readfromexcel(vars['root_path']+vars['input_file_1'], vars['input_file_1_sheet_name'])
    df_input_1.createOrReplaceTempView("df_input_1_T")
    df_input_1.cache()
    logger.info("Count {}".format(df_input_1.count()))
    
    ### Reading data frome input_2
    logger.info("Reading from {}".format(vars['input_file_2']))
    df_input_2 = readfromexcel(vars['root_path']+vars['input_file_2'], vars['input_file_2_sheet_name'])
    df_input_2.createOrReplaceTempView("df_input_2_T")
    df_input_2.cache()
    logger.info("Count {}".format(df_input_2.count()))
    #sys.exit()
    #### Creating dataframe with name field join
    logger.info("Joining using Name exact match")
    df_name = spark.sql("""select A.* ,'{0}' as {0},B.*,'{1}' as {1}
                        from df_input_1_T B inner join df_input_2_T A
                        on A.resellerName=B.ENDUSER_NAME
                        """.format(vars['input_file_1_name'],vars['input_file_2_name']))
    df_name_final = df_name.toPandas().replace('nan', '', regex=True)
    ### Translating to english
    #translator= Translator(to_lang="en")
    #gs = goslate.Goslate()
    #df_name_final['resellerName1']=df_name_final['resellerName'].apply(gs.translate,args=('en'))
    
    #### Creating dataframe with name and address field join
    logger.info("Joining using Name exact match and address exact match")
    df_name_address = spark.sql("""select A.* ,'{0}' as {0},B.*,'{1}' as {1}
                                from df_input_1_T B inner join df_input_2_T A
                                on lower(A.resellerName) = lower(B.ENDUSER_NAME)
                                and lower(A.resellerAddress1) = lower(B.ENDUSER_ADDRESS1)
                                and lower(A.resellerAddress2) = lower(B.ENDUSER_ADDRESS2)""".format(vars['input_file_1_name'],vars['input_file_2_name']))
    df_name_address_final = df_name_address.toPandas().replace('nan', '', regex=True)
    
    #### Creating dataframe with all fields join
    
    logger.info("Joining using Name, Adress,city,state,zip,country exact match")
    df_state_mapping = spark.read.option('header','True').option('inferschema','True').option('sep','|').csv(vars['root_path']+vars['state_mapping_file'])
    df_state_mapping.createOrReplaceTempView("df_state_mapping_T")
    df_all = spark.sql("""select A.* ,'{0}' as {0},B.*,'{1}' as {1}
                               from df_input_1_T B left join df_state_mapping_T SM
                               on B.ENDUSER_STATE = SM.Abbreviation
                               inner join df_input_2_T A  
                               on lower(A.resellerName) = lower(B.ENDUSER_NAME) 
                               and lower(A.resellerAddress1) = lower(B.ENDUSER_ADDRESS1)
                               and lower(A.resellerAddress2) = lower(B.ENDUSER_ADDRESS2)
                               and lower(A.resellerCity) = lower(B.ENDUSER_CITY)
                               and lower(A.resellerState) = lower(SM.State_Name)
                               and replace(lower(A.resellerZip),' ','')= replace(lower(B.ENDUSER_ZIP),' ','')
                               and lower(A.resellerCountry) = lower(B.ENDUSER_COUNTRY)""".format(vars['input_file_1_name'],vars['input_file_2_name']))
    df_all_final = df_all.toPandas().replace('nan', '', regex=True)
    
    #### Writing output to excel
    logger.info("Writing to output : {}, sheet : {}".format(vars['output_excel_name'],vars['output_sheet_1']))
    writetoexcel(df_name_final,vars['root_path']+vars['output_excel_name'],vars['output_sheet_1'])
    logger.info("Writing to output : {}, sheet : {}".format(vars['output_excel_name'],vars['output_sheet_2']))
    writetoexcel(df_name_address_final,vars['root_path']+vars['output_excel_name'],vars['output_sheet_2'])
    logger.info("Writing to output : {}, sheet : {}".format(vars['output_excel_name'],vars['output_sheet_3']))
    writetoexcel(df_all_final,vars['root_path']+vars['output_excel_name'],vars['output_sheet_3'])

