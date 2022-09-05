from configparser import ConfigParser
from distutils.command.config import config
from py_compile import _get_default_invalidation_mode
from pypika import Table, Query
from prettytable import PrettyTable
import click
import logging
import pyodbc
import json
import datetime
import decimal
import re

global INI_FILE_NAME
global CONFIG_SECTION_NAME
INI_FILE_NAME = 'JsonUserDBPython.ini'
CONFIG_SECTION_NAME = 'CONFIG'

global g_account_field_name
global g_exlusion_table_names
global g_exlusion_table_name_set

class ModeParameters:
    def __init__(self):
        self.__mode = ''
        self.__source = ''
        self.__target = ''
        self.__account_uid = ''
        self.__connect_section_name = ''
        self.__is_verbose= False
        self.__is_force_import = False

    def setMode(self, mode):
        self.__mode = mode

    def setSource(self, source):
        self.__source = source

    def setTarget(self, target):
        self.__target = target

    def setAccountUID(self, accountUID):
        self.__account_uid = accountUID

    def setConnectSectionName(self, connectSectionName):
        self.__connect_section_name = connectSectionName

    def setIsForceImport(self, isForceImport):
        self.__is_force_import = isForceImport

    def setVerbose(self, isVerbose):
        self.__is_verbose = isVerbose


    def getMode(self):
        return self.__mode

    def getSource(self):
        return self.__source

    def getTarget(self):
        return self.__target

    def getAccountUID(self):
        return self.__account_uid

    def getConnectSectionName(self):
        return self.__connect_section_name

    def getIsForceImport(self):
        return self.__is_force_import

    def getVerbose(self):
        return self.__is_verbose
    

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime)):
            return obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        if isinstance(obj, (datetime.date)):
            return str(obj)
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode(encoding="ISO-8859-1")
        if isinstance(obj, (decimal.Decimal)):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def loggingErrorAndExit(msg):
    logging.error(msg)
    exit()

def getConf(getConfValFromINIFile):
    def getConfFromINIFile(keyName):
        parser = ConfigParser()
        if not parser.read(INI_FILE_NAME):
            return None
        if not parser.has_section(CONFIG_SECTION_NAME):
            return None
        return getConfValFromINIFile(keyName, parser)
    return getConfFromINIFile

@getConf
def getAccountFieldName(keyAccountUIDFieldName, parser=None):
    return parser.get(CONFIG_SECTION_NAME, keyAccountUIDFieldName) if parser.has_option(CONFIG_SECTION_NAME, keyAccountUIDFieldName) else None

@getConf
def getExclusionTableName(keyExclusionTableNameList, parser=None):
    return parser.get(CONFIG_SECTION_NAME, keyExclusionTableNameList) if parser.has_option(CONFIG_SECTION_NAME, keyExclusionTableNameList) else None

def strToSet(inputStr, delimeter):
    word_list = inputStr.split(delimeter)
    return set(word_list)

def handleNoneValue(inputVal, msg):
    if inputVal is None:
        loggingErrorAndExit(msg)

def globalVarConfig():
    global g_account_field_name
    global g_exlusion_table_name_set
    global g_exlusion_table_names
    g_account_field_name = getAccountFieldName('AccountUIDFieldName')
    handleNoneValue(g_account_field_name, "Fail : get account Field name")
    g_exlusion_table_names = getExclusionTableName('ExclusionTableNameList')
    handleNoneValue(g_exlusion_table_names, "Fail : get exclustion table names")
    g_exlusion_table_name_set = strToSet(g_exlusion_table_names, ',')

@click.group(no_args_is_help=True)
@click.pass_context
@click.help_option("-h", "--help")
def setModeParam(ctx):
    pass

@setModeParam.command('export')
@click.pass_context
@click.option("-u", "--uid", 'accountUID',  default='', help="Target accountUID")
@click.option("-s", "--source", 'source', default='', help="Source DB connection string or source JSON file name")
@click.option("-t", "--target", 'target',  default='', help="Target JSON file name or target DB connection string")
@click.option("-c", "--connect", 'connectSectionName', default='', help="Section name in INI file for connection to DB")
def setExportMode(ctx, accountUID, source, target, connectSectionName):
    ctx.obj.setMode('export')
    ctx.obj.setAccountUID(accountUID)
    ctx.obj.setSource(source)
    ctx.obj.setTarget(target)
    ctx.obj.setConnectSectionName(connectSectionName)  

@setModeParam.command('import')
@click.pass_context
@click.option("-u", "--uid", 'accountUID',  default='', help="Target accountUID")
@click.option("-s", "--source", 'source', default='', help="Source DB connection string or source JSON file name")
@click.option("-t", "--target", 'target',  default='', help="Target JSON file name or target DB connection string")
@click.option("-c", "--connect", 'connectSectionName', default='', help="Section name in INI file for connection to DB")
@click.option("-f", "--force", 'isForceImport',  is_flag=True, show_default=False, default=False, help="Delete accounUID data and import JSON file into DB")
def setImportMode(ctx, accountUID, source, target, connectSectionName, isForceImport):
    ctx.obj.setMode('import')
    ctx.obj.setAccountUID(accountUID)
    ctx.obj.setSource(source)
    ctx.obj.setTarget(target)
    ctx.obj.setConnectSectionName(connectSectionName)
    ctx.obj.setIsForceImport(isForceImport)

@setModeParam.command('delete')
@click.pass_context
@click.option("-u", "--uid", 'accountUID',  default='', help="Target accountUID")
@click.option("-t", "--target", 'target',  default='', help="Target JSON file name or target DB connection string")
@click.option("-c", "--connect", 'connectSectionName', default='', help="Section name in INI file for connection to DB")
def setDeleteMode(ctx, accountUID, target, connectSectionName):
    ctx.obj.setMode('delete')
    ctx.obj.setAccountUID(accountUID)
    ctx.obj.setTarget(target)
    ctx.obj.setConnectSectionName(connectSectionName)

@setModeParam.command('print')
@click.pass_context
@click.option("-u", "--uid", 'accountUID',  default='', help="Target accountUID")
@click.option("-t", "--target", 'target',  default='', help="Target JSON file name or target DB connection string")
@click.option("-c", "--connect", 'connectSectionName', default='', help="Section name in INI file for connection to DB")
def setPrintMode(ctx, accountUID, target, connectSectionName):
    ctx.obj.setMode('print')
    ctx.obj.setAccountUID(accountUID)
    ctx.obj.setTarget(target)
    ctx.obj.setConnectSectionName(connectSectionName)


def getConnectionStrFromINIFile(connectSectionName):
    parser = ConfigParser()
    parser.read(INI_FILE_NAME)
    val_server = parser.get(connectSectionName, 'Server', fallback = '')
    val_driver = parser.get(connectSectionName, 'Driver', fallback = '')
    val_dsn = parser.get(connectSectionName, 'DSN', fallback = '')
    val_trusted_conneciton = parser.get(connectSectionName, 'Trusted_connection', fallback = 'No')
    val_uid = parser.get(connectSectionName, 'UID', fallback = '')
    val_pwd = parser.get(connectSectionName, 'PWD', fallback = '')
    val_database = parser.get(connectSectionName, 'Database', fallback = '')
    if val_dsn:
        conn_string = 'DSN={0};Trusted_connection={1};UID={2};PWD={3};Database={4};'.format(val_dsn, val_trusted_conneciton, val_uid, val_pwd, val_database)
    else:
        conn_string = 'Server={0};Driver={1};Trusted_connection={2};UID={3};PWD={4};Database={5};'.format(val_server, val_driver, val_trusted_conneciton, val_uid, val_pwd, val_database)
    return conn_string



def getConnectionStr(getModeConnectionStr):
    def getInnerConnectionStr(modeParameters):
        source = modeParameters.getSource()
        target = modeParameters.getTarget()
        connect_section_name = modeParameters.getConnectSectionName()
        return getModeConnectionStr(source, target, connect_section_name)
    return getInnerConnectionStr

@getConnectionStr
def getExportModeConnectionStr(source='', target='', connectSectionName=''):
    conn_str = source
    if not conn_str:
        conn_str = getConnectionStrFromINIFile(connectSectionName)
    return conn_str

@getConnectionStr
def getImportModeConnectionStr(source='', target='', connectSectionName=''):
    conn_str = target
    if not conn_str:
        conn_str = getConnectionStrFromINIFile(connectSectionName)
    return conn_str

@getConnectionStr
def getDeleteModeConnectionStr(source='', target='', connectSectionName=''):
    conn_str = target
    if not conn_str:
        conn_str = getConnectionStrFromINIFile(connectSectionName)
    return conn_str

@getConnectionStr
def getPrintModeConnectionStr(source='', target='', connectSectionName=''):
    conn_str = target
    if not conn_str:
        conn_str = getConnectionStrFromINIFile(connectSectionName)
    return conn_str



def getJsonFileName(getModeJsonFileName):
    def getInnerJsonFileName(modeParameters):
        source = modeParameters.getSource()
        target = modeParameters.getTarget()
        return getModeJsonFileName(source, target)
    return getInnerJsonFileName

@getJsonFileName
def getExportModeJsonFileName(source='', target=''):
    return target

@getJsonFileName
def getImportModeJsonFileName(source='', target=''):
    return source


#def getConnectionStr(ModeParameters):
#    global g_json_file_name
#    conn_string = ''
#    g_json_file_name = ''
#        if forceImport:
#            loggingErrorAndExit('Force option is supported when import mode')
#    global g_force_import
#    g_force_import = forceImport
#    if g_mode == 'export' or g_mode == 'import':
#        if not g_json_file_name:
#            loggingErrorAndExit('JSON file name is needed')
#    if not conn_string and not connSection:
#        loggingErrorAndExit('Connection string or connect section is needed')
#    if not conn_string:
#        parser = ConfigParser()
#        parser.read(INI_FILE_NAME)
#        val_server = parser.get(connSection, 'Server', fallback = '')
#        val_driver = parser.get(connSection, 'Driver', fallback = '')
#        val_dsn = parser.get(connSection, 'DSN', fallback = '')
#        val_trusted_conneciton = parser.get(connSection, 'Trusted_connection', fallback = 'No')
#        val_uid = parser.get(connSection, 'UID', fallback = '')
#        val_pwd = parser.get(connSection, 'PWD', fallback = '')
#        val_database = parser.get(connSection, 'Database', fallback = '')
#        if val_dsn:
#            conn_string = 'DSN={0};Trusted_connection={1};UID={2};PWD={3};Database={4};'.format(val_dsn, val_trusted_conneciton, val_uid, val_pwd, val_database)
#        else:
#            conn_string = 'Server={0};Driver={1};Trusted_connection={2};UID={3};PWD={4};Database={5};'.format(val_server, val_driver, val_trusted_conneciton, val_uid, val_pwd, val_database)
#    return conn_string

#@click.help_option("-h", "--help")
#@click.option("-f", "--force", 'forceImport',  is_flag=True, show_default=False, default=False, help="Delete accounUID data and import JSON file into DB")
#@click.option("-s", "--source", 'source', default='', help="Source DB connection string or source JSON file name")
#@click.option("-t", "--target", 'target',  default='', help="Target JSON file name or target DB connection string")
#@click.option("-c", "--connect", 'connSection', default='', help="Section name in INI file for connection to DB")
#@click.option("-u", "--uid", 'accountUID',  default='', help="Target accountUID", callback=validateUID)
#@click.option("-v", "--verbose", 'verbose', is_flag=True, show_default=False, default=False, help="Provides additional details")
#def argParse(accountUID, forceImport, source, target, connSection, verbose):
#    global g_json_file_name
#    global g_mode
#    conn_string = ''
#    g_json_file_name = ''
#    global g_verbose
#    g_verbose = verbose
#    if g_mode == None:
#        loggingErrorAndExit('This mode is not supported')
#    if g_mode == 'export':
#        if source:
#            conn_string = source
#        if target:
#            g_json_file_name = target
#    if g_mode == 'import':
#        if target:
#            conn_string = target
#        if source:
#            g_json_file_name = source
#    else:
#        if forceImport:
#            loggingErrorAndExit('Force option is supported when import mode')
#    global g_force_import
#    g_force_import = forceImport
#    if g_mode == 'export' or g_mode == 'import':
#        if not g_json_file_name:
#            loggingErrorAndExit('JSON file name is needed')
#    if not conn_string and not connSection:
#        loggingErrorAndExit('Connection string or connect section is needed')
#    if not conn_string:
#        parser = ConfigParser()
#        parser.read(INI_FILE_NAME)
#        val_server = parser.get(connSection, 'Server', fallback = '')
#        val_driver = parser.get(connSection, 'Driver', fallback = '')
#        val_dsn = parser.get(connSection, 'DSN', fallback = '')
#        val_trusted_conneciton = parser.get(connSection, 'Trusted_connection', fallback = 'No')
#        val_uid = parser.get(connSection, 'UID', fallback = '')
#        val_pwd = parser.get(connSection, 'PWD', fallback = '')
#        val_database = parser.get(connSection, 'Database', fallback = '')
#        if val_dsn:
#            conn_string = 'DSN={0};Trusted_connection={1};UID={2};PWD={3};Database={4};'.format(val_dsn, val_trusted_conneciton, val_uid, val_pwd, val_database)
#        else:
#            conn_string = 'Server={0};Driver={1};Trusted_connection={2};UID={3};PWD={4};Database={5};'.format(val_server, val_driver, val_trusted_conneciton, val_uid, val_pwd, val_database)
#    return conn_string


def getConnString():
    try:
        return argParse(standalone_mode=False)
    except click.NoSuchOption as e:
        print(e)
        exit()

def sqlSingleCol(cursor, sql):
    single_col_set = set()
    cursor.execute(sql)
    row_list = cursor.fetchall()
    for row in row_list:
        single_col_set.add(row[0])
    return single_col_set

def sqlMultiCol(cursor, tableName, sql):
    cursor.execute(sql)
    column_name_list = [column[0] for column in cursor.description]
    result_py_obj = {tableName:[]}
    row_list = cursor.fetchall()
    for row in row_list:
        result_row_py_obj = dict()
        for col_idx in range(0, len(row)):
            if row[col_idx] != None:
                result_row_py_obj.update({column_name_list[col_idx]:row[col_idx]})
        if len(result_row_py_obj) > 0:
            result_py_obj[tableName].append(result_row_py_obj)
    result_json = json.dumps(result_py_obj, cls=CustomJSONEncoder)
    return result_json

def connectToDB(connString):
    global g_conn
    logging.info('Start : Connecting DB')
    try:
        g_conn = pyodbc.connect(connString)
        logging.info('Success : Connecting DB')
    except pyodbc.Error as e:
        loggingErrorAndExit(e.args)

def disconnectDB():
    global g_conn
    logging.info('Start : Disconnecting DB')
    try:
        g_conn.close()
        logging.info('Success : Disconnecting DB')
    except pyodbc.Error as e:
        loggingErrorAndExit(e.args)

def exportJsonFromDB(cursor, tableNameSet):
    logging.info('Start : Exporting JSON from DB')
    result_py_obj = {g_account_field_name:g_account_uid}
    for table_name in tableNameSet:
        auto_col_name_set = sqlSingleCol(cursor, "SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{}'".format(table_name))
        table_json = sqlMultiCol(cursor, table_name, "SELECT * FROM {0} WHERE {1} = {2}".format(table_name, g_account_field_name, g_account_uid))
        table_py_obj = json.loads(table_json)
        for row_idx in range(0, len(table_py_obj[table_name])):
            del table_py_obj[table_name][row_idx][g_account_field_name]
            for auto_col_name in auto_col_name_set:
                del table_py_obj[table_name][row_idx][auto_col_name]
        if len(table_py_obj[table_name]) > 0:
            result_py_obj.update(table_py_obj)
    result_json = json.dumps(result_py_obj, indent=4)
    logging.info('Success : Exporting JSON from DB')
    return result_json

def writeJsonFile(json_data, jsonFileName):
    logging.info('Start : Writing JSON file')
    try:
        with open(jsonFileName,'w') as file:
            file.write(json_data)
        logging.info('Success : Writing JSON file')
    except OSError:
        loggingErrorAndExit('Fail : Writing JSON file')

def readJsonFile(jsonFileName):
    logging.info('Start : Reading JSON file')
    try:
        with open(jsonFileName,'r') as file:
            json_py_obj = json.load(file)
            logging.info('Success : Reading JSON file')
            return json_py_obj
    except OSError:
        loggingErrorAndExit("Fail : Reading JSON file")

def deleteAccountDataFromTables(cursor, tableNameSet):
    logging.info('Start : Deleteing table rows')
    try:
        for table_name in tableNameSet:
            cursor.execute("IF EXISTS(SELECT * FROM {0} WHERE {1} = {2}) BEGIN DELETE FROM {3} WHERE {4} = {5} END".format(table_name, g_account_field_name, g_account_uid, table_name, g_account_field_name, g_account_uid))
        cursor.commit()
        logging.info('Success : Deleteing table rows')
    except pyodbc.Error as e:
        cursor.rollback()
        loggingErrorAndExit(str(e))

def importJsonIntoDB(cursor, jsonPyObj):
    logging.info('Start : Importing Json into DB')
    try:
        for table_name, table_val in jsonPyObj.items():
            if table_name == g_account_field_name:
                continue
            col_infos = sqlMultiCol(cursor, table_name, "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(table_name))
            col_infos_py_obj = json.loads(col_infos)
            col_infos_dict = dict()
            for col_info in col_infos_py_obj[table_name]:
                col_infos_dict[col_info['COLUMN_NAME']] = col_info['DATA_TYPE']
            for row in table_val:
                table = Table(table_name)
                insert_query = Query.into(table).columns(g_account_field_name)
                col_val_list = [g_account_uid]
                for col_name, col_val in row.items():
                    if col_name not in col_infos_dict.keys():
                        loggingErrorAndExit("Fail : Importing Json into DB")
                    insert_query = insert_query.columns(col_name)
                    if col_infos_dict[col_name] == 'datetime':
                        col_val_list.append(col_val)
                    elif col_infos_dict[col_name] == 'bit':
                        if col_val:
                            col_val_list.append(1)
                        else:
                            col_val_list.append(0)
                    elif col_infos_dict[col_name] == 'binary':
                        col_val_list.append('0x{}'.format(col_val.encode(encoding="ISO-8859-1").hex()))
                    elif col_infos_dict[col_name] == 'varbinary':
                        col_val_list.append('0x{}'.format(col_val.encode(encoding="ISO-8859-1").hex()))
                    else:
                        col_val_list.append(col_val)
                insert_query = insert_query.insert(col_val_list)
                executed_query = str(insert_query)
                executed_query = re.sub("'(0x[0-9a-fA-F]+)'", r"\1", executed_query)
                cursor.execute(executed_query)
        cursor.commit()
        logging.info('Success : Importing Json into DB')
    except pyodbc.Error as e:
        cursor.rollback()
        loggingErrorAndExit(str(e))
        
def printTable(cursor, tableName):
    logging.info('Start : Printing tables')
    try:
        cursor.execute("SELECT * FROM {0} WHERE {1} = '{2}';".format(tableName, g_account_field_name, g_account_uid))
        column_name_list = [column[0] for column in cursor.description]
        pretty_table = PrettyTable() 
        pretty_table.field_names = column_name_list
        row = cursor.fetchone() 
        while row: 
            pretty_table.add_row(list(row))
            row = cursor.fetchone()
        print(pretty_table)
        logging.info('Success : Printing tables')
    except pyodbc.Error as e:
        loggingErrorAndExit(str(e))

def excuteTaskDependingOnMode(cursor, tableNameSet):
    if g_mode == 'export':
        json_accountuid_data = exportJsonFromDB(cursor, tableNameSet)

        writeJsonFile(json_accountuid_data, g_json_file_name)
    elif g_mode == 'import':
        json_py_obj = readJsonFile(g_json_file_name)

        if g_force_import:
            deleteAccountDataFromTables(cursor, tableNameSet)
        
        importJsonIntoDB(cursor, json_py_obj)
    elif g_mode == 'delete':
        deleteAccountDataFromTables(cursor, tableNameSet)
    elif g_mode == 'print':
        for table_name in tableNameSet:
            print(table_name)
            printTable(cursor, table_name)
             
def main():
    logging.basicConfig(level=logging.INFO)
    globalVarConfig()
    mode_parameters = ModeParameters()
    setModeParam(obj=mode_parameters, standalone_mode=False)


    conn_string = getPrintModeConnectionStr(mode_parameters)
    connectToDB(conn_string)
    cursor = g_conn.cursor()
    uid_exist_table_name_set = sqlSingleCol(cursor, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '{}' INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'".format(g_account_field_name))
    uid_exist_table_name_set = uid_exist_table_name_set.difference(g_exlusion_table_name_set)
    excuteTaskDependingOnMode(cursor, uid_exist_table_name_set)
    cursor.close()
    disconnectDB()

if __name__ == "__main__":
    main()
