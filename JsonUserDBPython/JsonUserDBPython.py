from configparser import ConfigParser
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

def getModeParamFromCli():
    try:
        mode_parameters = ModeParameters()
        setModeParam(obj=mode_parameters, standalone_mode=False)
        return mode_parameters
    except click.ClickException as e:
        print(e)
        exit()

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
    val_pwd = ''
    val_server = parser.get(connectSectionName, 'Server', fallback = '')
    val_driver = parser.get(connectSectionName, 'Driver', fallback = '')
    val_dsn = parser.get(connectSectionName, 'DSN', fallback = '')
    val_trusted_conneciton = parser.get(connectSectionName, 'Trusted_connection', fallback = 'No')
    val_uid = parser.get(connectSectionName, 'UID', fallback = '')
    val_database = parser.get(connectSectionName, 'Database', fallback = '')
    if val_trusted_conneciton == 'No':
        val_pwd = click.prompt('Please enter password', hide_input=True, type=str)
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


def getConnectionStrOnCurrentMode(modeParameters):
    mode = modeParameters.getMode()
    if mode == 'export':
        return getExportModeConnectionStr(modeParameters)
    elif mode == 'import':
        return getImportModeConnectionStr(modeParameters)
    elif mode == 'delete':
        return getDeleteModeConnectionStr(modeParameters)
    elif mode == 'print':
        return getPrintModeConnectionStr(modeParameters)


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

def getConnectionToDB(connString):
    logging.info('Start : Connecting DB')
    try:
        connection = pyodbc.connect(connString)
        logging.info('Success : Connecting DB')
        return connection
    except pyodbc.Error as e:
        loggingErrorAndExit(e.args)

def disconnectDB(connection):
    logging.info('Start : Disconnecting DB')
    try:
        connection.close()
        logging.info('Success : Disconnecting DB')
    except pyodbc.Error as e:
        loggingErrorAndExit(e.args)

def exportJsonFromDB(cursor, tableNameSet, accountUID):
    logging.info('Start : Exporting JSON from DB')
    result_py_obj = {g_account_field_name:accountUID}
    for table_name in tableNameSet:
        auto_col_name_set = sqlSingleCol(cursor, "SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{}'".format(table_name))
        table_json = sqlMultiCol(cursor, table_name, "SELECT * FROM {0} WHERE {1} = {2}".format(table_name, g_account_field_name, accountUID))
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

def deleteAccountDataFromTables(cursor, tableNameSet, accountUID):
    logging.info('Start : Deleteing table rows')
    try:
        for table_name in tableNameSet:
            cursor.execute("IF EXISTS(SELECT * FROM {0} WHERE {1} = {2}) BEGIN DELETE FROM {3} WHERE {4} = {5} END".format(table_name, g_account_field_name, accountUID, table_name, g_account_field_name, accountUID))
        cursor.commit()
        logging.info('Success : Deleteing table rows')
    except pyodbc.Error as e:
        cursor.rollback()
        loggingErrorAndExit(str(e))

def importJsonIntoDB(cursor, jsonPyObj, accountUID):
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
                col_val_list = [accountUID]
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
        
def printTable(cursor, tableName, accountUID):
    logging.info('Start : Printing tables')
    try:
        cursor.execute("SELECT * FROM {0} WHERE {1} = '{2}';".format(tableName, g_account_field_name, accountUID))
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

def excuteTaskOnCurrentMode(cursor, tableNameSet, modeParameters):
    mode = modeParameters.getMode()
    account_uid = modeParameters.getAccountUID()
    if mode == 'export':
        json_file_name = getExportModeJsonFileName(modeParameters)

        json_accountuid_data = exportJsonFromDB(cursor, tableNameSet, account_uid)

        writeJsonFile(json_accountuid_data, json_file_name)
    elif mode == 'import':
        json_file_name = getImportModeJsonFileName(modeParameters)

        json_py_obj = readJsonFile(json_file_name)

        if modeParameters.getIsForceImport():
            deleteAccountDataFromTables(cursor, tableNameSet, account_uid)
        
        importJsonIntoDB(cursor, json_py_obj, account_uid)
    elif mode == 'delete':
        deleteAccountDataFromTables(cursor, tableNameSet, account_uid)
    elif mode == 'print':
        for table_name in tableNameSet:
            print(table_name)
            printTable(cursor, table_name, account_uid)
             
def main():
    logging.basicConfig(level=logging.INFO)
    globalVarConfig()
    mode_parameters = getModeParamFromCli()
    conn_string = getConnectionStrOnCurrentMode(mode_parameters)
    conn = getConnectionToDB(conn_string)
    cursor = conn.cursor()
    uid_exist_table_name_set = sqlSingleCol(cursor, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '{}' INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'".format(g_account_field_name))
    uid_exist_table_name_set = uid_exist_table_name_set.difference(g_exlusion_table_name_set)
    excuteTaskOnCurrentMode(cursor, uid_exist_table_name_set, mode_parameters)
    cursor.close()
    disconnectDB(conn)

if __name__ == "__main__":
    main()
