from asyncio.windows_events import NULL
from configparser import ConfigParser
from py_compile import _get_default_invalidation_mode
import click
import logging
import pyodbc 
import json
import pandas as pd

global g_ini_file_name
global g_config_section_name
global g_verbose
global g_log_severity_level
global g_account_field_name
global g_exlusion_table_names
global g_exlusion_table_name_set
global g_json_file_name
global g_mode
global g_account_uid
g_ini_file_name = 'JsonUserDBPython.ini'
g_config_section_name = 'CONFIG'

def loggingErrorAndExit(msg):
    logging.error(msg)
    exit()

def configFileParse():
    global g_log_severity_level
    global g_account_field_name
    global g_exlusion_table_names
    global g_exlusion_table_name_set
    parser = ConfigParser()
    parser.read(g_ini_file_name)
    g_log_severity_level = parser.get(g_config_section_name, 'LogStandardSeverityLevel')
    g_account_field_name = parser.get(g_config_section_name, 'AccountUIDFieldName')
    g_exlusion_table_names = parser.get(g_config_section_name, 'ExclusionTableNameList')
    exlusion_table_name_list = g_exlusion_table_names.split(',')
    g_exlusion_table_name_set = set(exlusion_table_name_list)

@click.command()
@click.option("-e", "--export", 'mode', flag_value='export', help="Export JSON file from DB")
@click.option("-i", "--import", 'mode',  flag_value='import', help="Import JSON file into DB")
@click.option("-d", "--delete", 'mode', flag_value='delete', help="Delete rows(same accounUID exists) of DB")
@click.option("-p", "--print", 'mode',  flag_value='print', help="Print all columns from tables where same accountUID exists")
@click.option("-f", "--force", 'forceImport',  is_flag=True, show_default=False, default=False, help="Delete accounUID data and import JSON file into DB")
@click.option("-s", "--source", 'source', default='', help="Source DB connection string or source JSON file name")
@click.option("-t", "--target", 'target',  default='', help="Target JSON file name or target DB connection string")
@click.option("-c", "--connect", 'connSection', default='', help="Section name in INI file for connection to DB")
@click.option("-u", "--uid", 'accountUID',  default='', help="Target accountUID")
@click.option("-v", "--verbose", 'verbose', is_flag=True, show_default=False, default=False, help="Provides additional details")
def argParse(mode, forceImport, source, target, connSection, accountUID, verbose):
    global g_json_file_name
    conn_string = ''
    g_json_file_name = ''
    if not accountUID:
        loggingErrorAndExit('Target accountUID is needed')
    global g_account_uid
    g_account_uid = accountUID
    global g_verbose
    g_verbose = verbose
    if mode == None:
        loggingErrorAndExit('This mode is not supported')
    global g_mode
    g_mode = mode
    if g_mode == 'export':
        if source:
            conn_string = source
        if target:
            g_json_file_name = target
    if g_mode == 'import':
        if target:
            conn_string = target
        if source:
            g_json_file_name = source
    else:
        if forceImport:
            loggingErrorAndExit('Force option is supported when import mode')
    if g_mode == 'export' or g_mode == 'import':
        if not g_json_file_name:
            loggingErrorAndExit('JSON file name is needed')
    if not conn_string and not connSection:
        loggingErrorAndExit('Connection string or connect section is needed')
    if not conn_string:
        parser = ConfigParser()
        parser.read(g_ini_file_name)
        val_server = parser.get(connSection, 'Server', fallback = '')
        val_driver = parser.get(connSection, 'Driver', fallback = '')
        val_dsn = parser.get(connSection, 'DSN', fallback = '')
        val_trusted_conneciton = parser.get(connSection, 'Trusted_connection', fallback = 'No')
        val_uid = parser.get(connSection, 'UID', fallback = '')
        val_pwd = parser.get(connSection, 'PWD', fallback = '')
        val_database = parser.get(connSection, 'Database', fallback = '')
        if val_dsn:
            conn_string = 'DSN={0};Trusted_connection={1};UID={2};PWD={3};Database={4};'.format(val_dsn, val_trusted_conneciton, val_uid, val_pwd, val_database)
        else:
            conn_string = 'Server={0};Driver={1};Trusted_connection={2};UID={3};PWD={4};Database={5};'.format(val_server, val_driver, val_trusted_conneciton, val_uid, val_pwd, val_database)
    return conn_string

def sqlFirstCol(cursor, sql):
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
            result_row_py_obj.update({column_name_list[col_idx]:row[col_idx]})
        result_py_obj[tableName].append(result_row_py_obj)
    result_json = json.dumps(result_py_obj, default=str)
    return result_json

def exportJsonFromDB(cursor, tableNameSet):
    result_py_obj = {g_account_field_name:g_account_uid}
    for table_name in tableNameSet:
        auto_col_name_set = sqlFirstCol(cursor, "SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{}'".format(table_name))
        table_json = sqlMultiCol(cursor, table_name, "SELECT * FROM {0} WHERE {1} = {2}".format(table_name, g_account_field_name, g_account_uid))
        table_py_obj = json.loads(table_json)
        for row_idx in range(0, len(table_py_obj[table_name])):
            del table_py_obj[table_name][row_idx][g_account_field_name]
            for auto_col_name in auto_col_name_set:
                del table_py_obj[table_name][row_idx][auto_col_name]
        if len(table_py_obj[table_name]) > 0:
            result_py_obj.update(table_py_obj)
    result_json = json.dumps(result_py_obj, indent=4)
    return result_json

def writeJsonFile(json_data, jsonFileName):
    with open(jsonFileName,'w') as file:
        file.write(json_data)

def deleteAccountDataFromTable(cursor, tableName):
    cursor.execute("IF EXISTS(SELECT * FROM {0} WHERE {1} = {2}) BEGIN DELETE FROM {3} WHERE {4} = {5} END".format(table_name, g_account_field_name, g_account_uid, table_name, g_account_field_name, g_account_uid))

def printTable(cursor, tableName):
    cursor.execute("SELECT * FROM {0} WHERE {1} = '{2}';".format(tableName, g_account_field_name, g_account_uid))
    column_name_list = [column[0] for column in cursor.description]
    df = pd.DataFrame(columns = column_name_list)
    row = cursor.fetchone() 
    print(type(row))
    while row: 
        df.loc[len(df.index)] = list(row)
        row = cursor.fetchone()
    print(df)

def excuteTaskDependingOnMode(cursor, tableNameSet):
    if g_mode == 'export':
        json_accountuid_data = exportJsonFromDB(cursor, tableNameSet)

        writeJsonFile(json_accountuid_data, g_json_file_name)
    elif g_mode == 'import':
        # read json file

        # if force import mode

        # import json into DB
        return
    elif g_mode == 'delete':
        for table_name in tableNameSet:
            deleteAccountDataFromTable(cursor, table_name)
    elif g_mode == 'print':
        for table_name in tableNameSet:
            print(table_name)
            printTable(cursor, table_name)

def main():
    pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None, 'display.max_columns', None)
    configFileParse()
    conn_string = argParse(standalone_mode=False)
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    uid_exist_table_name_set = sqlFirstCol(cursor, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '{}' INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'".format(g_account_field_name))
    uid_exist_table_name_set = uid_exist_table_name_set.difference(g_exlusion_table_name_set)
    excuteTaskDependingOnMode(cursor, uid_exist_table_name_set)
    cursor.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
