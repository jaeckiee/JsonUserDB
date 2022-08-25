from asyncio.windows_events import NULL
from configparser import ConfigParser
from py_compile import _get_default_invalidation_mode
import click
import logging
import pyodbc 

global g_ini_file_name
global g_config_section_name
global g_verbose
global g_log_severity_level
global g_account_field_name
global g_exlusion_table_names
global g_exlusion_table_name_set
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
    conn_string = ''
    json_file_name = ''
    if not accountUID:
        loggingErrorAndExit('Target accountUID is needed')
    global g_account_uid
    g_account_uid = accountUID
    global g_verbose
    g_verbose = verbose
    print(mode)
    if mode == None:
        loggingErrorAndExit('This mode is not supported')
    global g_mode
    g_mode = mode
    if g_mode == 'export':
        if source:
            conn_string = source
        if target:
            json_file_name = target
    if g_mode == 'import':
        if target:
            conn_string = target
        if source:
            json_file_name = source
    else:
        if forceImport:
            loggingErrorAndExit('Force option is supported when import mode')
    if g_mode == 'export' or g_mode == 'import':
        if not json_file_name:
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
            conn_string = 'DSN=' + val_dsn + ';Trusted_connection=' + val_trusted_conneciton + ';UID=' + val_uid + ';PWD=' + val_pwd + ';Database=' +  val_database + ';'
        else:
            conn_string = 'Server=' + val_server + ';Driver=' + val_driver + ';Trusted_connection=' + val_trusted_conneciton + ';UID=' + val_uid + ';PWD=' + val_pwd + ';Database=' +  val_database + ';'
    return conn_string, json_file_name

def sqlFirstCol(cursor, sql):
    single_col_set = set()
    cursor.execute(sql)
    row = cursor.fetchone() 
    while row: 
        single_col_set.add(row[0])
        row = cursor.fetchone()
    return single_col_set

def excuteTaskDependingOnMode():
    if g_mode == 'export':
        return
    elif g_mode == 'import':
        return
    elif g_mode == 'delete':
        return
    elif g_mode == 'print':
        return

def main():
    configFileParse()
    conn_string, json_file_name = argParse(standalone_mode=False)
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    uid_exist_table_name_set = sqlFirstCol(cursor, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '{}' INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'".format(g_account_field_name))
    uid_exist_table_name_set = uid_exist_table_name_set.difference(g_exlusion_table_name_set)
    excuteTaskDependingOnMode()
    conn.close()

if __name__ == "__main__":
    main()
