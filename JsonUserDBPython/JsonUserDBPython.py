from asyncio.windows_events import NULL
from configparser import ConfigParser
import click
import logging
import pyodbc 

global g_ini_file_name
global g_config_section_name
global g_verbose
global g_log_severity_level
global g_account_field_name
global g_exlusion_table_names
global g_exlusion_table_name_list
g_ini_file_name = 'JsonUserDBPython.ini'
g_config_section_name = 'CONFIG'
global g_account_uid

def loggingErrorAndExit(msg):
    logging.error(msg)
    exit()

def configFileParse():
    global g_log_severity_level
    global g_account_field_name
    global g_exlusion_table_names
    global g_exlusion_table_name_list
    parser = ConfigParser()
    parser.read(g_ini_file_name)
    g_log_severity_level = parser.get(g_config_section_name, 'LogStandardSeverityLevel')
    g_account_field_name = parser.get(g_config_section_name, 'AccountUIDFieldName')
    g_exlusion_table_names = parser.get(g_config_section_name, 'ExclusionTableNameList')
    g_exlusion_table_name_list = g_exlusion_table_names.split(',')

@click.command()
@click.option("-e", "--export", 'exportMode', is_flag=True, show_default=False, default=False, help="Export JSON file from DB")
@click.option("-i", "--import", 'importMode',  is_flag=True, show_default=False, default=False, help="Import JSON file into DB")
@click.option("-d", "--delete", 'deleteMode', is_flag=True, show_default=False, default=False, help="Delete rows(same accounUID exists) of DB")
@click.option("-p", "--print", 'printMode',  is_flag=True, show_default=False, default=False, help="Print all columns from tables where same accountUID exists")
@click.option("-f", "--force", 'forceImport',  is_flag=True, show_default=False, default=False, help="Delete accounUID data and import JSON file into DB")
@click.option("-s", "--source", 'source', default='', help="Source DB connection string or source JSON file name")
@click.option("-t", "--target", 'target',  default='', help="Target JSON file name or target DB connection string")
@click.option("-c", "--connect", 'connSection', default='', help="Section name in INI file for connection to DB")
@click.option("-u", "--uid", 'accountUID',  default='', help="Target accountUID")
@click.option("-v", "--verbose", 'verbose', is_flag=True, show_default=False, default=False, help="Provides additional details")
def argParse(exportMode, importMode, deleteMode, printMode, forceImport, source, target, connSection, accountUID, verbose):
    conn_string = ''
    json_file_name = ''
    if not accountUID:
        loggingErrorAndExit('Target accountUID is needed')
    global g_account_uid
    g_account_uid = accountUID
    global g_verbose
    g_verbose = verbose
    if exportMode + importMode + deleteMode + printMode != 1:
        loggingErrorAndExit('This mode is not supported')
    if exportMode:
        if source:
            conn_string = source
        if target:
            json_file_name = target
    if importMode:
        if target:
            conn_string = target
        if source:
            json_file_name = source
    else:
        if forceImport:
            loggingErrorAndExit('Force option is supported when import mode')
    if exportMode or importMode:
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

def connectToDB(connString):
    connection = pyodbc.connect(connString)
    cursor = connection.cursor()
    return cursor

def main():
    configFileParse()
    conn_string, json_file_name = argParse(standalone_mode=False)
    cursor = connectToDB(conn_string)


if __name__ == "__main__":
    main()
