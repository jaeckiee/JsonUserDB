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

@click.command()
@click.option("-e", "--export", 'export_mode', is_flag=True, show_default=False, default=False, help="Export JSON file from DB")
@click.option("-i", "--import", 'import_mode',  is_flag=True, show_default=False, default=False, help="Import JSON file into DB")
@click.option("-d", "--delete", 'delete_mode', is_flag=True, show_default=False, default=False, help="Delete rows(same accounUID exists) of DB")
@click.option("-p", "--print", 'print_mode',  is_flag=True, show_default=False, default=False, help="Print all columns from tables where same accountUID exists")
@click.option("-f", "--force", 'force_import',  is_flag=True, show_default=False, default=False, help="Delete accounUID data and import JSON file into DB")
@click.option("-s", "--source", 'source', default='', help="Source DB connection string or source JSON file name")
@click.option("-t", "--target", 'target',  default='', help="Target JSON file name or target DB connection string")
@click.option("-c", "--connect", 'conn_section', default='', help="Section name in INI file for connection to DB")
@click.option("-u", "--uid", 'account_uid',  default='', help="Target accountUID")
@click.option("-v", "--verbose", 'verbose', is_flag=True, show_default=False, default=False, help="Provides additional details")
def argParse(export_mode, import_mode, delete_mode, print_mode, force_import, source, target, conn_section, account_uid, verbose):
    conn_string = ''
    json_file_name = ''
    if not account_uid:
        loggingErrorAndExit('Target accountUID is needed')
    global g_account_uid
    g_account_uid = account_uid
    global g_verbose
    g_verbose = verbose
    if export_mode + import_mode + delete_mode + print_mode != 1:
        loggingErrorAndExit('This mode is not supported')
    if export_mode:
        if source:
            conn_string = source
        if target:
            json_file_name = target
    if import_mode:
        if target:
            conn_string = target
        if source:
            json_file_name = source
    else:
        if force_import:
            loggingErrorAndExit('Force option is supported when import mode')
    if export_mode or import_mode:
        if not json_file_name:
            loggingErrorAndExit('JSON file name is needed')
    if not conn_string and not conn_section:
        loggingErrorAndExit('Connection string or connect section is needed')
    if not conn_string:
        parser = ConfigParser()
        parser.read(g_ini_file_name)
        val_server = parser.get(conn_section, 'Server', fallback = '')
        val_driver = parser.get(conn_section, 'Driver', fallback = '')
        val_dsn = parser.get(conn_section, 'DSN', fallback = '')
        val_trusted_conneciton = parser.get(conn_section, 'Trusted_connection', fallback = 'No')
        val_uid = parser.get(conn_section, 'UID', fallback = '')
        val_pwd = parser.get(conn_section, 'PWD', fallback = '')
        val_database = parser.get(conn_section, 'Database', fallback = '')
        if val_dsn:
            conn_string = 'DSN=' + val_dsn + ';Trusted_connection=' + val_trusted_conneciton + ';UID=' + val_uid + ';PWD=' + val_pwd + ';Database=' +  val_database + ';'
        else:
            conn_string = 'Server=' + val_server + ';Driver=' + val_driver + ';Trusted_connection=' + val_trusted_conneciton + ';UID=' + val_uid + ';PWD=' + val_pwd + ';Database=' +  val_database + ';'
    return conn_string, json_file_name

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

def main():
    configFileParse()
    conn_string, json_file_name = argParse(standalone_mode=False)
    print("finish")

if __name__ == "__main__":
    main()
