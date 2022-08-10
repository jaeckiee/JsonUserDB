#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <conio.h>
#include <tchar.h>
#include <stdlib.h>
#include <sal.h>
#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <memory>
#include <wchar.h>
#include <fstream>
#include "strutility.h"
#include "sqlutility.h"
#include "argparse.h"
#include "json/json.h"
#include "sqlbuilder.h"
#include <cassert>
#include <sstream>
#include <unordered_map>

#define APP_NAME "JsonUserDB"

#define LOG_OFF		0
#define LOG_FATAL	1
#define LOG_ERROR	2
#define LOG_WARN	3
#define LOG_INFO	4
#define LOG_DEBUG	5
#define LOG_TRACE	6
#define LOG_ALL		7

#define SUCCEEDED_CHECK(w, x, y) 		\
						fwprintf(stdout, L"START : %s\n", y);\
						if (w) {\
						fwprintf(stdout, L"SUCCESS : %s\n", y);\
						}\
						else{\
						fwprintf(stderr, L"FAIL : %s\n", y);\
						return_val = x;\
						goto Exit;\
						}

// Usages Of Argparse
static const WCHAR* const USAGES[] = {
	L"" APP_NAME " [options] [[--] args]",
	L"" APP_NAME " [options]",
	NULL,
};

// return codes
const int SUCCESS = 0;
const int ERROR_BAD_ARG = 1;
const int ERROR_READING_FILE = 2;
const int ERROR_WRITING_FILE = 3;
const int ERROR_ALLOCATE_ENVHANDLE = 4;
const int ERROR_CONNECT_DB = 5;
const int ERROR_DISCONNECT_DB = 6;
const int ERROR_IMPORT_JSON = 7;
const int ERROR_EXPORT_JSON = 8;
const int ERROR_DELETE_TABLEROWS = 9;
const int ERROR_PRINT_TABLES = 10;
const int ERROR_TABLE_NOT_EXSIT = 11;
const int ERROR_PRINT_TABLE = 12;

// ini FILE
const WCHAR* DEFALUT_EMPTY_VAL = L"";
const WCHAR* DEFAULT_TRRUSTED_CONNECTION_VAL = L"No";
const WCHAR* INI_FILE_NAME = L".\\JsonUserDB.ini";

//global variation
std::wstring g_accout_uid_field_name;
int g_standard_log_severity_lv = LOG_ALL;

void Log(int severityLv, std::wstring msg) {
	if (severityLv > g_standard_log_severity_lv)
		return;
	fwprintf((severityLv > LOG_ERROR) ? stdout : stderr, L"%s\n", msg.c_str());
}

bool checkJsonTableNameInTableList(std::wstring currentTableName, std::vector<std::wstring> tableNameList) {
	if (!(std::find(tableNameList.begin(), tableNameList.end(), currentTableName) != tableNameList.end())) {
		Log(LOG_ERROR, L"Failed to find current table name in table name list");
		return false;
	}
	return true;
}

bool importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUid) {
	bool is_succeeded = false;
	SQLHSTMT hstmt = NULL;
	if (root == NULL) {
		Log(LOG_ERROR, L"Failed to import json cause root is null");
		return is_succeeded;
	}
	for (Json::Value::iterator iter = root.begin(); iter != root.end(); ++iter) {
		Json::Value current_key = iter.key();
		std::wstring current_tablename = get_utf16(current_key.asString());
		if (current_tablename.compare(g_accout_uid_field_name) != 0) {
			Json::Value current_table = root[get_utf8(current_tablename)];
			Json::Value col_infos = sqlfMultiCol(hDbc, current_tablename, L"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'", current_tablename.c_str());
			std::unordered_map<std::wstring, std::pair<std::wstring, std::wstring>> hm_col_infos;
			for (const Json::Value& col_info : col_infos) {
				if (col_info.size() <= 0)
					continue;
				hm_col_infos[get_utf16(col_info["COLUMN_NAME"].asString())] = std::make_pair(get_utf16(col_info["DATA_TYPE"].asString()), get_utf16(col_info["CHARACTER_MAXIMUM_LENGTH"].asString()));
			}
			for (int rowidx = 0; rowidx < (int)current_table.size(); rowidx++) {
				sqlbuilder::InsertModel insert_model;
				Json::Value current_row = current_table[rowidx];
				if (current_row.size() <= 0)
					continue;
				insert_model.insert(g_accout_uid_field_name, accountUid).into(current_tablename);
				for (Json::Value::const_iterator current_row_iter = current_row.begin(); current_row_iter != current_row.end(); current_row_iter++) {
					Json::String json_colname = current_row_iter.key().asCString();
					std::wstring colname = get_utf16(json_colname);
					std::wstring data_type = hm_col_infos[colname].first;
					if (data_type.compare(L"int") == 0 || data_type.compare(L"tinyint") == 0 || data_type.compare(L"smallint") == 0) {
						int val = current_row[json_colname].asInt();
						insert_model.insert(colname, val);
					}
					else if (data_type.compare(L"float") == 0) {
						float val = current_row[json_colname].asFloat();
						insert_model.insert(colname, val);
					}
					else if (data_type.compare(L"double") == 0) {
						double val = current_row[json_colname].asDouble();
						insert_model.insert(colname, val);
					}
					else if (data_type.compare(L"bit") == 0) {
						bool val = current_row[json_colname].asBool();
						insert_model.insert(colname, val);
					}
					else if (data_type.compare(L"binary") == 0 || data_type.compare(L"varbinary") == 0) {
						std::wstring val = get_utf16(current_row[json_colname].asString());
						insert_model.insertBinaryType(colname, val);
					}
					else {
						std::wstring val = get_utf16(current_row[json_colname].asString());
						insert_model.insert(colname, val);
					}
				}
				is_succeeded = sqlfExec(hstmt, hDbc, (insert_model.str()).c_str());
				if (!is_succeeded) {
					Log(LOG_ERROR, L"Failed to insert values in DB");
					fwprintf(stderr, L"FAIL : %s\n", current_tablename.c_str());
					goto Exit;
				}
				TRYODBC(hstmt, SQL_HANDLE_STMT, SQLFreeStmt(hstmt, SQL_CLOSE));
				hstmt = NULL;
			}
		}
	}
Exit:
	if (hstmt != NULL) {
		SQLFreeStmt(hstmt, SQL_CLOSE);
		hstmt = NULL;
	}
	return is_succeeded;
}

bool exportJsonFromDB(std::vector<std::wstring> tableNameList, std::wstring accountUid, SQLHDBC hDbc, Json::Value& root) {
	if (root == NULL) {
		Log(LOG_ERROR, L"Failed to export JSON cause root is NULL");
		return false;
	}
	root[get_utf8(g_accout_uid_field_name)] = get_utf8(accountUid);
	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
		std::wstring current_tablename = tableNameList[tableidx];
		std::vector<std::wstring> auto_colname_list;
		Json::Value current_table;
		auto_colname_list = sqlfSingleCol(hDbc, L"SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '%s'", current_tablename.c_str());
		current_table = sqlfMultiCol(hDbc, current_tablename, L"SELECT * FROM %s WHERE %s = %s", current_tablename.c_str(), g_accout_uid_field_name.c_str(), accountUid.c_str());
		for (int rowidx = 0; rowidx < (int) current_table.size(); rowidx++) {
			current_table[rowidx].removeMember(get_utf8(g_accout_uid_field_name));
			for (int autoincidx = 0; autoincidx < auto_colname_list.size(); autoincidx++) {
				current_table[rowidx].removeMember(get_utf8(auto_colname_list[autoincidx]));
			}
		}
		if (!current_table.empty()) {
			root[get_utf8(current_tablename)] = current_table;
		}
	}
	return true;
}

bool writeJsonFile(Json::Value root, std::wstring fileName) {
	errno_t file_err;
	FILE* json_file = NULL;
	Json::StyledWriter writer;
	std::string output_config;
	output_config = writer.write(root);
	file_err = _wfopen_s(&json_file, fileName.c_str(), L"wb");
	if (file_err != 0) {
		Log(LOG_ERROR, L"Failed to create JSON file");
		return false;
	}
	size_t written_file_size = fwrite(output_config.c_str(), 1, output_config.length(), json_file);
	if (written_file_size != output_config.length()) {
		Log(LOG_ERROR, L"Failed to write JSON file");
		return false;
	}
	if (ferror(json_file)) {
		Log(LOG_ERROR, L"Failed to set stream");
		return false;
	}
	if (fclose(json_file) != 0) {
		Log(LOG_ERROR, L"Failed to close JSON file");
		return false;
	}
	return true;
}

bool readJsonFile(Json::Value& root, std::wstring fileName) {
	std::ifstream ifs;
	Json::CharReaderBuilder builder;
	JSONCPP_STRING errs;
	ifs.open(fileName);
	if (ifs.good()) {
		if (parseFromStream(builder, ifs, &root, &errs)) {
			Log(LOG_ERROR, get_utf16(errs));
			return true;
		}
	}
	return false;
}

bool deleteAccountDataFromTable(SQLHDBC hDbc, std::wstring tableName, std::wstring accountUid) {
	SQLHSTMT hstmt = NULL;
	if (!sqlfExec(hstmt, hDbc, L"IF EXISTS(SELECT * FROM %s WHERE %s = %s) BEGIN DELETE FROM %s WHERE %s = %s END", tableName.c_str(), g_accout_uid_field_name.c_str(), accountUid.c_str(), tableName.c_str(), g_accout_uid_field_name.c_str(), accountUid.c_str())) {
		Log(LOG_ERROR, L"Failed to delete account data from table");
		return false;
	}
	if (hstmt != NULL) {
		SQLFreeStmt(hstmt, SQL_CLOSE);
		hstmt = NULL;
	}
	return true;
}

std::wstring getValFromINIFile(const WCHAR* connSection, const WCHAR* key, const WCHAR* defaultVal) {
	const int val_bufsize = 1024;
	WCHAR val_buf[val_bufsize];
	GetPrivateProfileString(connSection, key, defaultVal, val_buf, val_bufsize, INI_FILE_NAME);
	return std::wstring(val_buf);
}

int wmain(int argc, _In_reads_(argc) const WCHAR** argv) {
	int return_val = SUCCESS;
	SQLHENV henv = NULL;
	SQLHDBC hdbc = NULL;
	SQLHSTMT hstmt = NULL;
	std::vector<std::wstring> uid_exist_tablename_list;
	std::vector<std::wstring> exclusion_table_name_list;
	Json::Value root;
	const WCHAR* accountuid = NULL;
	const WCHAR* source = NULL;
	const WCHAR* target = NULL;
	const WCHAR* conn_section = NULL;
	const WCHAR* conn_string = NULL;
	setlocale(LC_ALL, "en-US.UTF-8");

	// ARGOARSE SET
	int export_json = 0;
	int import_json = 0;
	int delete_rows = 0;
	int print_tables = 0;
	int verbose = 0;

	// CONFIG SECTION
	g_standard_log_severity_lv = GetPrivateProfileInt(L"CONFIG", L"LOG_STANDARD_SEVERITY_LEVEL", LOG_ALL, INI_FILE_NAME);
	g_accout_uid_field_name = getValFromINIFile(L"CONFIG", L"ACCOUNT_UID_FIELD_NAME", DEFALUT_EMPTY_VAL);
	std::wstring exclusion_table_names = getValFromINIFile(L"CONFIG", L"IGNORE_TABLE_NAME_LIST", DEFALUT_EMPTY_VAL);
	std::wstring temp;
	std::wstringstream wss(exclusion_table_names);
	while (std::getline(wss, temp, L','))
		exclusion_table_name_list.push_back(temp);

	// Argparse
	struct argparse_option options[] = {
		OPT_HELP(),
		OPT_GROUP(L"Basic options"),
		OPT_BOOLEAN(L'e', L"export", &export_json, L"Export JSON file from DB", NULL, 0, 0),
		OPT_BOOLEAN(L'i', L"import", &import_json, L"Import JSON file into DB", NULL, 0, 0),
		OPT_BOOLEAN(L'd', L"delete", &delete_rows, L"Delete rows(same accounUID exists) of DB", NULL, 0, 0),
		OPT_BOOLEAN(L'p', L"print", &print_tables, L"Print all columns from tables where same accountUID exists ", NULL, 0, 0),
		OPT_STRING(L's', L"source", &source, L"Source DB connection string or source JSON file name", NULL, 0, 0),
		OPT_STRING(L't', L"target", &target, L"Target JSON file name or target DB connection string", NULL, 0, 0),
		OPT_STRING(L'c', L"connect", &conn_section, L"Section name in INI file for connection to DB", NULL, 0, 0),
		OPT_STRING(L'u', L"uid", &accountuid, L"Target accountUID", NULL, 0, 0),
		OPT_BOOLEAN(L'v', L"verbosse", &verbose, L"Provides additional details", NULL, 0, 0),
		OPT_END(),
	};

	struct argparse argparse;
	argparse_init(&argparse, options, USAGES, 0);
	argparse_describe(&argparse, L"This program supports export and import JSON file from or to DB.\n", L"ex) -e -u 100000000 -s connecttionstring -t jsonfilename\nex) -i -u 100000000 -s jsonfilename -t connecttionstring\nex) -d -u 100000000 -t connecttionstring\nex) -p -u 100000000 -t connecttionstring\nex) -e -u 100000000 -c connectsection -t jsonfilename\nex) -i -u 100000000 -s jsonfilename -c connectsection\nex) -d -u 100000000 -c connectsection\nex) -p -u 100000000 -c connectsection\n");
	argparse_parse(&argparse, argc, argv);
	if (argc < 2) {
		argparse_usage(&argparse);
		return SUCCESS;
	}
	if (accountuid == NULL) {
		Log(LOG_ERROR, L"Target accountUID is needed");
		return ERROR_BAD_ARG;
	}
	if (verbose != 0) {
		g_verbose = verbose;
	}
	if (export_json == 1) {
		if (target == NULL || ((source == NULL) == (conn_section == NULL))) {
			Log(LOG_ERROR, L"Invalid argument");
			return ERROR_BAD_ARG;
		}
	}
	if (import_json == 1) {
		if (source == NULL || ((target == NULL) == (conn_section == NULL))) {
			Log(LOG_ERROR, L"Invalid argument");
			return ERROR_BAD_ARG;
		}
	}

	// Mode Choice
	if (export_json + import_json + delete_rows + print_tables != 1) {
		Log(LOG_ERROR, L"This mode is not supported");
		return ERROR_BAD_ARG;
	}

	// Using Connectionstring OR ConnectSection
	if (conn_section == NULL) {
		if (export_json == 1) {
			conn_string = source;
		}
		else {
			conn_string = target;
		}
	}
	else {
		const int wcsbufsize = 1024;
		WCHAR wcsbuf[wcsbufsize];
		std::wstring val_dsn = getValFromINIFile(conn_section, L"DSN", DEFALUT_EMPTY_VAL);
		std::wstring val_trusted_connection = getValFromINIFile(conn_section, L"trusted_connection", DEFAULT_TRRUSTED_CONNECTION_VAL);
		std::wstring val_uid = getValFromINIFile(conn_section, L"UID", DEFALUT_EMPTY_VAL);
		std::wstring val_pwd = getValFromINIFile(conn_section, L"PWD", DEFALUT_EMPTY_VAL);
		std::wstring val_database = getValFromINIFile(conn_section, L"Database", DEFALUT_EMPTY_VAL);
		swprintf_s(wcsbuf, wcsbufsize, L"DSN=%s;trusted_connection=%s;UID=%s;PWD=%s;Database=%s;", val_dsn.c_str(), val_trusted_connection.c_str(), val_uid.c_str(), val_pwd.c_str(), val_database.c_str());
		conn_string = wcsbuf;
	}

	// Connect DB
	SUCCEEDED_CHECK(connectToDB(henv, hdbc, conn_string), ERROR_CONNECT_DB, L"Connecting DB");

	uid_exist_tablename_list = getSubtractedWstrList(sqlfSingleCol(hdbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' \
        INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", g_accout_uid_field_name.c_str()), exclusion_table_name_list);

	if (export_json == 1) {
		SUCCEEDED_CHECK(exportJsonFromDB(uid_exist_tablename_list, accountuid, hdbc, root), ERROR_EXPORT_JSON, L"Exporting JSON from DB");

		SUCCEEDED_CHECK(writeJsonFile(root, target), ERROR_WRITING_FILE, L"Writing JSON file");
	}
	else if (import_json == 1) {
		SUCCEEDED_CHECK(readJsonFile(root, source), ERROR_READING_FILE, L"Reading JSON file");

		if (root == NULL) {
			Log(LOG_ERROR, L"Failed to check current table name cause root is NULL");
			return false;
		}
		for (Json::Value::iterator iter = root.begin(); iter != root.end(); ++iter) {
			Json::Value current_key = iter.key();
			std::wstring current_tablename = get_utf16(current_key.asString());
			if (current_tablename.compare(g_accout_uid_field_name) == 0)
				continue;
			SUCCEEDED_CHECK(checkJsonTableNameInTableList(current_tablename, uid_exist_tablename_list), ERROR_TABLE_NOT_EXSIT, L"Check if table name in Json exists in TableList");
		}

		SUCCEEDED_CHECK(importJsonIntoDB(root, hdbc, accountuid), ERROR_READING_FILE, L"Importing Json into DB");
	}
	else if (delete_rows == 1) {
		for (int tableidx = 0; tableidx < uid_exist_tablename_list.size(); tableidx++) {
			SUCCEEDED_CHECK(deleteAccountDataFromTable(hdbc, uid_exist_tablename_list[tableidx], accountuid), ERROR_DELETE_TABLEROWS, L"Deleteing table rows");
		}
	}
	else if (print_tables == 1) {
		for (int tableidx = 0; tableidx < uid_exist_tablename_list.size(); tableidx++) {
			std::wstring current_tablename = uid_exist_tablename_list[tableidx];
			fwprintf(stdout, L"TABLE NAME : %s\n", current_tablename.c_str());
			SUCCEEDED_CHECK(printTable(hdbc, L"SELECT * FROM %s WHERE %s = %s", current_tablename.c_str(), g_accout_uid_field_name.c_str(), accountuid), ERROR_PRINT_TABLE, L"Printing tables");
		}
	}
Exit:
	if (!disconnectDB(henv, hdbc, hstmt)) {
		fwprintf(stderr, L"Failed : Disconnecting DB\n");
		return ERROR_DISCONNECT_DB;
	}
	fwprintf(stdout, L"SUCCESS : Disconnecting DB\n");
	return return_val;
}

