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
#include <unordered_set>

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

static std::unordered_map<std::wstring, int> COLUMN_DATA_TYPE({ {L"int", SQL_INTEGER},\
																	{L"tinyint", SQL_TINYINT},\
																	{L"smallint", SQL_SMALLINT},\
																	{L"float", SQL_FLOAT},\
																	{L"double", SQL_DOUBLE},\
																	{L"bit", SQL_BIT},\
																	{L"binary", SQL_BINARY},\
																	{L"varbinary", SQL_VARBINARY},\
																	{L"bigint", SQL_BIGINT},\
																	{L"char", SQL_CHAR},\
																	{L"varchar", SQL_VARCHAR},\
																	{L"decimal", SQL_DECIMAL},\
																	{L"numeric", SQL_NUMERIC}, \
																	{L"nchar", SQL_WCHAR},\
																	{L"nvarchar", SQL_WVARCHAR},\
																	{L"date", SQL_DATE},\
																	{L"time", SQL_TIME},\
																	{L"timestamp", SQL_TIMESTAMP},\
																	{L"smalldatetime", SQL_TIMESTAMP},\
																	{L"datetime", SQL_TIMESTAMP},\
																	{L"datetime2", SQL_TIMESTAMP}});

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

bool importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUid, std::unordered_set<std::wstring> tableNameSet) {
	bool is_succeeded = false;
	SQLHSTMT hstmt = NULL;
	if (root == NULL) {
		Log(LOG_ERROR, L"Failed to import json cause root is null");
		return false;
	}
	if (!SQL_SUCCEEDED(SQLSetConnectAttr(hDbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER))) {
		Log(LOG_ERROR, L"Failed to autocommit off");
		return false;
	}
	for (Json::Value::iterator iter = root.begin(); iter != root.end(); ++iter) {
		Json::Value current_key = iter.key();
		std::wstring current_table_name = current_key.asWstring();
		if (current_table_name.compare(g_accout_uid_field_name) == 0)
			continue;
		if (!checkWstrInWstrSet(current_table_name, tableNameSet)) {
			is_succeeded = false;
			Log(LOG_ERROR, L"Failed to find current table name in table name list");
			goto Exit;
		}
		Json::Value current_table = root[get_utf8(current_table_name)];
		Json::Value col_infos = sqlfMultiCol(hDbc, current_table_name, L"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'", current_table_name.c_str());
		std::unordered_map<std::wstring, std::pair<std::wstring, std::wstring>> hm_col_infos;
		for (const Json::Value& col_info : col_infos) {
			if (col_info.size() <= 0)
				continue;
			hm_col_infos[col_info["COLUMN_NAME"].asWstring()] = std::make_pair(col_info["DATA_TYPE"].asWstring(), col_info["CHARACTER_MAXIMUM_LENGTH"].asWstring());
		}
		for (int rowidx = 0; rowidx < (int)current_table.size(); rowidx++) {
			sqlbuilder::InsertModel insert_model;
			Json::Value current_row = current_table[rowidx];
			if (current_row.size() <= 0)
				continue;
			insert_model.insert(g_accout_uid_field_name, accountUid).into(current_table_name);
			for (Json::Value::const_iterator current_row_iter = current_row.begin(); current_row_iter != current_row.end(); current_row_iter++) {
				Json::String json_col_name = current_row_iter.key().asCString();
				std::wstring col_name = get_utf16(json_col_name);
				std::wstring data_type = hm_col_infos[col_name].first;
				int col_type = COLUMN_DATA_TYPE[data_type];
				switch (col_type) {
				case SQL_INTEGER:
				case SQL_TINYINT:
				case SQL_SMALLINT: {
					int val = current_row[json_col_name].asInt();
					insert_model.insert(col_name, val);
					break;
				}
				case SQL_FLOAT: {
					float val = current_row[json_col_name].asFloat();
					insert_model.insert(col_name, val);
					break;
				}
				case SQL_DOUBLE: {
					double val = current_row[json_col_name].asDouble();
					insert_model.insert(col_name, val);
					break;
				}
				case SQL_BIT: {
					bool val = current_row[json_col_name].asBool();
					insert_model.insert(col_name, val);
					break;
				}
				case SQL_BINARY:
				case SQL_VARBINARY: {
					std::wstring val = current_row[json_col_name].asWstring();
					insert_model.insertBinaryType(col_name, val);
					break;
				}
				case SQL_BIGINT:
				case SQL_CHAR:
				case SQL_VARCHAR:
				case SQL_DECIMAL:
				case SQL_NUMERIC:
				case SQL_WCHAR:
				case SQL_WVARCHAR:
				case SQL_DATE:
				case SQL_TIME:
				case SQL_TIMESTAMP: {
					std::wstring val = current_row[json_col_name].asWstring();
					insert_model.insert(col_name, val);
					break;
				}
				default:
					Log(LOG_ERROR, L"Unsupported data type");
					goto Exit;
				}
			}
			is_succeeded = sqlfExec(hstmt, hDbc, (insert_model.str()).c_str());
			if (!is_succeeded) {
				Log(LOG_ERROR, L"Failed to insert values in DB");
				goto Exit;
			}
			TRYODBC(hstmt, SQL_HANDLE_STMT, SQLFreeStmt(hstmt, SQL_CLOSE));
			hstmt = NULL;
		}
	}
Exit:
	if (is_succeeded) {
		if (!SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, hDbc, SQL_COMMIT))) {
			Log(LOG_ERROR, L"Failed to end transaction");
			is_succeeded = false;
		}
	}
	else {
		if (!SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, hDbc, SQL_ROLLBACK))) {
			Log(LOG_ERROR, L"Failed to end transaction");
		}
	}
	if (!SQL_SUCCEEDED(SQLSetConnectAttr(hDbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, SQL_IS_UINTEGER))) {
		Log(LOG_ERROR, L"Failed to autocommit on");
		is_succeeded = false;
	}
	if (hstmt != NULL) {
		SQLFreeStmt(hstmt, SQL_CLOSE);
		hstmt = NULL;
	}
	return is_succeeded;
}

bool exportJsonFromDB(std::unordered_set<std::wstring> tableNameSet, std::wstring accountUid, SQLHDBC hDbc, Json::Value& root) {
	if (root == NULL) {
		Log(LOG_ERROR, L"Failed to export JSON cause root is NULL");
		return false;
	}
	root[get_utf8(g_accout_uid_field_name)] = get_utf8(accountUid);
	for (const std::wstring current_table_name : tableNameSet) {
		std::unordered_set<std::wstring> auto_col_name_set = sqlfSingleCol(hDbc, L"SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '%s'", current_table_name.c_str());
		Json::Value current_table = sqlfMultiCol(hDbc, current_table_name, L"SELECT * FROM %s WHERE %s = %s", current_table_name.c_str(), g_accout_uid_field_name.c_str(), accountUid.c_str());
		for (int rowidx = 0; rowidx < (int) current_table.size(); rowidx++) {
			current_table[rowidx].removeMember(get_utf8(g_accout_uid_field_name));
			for (const std::wstring auto_col_name : auto_col_name_set)
				current_table[rowidx].removeMember(get_utf8((auto_col_name)));
		}
		if (!current_table.empty()) {
			root[get_utf8(current_table_name)] = current_table;
		}
	}
	return true;
}

bool writeJsonFile(Json::Value root, std::wstring fileName) {
	errno_t file_err;
	FILE* json_file = NULL;
	Json::StyledWriter writer;
	std::string output_config = writer.write(root);
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
	const int val_buf_size = 1024;
	WCHAR val_buf[val_buf_size];
	GetPrivateProfileString(connSection, key, defaultVal, val_buf, val_buf_size, INI_FILE_NAME);
	return std::wstring(val_buf);
}

int wmain(int argc, _In_reads_(argc) const WCHAR** argv) {
	int return_val = SUCCESS;
	SQLHENV henv = NULL;
	SQLHDBC hdbc = NULL;
	SQLHSTMT hstmt = NULL;
	std::unordered_set<std::wstring> uid_exist_table_name_set;
	std::unordered_set<std::wstring> exclusion_table_name_set;
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
		exclusion_table_name_set.insert(temp);

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
		const int wcsbuf_size = 1024;
		WCHAR wcs_buf[wcsbuf_size];
		std::wstring val_dsn = getValFromINIFile(conn_section, L"DSN", DEFALUT_EMPTY_VAL);
		std::wstring val_trusted_connection = getValFromINIFile(conn_section, L"trusted_connection", DEFAULT_TRRUSTED_CONNECTION_VAL);
		std::wstring val_uid = getValFromINIFile(conn_section, L"UID", DEFALUT_EMPTY_VAL);
		std::wstring val_pwd = getValFromINIFile(conn_section, L"PWD", DEFALUT_EMPTY_VAL);
		std::wstring val_database = getValFromINIFile(conn_section, L"Database", DEFALUT_EMPTY_VAL);
		swprintf_s(wcs_buf, wcsbuf_size, L"DSN=%s;trusted_connection=%s;UID=%s;PWD=%s;Database=%s;", val_dsn.c_str(), val_trusted_connection.c_str(), val_uid.c_str(), val_pwd.c_str(), val_database.c_str());
		conn_string = wcs_buf;
	}

	// Connect DB
	SUCCEEDED_CHECK(connectToDB(henv, hdbc, conn_string), ERROR_CONNECT_DB, L"Connecting DB");

	uid_exist_table_name_set = getDiffWstrSet(sqlfSingleCol(hdbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' \
        INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", g_accout_uid_field_name.c_str()), exclusion_table_name_set);

	if (export_json == 1) {
		SUCCEEDED_CHECK(exportJsonFromDB(uid_exist_table_name_set, accountuid, hdbc, root), ERROR_EXPORT_JSON, L"Exporting JSON from DB");

		SUCCEEDED_CHECK(writeJsonFile(root, target), ERROR_WRITING_FILE, L"Writing JSON file");
	}
	else if (import_json == 1) {
		SUCCEEDED_CHECK(readJsonFile(root, source), ERROR_READING_FILE, L"Reading JSON file");

		SUCCEEDED_CHECK(importJsonIntoDB(root, hdbc, accountuid, uid_exist_table_name_set), ERROR_READING_FILE, L"Importing Json into DB");
	}
	else if (delete_rows == 1) {
		for (const std::wstring current_table_name : uid_exist_table_name_set) {
			SUCCEEDED_CHECK(deleteAccountDataFromTable(hdbc, current_table_name, accountuid), ERROR_DELETE_TABLEROWS, L"Deleteing table rows");
		}
	}
	else if (print_tables == 1) {
		for (const std::wstring current_table_name : uid_exist_table_name_set) {
			fwprintf(stdout, L"TABLE NAME : %s\n", current_table_name.c_str());
			SUCCEEDED_CHECK(printTable(hdbc, L"SELECT * FROM %s WHERE %s = %s", current_table_name.c_str(), g_accout_uid_field_name.c_str(), accountuid), ERROR_PRINT_TABLE, L"Printing tables");
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

