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

#define APP_NAME "JsonUserDB"

#define LOG_OFF 0
#define LOG_FATAL 1
#define LOG_ERROR 2
#define LOG_WARN 3
#define LOG_INFO 4
#define LOG_DEBUG 5
#define LOG_TRACE 6
#define LOG_ALL 7

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

std::wstring g_accout_uid_field_name;

// ini FILE
const WCHAR* DEFALUT_EMPTY_VAL = L"";
const WCHAR* DEFAULT_TRRUSTED_CONNECTION_VAL = L"No";
const WCHAR* INI_FILE_NAME = L".\\JsonUserDB.ini";

int g_standard_log_severity_lv;

class LOG {
public:
	LOG(int severityLv, std::wstring msg) {
		this->severity_lv = severityLv;
		this->msg = msg;
	}
	void setSeverityLV(int severityLv) {
		this->severity_lv = severityLv;
	}
	void setContent(std::wstring content) {
		this->msg = content;
	}
	int getSeverityLV() {
		return this->severity_lv;
	}
	std::wstring getContent() {
		return this->msg;
	}
	void printMsg() {
		fwprintf(stderr, L"%s\n", this->msg.c_str());
	}
	void printMsgAccordingToStandard() {
		if (this->severity_lv <= g_standard_log_severity_lv) {
			printMsg();
		}
	}
private:
	int severity_lv;
	std::wstring msg;
};


bool checkJsonTableNamseInTableList(Json::Value root, std::vector<std::wstring> tableNameList) {
	if (root == NULL) {
		LOG(LOG_ERROR, L"Failed to check current table name cause root is NULL").printMsgAccordingToStandard();
		return false;
	}
	for (Json::Value::iterator iter = root.begin(); iter != root.end(); ++iter) {
		Json::Value current_key = iter.key();
		std::wstring current_tablename = get_utf16(current_key.asString());
		if (current_tablename.compare(g_accout_uid_field_name) == 0)
			continue;
		if (!(std::find(tableNameList.begin(), tableNameList.end(), current_tablename) != tableNameList.end())) {
			LOG(LOG_ERROR, L"Failed to find current table name in table name list").printMsgAccordingToStandard();
			return false;
		}
	}
	return true;
}

bool importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUid) {
	bool is_succeeded = false;
	SQLHSTMT hstmt = NULL;
	if (root == NULL) {
		LOG(LOG_ERROR, L"Failed to import json cause root is null").printMsgAccordingToStandard();
		return is_succeeded;
	}
	for (Json::Value::iterator iter = root.begin(); iter != root.end(); ++iter) {
		Json::Value current_key = iter.key();
		std::wstring current_tablename = get_utf16(current_key.asString());
		if (current_tablename.compare(g_accout_uid_field_name) != 0) {
			Json::Value current_table = root[get_utf8(current_tablename)];
			Json::Value col_infos = sqlfMultiCol(hDbc, current_tablename, L"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'", current_tablename.c_str());
			for (int rowidx = 0; rowidx < (int)current_table.size(); rowidx++) {
				sqlbuilder::InsertModel insert_model;
				Json::Value current_row = current_table[rowidx];
				if (current_row.size() <= 0)
					continue;
				insert_model.insert(g_accout_uid_field_name, accountUid).into(current_tablename);
				for (Json::Value::const_iterator current_row_iter = current_row.begin(); current_row_iter != current_row.end(); current_row_iter++) {
					Json::String json_colname = current_row_iter.key().asCString();
					std::wstring colname = get_utf16(json_colname);
					std::wstring val = get_utf16(current_row[json_colname].asString());
					for (const Json::Value& col_info : col_infos){
						if (col_info["COLUMN_NAME"].asString() == json_colname){
							if (col_info["DATA_TYPE"].asString() == "binary") {
								insert_model.insertBinaryType(colname, val);
								break;
							}
							else {
								insert_model.insert(colname, val);
								break;
							}
						}
					}
				}
				is_succeeded = sqlfExec(hstmt, hDbc, (insert_model.str()).c_str());
				if (!is_succeeded) {
					LOG(LOG_ERROR, L"Failed to insert values in DB").printMsgAccordingToStandard();
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
		LOG(LOG_ERROR, L"Failed to export JSON cause root is NULL").printMsgAccordingToStandard();
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
		LOG(LOG_ERROR, L"Failed to create JSON file").printMsgAccordingToStandard();
		return false;
	}
	size_t written_file_size = fwrite(output_config.c_str(), 1, output_config.length(), json_file);
	if (written_file_size != output_config.length()) {
		LOG(LOG_ERROR, L"Failed to write JSON file").printMsgAccordingToStandard();
		return false;
	}
	if (ferror(json_file)) {
		LOG(LOG_ERROR, L"Failed to set stream").printMsgAccordingToStandard();
		return false;
	}
	if (fclose(json_file) != 0) {
		LOG(LOG_ERROR, L"Failed to close JSON file").printMsgAccordingToStandard();
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
			LOG(LOG_ERROR, get_utf16(errs)).printMsgAccordingToStandard();
			return true;
		}
	}
	return false;
}

bool deleteAccountDataFromTable(SQLHDBC hDbc, std::wstring tableName, std::wstring accountUid) {
	SQLHSTMT hstmt = NULL;
	if (!sqlfExec(hstmt, hDbc, L"IF EXISTS(SELECT * FROM %s WHERE %s = %s) BEGIN DELETE FROM %s WHERE %s = %s END", tableName.c_str(), g_accout_uid_field_name.c_str(), accountUid.c_str(), tableName.c_str(), g_accout_uid_field_name.c_str(), accountUid.c_str())) {
		LOG(LOG_ERROR, L"Failed to delete account data from table").printMsgAccordingToStandard();
		return false;
	}
	if (hstmt != NULL) {
		SQLFreeStmt(hstmt, SQL_CLOSE);
		hstmt = NULL;
	}
	return true;
}

//bool isUidInTables(SQLHDBC hDbc, std::wstring accountUid, std::vector<std::wstring> tableNameList) {
//	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
//		std::vector<std::wstring> accountuidlist = sqlfSingleCol(hDbc, L"SELECT %s FROM %s WHERE %s = %s", ACCOUNT_UID_FIELD_NAME.c_str(), tableNameList[tableidx].c_str(), ACCOUNT_UID_FIELD_NAME.c_str(), accountUid.c_str());
//		if (!accountuidlist.empty()) {
//			return true;
//		}
//	}
//	return false;
//}

int wmain(int argc, _In_reads_(argc) const WCHAR** argv) {
	int return_val = SUCCESS;
	SQLHENV henv = NULL;
	SQLHDBC hdbc = NULL;
	SQLHSTMT hStmt = NULL;
	std::vector<std::wstring> accountuid_list;
	std::vector<std::wstring> uid_exist_tablename_list;
	std::vector<std::wstring> exclusion_table_name_list;
	Json::Value root;
	const WCHAR* accountuid = NULL;
	const WCHAR* source = NULL;
	const WCHAR* target = NULL;
	const WCHAR* connsection = NULL;
	const WCHAR* connstring = NULL;
	setlocale(LC_ALL, "en-US.UTF-8");

	// ARGOARSE SET
	int exportjson = 0;
	int importjson = 0;
	int deleterows = 0;
	int printtables = 0;
	int verbose = 0;

	// Argparse
	struct argparse_option options[] = {
		OPT_HELP(),
		OPT_GROUP(L"Basic options"),
		OPT_BOOLEAN(L'e', L"export", &exportjson, L"Export JSON File From DB", NULL, 0, 0),
		OPT_BOOLEAN(L'i', L"import", &importjson, L"Import JSON File Into DB", NULL, 0, 0),
		OPT_BOOLEAN(L'd', L"delete", &deleterows, L"Delete Rows(Same AccounUID Exists) of DB", NULL, 0, 0),
		OPT_BOOLEAN(L'p', L"print", &printtables, L"Print all columns From Tables Where Same AccountUID Exists ", NULL, 0, 0),
		OPT_STRING(L's', L"source", &source, L"Source DB connectionstring or Source jsonfile name", NULL, 0, 0),
		OPT_STRING(L't', L"target", &target, L"Target jsonfile name or Target DB connectionstring", NULL, 0, 0),
		OPT_STRING(L'c', L"connect", &connsection, L"Section Name in ini File for Connecttion to DB", NULL, 0, 0),
		OPT_STRING(L'u', L"uid", &accountuid, L"Target AccountUID", NULL, 0, 0),
		OPT_BOOLEAN(L'v', L"verbosse", &verbose, L"Provides additional details", NULL, 0, 0),
		OPT_END(),
	};

	struct argparse argparse;
	argparse_init(&argparse, options, USAGES, 0);
	argparse_describe(&argparse, L"This program supports Export and Import JSON FILE From or To DB.\n", L"ex) -e -u 100000000 -s connecttionstring -t jsonfilename\nex) -i -u 100000000 -s jsonfilename -t connecttionstring\nex) -d -u 100000000 -t connecttionstring\nex) -p -u 100000000 -t connecttionstring\nex) -e -u 100000000 -c connectsection -t jsonfilename\nex) -i -u 100000000 -s jsonfilename -c connectsection\nex) -d -u 100000000 -c connectsection\nex) -p -u 100000000 -c connectsection\n");
	argparse_parse(&argparse, argc, argv);
	if (argc < 2) {
		argparse_usage(&argparse);
		return SUCCESS;
	}
	if (accountuid == NULL) {
		fwprintf(stderr, L"Invalid argument\n");
		return ERROR_BAD_ARG;
	}
	if (verbose != 0) {
		g_verbose = verbose;
	}
	if (exportjson == 1) {
		if (target == NULL || ((source == NULL) == (connsection == NULL))) {
			fwprintf(stderr, L"Invalid argument\n");
			return ERROR_BAD_ARG;
		}
	}
	if (importjson == 1) {
		if (source == NULL || ((target == NULL) == (connsection == NULL))) {
			fwprintf(stderr, L"Invalid argument\n");
			return ERROR_BAD_ARG;
		}
	}

	// Mode Choice
	if (exportjson + importjson + deleterows + printtables != 1) {
		fwprintf(stderr, L"This mode is not supported\n");
		return ERROR_BAD_ARG;
	}

	// Using Connectionstring OR ConnectSection
	if (connsection == NULL) {
		if (exportjson == 1) {
			connstring = source;
		}
		else {
			connstring = target;
		}
	}
	else {
		const int wcsbufsize = 1024;
		WCHAR wcsbuf[wcsbufsize];
		const int val_bufsize = 1024;
		WCHAR val_dsn_buf[val_bufsize];
		WCHAR val_database_buf[val_bufsize];
		WCHAR val_trusted_connection_buf[val_bufsize];
		WCHAR val_uid_buf[val_bufsize];
		WCHAR val_pwd_buf[val_bufsize];
		GetPrivateProfileString(connsection, L"DSN", DEFALUT_EMPTY_VAL, val_dsn_buf, val_bufsize, INI_FILE_NAME);
		GetPrivateProfileString(connsection, L"trusted_connection", DEFAULT_TRRUSTED_CONNECTION_VAL, val_trusted_connection_buf, val_bufsize, INI_FILE_NAME);
		GetPrivateProfileString(connsection, L"UID", DEFALUT_EMPTY_VAL, val_uid_buf, val_bufsize, INI_FILE_NAME);
		GetPrivateProfileString(connsection, L"PWD", DEFALUT_EMPTY_VAL, val_pwd_buf, val_bufsize, INI_FILE_NAME);
		GetPrivateProfileString(connsection, L"Database", DEFALUT_EMPTY_VAL, val_database_buf, val_bufsize, INI_FILE_NAME);
		swprintf_s(wcsbuf, wcsbufsize, L"DSN=%s;trusted_connection=%s;UID=%s;PWD=%s;Database=%s;", val_dsn_buf, val_trusted_connection_buf, val_uid_buf, val_pwd_buf, val_database_buf);
		connstring = wcsbuf;
	}

	// CONFIG SECTION
	const int val_bufsize = 1024;
	std::wstring wstignore_table_names;
	WCHAR accout_uid_field[val_bufsize];
	WCHAR ignore_table_names[val_bufsize];
	g_standard_log_severity_lv = GetPrivateProfileInt(L"CONFIG", L"LOG_STANDARD_SEVERITY_LEVEL", LOG_OFF, INI_FILE_NAME);
	GetPrivateProfileString(L"CONFIG", L"ACCOUNT_UID_FIELD_NAME", DEFALUT_EMPTY_VAL, accout_uid_field, val_bufsize, INI_FILE_NAME);
	g_accout_uid_field_name = std::wstring(accout_uid_field);
	GetPrivateProfileString(L"CONFIG", L"IGNORE_TABLE_NAME_LIST", DEFALUT_EMPTY_VAL, ignore_table_names, val_bufsize, INI_FILE_NAME);
	wstignore_table_names = std::wstring(ignore_table_names);
	WCHAR* pwc;
	WCHAR* pt;
	pwc = wcstok_s(ignore_table_names, L",", &pt);
	while (pwc != NULL)
	{
		exclusion_table_name_list.push_back(std::wstring(pwc));
		pwc = wcstok_s(NULL, L",", &pt);
	}

	// Connect DB
	SUCCEEDED_CHECK(connectToDB(henv, hdbc, connstring), ERROR_CONNECT_DB, L"Connecting DB");

	uid_exist_tablename_list = getSubtractedWstrList(sqlfSingleCol(hdbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' \
        INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", g_accout_uid_field_name.c_str()), exclusion_table_name_list);

	if (exportjson == 1) {
		SUCCEEDED_CHECK(exportJsonFromDB(uid_exist_tablename_list, accountuid, hdbc, root), ERROR_EXPORT_JSON, L"Exporting JSON from DB");

		SUCCEEDED_CHECK(writeJsonFile(root, target), ERROR_WRITING_FILE, L"Writing JSON file");
	}
	else if (importjson == 1) {
		SUCCEEDED_CHECK(readJsonFile(root, source), ERROR_READING_FILE, L"Reading JSON file");

		SUCCEEDED_CHECK(checkJsonTableNamseInTableList(root, uid_exist_tablename_list), ERROR_TABLE_NOT_EXSIT, L"Check if table name in Json exists in DB");

		SUCCEEDED_CHECK(importJsonIntoDB(root, hdbc, accountuid), ERROR_READING_FILE, L"Importing Json into DB");
	}
	else if (deleterows == 1) {
		for (int tableidx = 0; tableidx < uid_exist_tablename_list.size(); tableidx++) {
			SUCCEEDED_CHECK(deleteAccountDataFromTable(hdbc, uid_exist_tablename_list[tableidx], accountuid), ERROR_DELETE_TABLEROWS, L"Deleteing table rows");
		}
	}
	else if (printtables == 1) {
		for (int tableidx = 0; tableidx < uid_exist_tablename_list.size(); tableidx++) {
			std::wstring current_tablename = uid_exist_tablename_list[tableidx];
			fwprintf(stdout, L"TABLE NAME : %s\n", current_tablename.c_str());
			SUCCEEDED_CHECK(printTable(hdbc, L"SELECT * FROM %s WHERE %s = %s", current_tablename.c_str(), g_accout_uid_field_name.c_str(), accountuid), ERROR_PRINT_TABLE, L"Printing tables");
		}
	}
Exit:
	if (!disconnectDB(henv, hdbc, hStmt)) {
		fwprintf(stderr, L"Failed : Disconnecting DB\n");
		return ERROR_DISCONNECT_DB;
	}
	fwprintf(stdout, L"SUCCESS : Disconnecting DB\n");
	return return_val;
}

