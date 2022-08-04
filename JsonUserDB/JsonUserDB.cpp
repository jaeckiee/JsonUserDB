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
int RETURNVAL = SUCCESS;

#define APP_NAME "JsonUserDB"

// Usages Of Argparse
static const WCHAR* const USAGES[] = {
	L"" APP_NAME " [options] [[--] args]",
	L"" APP_NAME " [options]",
	NULL,
};

#define SUCCEEDED_CHECK(w, x, y) 		\
						fwprintf(stdout, L"START : %s\n", y);\
						if (w) {\
						fwprintf(stdout, L"SUCCESS : %s\n", y);\
						}\
						else{\
						fwprintf(stderr, L"FAIL : %s\n", y);\
						RETURNVAL = x;\
						goto Exit;\
						}

std::wstring ACCOUNT_UID_FIELD_NAME = L"AccountUID";

// ini FILE
const WCHAR* DEFALUT_EMPTY_VAL = L"";
const WCHAR* DEFAULT_TRRUSTED_CONNECTION_VAL = L"No";
const WCHAR* INI_FILE_NAME = L".\\JsonUserDB.ini";

// Modify DB tables
bool deleteTableRows(SQLHDBC hDbc, std::wstring tableName, std::wstring accountUid);
bool importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUid);

// Make JSON object
bool exportJsonFromDB(std::vector<std::wstring> tableNameList, std::wstring accountUid, SQLHDBC hDbc, Json::Value & root);

// Read And Write JSONFILE
bool writeJsonFile(Json::Value root, std::wstring fileName);
bool readJsonFile(Json::Value & root, std::wstring fileName);

// ETC
bool isUidInTables(SQLHDBC hDbc, std::wstring accountUid, std::vector<std::wstring> tableNameList);
void makeValNamesColNames(std::wstring& colNames, std::wstring& valNames, int colIdx, std::vector<std::wstring> binColNameList, Json::Value::Members colNameList, Json::Value currentRow);
bool isJsonTableNamseInTableList(Json::Value root, std::vector<std::wstring> tableNameList);

int wmain(int argc, _In_reads_(argc) const WCHAR** argv) {
	SQLHENV hEnv = NULL;
	SQLHDBC hDbc = NULL;
	SQLHSTMT hStmt = NULL;
	std::vector<std::wstring> accountuidlist;
	std::vector<std::wstring> uidexisttablenamelist;
	std::vector<std::wstring> exclusiontablenamelist;
	Json::Value root;
	const WCHAR* accountuid = NULL;
	const WCHAR* source = NULL;
	const WCHAR* target = NULL;
	const WCHAR* connsection = NULL;
	const WCHAR* connstring = NULL;
	setlocale(LC_ALL, "en-US.UTF-8");
	// Set IgnoreTableNameList
	exclusiontablenamelist.push_back(L"AccountWhisper");
	exclusiontablenamelist.push_back(L"AccountWhisperCount");

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
		fwprintf(stderr, L"Argument Not correct\n");
		return ERROR_BAD_ARG;
	}
	if (verbose != 0) {
		GVERBOSE = verbose;
	}
	if (exportjson == 1) {
		if (target == NULL || ((source == NULL) == (connsection == NULL))) {
			fwprintf(stderr, L"Argument Not correct\n");
			return ERROR_BAD_ARG;
		}
	}
	if (importjson == 1) {
		if (source == NULL || ((target == NULL) == (connsection == NULL))) {
			fwprintf(stderr, L"Argument Not correct\n");
			return ERROR_BAD_ARG;
		}
	}

	// Mode Choice
	if (exportjson + importjson + deleterows + printtables != 1) {
		fwprintf(stderr, L"This Mode Not Supported\n");
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
		const int wcsbufsize = 1000;
		WCHAR wcsbuf[wcsbufsize];
		const int valbufsize = 512;
		WCHAR valdsnbuf[valbufsize];
		WCHAR valdatabasebuf[valbufsize];
		WCHAR valtrustedconnectionbuf[valbufsize];
		WCHAR valuidbuf[valbufsize];
		WCHAR valpwdbuf[valbufsize];
		GetPrivateProfileString(connsection, L"DSN", DEFALUT_EMPTY_VAL, valdsnbuf, valbufsize, INI_FILE_NAME);
		GetPrivateProfileString(connsection, L"trusted_connection", DEFAULT_TRRUSTED_CONNECTION_VAL, valtrustedconnectionbuf, valbufsize, INI_FILE_NAME);
		GetPrivateProfileString(connsection, L"UID", DEFALUT_EMPTY_VAL, valuidbuf, valbufsize, INI_FILE_NAME);
		GetPrivateProfileString(connsection, L"PWD", DEFALUT_EMPTY_VAL, valpwdbuf, valbufsize, INI_FILE_NAME);
		GetPrivateProfileString(connsection, L"Database", DEFALUT_EMPTY_VAL, valdatabasebuf, valbufsize, INI_FILE_NAME);
		swprintf_s(wcsbuf, wcsbufsize, L"DSN=%s;trusted_connection=%s;UID=%s;PWD=%s;Database=%s;", valdsnbuf, valtrustedconnectionbuf, valuidbuf, valpwdbuf, valdatabasebuf);
		connstring = wcsbuf;
	}

	// Connect DB
	SUCCEEDED_CHECK(connectToDB(hEnv, hDbc, connstring), ERROR_CONNECT_DB, L"Connect DB");

	uidexisttablenamelist = getSubtractedWstrList(sqlfSingleCol(hDbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' \
        INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", ACCOUNT_UID_FIELD_NAME.c_str()), exclusiontablenamelist);
	
	if (exportjson == 1) {
		SUCCEEDED_CHECK(isUidInTables(hDbc, accountuid, uidexisttablenamelist), ERROR_BAD_ARG, L"CHECK UID");

		SUCCEEDED_CHECK(exportJsonFromDB(uidexisttablenamelist, accountuid, hDbc, root), ERROR_EXPORT_JSON, L"Export JSON");

		SUCCEEDED_CHECK(writeJsonFile(root, target), ERROR_WRITING_FILE, L"Write JSON FILE");
	}
	else if (importjson == 1) {
		SUCCEEDED_CHECK(readJsonFile(root, source), ERROR_READING_FILE, L"Read JSON FILE");

		SUCCEEDED_CHECK(isJsonTableNamseInTableList(root, uidexisttablenamelist), ERROR_TABLE_NOT_EXSIT, L"CHECK JSON Table Name");

		SUCCEEDED_CHECK(importJsonIntoDB(root, hDbc, accountuid), ERROR_READING_FILE, L"Import Json To DB");
	}
	else if (deleterows == 1) {
		for (int tableidx = 0; tableidx < uidexisttablenamelist.size(); tableidx++) {
			SUCCEEDED_CHECK(deleteTableRows(hDbc, uidexisttablenamelist[tableidx], accountuid), ERROR_DELETE_TABLEROWS, L"Delete Table Rows");
		}
	}
	else if (printtables == 1) {
		for (int tableidx = 0; tableidx < uidexisttablenamelist.size(); tableidx++) {
			std::wstring currenttablename = uidexisttablenamelist[tableidx];
			fwprintf(stdout, L"TABLE NAME : %s\n", currenttablename.c_str());
			SUCCEEDED_CHECK(printTable(hDbc, L"SELECT * FROM %s WHERE %s = %s", currenttablename.c_str(), ACCOUNT_UID_FIELD_NAME.c_str(), accountuid), ERROR_PRINT_TABLE, L"Print Tables");
		}
	}
Exit:
	if (!disconnectDB(hEnv, hDbc, hStmt)) {
		fwprintf(stderr, L"FAIL : Disconnect DB\n");
		return ERROR_DISCONNECT_DB;
	}
	fwprintf(stdout, L"SUCCESS : Disconnect DB\n");
	return RETURNVAL;
}

bool deleteTableRows(SQLHDBC hDbc, std::wstring tableName, std::wstring accountUid) {
	bool issucceeded = false;
	SQLHSTMT hStmt = NULL;
	issucceeded = sqlfExec(hStmt, hDbc, L"IF EXISTS(SELECT * FROM %s WHERE %s = %s) BEGIN DELETE FROM %s WHERE %s = %s END", tableName.c_str(), ACCOUNT_UID_FIELD_NAME.c_str(), accountUid.c_str(), tableName.c_str(), ACCOUNT_UID_FIELD_NAME.c_str(), accountUid.c_str());
	TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
Exit:
	if (hStmt != SQL_CLOSE) {
		SQLFreeStmt(hStmt, SQL_CLOSE);
	}
	return issucceeded;
}


bool isJsonTableNamseInTableList(Json::Value root, std::vector<std::wstring> tableNameList) {
	if (root == NULL) {
		return false;
	}
	for (Json::Value::iterator iter = root.begin(); iter != root.end(); ++iter) {
		Json::Value currentkey = iter.key();
		std::wstring currenttablename = get_utf16(currentkey.asString());
		if (!(std::find(tableNameList.begin(), tableNameList.end(), currenttablename) != tableNameList.end()) && currenttablename.compare(ACCOUNT_UID_FIELD_NAME) != 0) {
			return false;
		}
	}
	return true;
}

bool importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUid) {
	bool issucceeded = false;
	SQLHSTMT hStmt = NULL;
	if(root == NULL) {
		return issucceeded;
	}
	for (Json::Value::iterator iter = root.begin(); iter != root.end(); ++iter) {
		std::vector<std::wstring> bincolnamelist;
		Json::Value currentkey = iter.key();
		std::wstring currenttablename = get_utf16(currentkey.asString());
		bincolnamelist = sqlfSingleCol(hDbc, L"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE DATA_TYPE = 'binary' AND TABLE_NAME = '%s'", currenttablename.c_str());
		if (currenttablename.compare(ACCOUNT_UID_FIELD_NAME) != 0) {
			Json::Value currenttable = root[get_utf8(currenttablename)];
			for (int rowidx = 0; rowidx < currenttable.size(); rowidx++) {
				Json::Value currentrow = currenttable[rowidx];
				Json::Value::Members currentrowcolnamelist = currentrow.getMemberNames();
				int currentrowcolnamelistsize = currentrowcolnamelist.size();
				if (currentrowcolnamelistsize > 0) {
					std::wstring colnames = L"";
					std::wstring valnames = L"";
					for (int colidx = 0; colidx < currentrowcolnamelistsize; colidx++) {
						makeValNamesColNames(colnames, valnames, colidx, bincolnamelist, currentrowcolnamelist, currentrow);
					}
					issucceeded = sqlfExec(hStmt, hDbc, L"INSERT INTO %s(%s, %s) VALUES('%s', %s)", currenttablename.c_str(), ACCOUNT_UID_FIELD_NAME.c_str(), colnames.c_str(), accountUid.c_str(), valnames.c_str());
					if (!issucceeded) {
						goto Exit;
					}
					TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
				}
			}
		}
	}
Exit:
	if (hStmt != SQL_CLOSE) {
		SQLFreeStmt(hStmt, SQL_CLOSE);
	}
	return issucceeded;
}

bool exportJsonFromDB(std::vector<std::wstring> tableNameList, std::wstring accountUid, SQLHDBC hDbc, Json::Value& root) {
	if (root == NULL) {
		return false;
	}
	root[get_utf8(ACCOUNT_UID_FIELD_NAME)] = get_utf8(accountUid);
	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
		std::wstring currenttablename = tableNameList[tableidx];
		std::vector<std::wstring> autocolnamelist;
		Json::Value currenttable;
		autocolnamelist = sqlfSingleCol(hDbc, L"SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '%s'", currenttablename.c_str());
		currenttable = sqlfMultiCol(hDbc, currenttablename, L"SELECT * FROM %s WHERE %s = %s", currenttablename.c_str(), ACCOUNT_UID_FIELD_NAME.c_str(), accountUid.c_str());
		for (int rowidx = 0; rowidx < currenttable.size(); rowidx++) {
			currenttable[rowidx].removeMember(get_utf8(ACCOUNT_UID_FIELD_NAME));
			for (int autoincidx = 0; autoincidx < autocolnamelist.size(); autoincidx++) {
				currenttable[rowidx].removeMember(get_utf8(autocolnamelist[autoincidx]));
			}
		}
		if (!currenttable.empty()) {
			root[get_utf8(currenttablename)] = currenttable;
		}
	}
	return true;
}

bool writeJsonFile(Json::Value root, std::wstring fileName) {
	errno_t fileerr;
	FILE* jsonfile = NULL;
	Json::StyledWriter writer;
	std::string outputconfig;
	int itemsize = 1;
	outputconfig = writer.write(root);
	fileerr = _wfopen_s(&jsonfile, fileName.c_str(), L"wb");
	if (fileerr != 0) {
		return false;
	}
	size_t fileSize = fwrite(outputconfig.c_str(), itemsize, outputconfig.length(), jsonfile);
	if (ferror(jsonfile)) {
		return false;
	}
	fclose(jsonfile);
	return true;
}

bool readJsonFile(Json::Value& root, std::wstring fileName) {
	std::ifstream ifs;
	Json::CharReaderBuilder builder;
	JSONCPP_STRING errs;
	ifs.open(fileName);
	if (ifs.good()) {
		if (parseFromStream(builder, ifs, &root, &errs)) {
			return true;
		}
	}
	return false;
}

//	변경 예정
void makeValNamesColNames(std::wstring& colNames, std::wstring& valNames, int colIdx, std::vector<std::wstring> binColNameList, Json::Value::Members colNameList, Json::Value currentRow) {
	std::wstring colname;
	std::wstring valname;
	std::wstring delimitervalfront;
	std::wstring delimitervalend;
	std::wstring delimitercolfront = L"\"";
	std::wstring delimitercolend = L"\"";
	std::wstring delimiterand = L", ";
	bool isbinarycol = false;
	colname = get_utf16(colNameList[colIdx]);
	valname = get_utf16(currentRow[colNameList[colIdx]].asString());
	for (int colidx = 0; colidx < binColNameList.size(); colidx++) {
		if (binColNameList[colidx].compare(colname) == 0) {
			isbinarycol = true;
			break;
		}
	}
	if (isbinarycol) {
		delimitervalfront = L"0x";
		delimitervalend = L"";
	}
	else {
		delimitervalfront = L"'";
		delimitervalend = L"'";
	}
	if (colIdx > 0) {
		delimitercolfront = delimiterand + delimitercolfront;
		delimitervalfront = delimiterand + delimitervalfront;
	}
	colNames = colNames + delimitercolfront + colname + delimitercolend;
	valNames = valNames + delimitervalfront + valname + delimitervalend;
}

bool isUidInTables(SQLHDBC hDbc, std::wstring accountUid, std::vector<std::wstring> tableNameList) {
	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
		std::vector<std::wstring> accountuidlist = sqlfSingleCol(hDbc, L"SELECT %s FROM %s WHERE %s = %s", ACCOUNT_UID_FIELD_NAME.c_str(), tableNameList[tableidx].c_str(), ACCOUNT_UID_FIELD_NAME.c_str(), accountUid.c_str());
		if (!accountuidlist.empty()) {
			return true;
		}
	}
	return false;
}
