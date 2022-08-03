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
const int ERROR_DELETE_TABLESROWS = 8;
const int ERROR_PRINT_TABLES = 9;
int RETURNVAL = SUCCESS;

#define APP_NAME "JsonUserDB"

// Usages Of Argparse
static const WCHAR* const usages[] = {
	L"" APP_NAME " [options] [[--] args]",
	L"" APP_NAME " [options]",
	NULL,
};

#define SUCCEEDED_CHECK(x, y, z) 		\
						if (!x) {\
						fwprintf(stderr, y);\
						RETURNVAL = z;\
						goto Exit;\
						}

std::wstring Account_UID_Field_Name = L"AccountUID";

// ini FILE
const WCHAR* DefaultEmptyVal = L"";
const WCHAR* DefaultTrustedConnectionVal = L"No";
const WCHAR* IniFileName = L".\\JsonUserDB.ini";

// Modify DB tables
void deleteTablesRows(std::vector<std::wstring> tableNameList, std::wstring accountUID, SQLHDBC hDbc);
bool importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUID);

// Make JSON object
void exportJsonFromDB(std::vector<std::wstring> tableNameList, std::wstring accountUID, SQLHDBC hDbc, Json::Value& root);

// Read And Write JSONFILE
bool writeJsonFile(Json::Value root, std::wstring filename);
bool readJsonFile(Json::Value & root, std::wstring filename);

// Print Table
void printTable(SQLHDBC hDbc, const WCHAR * wszInput, ...);
void printTablesInList(SQLHDBC hDbc, const WCHAR * wszInput, std::vector<std::wstring> tableNameList, std::wstring accountUID);

// ETC
std::wstring makeConditionIgnoreTableNames(std::vector<std::wstring> exceptTableNameList);
bool isUIDInAccountTable(SQLHDBC hDbc, std::wstring accountUID);
void makeValNamesColNames(std::wstring& colNames, std::wstring& valNames, int colIdx, std::vector<std::wstring> binColNameList, Json::Value::Members colNameList, Json::Value current_Row);

//std::vector<std::wstring> GetAccountUIDTableList(std::vector<std::wstring>* exclusionTableList)
//{
//
//}

int wmain(int argc, _In_reads_(argc) const WCHAR** argv) {
	SQLHENV hEnv = NULL;
	SQLHDBC hDbc = NULL;
	SQLHSTMT hStmt = NULL;
	std::vector<std::wstring> accountUIDList;
	std::vector<std::wstring> tableNameList_uidExist;
	std::vector<std::wstring> ignoreTableNameList;
	Json::Value root;
	std::wstring condition_excepttablenames;
	const WCHAR* accountuid = NULL;
	const WCHAR* source = NULL;
	const WCHAR* target = NULL;
	const WCHAR* connsection = NULL;
	const WCHAR* connstring = NULL;
	setlocale(LC_ALL, "en-US.UTF-8");
	// Set IgnoreTableNameList
	ignoreTableNameList.push_back(L"AccountWhisper");
	ignoreTableNameList.push_back(L"AccountWhisperCount");

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
	argparse_init(&argparse, options, usages, 0);
	argparse_describe(&argparse, L"\nThis program supports Export and Import JSON FILE From or To DB.", L"\nex) -e -u 100000000 -s connecttionstring -t jsonfilename\nex) -i -u 100000000 -s jsonfilename -t connecttionstring\nex) -d -u 100000000 -t connecttionstring\nex) -p -u 100000000 -t connecttionstring\nex) -e -u 100000000 -c connectsection -t jsonfilename\nex) -i -u 100000000 -s jsonfilename -c connectsection\nex) -d -u 100000000 -c connectsection\nex) -p -u 100000000 -c connectsection");
	argparse_parse(&argparse, argc, argv);
	if (argc < 2) {
		argparse_usage(&argparse);
		return SUCCESS;
	}
	if (accountuid == NULL) {
		fwprintf(stderr, L"\nArgument Not correct");
		return ERROR_BAD_ARG;
	}
	if (verbose != 0) {
		g_VERBOSE = verbose;
	}
	if (exportjson == 1) {
		if (target == NULL || ((source == NULL) == (connsection == NULL))) {
			fwprintf(stderr, L"\nArgument Not correct");
			return ERROR_BAD_ARG;
		}
	}
	if (importjson == 1) {
		if (source == NULL || ((target == NULL) == (connsection == NULL))) {
			fwprintf(stderr, L"\nArgument Not correct");
			return ERROR_BAD_ARG;
		}
	}

	// Mode Choice
	if (exportjson + importjson + deleterows + printtables != 1) {
		fwprintf(stderr, L"\nThis Mode Not Supported");
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
		WCHAR wcsbuffer[wcsbufsize];
		const int valbufsize = 512;
		WCHAR valDSNbuf[valbufsize];
		WCHAR valDatabasebuf[valbufsize];
		WCHAR valTrusted_Connectionbuf[valbufsize];
		WCHAR valUIDbuf[valbufsize];
		WCHAR valPWDbuf[valbufsize];
		GetPrivateProfileString(connsection, L"DSN", DefaultEmptyVal, valDSNbuf, valbufsize, IniFileName);
		GetPrivateProfileString(connsection, L"trusted_connection", DefaultTrustedConnectionVal, valTrusted_Connectionbuf, valbufsize, IniFileName);
		GetPrivateProfileString(connsection, L"UID", DefaultEmptyVal, valUIDbuf, valbufsize, IniFileName);
		GetPrivateProfileString(connsection, L"PWD", DefaultEmptyVal, valPWDbuf, valbufsize, IniFileName);
		GetPrivateProfileString(connsection, L"Database", DefaultEmptyVal, valDatabasebuf, valbufsize, IniFileName);
		swprintf_s(wcsbuffer, wcsbufsize, L"DSN=%s;trusted_connection=%s;UID=%s;PWD=%s;Database=%s;", valDSNbuf, valTrusted_Connectionbuf, valUIDbuf, valPWDbuf, valDatabasebuf);
		connstring = wcsbuffer;
	}

	// Make Wstring(Ignore Table Name List) For Where
	condition_excepttablenames = makeConditionIgnoreTableNames(ignoreTableNameList);

	// Connect DB
	SUCCEEDED_CHECK(connectToDB(hEnv, hDbc, connstring), L"\nFail : Connect DB", ERROR_CONNECT_DB);
	wprintf(L"\nSUCCESS : Connect DB");

	if (exportjson == 1) {
		SUCCEEDED_CHECK(isUIDInAccountTable(hDbc, accountuid), L"\nThis UID Doesn't EXIST!", ERROR_BAD_ARG);

		tableNameList_uidExist = sqlf_SingleCol(hDbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' \
            INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'%s", Account_UID_Field_Name.c_str(), condition_excepttablenames.c_str());

		exportJsonFromDB(tableNameList_uidExist, accountuid, hDbc, root);

		SUCCEEDED_CHECK(writeJsonFile(root, target), L"\nFAIL : Write JSON FILE", ERROR_WRITING_FILE);
	}
	else if (importjson == 1) {
		SUCCEEDED_CHECK(readJsonFile(root, source), L"\nFAIL : Read JSON FILE", ERROR_READING_FILE);

		SUCCEEDED_CHECK(importJsonIntoDB(root, hDbc, accountuid), L"\nFAIL : Import Json To DB", ERROR_READING_FILE);
	}
	else if (deleterows == 1) {
		tableNameList_uidExist = sqlf_SingleCol(hDbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' \
			INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'%s", Account_UID_Field_Name.c_str(), condition_excepttablenames.c_str());

		//VALIDATION_CHECK(deleteTablesRows(tableNameList_uidExist, accountuid, hDbc), L"\nFAIL : Delete Rows of Tables", ERROR_DELETE_TABLESROWS);
		deleteTablesRows(tableNameList_uidExist, accountuid, hDbc);
	}
	else if (printtables == 1) {
		tableNameList_uidExist = sqlf_SingleCol(hDbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' \
			INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'%s", Account_UID_Field_Name.c_str(), condition_excepttablenames.c_str());

		printTablesInList(hDbc, L"SELECT * FROM %s WHERE %s = %s", tableNameList_uidExist, accountuid);
	}
Exit:
	if (!disconnectDB(hEnv, hDbc, hStmt)) {
		fwprintf(stderr, L"\nFAIL : Disconnect DB");
		return ERROR_DISCONNECT_DB;
	}
	fwprintf(stdout, L"\nSUCCESS : Disconnect DB");

	return RETURNVAL;
}


void deleteTablesRows(std::vector<std::wstring> tableNameList, std::wstring accountUID, SQLHDBC hDbc) {
	SQLHSTMT hStmt = NULL;
	fwprintf(stdout, L"\nSTART : Delete Rows of Tables");
	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
		std::wstring current_tablename = tableNameList[tableidx];
		sqlfExec(hStmt, hDbc, L"DELETE FROM %s WHERE %s = %s", current_tablename.c_str(), Account_UID_Field_Name.c_str(), accountUID.c_str());
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
	}
	fwprintf(stdout, L"\nSUCCESS : Delete Rows of Tables");
Exit:
	if (hStmt != SQL_CLOSE) {
		SQLFreeStmt(hStmt, SQL_CLOSE);
	}
}

bool importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUID) {
	bool isSucceeded = false;
	SQLHSTMT hStmt = NULL;
	fwprintf(stdout, L"\nSTART : Insert Json To DB");
	for (Json::Value::iterator iter = root.begin(); iter != root.end(); ++iter) {
		std::vector<std::wstring> bincolnamelist;
		Json::Value current_key = iter.key();
		std::wstring current_tablename = get_utf16(current_key.asString());
		bincolnamelist = sqlf_SingleCol(hDbc, L"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE DATA_TYPE = 'binary' AND TABLE_NAME = '%s'", current_tablename.c_str());
		if (current_tablename.compare(Account_UID_Field_Name) != 0) {
			Json::Value current_table = root[get_utf8(current_tablename)];
			for (int rowidx = 0; rowidx < current_table.size(); rowidx++) {
				Json::Value current_row = current_table[rowidx];
				Json::Value::Members current_rowcolnamelist = current_row.getMemberNames();
				int current_rowcolnamelistsize = current_rowcolnamelist.size();
				if (current_rowcolnamelistsize > 0) {
					std::wstring colnames = L"";
					std::wstring valnames = L"";
					for (int colidx = 0; colidx < current_rowcolnamelistsize; colidx++) {
						makeValNamesColNames(colnames, valnames, colidx, bincolnamelist, current_rowcolnamelist, current_row);
					}
					isSucceeded = sqlfExec(hStmt, hDbc, L"INSERT INTO %s(%s, %s) VALUES('%s', %s)", current_tablename.c_str(), Account_UID_Field_Name.c_str(), colnames.c_str(), accountUID.c_str(), valnames.c_str());
					if (!isSucceeded) {
						goto Exit;
					}
					TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
				}
			}
		}
	}
	fwprintf(stdout, L"\nSUCCESS : import Json To DB");
Exit:
	if (hStmt != SQL_CLOSE) {
		SQLFreeStmt(hStmt, SQL_CLOSE);
	}
	return isSucceeded;
}

void exportJsonFromDB(std::vector<std::wstring> tableNameList, std::wstring accountUID, SQLHDBC hDbc, Json::Value& root) {
	root[get_utf8(Account_UID_Field_Name)] = get_utf8(accountUID);
	fwprintf(stdout, L"\nSTART : All Table to JSON");
	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
		std::wstring current_tablename = tableNameList[tableidx];
		std::vector<std::wstring> autocolnamelist;
		Json::Value current_table;
		autocolnamelist = sqlf_SingleCol(hDbc, L"SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '%s'", current_tablename.c_str());
		current_table = sqlf_MultiCol(hDbc, current_tablename, L"SELECT * FROM %s WHERE %s = %s", current_tablename.c_str(), Account_UID_Field_Name.c_str(), accountUID.c_str());
		for (int rowidx = 0; rowidx < current_table.size(); rowidx++) {
			current_table[rowidx].removeMember(get_utf8(Account_UID_Field_Name));
			for (int autoincidx = 0; autoincidx < autocolnamelist.size(); autoincidx++) {
				current_table[rowidx].removeMember(get_utf8(autocolnamelist[autoincidx]));
			}
		}
		if (!current_table.empty()) {
			root[get_utf8(current_tablename)] = current_table;
		}
	}
	fwprintf(stdout, L"\nSUCCESS : All Table to JSON");
}

bool writeJsonFile(Json::Value root, std::wstring filename) {
	bool isSucceeded = false;
	errno_t fileerr;
	FILE* JSonFile = NULL;
	Json::StyledWriter writer;
	std::string outputConfig;
	outputConfig = writer.write(root);
	fileerr = _wfopen_s(&JSonFile, filename.c_str(), L"wb");
	fwprintf(stdout, L"\nSTART : Write resultjson.json");
	if (fileerr == 0) {
		int itemsize = 1;
		size_t fileSize = fwrite(outputConfig.c_str(), itemsize, outputConfig.length(), JSonFile);
		fclose(JSonFile);
		fwprintf(stdout, L"\nSUCCESS : Write JSON FILE");
		isSucceeded = true;
	}
	return isSucceeded;
}

bool readJsonFile(Json::Value& root, std::wstring filename) {
	bool isSucceeded = false;
	std::ifstream ifs;
	ifs.open(filename);
	Json::CharReaderBuilder builder;
	JSONCPP_STRING errs;
	fwprintf(stdout, L"START : READ JSON FILE");
	if (parseFromStream(builder, ifs, &root, &errs)) {
		isSucceeded = true;
		fwprintf(stdout, L"\nSUCCESS : Read JSON FILE");
	}
	return isSucceeded;
}

void printTable(SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	SQLSMALLINT sNumResults;
	SQLHSTMT hStmt = NULL;
	TRYODBC(hDbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt));
	WCHAR fwszInput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszInput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	if (sqlfExec(hStmt, hDbc, fwszInput)) {
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLNumResultCols(hStmt, &sNumResults));
		if (sNumResults > 0) {
			printResults(hStmt, sNumResults);
		}
	}
Exit:
	if (hStmt != SQL_CLOSE) {
		SQLFreeStmt(hStmt, SQL_CLOSE);
	}
}

void printTablesInList(SQLHDBC hDbc, const WCHAR* wszInput, std::vector<std::wstring> tableNameList, std::wstring accountUID) {
	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
		std::wstring current_tablename = tableNameList[tableidx];
		fwprintf(stdout, L"\nTABLE NAME : %s\n", current_tablename.c_str());
		printTable(hDbc, wszInput, current_tablename.c_str(), Account_UID_Field_Name.c_str(), accountUID.c_str());
	}
}

void makeValNamesColNames(std::wstring& colNames, std::wstring& valNames, int colIdx, std::vector<std::wstring> binColNameList, Json::Value::Members colNameList, Json::Value current_Row) {
	std::wstring colname;
	std::wstring valname;
	std::wstring delimiter_valfront;
	std::wstring delimiter_valend;
	std::wstring delimiter_colfront = L"\"";
	std::wstring delimiter_colend = L"\"";
	std::wstring delimiter_and = L", ";
	bool isbinarycol = false;
	colname = get_utf16(colNameList[colIdx]);
	valname = get_utf16(current_Row[colNameList[colIdx]].asString());
	for (int colidx = 0; colidx < binColNameList.size(); colidx++) {
		if (binColNameList[colidx].compare(colname) == 0) {
			isbinarycol = true;
			break;
		}
	}
	if (isbinarycol) {
		delimiter_valfront = L"0x";
		delimiter_valend = L"";
	}
	else {
		delimiter_valfront = L"'";
		delimiter_valend = L"'";
	}
	if (colIdx > 0) {
		delimiter_colfront = delimiter_and + delimiter_colfront;
		delimiter_valfront = delimiter_and + delimiter_valfront;
	}
	colNames = colNames + delimiter_colfront + colname + delimiter_colend;
	valNames = valNames + delimiter_valfront + valname + delimiter_valend;
}

std::wstring makeConditionIgnoreTableNames(std::vector<std::wstring> exceptTableNameList) {
	std::wstring condition = L"";
	for (int tableidx = 0; tableidx < exceptTableNameList.size(); tableidx++) {
		condition = condition + L" AND TABLE_NAME != '" + exceptTableNameList[tableidx] + L"'";
	}
	return condition;
}

bool isUIDInAccountTable(SQLHDBC hDbc, std::wstring accountUID) {
	std::vector<std::wstring> accountuidlist;
	accountuidlist = sqlf_SingleCol(hDbc, L"SELECT %s FROM Account", Account_UID_Field_Name.c_str());
	bool isaccountuidexist = (std::find(accountuidlist.begin(), accountuidlist.end(), accountUID) != accountuidlist.end());
	return isaccountuidexist;
}