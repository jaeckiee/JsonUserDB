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

#define APP_NAME "JsonUserDB"

// Usages Of Argparse
static const WCHAR* const usages[] = {
	L"" APP_NAME " [options] [[--] args]",
	L"" APP_NAME " [options]",
	NULL,
};

int g_VERBOSE = 0;

std::wstring Account_UID_Field_Name = L"AccountUID";

// For SQL Execute
RETCODE sqlfExec(SQLHSTMT& hStmt, SQLHDBC hDbc, const WCHAR* wszInput, ...);

// Return Result Of Query
std::vector<std::wstring> sqlf_SingleCol(SQLHDBC hDbc, const WCHAR* wszInput, ...);
Json::Value sqlf_MultiCol(SQLHDBC hDbc, const std::wstring tableName, const WCHAR* wszInput, ...);

// Connect And Diconnect DB
RETCODE connectToDB(SQLHENV& hEnv, SQLHDBC& hDbc, std::wstring pwszConnStr);
RETCODE disconnectDB(SQLHENV& hEnv, SQLHDBC& hDbc, SQLHSTMT& hStmt);

// Modify DB tables
RETCODE deleteTablesRows(std::vector<std::wstring> tableNameList, std::wstring accountUID, SQLHDBC hDbc);
RETCODE importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUID);

// Make JSON object
void allTableToJson(std::vector<std::wstring> tableNameList, std::wstring accountUID, SQLHDBC hDbc, Json::Value& root);

// Read And Write JSONFILE
const int writeJsonFile(Json::Value root, std::wstring filename);
const int readJsonFile(Json::Value& root, std::wstring filename);

// Print Table
RETCODE printTable(SQLHDBC hDbc, const WCHAR* wszInput, ...);
RETCODE printTablesInList(SQLHDBC hDbc, const WCHAR* wszInput, std::vector<std::wstring> tableNameList, std::wstring accountUID);

// ETC
std::wstring conditionExceptTableNames(std::vector<std::wstring> exceptTableNameList);
bool isInAccountUIDList(SQLHDBC hDbc, std::wstring accountUID);
void concateValNameColName(std::wstring& colNames, std::wstring& valNames, int colIdx, std::vector<std::wstring> binColNameList, Json::Value::Members colNameList, Json::Value current_Row);

int wmain(int argc, _In_reads_(argc) const WCHAR** argv) {
	SQLHENV hEnv = NULL;
	SQLHDBC hDbc = NULL;
	SQLHSTMT hStmt = NULL;
	std::vector<std::wstring> accountUIDList;
	std::vector<std::wstring> tableNameList_uidExist;
	std::vector<std::wstring> tableNameList_except;
	Json::Value root;
	std::wstring condition_excepttablenames;
	const WCHAR* accountuid = NULL;
	const WCHAR* source = NULL;
	const WCHAR* target = NULL;
	const WCHAR* connectionstring = NULL;
	setlocale(LC_ALL, "en-US.UTF-8");
	// Set IgnoreTableNameList
	tableNameList_except.push_back(L"AccountWhisper");
	tableNameList_except.push_back(L"AccountWhisperCount");

	// ARGOARSE SET
	int exportjson = 0;
	int importjson = 0;
	int deleterows = 0;
	int printtables = 0;
	int verbose = 0;
	struct argparse_option options[] = {
		OPT_HELP(),
		OPT_GROUP(L"Basic options"),
		OPT_BOOLEAN(L'e', L"export", &exportjson, L"Export JSON File From DB", NULL, 0, 0),
		OPT_BOOLEAN(L'i', L"import", &importjson, L"Import JSON File Into DB", NULL, 0, 0),
		OPT_BOOLEAN(L'd', L"delete", &deleterows, L"Delete Rows(Same AccounUID Exists) of DB", NULL, 0, 0),
		OPT_BOOLEAN(L'p', L"print", &printtables, L"Print all columns From Tables Where Same AccountUID Exists ", NULL, 0, 0),
		OPT_STRING(L's', L"source", &source, L"Source DB connectionstring or Source jsonfile name", NULL, 0, 0),
		OPT_STRING(L't', L"target", &target, L"Target jsonfile name or Target DB connectionstring", NULL, 0, 0),
		OPT_STRING(L'u', L"uid", &accountuid, L"Target AccountUID", NULL, 0, 0),
		OPT_BOOLEAN(L'v', L"verbosse", &verbose, L"Provides additional details", NULL, 0, 0),
		OPT_END(),
	};

	struct argparse argparse;
	argparse_init(&argparse, options, usages, 0);
	argparse_describe(&argparse, L"\nThis program supports Export and Import JSON FILE From or To DB.", L"\nex) -e -u 100000000 -s connecttionstring -t jsonfilename\nex) -i -u 100000000 -s jsonfilename -t connecttionstring\nex) -d -u 100000000 -t connecttionstring\nex) -p -u 100000000 -t connecttionstring");
	argc = argparse_parse(&argparse, argc, argv);
	if (argc < 1) {
		argparse_usage(&argparse);
		return SUCCESS;
	}

	if (accountuid == NULL || target == NULL) {
		fwprintf(stderr, L"\nArgument Not correct");
		return ERROR_BAD_ARG;
	}
	if (verbose != 0) {
		g_VERBOSE = verbose;
	}
	if (deleterows == 0 && printtables == 0) {
		if (source == NULL) {
			fwprintf(stderr, L"\nArgument Not correct");
			return ERROR_BAD_ARG;
		}
	}

	// Make Wstring(Ignore Table Name List) For Where
	condition_excepttablenames = conditionExceptTableNames(tableNameList_except);

	if (exportjson + importjson + deleterows + printtables != 1) { // Mode Choice
		fwprintf(stderr, L"\nThis Mode Not Supported");
		return ERROR_BAD_ARG;
	}

	// Connect DB
	if (exportjson == 1) {
		connectionstring = source;
	}
	else {
		connectionstring = target;
	}
	if (!SQL_SUCCEEDED(connectToDB(hEnv, hDbc, connectionstring))) {
		fwprintf(stderr, L"\nFail : Disconnect DB");
		return ERROR_CONNECT_DB;
	}
	wprintf(L"\nSUCCESS : Connect DB");

	if (exportjson == 1) {

		// Check Whether AccounUid Exists
		if (!isInAccountUIDList(hDbc, accountuid)) {
			fwprintf(stderr, L"\nThis %s Doesn't EXIST!", Account_UID_Field_Name.c_str());
			return ERROR_BAD_ARG;
		}

		// Get Table Name List(AccountUID Exists)
		tableNameList_uidExist = sqlf_SingleCol(hDbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' \
            INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'%s", Account_UID_Field_Name.c_str(), condition_excepttablenames.c_str());

		allTableToJson(tableNameList_uidExist, accountuid, hDbc, root);

		if (!SQL_SUCCEEDED(writeJsonFile(root, target))) {
			fwprintf(stderr, L"\nFAIL : Write JSON FILE");
			return ERROR_WRITING_FILE;
		}
	}
	else if (importjson == 1) {
		if (!SQL_SUCCEEDED(readJsonFile(root, source))) {
			fwprintf(stderr, L"\nFAIL : Read JSON FILE");
			return ERROR_READING_FILE;
		}
		
		if (!SQL_SUCCEEDED(importJsonIntoDB(root, hDbc, accountuid))) {
			fwprintf(stderr, L"\nFAIL : Insert Json To DB");
			return ERROR_IMPORT_JSON;
		}
	}
	else if (deleterows == 1) {
		// Get Table Name List(AccountUID Exists)
		tableNameList_uidExist = sqlf_SingleCol(hDbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'%s", Account_UID_Field_Name.c_str(), condition_excepttablenames.c_str());

		if (!SQL_SUCCEEDED(deleteTablesRows(tableNameList_uidExist, accountuid, hDbc))) {
			fwprintf(stderr, L"\nFAIL : Delete Rows of Tables");
			return ERROR_DELETE_TABLESROWS;
		}
	}
	else if (printtables == 1) {
		// Get Table Name List(AccountUID Exists)
		tableNameList_uidExist = sqlf_SingleCol(hDbc, L"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '%s' INTERSECT SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'%s", Account_UID_Field_Name.c_str(), condition_excepttablenames.c_str());

		// Print tables
		if (!SQL_SUCCEEDED(printTablesInList(hDbc, L"SELECT * FROM %s WHERE %s = %s", tableNameList_uidExist, accountuid))) {
			fwprintf(stderr, L"\nFAIL : Print Tables");
			return ERROR_PRINT_TABLES;
		}
	}
Exit:
	if (!SQL_SUCCEEDED(disconnectDB(hEnv, hDbc, hStmt))) {
		fwprintf(stderr, L"\nFAIL : Disconnect DB");
		return ERROR_DISCONNECT_DB;
	}
	else {
		fwprintf(stdout, L"\nSUCCESS : Disconnect DB");
	}
	return SUCCESS;
}

RETCODE sqlfExec(SQLHSTMT &hStmt, SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	WCHAR fwszInput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszInput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	RETCODE RetCode = SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt);
	TRYODBC(hDbc, SQL_HANDLE_DBC, RetCode);
	if (g_VERBOSE) {
		fwprintf(stderr, L"\n%s", fwszInput);
	}
	RetCode = SQLExecDirect(hStmt, fwszInput, SQL_NTS);
	TRYODBC(hStmt, SQL_HANDLE_STMT, RetCode);
Exit:
	return RetCode;
}

std::vector<std::wstring> sqlf_SingleCol(SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	RETCODE     RetCode;
	SQLSMALLINT sNumResults;
	SQLHSTMT hStmt = NULL;
	std::vector<std::wstring> rowValList;
	WCHAR fwszInput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszInput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	RetCode = sqlfExec(hStmt, hDbc, fwszInput);
	TRYODBC(hStmt, SQL_HANDLE_STMT, RetCode);
	if (RetCode == SQL_SUCCESS) {
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLNumResultCols(hStmt, &sNumResults));
		if (sNumResults == 1) {
			while (SQL_SUCCEEDED(RetCode = SQLFetch(hStmt))) {
				SQLUSMALLINT colnum = 1;
				SQLLEN indicator;
				const int bufsize = 512;
				wchar_t buf[bufsize];
				RetCode = SQLGetData(hStmt, colnum, SQL_UNICODE, buf, sizeof(buf), &indicator);
				if (SQL_SUCCEEDED(RetCode)) {
					if (indicator != SQL_NULL_DATA) {
						rowValList.push_back(buf);
					}
				}

			}
		}
	}
	TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
Exit:
	return rowValList;
}

Json::Value sqlf_MultiCol(SQLHDBC hDbc, const std::wstring tableName, const WCHAR* wszInput, ...) {
	RETCODE     RetCode;
	SQLSMALLINT sNumResults;
	SQLHSTMT hStmt = NULL;
	Json::Value resultJSON;
	WCHAR fwszInput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszInput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	RetCode = sqlfExec(hStmt, hDbc, fwszInput);
	TRYODBC(hStmt, SQL_HANDLE_STMT, RetCode);
	if (RetCode == SQL_SUCCESS) {
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLNumResultCols(hStmt, &sNumResults));
		if (sNumResults > 0) {
			while (SQL_SUCCEEDED(RetCode = SQLFetch(hStmt))) {
				SQLUSMALLINT colnum;
				Json::Value current_record;
				for (colnum = 1; colnum <= sNumResults; colnum++) {
					const SQLSMALLINT buflen = 512;
					SQLWCHAR colName[buflen];
					SQLSMALLINT colType;
					SQLDescribeCol(hStmt, colnum, colName, buflen, NULL, &colType, NULL, NULL, NULL);
					SQLLEN indicator;
					int colIntValue;
					char colTinyIntValue;
					short colSmallIntValue;
					bool colBitValue;
					float colFloatValue;
					double colDoubleValue;
					SQLWCHAR colWcharValue[buflen];
					#define STORE_RECORD(x, y) 		\
						SQLGetData(hStmt, colnum, x, &y, sizeof(&y), &indicator); \
						if (SQL_SUCCEEDED(RetCode) && (indicator != SQL_NULL_DATA)) current_record[get_utf8(colName)] = y;

					#define STORE_RECORD_STR(x, y)		\
						SQLGetData(hStmt, colnum, x, &y, sizeof(&y), &indicator); \
						if (SQL_SUCCEEDED(RetCode) && (indicator != SQL_NULL_DATA)) current_record[get_utf8(colName)] = get_utf8(y);
					switch (colType) {
					case SQL_INTEGER:
						STORE_RECORD(SQL_INTEGER, colIntValue);
						break;
					case SQL_TINYINT:
						STORE_RECORD(SQL_TINYINT, colTinyIntValue);
						break;
					case SQL_SMALLINT:
						STORE_RECORD(SQL_SMALLINT, colSmallIntValue);
						break;
					case SQL_FLOAT:
						STORE_RECORD(SQL_FLOAT, colFloatValue);
						break;
					case SQL_DOUBLE:
						STORE_RECORD(SQL_DOUBLE, colDoubleValue);
						break;
					case SQL_BIT:
						STORE_RECORD(SQL_BIT, colBitValue);
						break;
					case SQL_UNICODE:
					case SQL_CHAR:
					case SQL_BIGINT:
					case SQL_BINARY:
					case SQL_VARCHAR:
					case SQL_LONGVARBINARY:
					case SQL_DATE:
					case SQL_TIME:
					case SQL_TYPE_TIMESTAMP:
					case SQL_TYPE_TIME:
					case SQL_TYPE_DATE:
					case SQL_TIMESTAMP:
					case SQL_WVARCHAR:
						STORE_RECORD_STR(SQL_UNICODE, colWcharValue);
						break;
					default:
						fwprintf(stderr, L"\nTable : %s column : %s coltype : %d", tableName.c_str(), colName, colType);
					}
				}
				resultJSON.append(current_record);
			}
		}
	}
	TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
Exit:
	return resultJSON;
}

RETCODE connectToDB(SQLHENV& hEnv, SQLHDBC& hDbc, std::wstring pwszConnStr) {
	SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);

	SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

	SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);

	RETCODE RetCode = SQLDriverConnect(hDbc, GetDesktopWindow(), const_cast<SQLWCHAR*>(pwszConnStr.c_str()), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
	TRYODBC(hDbc, SQL_HANDLE_DBC, RetCode);
Exit:
	return RetCode;
}

RETCODE disconnectDB(SQLHENV& hEnv, SQLHDBC& hDbc, SQLHSTMT& hStmt) {
	// Free ODBC handles and exit
	SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
	hStmt = NULL;

	SQLFreeHandle(SQL_HANDLE_ENV, hEnv);
	hEnv = NULL;

	RETCODE RetCode = SQLDisconnect(hDbc);
	TRYODBC(hDbc, SQL_HANDLE_DBC, RetCode);
	SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
	hDbc = NULL;
Exit:	
	return RetCode;
}

RETCODE deleteTablesRows(std::vector<std::wstring> tableNameList, std::wstring accountUID, SQLHDBC hDbc) {
	SQLHSTMT hStmt = NULL;
	RETCODE RetCode;
	fwprintf(stdout, L"\nSTART : Delete Rows of Tables");
	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
		std::wstring current_tablename = tableNameList[tableidx];
		RetCode = sqlfExec(hStmt, hDbc, L"DELETE FROM %s WHERE %s = %s", current_tablename.c_str(), Account_UID_Field_Name.c_str(), accountUID.c_str());
		TRYODBC(hStmt, SQL_HANDLE_STMT, RetCode);
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
	}
	fwprintf(stdout, L"\nSUCCESS : Delete Rows of Tables");
Exit:
	return RetCode;
}

RETCODE importJsonIntoDB(Json::Value root, SQLHDBC hDbc, std::wstring accountUID) {
	RETCODE RetCode;
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
						concateValNameColName(colnames, valnames, colidx, bincolnamelist, current_rowcolnamelist, current_row);
					}
					SQLHSTMT hStmt = NULL;
					RetCode = sqlfExec(hStmt, hDbc, L"INSERT INTO %s(%s, %s) VALUES('%s', %s)", current_tablename.c_str(), Account_UID_Field_Name.c_str(), colnames.c_str(), accountUID.c_str(), valnames.c_str());
					TRYODBC(hStmt, SQL_HANDLE_STMT, RetCode);
					TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
				}
			}
		}
	}
	fwprintf(stdout, L"\nSUCCESS : Insert Json To DB");
Exit:
	return RetCode;
}

void allTableToJson(std::vector<std::wstring> tableNameList, std::wstring accountUID, SQLHDBC hDbc, Json::Value& root) {
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
Exit:
	return;
}

const int writeJsonFile(Json::Value root, std::wstring filename) {
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
		return SUCCESS;
	}
	else {
		return ERROR_WRITING_FILE;
	}
}

const int readJsonFile(Json::Value& root, std::wstring filename) {
	std::ifstream ifs;
	ifs.open(filename);
	Json::CharReaderBuilder builder;
	JSONCPP_STRING errs;
	fwprintf(stdout, L"START : READ JSON FILE");
	if (!parseFromStream(builder, ifs, &root, &errs)) {
		return ERROR_READING_FILE;
	}
	fwprintf(stdout, L"\nSUCCESS : Read JSON FILE");
	return SUCCESS;
}

RETCODE printTable(SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	RETCODE     RetCode;
	SQLSMALLINT sNumResults;
	SQLHSTMT hStmt = NULL;
	TRYODBC(hDbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt));
	WCHAR fwszInput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszInput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	RetCode = sqlfExec(hStmt, hDbc, fwszInput);
	TRYODBC(hStmt, SQL_HANDLE_STMT, RetCode);
	if (RetCode == SQL_SUCCESS) {
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLNumResultCols(hStmt, &sNumResults));
		if (sNumResults > 0) {
			printResults(hStmt, sNumResults);
		}
	}
	TRYODBC(hStmt, SQL_HANDLE_STMT, SQLFreeStmt(hStmt, SQL_CLOSE));
Exit:
	return RetCode;
}

RETCODE printTablesInList(SQLHDBC hDbc, const WCHAR* wszInput, std::vector<std::wstring> tableNameList, std::wstring accountUID) {
	RETCODE RetCode;
	for (int tableidx = 0; tableidx < tableNameList.size(); tableidx++) {
		std::wstring current_tablename = tableNameList[tableidx];
		fwprintf(stdout, L"\nTABLE NAME : %s\n", current_tablename.c_str());
		RetCode = printTable(hDbc, wszInput, current_tablename.c_str(), Account_UID_Field_Name.c_str(), accountUID.c_str());
	}
	return RetCode;
}

void concateValNameColName(std::wstring& colNames, std::wstring& valNames, int colIdx, std::vector<std::wstring> binColNameList, Json::Value::Members colNameList, Json::Value current_Row) {
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

std::wstring conditionExceptTableNames(std::vector<std::wstring> exceptTableNameList) {
	std::wstring condition = L"";
	for (int tableidx = 0; tableidx < exceptTableNameList.size(); tableidx++) {
		condition = condition + L" AND TABLE_NAME != '" + exceptTableNameList[tableidx] + L"'";
	}
	return condition;
}

bool isInAccountUIDList(SQLHDBC hDbc, std::wstring accountUID) {
	std::vector<std::wstring> accountuidlist;
	accountuidlist = sqlf_SingleCol(hDbc, L"SELECT %s FROM Account", Account_UID_Field_Name.c_str());
	bool isaccountuidexist = false;
	for (int listidx = 0; listidx < accountuidlist.size(); listidx++) {
		if (accountuidlist[listidx].compare(accountUID) == 0) {
			isaccountuidexist = true;
		}
	}
	if (!isaccountuidexist) {
		return false;
	}
	return true;
}