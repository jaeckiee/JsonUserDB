#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <conio.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <wchar.h>
#include "sqlutility.h"
#include "strutility.h"

bool sqlfExec(SQLHSTMT& hStmt, SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	bool isSucceeded = false;
	WCHAR fwszInput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszInput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	TRYODBC(hDbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt));
	if (g_VERBOSE) {
		fwprintf(stderr, L"\n%s", fwszInput);
	}
	isSucceeded = SQL_SUCCEEDED(SQLExecDirect(hStmt, fwszInput, SQL_NTS));
Exit:
	return isSucceeded;
}

std::vector<std::wstring> sqlf_SingleCol(SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	SQLSMALLINT sNumResults;
	SQLHSTMT hStmt = NULL;
	std::vector<std::wstring> rowValList;
	WCHAR fwszInput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszInput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	if (sqlfExec(hStmt, hDbc, fwszInput)) {
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLNumResultCols(hStmt, &sNumResults));
		if (sNumResults == 1) {
			while (SQL_SUCCEEDED(SQLFetch(hStmt))) {
				SQLUSMALLINT colnum = 1;
				SQLLEN indicator;
				const int bufsize = 512;
				wchar_t buf[bufsize];
				if (SQL_SUCCEEDED(SQLGetData(hStmt, colnum, SQL_UNICODE, buf, sizeof(buf), &indicator))) {
					if (indicator != SQL_NULL_DATA) {
						rowValList.push_back(buf);
					}
				}
			}
		}
	}
Exit:
	if (hStmt != SQL_CLOSE) {
		SQLFreeStmt(hStmt, SQL_CLOSE);
	}
	return rowValList;
}

Json::Value sqlf_MultiCol(SQLHDBC hDbc, const std::wstring tableName, const WCHAR* wszInput, ...) {
	SQLSMALLINT sNumResults;
	SQLHSTMT hStmt = NULL;
	Json::Value resultJSON;
	WCHAR fwszInput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszInput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	if (sqlfExec(hStmt, hDbc, fwszInput)) {
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLNumResultCols(hStmt, &sNumResults));
		if (sNumResults > 0) {
			while (SQL_SUCCEEDED(SQLFetch(hStmt))) {
				SQLUSMALLINT colnum;
				Json::Value current_record;
				for (colnum = 1; colnum <= sNumResults; colnum++) {
					const SQLSMALLINT buflen = 2048;
					SQLWCHAR colName[buflen];
					SQLSMALLINT colType;
					TRYODBC(hStmt, SQL_HANDLE_STMT, SQLDescribeCol(hStmt, colnum, colName, buflen, NULL, &colType, NULL, NULL, NULL););
					SQLLEN indicator;
					int colIntValue;
					char colTinyIntValue;
					short colSmallIntValue;
					bool colBitValue;
					float colFloatValue;
					double colDoubleValue;
					SQLWCHAR colWcharValue[buflen];
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
Exit:
	if (hStmt != SQL_CLOSE) {
		SQLFreeStmt(hStmt, SQL_CLOSE);
	}
	return resultJSON;
}

bool connectToDB(SQLHENV& hEnv, SQLHDBC& hDbc, std::wstring pwszConnStr) {
	bool isSucceeded = false;
	TRYODBC(hEnv, SQL_HANDLE_ENV, SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv));
	TRYODBC(hEnv, SQL_HANDLE_ENV, SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0));
	TRYODBC(hEnv, SQL_HANDLE_ENV, SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc));
	isSucceeded = SQL_SUCCEEDED(SQLDriverConnect(hDbc, NULL, const_cast<SQLWCHAR*>(pwszConnStr.c_str()), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE));
Exit:
	return isSucceeded;
}

bool disconnectDB(SQLHENV& hEnv, SQLHDBC& hDbc, SQLHSTMT& hStmt) {
	bool isSucceeded = false;
	// Free ODBC handles and exit
	if (hStmt != NULL) {
		SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
		hStmt = NULL;
	}
	if (hEnv != NULL) {
		SQLFreeHandle(SQL_HANDLE_ENV, hEnv);
		hEnv = NULL;
	}
	if (hDbc != NULL) {
		isSucceeded = SQL_SUCCEEDED(SQLDisconnect(hDbc));
		SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
		hDbc = NULL;
	}
	return isSucceeded;
}

void printResults(HSTMT hStmt, SQLSMALLINT cCols) {
	BINDING* pFirstBinding, * pThisBinding;
	SQLSMALLINT     cDisplaySize;
	RETCODE         RetCode = SQL_SUCCESS;
	int             iCount = 0;

	// Allocate memory for each column
	AllocateBindings(hStmt, cCols, &pFirstBinding, &cDisplaySize);

	// Set the display mode and write the titles
	printTitles(hStmt, cDisplaySize + 1, pFirstBinding);

	// Fetch and display the data
	bool fNoData = false;

	do {
		// Fetch a row

		if (iCount++ >= gHeight - 2) {
			int     nInputChar;
			bool    fEnterReceived = false;

			while (!fEnterReceived) {
				wprintf(L"              ");
				SetConsole(cDisplaySize + 2, TRUE);
				wprintf(L"   Press ENTER to continue, Q to quit (height:%hd)", gHeight);
				SetConsole(cDisplaySize + 2, FALSE);

				nInputChar = _getch();
				wprintf(L"\n");
				if ((nInputChar == 'Q') || (nInputChar == 'q')) {
					goto Exit;
				}
				else if ('\r' == nInputChar) {
					fEnterReceived = true;
				}
				// else loop back to display prompt again
			}
			iCount = 1;
			printTitles(hStmt, cDisplaySize + 1, pFirstBinding);
		}

		TRYODBC(hStmt, SQL_HANDLE_STMT, RetCode = SQLFetch(hStmt));

		if (RetCode == SQL_NO_DATA_FOUND) {
			fNoData = true;
		}
		else {
			// Display the data.   Ignore truncations
			for (pThisBinding = pFirstBinding; pThisBinding; pThisBinding = pThisBinding->sNext) {
				if (pThisBinding->indPtr != SQL_NULL_DATA) {
					wprintf(pThisBinding->fChar ? DISPLAY_FORMAT_C : DISPLAY_FORMAT,
						PIPE,
						pThisBinding->cDisplaySize,
						pThisBinding->cDisplaySize,
						pThisBinding->wszBuffer);
				}
				else {
					wprintf(DISPLAY_FORMAT_C,
						PIPE,
						pThisBinding->cDisplaySize,
						pThisBinding->cDisplaySize,
						L"<NULL>");
				}
			}
			wprintf(L" %c\n", PIPE);
		}
	} while (!fNoData);

	SetConsole(cDisplaySize + 2, TRUE);
	wprintf(L"%*.*s", cDisplaySize + 2, cDisplaySize + 2, L" ");
	SetConsole(cDisplaySize + 2, FALSE);
	wprintf(L"\n");

Exit:
	// Clean up the allocated buffers
	while (pFirstBinding) {
		pThisBinding = pFirstBinding->sNext;
		free(pFirstBinding->wszBuffer);
		free(pFirstBinding);
		pFirstBinding = pThisBinding;
	}
}

void AllocateBindings(HSTMT hStmt, SQLSMALLINT cCols, BINDING** ppBinding, SQLSMALLINT* pDisplay) {
	SQLSMALLINT     iCol;
	BINDING* pThisBinding, * pLastBinding = NULL;
	SQLLEN          cchDisplay, ssType;
	SQLSMALLINT     cchColumnNameLength;

	*pDisplay = 0;
	for (iCol = 1; iCol <= cCols; iCol++) {
		pThisBinding = (BINDING*)(malloc(sizeof(BINDING)));
		if (!(pThisBinding)) {
			fwprintf(stderr, L"Out of memory!\n");
			exit(-100);
		}
		if (iCol == 1) {
			*ppBinding = pThisBinding;
		}
		else {
			pLastBinding->sNext = pThisBinding;
		}
		pLastBinding = pThisBinding;


		// Figure out the display length of the column 
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLColAttribute(hStmt, iCol, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &cchDisplay));
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLColAttribute(hStmt, iCol, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &ssType));

		pThisBinding->fChar = (ssType == SQL_CHAR || ssType == SQL_VARCHAR || ssType == SQL_LONGVARCHAR);
		pThisBinding->sNext = NULL;

		// Arbitrary limit on display size
		if (cchDisplay > DISPLAY_MAX) cchDisplay = DISPLAY_MAX;

		// Allocate a buffer big enough to hold the text representation of the data.  Add one character for the null terminator
		pThisBinding->wszBuffer = (WCHAR*)malloc((cchDisplay + 1) * sizeof(WCHAR));
		if (!(pThisBinding->wszBuffer)) {
			fwprintf(stderr, L"Out of memory!\n");
			exit(-100);
		}

		// Map this buffer to the driver's buffer.   At Fetch time,
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLBindCol(hStmt, iCol, SQL_C_TCHAR, (SQLPOINTER)pThisBinding->wszBuffer, (cchDisplay + 1) * sizeof(WCHAR), &pThisBinding->indPtr));

		// Now set the display size that we will use to display the data. Figure out the length of the column name

		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLColAttribute(hStmt, iCol, SQL_DESC_NAME, NULL, 0, &cchColumnNameLength, NULL));

		pThisBinding->cDisplaySize = max((SQLSMALLINT)cchDisplay, cchColumnNameLength);
		if (pThisBinding->cDisplaySize < NULL_SIZE)
			pThisBinding->cDisplaySize = NULL_SIZE;

		*pDisplay += pThisBinding->cDisplaySize + DISPLAY_FORMAT_EXTRA;
	}
	return;

Exit:
	exit(-1);
	return;
}

void printTitles(HSTMT hStmt, DWORD cDisplaySize, BINDING* pBinding) {
	WCHAR           wszTitle[DISPLAY_MAX];
	SQLSMALLINT     iCol = 1;

	SetConsole(cDisplaySize + 2, TRUE);
	for (; pBinding; pBinding = pBinding->sNext) {
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLColAttribute(hStmt, iCol++, SQL_DESC_NAME, wszTitle, sizeof(wszTitle), NULL, NULL));
		wprintf(DISPLAY_FORMAT_C, PIPE, pBinding->cDisplaySize, pBinding->cDisplaySize, wszTitle);
	}

Exit:
	wprintf(L" %c", PIPE);
	SetConsole(cDisplaySize + 2, FALSE);
	wprintf(L"\n");

}

void SetConsole(DWORD dwDisplaySize, BOOL fInvert) {
	HANDLE                          hConsole;
	CONSOLE_SCREEN_BUFFER_INFO      csbInfo;

	// Reset the console screen buffer size if necessary
	hConsole = GetStdHandle(STD_OUTPUT_HANDLE);

	if (hConsole != INVALID_HANDLE_VALUE) {
		if (GetConsoleScreenBufferInfo(hConsole, &csbInfo)) {
			if (csbInfo.dwSize.X < (SHORT)dwDisplaySize) {
				csbInfo.dwSize.X = (SHORT)dwDisplaySize;
				SetConsoleScreenBufferSize(hConsole, csbInfo.dwSize);
			}
			gHeight = csbInfo.dwSize.Y;
		}

		if (fInvert) {
			SetConsoleTextAttribute(hConsole, (WORD)(csbInfo.wAttributes | BACKGROUND_BLUE));
		}
		else {
			SetConsoleTextAttribute(hConsole, (WORD)(csbInfo.wAttributes & ~(BACKGROUND_BLUE)));
		}
	}
}

void HandleDiagnosticRecord(SQLHANDLE hHandle, SQLSMALLINT hType, RETCODE RetCode) {
	SQLSMALLINT iRec = 0;
	SQLINTEGER  iError;
	WCHAR       wszMessage[1000];
	WCHAR       wszState[SQL_SQLSTATE_SIZE + 1];


	if (RetCode == SQL_INVALID_HANDLE) {
		fwprintf(stderr, L"Invalid handle!\n");
		return;
	}

	while (SQLGetDiagRec(hType, hHandle, ++iRec, wszState, &iError, wszMessage, (SQLSMALLINT)(sizeof(wszMessage) / sizeof(WCHAR)), (SQLSMALLINT*)NULL) == SQL_SUCCESS) {
		// Hide data truncated..
		if (wcsncmp(wszState, L"01004", 5)) {
			fwprintf(stderr, L"[%5.5s] %s (%d)\n", wszState, wszMessage, iError);
		}
	}
}
