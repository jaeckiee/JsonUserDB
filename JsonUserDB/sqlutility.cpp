#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <conio.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <wchar.h>
#include <unordered_set>
#include "sqlutility.h"
#include "strutility.h"
#include "logutility.h"

bool sqlfExec(SQLHSTMT& hStmt, SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	bool is_succeeded = false;
	RETCODE retcode;
	WCHAR fwszinput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszinput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	TRYODBC(hDbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt));
	if (g_verbose) {
		Log(LOG_INFO, std::wstring(fwszinput));
	}
	if (SQL_SUCCEEDED(retcode = SQLExecDirect(hStmt, fwszinput, SQL_NTS))) {
		is_succeeded = true;
	}
	else {
		HandleDiagnosticRecord(hStmt, SQL_HANDLE_STMT, retcode);
	}
Exit:
	return is_succeeded;
}

std::unordered_set<std::wstring> sqlfSingleCol(SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	SQLSMALLINT snum_results;
	SQLHSTMT hstmt = NULL;
	std::unordered_set<std::wstring> row_val_set;
	WCHAR fwszinput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszinput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	if (!sqlfExec(hstmt, hDbc, fwszinput))
		goto Exit;
	TRYODBC(hstmt, SQL_HANDLE_STMT, SQLNumResultCols(hstmt, &snum_results));
	if (snum_results != 1)
		goto Exit;
	while (SQL_SUCCEEDED(SQLFetch(hstmt))) {
		SQLLEN indicator;
		SQLSMALLINT col_num = 1;
		const int buf_size = 512;
		WCHAR buf[buf_size];
		if (SQL_SUCCEEDED(SQLGetData(hstmt, col_num, SQL_UNICODE, buf, sizeof(buf), &indicator))) {
			if (indicator != SQL_NULL_DATA) {
				row_val_set.insert(buf);
			}
		}
	}
Exit:
	SQL_SAFE_FREESTATEMENT(hstmt);
	return row_val_set;
}

Json::Value sqlfMultiCol(SQLHDBC hDbc, const std::wstring tableName, const WCHAR* wszInput, ...) {
	SQLSMALLINT snum_results;
	SQLHSTMT hstmt = NULL;
	Json::Value result_json;
	WCHAR fwszinput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszinput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	if (!sqlfExec(hstmt, hDbc, fwszinput))
		goto Exit;
	TRYODBC(hstmt, SQL_HANDLE_STMT, SQLNumResultCols(hstmt, &snum_results));
	if (snum_results <= 0)
		goto Exit;
	while (SQL_SUCCEEDED(SQLFetch(hstmt))) {
		SQLUSMALLINT col_num;
		Json::Value current_record;
		for (col_num = 1; col_num <= snum_results; col_num++) {
			const SQLSMALLINT col_name_buf_size = 128;
			SQLWCHAR col_name[col_name_buf_size];
			SQLSMALLINT col_type;
			SQLULEN col_size;
			TRYODBC(hstmt, SQL_HANDLE_STMT, SQLDescribeCol(hstmt, col_num, col_name, col_name_buf_size, NULL, &col_type, &col_size, NULL, NULL););
			SQLLEN indicator;
			int col_Int_val;
			bool col_bit_val;
			float col_float_val;
			double col_double_val;
			SQLWCHAR* col_wchar_val;
			switch (col_type) {
			case SQL_INTEGER:
			case SQL_TINYINT:
			case SQL_SMALLINT:
				STORE_RECORD(SQL_INTEGER, col_Int_val);
				break;
			case SQL_FLOAT:
				STORE_RECORD(SQL_FLOAT, col_float_val);
				break;
			case SQL_DOUBLE:
				STORE_RECORD(SQL_DOUBLE, col_double_val);
				break;
			case SQL_BIT:
				STORE_RECORD(SQL_BIT, col_bit_val);
				break;
			case SQL_BINARY:
			case SQL_VARBINARY:
				col_size = col_size * 2;
			case SQL_BIGINT:
			case SQL_CHAR:
			case SQL_VARCHAR:
			case SQL_DECIMAL:
			case SQL_NUMERIC:
			case SQL_WCHAR:
			case SQL_WVARCHAR:
			case SQL_DATE:
			case SQL_TYPE_DATE:
			case SQL_TIME:
			case SQL_TYPE_TIME:
			case SQL_TIMESTAMP:
			case SQL_TYPE_TIMESTAMP:
				STORE_RECORD_STR(SQL_UNICODE, col_size);
				break;
			default:
				Log(LOG_FATAL, L"This column type is not supported");
				exit(-1);
			}
		}
		result_json.append(current_record);
	}
Exit:
	SQL_SAFE_FREESTATEMENT(hstmt);
	return result_json;
}

bool connectToDB(SQLHENV& hEnv, SQLHDBC& hDbc, std::wstring pwszConnStr) {
	bool is_succeeded = false;
	TRYODBC(hEnv, SQL_HANDLE_ENV, SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv));
	TRYODBC(hEnv, SQL_HANDLE_ENV, SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0));
	TRYODBC(hEnv, SQL_HANDLE_ENV, SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc));
	is_succeeded = SQL_SUCCEEDED(SQLDriverConnect(hDbc, NULL, const_cast<SQLWCHAR*>(pwszConnStr.c_str()), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE));
Exit:
	return is_succeeded;
}

bool disconnectDB(SQLHENV& hEnv, SQLHDBC& hDbc, SQLHSTMT& hStmt) {
	bool is_succeeded = false;
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
		is_succeeded = SQL_SUCCEEDED(SQLDisconnect(hDbc));
		SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
		hDbc = NULL;
	}
	return is_succeeded;
}

bool printTable(SQLHDBC hDbc, const WCHAR* wszInput, ...) {
	bool is_succeeded = false;
	SQLSMALLINT snum_results;
	SQLHSTMT hstmt = NULL;
	TRYODBC(hDbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hstmt));
	WCHAR fwszinput[SQL_QUERY_SIZE];
	va_list args;
	va_start(args, wszInput);
	_vsnwprintf_s(fwszinput, SQL_QUERY_SIZE, wszInput, args);
	va_end(args);
	if (sqlfExec(hstmt, hDbc, fwszinput)) {
		TRYODBC(hstmt, SQL_HANDLE_STMT, SQLNumResultCols(hstmt, &snum_results));
		if (snum_results > 0) {
			printResults(hstmt, snum_results);
			is_succeeded = true;
		}
	}
Exit:
	SQL_SAFE_FREESTATEMENT(hstmt);
	return is_succeeded;
}

void printResults(HSTMT hStmt, SQLSMALLINT cCols) {
	BINDING* p_first_binding, * p_this_binding;
	SQLSMALLINT     c_display_size;
	RETCODE         retcode = SQL_SUCCESS;
	int             icount = 0;

	// Allocate memory for each column
	AllocateBindings(hStmt, cCols, &p_first_binding, &c_display_size);

	// Set the display mode and write the titles
	printTitles(hStmt, c_display_size + 1, p_first_binding);

	// Fetch and display the data
	bool fNoData = false;

	do {
		// Fetch a row

		if (icount++ >= g_height - 2) {
			int     nInputChar;
			bool    fEnterReceived = false;

			while (!fEnterReceived) {
				wprintf(L"              ");
				SetConsole(c_display_size + 2, TRUE);
				wprintf(L"   Press ENTER to continue, Q to quit (height:%hd)", g_height);
				SetConsole(c_display_size + 2, FALSE);

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
			icount = 1;
			printTitles(hStmt, c_display_size + 1, p_first_binding);
		}

		TRYODBC(hStmt, SQL_HANDLE_STMT, retcode = SQLFetch(hStmt));

		if (retcode == SQL_NO_DATA_FOUND) {
			fNoData = true;
		}
		else {
			// Display the data.   Ignore truncations
			for (p_this_binding = p_first_binding; p_this_binding; p_this_binding = p_this_binding->sNext) {
				if (p_this_binding->indPtr != SQL_NULL_DATA) {
					wprintf(p_this_binding->fChar ? DISPLAY_FORMAT_C : DISPLAY_FORMAT,
						PIPE,
						p_this_binding->cDisplaySize,
						p_this_binding->cDisplaySize,
						p_this_binding->wszBuffer);
				}
				else {
					wprintf(DISPLAY_FORMAT_C,
						PIPE,
						p_this_binding->cDisplaySize,
						p_this_binding->cDisplaySize,
						L"<NULL>");
				}
			}
			wprintf(L" %c\n", PIPE);
		}
	} while (!fNoData);

	SetConsole(c_display_size + 2, TRUE);
	wprintf(L"%*.*s", c_display_size + 2, c_display_size + 2, L" ");
	SetConsole(c_display_size + 2, FALSE);
	wprintf(L"\n");

Exit:
	// Clean up the allocated buffers
	while (p_first_binding) {
		p_this_binding = p_first_binding->sNext;
		free(p_first_binding->wszBuffer);
		free(p_first_binding);
		p_first_binding = p_this_binding;
	}
}

void AllocateBindings(HSTMT hStmt, SQLSMALLINT cCols, BINDING** ppBinding, SQLSMALLINT* pDisplay) {
	SQLSMALLINT     icol;
	BINDING* p_this_binding, * p_last_binding = NULL;
	SQLLEN          cch_display, sstype;
	SQLSMALLINT     cch_column_name_length;

	*pDisplay = 0;
	for (icol = 1; icol <= cCols; icol++) {
		p_this_binding = (BINDING*)(malloc(sizeof(BINDING)));
		if (!(p_this_binding)) {
			fwprintf(stderr, L"Out of memory!\n");
			exit(-100);
		}
		if (icol == 1) {
			*ppBinding = p_this_binding;
		}
		else {
			p_last_binding->sNext = p_this_binding;
		}
		p_last_binding = p_this_binding;


		// Figure out the display length of the column 
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLColAttribute(hStmt, icol, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &cch_display));
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLColAttribute(hStmt, icol, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &sstype));

		p_this_binding->fChar = (sstype == SQL_CHAR || sstype == SQL_VARCHAR || sstype == SQL_LONGVARCHAR);
		p_this_binding->sNext = NULL;

		// Arbitrary limit on display size
		if (cch_display > DISPLAY_MAX) cch_display = DISPLAY_MAX;

		// Allocate a buffer big enough to hold the text representation of the data.  Add one character for the null terminator
		p_this_binding->wszBuffer = (WCHAR*)malloc((cch_display + 1) * sizeof(WCHAR));
		if (!(p_this_binding->wszBuffer)) {
			fwprintf(stderr, L"Out of memory!\n");
			exit(-100);
		}

		// Map this buffer to the driver's buffer.   At Fetch time,
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLBindCol(hStmt, icol, SQL_C_TCHAR, (SQLPOINTER)p_this_binding->wszBuffer, (cch_display + 1) * sizeof(WCHAR), &p_this_binding->indPtr));

		// Now set the display size that we will use to display the data. Figure out the length of the column name

		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLColAttribute(hStmt, icol, SQL_DESC_NAME, NULL, 0, &cch_column_name_length, NULL));

		p_this_binding->cDisplaySize = max((SQLSMALLINT)cch_display, cch_column_name_length);
		if (p_this_binding->cDisplaySize < NULL_SIZE)
			p_this_binding->cDisplaySize = NULL_SIZE;

		*pDisplay += p_this_binding->cDisplaySize + DISPLAY_FORMAT_EXTRA;
	}
	return;

Exit:
	exit(-1);
	return;
}

void printTitles(HSTMT hStmt, DWORD cDisplaySize, BINDING* pBinding) {
	WCHAR           wsztitle[DISPLAY_MAX];
	SQLSMALLINT     icol = 1;

	SetConsole(cDisplaySize + 2, TRUE);
	for (; pBinding; pBinding = pBinding->sNext) {
		TRYODBC(hStmt, SQL_HANDLE_STMT, SQLColAttribute(hStmt, icol++, SQL_DESC_NAME, wsztitle, sizeof(wsztitle), NULL, NULL));
		wprintf(DISPLAY_FORMAT_C, PIPE, pBinding->cDisplaySize, pBinding->cDisplaySize, wsztitle);
	}

Exit:
	wprintf(L" %c", PIPE);
	SetConsole(cDisplaySize + 2, FALSE);
	wprintf(L"\n");

}

void SetConsole(DWORD dwDisplaySize, BOOL fInvert) {
	HANDLE                          hconsole;
	CONSOLE_SCREEN_BUFFER_INFO      csb_info;

	// Reset the console screen buffer size if necessary
	hconsole = GetStdHandle(STD_OUTPUT_HANDLE);

	if (hconsole != INVALID_HANDLE_VALUE) {
		if (GetConsoleScreenBufferInfo(hconsole, &csb_info)) {
			if (csb_info.dwSize.X < (SHORT)dwDisplaySize) {
				csb_info.dwSize.X = (SHORT)dwDisplaySize;
				SetConsoleScreenBufferSize(hconsole, csb_info.dwSize);
			}
			g_height = csb_info.dwSize.Y;
		}

		if (fInvert) {
			SetConsoleTextAttribute(hconsole, (WORD)(csb_info.wAttributes | BACKGROUND_BLUE));
		}
		else {
			SetConsoleTextAttribute(hconsole, (WORD)(csb_info.wAttributes & ~(BACKGROUND_BLUE)));
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
