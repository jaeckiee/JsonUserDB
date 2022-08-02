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


SHORT gHeight = 80;

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
