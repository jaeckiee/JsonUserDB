#pragma once
#include "json/json.h"

// Macro to call ODBC functions and report an error on failure and Takes handle, handle type, and stmt
#define TRYODBC(h, ht, x)   {   RETCODE rc = x;\
                                if (rc != SQL_SUCCESS) \
                                { \
                                    HandleDiagnosticRecord (h, ht, rc); \
                                } \
                                if (rc == SQL_ERROR) \
                                { \
                                    fwprintf(stderr, L"Error in " L#x L"\n"); \
                                    goto Exit;  \
                                }  \
                            }

// Some constants
#define DISPLAY_MAX 50          // Arbitrary limit on column width to display
#define DISPLAY_FORMAT_EXTRA 3  // Per column extra display bytes (| <data> )
#define DISPLAY_FORMAT      L"%c %*.*s "
#define DISPLAY_FORMAT_C    L"%c %-*.*s "
#define NULL_SIZE           6   // <NULL>
#define SQL_QUERY_SIZE      5000 // Max. Num characters for SQL Query passed in.

#define PIPE                L'|'

#define STORE_RECORD(x, y) 		\
						SQLGetData(hStmt, colnum, x, &y, sizeof(&y), &indicator); \
						if (SQL_SUCCEEDED(RetCode) && (indicator != SQL_NULL_DATA)) current_record[get_utf8(colName)] = y;

#define STORE_RECORD_STR(x, y)		\
						SQLGetData(hStmt, colnum, x, &y, sizeof(&y), &indicator); \
						if (SQL_SUCCEEDED(RetCode) && (indicator != SQL_NULL_DATA)) current_record[get_utf8(colName)] = get_utf8(y);

static SHORT gHeight = 80;
static int g_VERBOSE = 0;

typedef struct STR_BINDING {
    SQLSMALLINT         cDisplaySize;           /* size to display  */
    WCHAR* wszBuffer;             /* display buffer   */
    SQLLEN              indPtr;                 /* size or null     */
    BOOL                fChar;                  /* character col?   */
    struct STR_BINDING* sNext;                 /* linked list      */
} BINDING;

// For SQL Execute
RETCODE sqlfExec(SQLHSTMT& hStmt, SQLHDBC hDbc, const WCHAR* wszInput, ...);
Json::Value sqlf_MultiCol(SQLHDBC hDbc, const std::wstring tableName, const WCHAR* wszInput, ...);

// Return Result Of Query
std::vector<std::wstring> sqlf_SingleCol(SQLHDBC hDbc, const WCHAR* wszInput, ...);

// Connect And Diconnect DB
bool connectToDB(SQLHENV& hEnv, SQLHDBC& hDbc, std::wstring pwszConnStr);
bool disconnectDB(SQLHENV& hEnv, SQLHDBC& hDbc, SQLHSTMT& hStmt);

// Print Info
void HandleDiagnosticRecord(SQLHANDLE hHandle, SQLSMALLINT hType, RETCODE RetCode);
void printResults(HSTMT hStmt, SQLSMALLINT cCols);
void AllocateBindings(HSTMT hStmt, SQLSMALLINT cCols, BINDING** ppBinding, SQLSMALLINT* pDisplay);
void printTitles(HSTMT hStmt, DWORD cDisplaySize, BINDING* pBinding);
void SetConsole(DWORD cDisplaySize, BOOL fInvert);