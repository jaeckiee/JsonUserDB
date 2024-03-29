#pragma once
#include "ThirdParty/json/json.h"
#include "LogUtility.h"

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

#define SQL_SAFE_FREESTATEMENT(h) {	if (h != NULL) { SQLFreeStmt(h, SQL_CLOSE);	h = NULL; } }


#define SQL_SET_AUTOCOMMIT_OFF(h)\
					if (!SQL_SUCCEEDED(SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER))) {\
					Log(LOG_FATAL, L"Failed to set autocommit off");\
					exit(ERROR_FATAL);\
					}

#define SQL_END_TRANSACTION(h, i)\
					if (!SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, h, i ? SQL_COMMIT : SQL_ROLLBACK))) {\
					Log(LOG_FATAL, L"Failed to end transaction");\
					exit(ERROR_FATAL);\
					}

#define SQL_SET_AUTOCOMMIT_ON(h)\
					if (!SQL_SUCCEEDED(SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, SQL_IS_UINTEGER))) {\
					Log(LOG_FATAL, L"Failed to set autocommit on");\
					exit(ERROR_FATAL);\
					}

// Some constants
#define DISPLAY_MAX 50          // Arbitrary limit on column width to display
#define DISPLAY_FORMAT_EXTRA 3  // Per column extra display bytes (| <data> )
#define DISPLAY_FORMAT      L"%c %*.*s "
#define DISPLAY_FORMAT_C    L"%c %-*.*s "
#define NULL_SIZE           6   // <NULL>

#define PIPE                L'|'

#define STORE_RECORD(x, y) 		\
						SQLGetData(hstmt, col_num, x, &y, sizeof(y), &indicator); \
						if (indicator != SQL_NULL_DATA) current_record[get_utf8(col_name)] = y;

#define STORE_RECORD_STR(x, y)		\
                        col_wchar_val = new SQLWCHAR[y + 1];\
						SQLGetData(hstmt, col_num, x, col_wchar_val, (y + 1) * sizeof(WCHAR), &indicator);\
						if (indicator != SQL_NULL_DATA) current_record[get_utf8(col_name)] = get_utf8(col_wchar_val);\
                        delete[] col_wchar_val;

static SHORT g_height = 80;
static int g_verbose = 0;

typedef struct STR_BINDING {
    SQLSMALLINT         cDisplaySize;           /* size to display  */
    WCHAR* wszBuffer;             /* display buffer   */
    SQLLEN              indPtr;                 /* size or null     */
    BOOL                fChar;                  /* character col?   */
    struct STR_BINDING* sNext;                 /* linked list      */
} BINDING;

// For SQL Execute
bool sqlfExec(SQLHSTMT& hStmt, SQLHDBC hDbc, std::wstring wszInput);

// Return Result Of Query
std::unordered_set<std::wstring> sqlfSingleCol(SQLHDBC hDbc, std::wstring wszInput);
Json::Value sqlfMultiCol(SQLHDBC hDbc, const std::wstring tableName, std::wstring wszInput);

// Connect And Diconnect DB
bool connectToDB(SQLHENV& hEnv, SQLHDBC& hDbc, std::wstring pwszConnStr);
bool disconnectDB(SQLHENV& hEnv, SQLHDBC& hDbc, SQLHSTMT& hStmt);

// Print Table
bool printTable(SQLHDBC hDbc, std::wstring wszInput);

// Print Info
void HandleDiagnosticRecord(SQLHANDLE hHandle, SQLSMALLINT hType, RETCODE RetCode);
void printResults(HSTMT hStmt, SQLSMALLINT cCols);
void AllocateBindings(HSTMT hStmt, SQLSMALLINT cCols, BINDING** ppBinding, SQLSMALLINT* pDisplay);
void printTitles(HSTMT hStmt, DWORD cDisplaySize, BINDING* pBinding);
void SetConsole(DWORD cDisplaySize, BOOL fInvert);