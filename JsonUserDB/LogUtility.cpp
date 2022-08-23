#include "LogUtility.h"

void Log(int severityLv, std::wstring msg) {
	if (severityLv > g_standard_log_severity_lv)
		return;
	fwprintf((severityLv > LOG_ERROR) ? stdout : stderr, L"%s\n", msg.c_str());
}