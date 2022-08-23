#pragma once
#include <string>

enum LOG {
	LOG_OFF,
	LOG_FATAL,
	LOG_ERROR,
	LOG_WARN,
	LOG_INFO,
	LOG_DEBUG,
	LOG_TRACE,
	LOG_ALL
};

#define LOG_AND_RETURN_VALUE(x, y, z)\
					Log(x, y);\
					return z;

static int g_standard_log_severity_lv = LOG_ALL;

void Log(int severityLv, std::wstring msg);