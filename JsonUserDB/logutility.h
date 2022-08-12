#pragma once
#include <string>

#define LOG_OFF		0
#define LOG_FATAL	1
#define LOG_ERROR	2
#define LOG_WARN	3
#define LOG_INFO	4
#define LOG_DEBUG	5
#define LOG_TRACE	6
#define LOG_ALL		7

static int g_standard_log_severity_lv = LOG_ALL;

void Log(int severityLv, std::wstring msg);