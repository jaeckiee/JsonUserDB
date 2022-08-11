#pragma once
#include <unordered_set>

std::string get_utf8(const std::wstring& wstr);

std::wstring get_utf16(const std::string& str);

std::unordered_set<std::wstring> getDiffSet(std::unordered_set<std::wstring> subtractedWstrset, std::unordered_set<std::wstring> subtractingWstrset);
