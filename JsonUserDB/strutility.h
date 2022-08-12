#pragma once
#include <unordered_set>

#define splitStr splitStrW

std::string get_utf8(const std::wstring& wstr);

std::wstring get_utf16(const std::string& str);

std::unordered_set<std::wstring> splitStrW(const std::wstring& InputString, wchar_t delimiterChar);

std::unordered_set<std::wstring>& splitStrW(const std::wstring& InputString, wchar_t delimiterChar, std::unordered_set<std::wstring>& resultSet);

bool checkWstrInWstrSet(std::wstring wstr, std::unordered_set<std::wstring> wstrSet);

std::unordered_set<std::wstring> getDiffWstrSet(std::unordered_set<std::wstring> subtractedWstrset, std::unordered_set<std::wstring> subtractingWstrset);
