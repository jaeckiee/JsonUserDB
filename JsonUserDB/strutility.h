#pragma once
#include <unordered_set>

std::string get_utf8(const std::wstring& wstr);

std::wstring get_utf16(const std::string& str);

bool checkWstrInWstrSet(std::wstring wstr, std::unordered_set<std::wstring> wstrSet);

std::unordered_set<std::wstring> getDiffWstrSet(std::unordered_set<std::wstring> subtractedWstrset, std::unordered_set<std::wstring> subtractingWstrset);
