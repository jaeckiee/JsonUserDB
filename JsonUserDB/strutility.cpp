#include <windows.h>
#include <string>
#include <vector>
#include "strutility.h"

std::string get_utf8(const std::wstring& wstr) {
	if (wstr.empty()) return std::string();
	int sz = WideCharToMultiByte(CP_UTF8, 0, &wstr[0], (int)wstr.size(), 0, 0, 0, 0);
	std::string res(sz, 0);
	WideCharToMultiByte(CP_UTF8, 0, &wstr[0], (int)wstr.size(), &res[0], sz, 0, 0);
	return res;
}

std::wstring get_utf16(const std::string& str) {
	if (str.empty()) return std::wstring();
	int sz = MultiByteToWideChar(CP_UTF8, 0, &str[0], (int)str.size(), 0, 0);
	std::wstring res(sz, 0);
	MultiByteToWideChar(CP_UTF8, 0, &str[0], (int)str.size(), &res[0], sz);
	return res;
}

bool checkWstrInWstrSet(std::wstring wstr, std::unordered_set<std::wstring> wstrSet) {
	if (wstrSet.find(wstr) == wstrSet.end()) {
		return false;
	}
	return true;
}

std::unordered_set<std::wstring> getDiffWstrSet(std::unordered_set<std::wstring> subtractedSet, std::unordered_set<std::wstring> subtractingSet) {
	for (const std::wstring elem : subtractingSet) {
		subtractedSet.erase(elem);
	}
	return subtractedSet;
}
