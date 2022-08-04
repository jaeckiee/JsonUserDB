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

std::vector<std::wstring> getSubtractedWstrList(std::vector<std::wstring> subtractedWstrLiST, std::vector<std::wstring> subtractingWstrList) {
	for (int tableidx = 0; tableidx < subtractingWstrList.size(); tableidx++) {
		subtractedWstrLiST.erase(std::remove(subtractedWstrLiST.begin(), subtractedWstrLiST.end(), subtractingWstrList[tableidx]), subtractedWstrLiST.end());
	}
	return subtractedWstrLiST;
}
