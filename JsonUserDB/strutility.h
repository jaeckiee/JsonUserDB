#pragma once

std::string get_utf8(const std::wstring& wstr);

std::wstring get_utf16(const std::string& str);

std::vector<std::wstring> getSubtractedWstrList(std::vector<std::wstring> subtractedWstrLiST, std::vector<std::wstring> subtractingWstrList);
