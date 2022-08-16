#include "logutility.h"

void handleNewAllocFail() {
	Log(LOG_FATAL, L"Failed to allocate memory");
	std::exit(-1);
}