#ifndef EXTERNAL_SORT_NOLOG_HPP
#define EXTERNAL_SORT_NOLOG_HPP

// The external sort source files use LOG* and TRACE* macros to
// log info and trace debug messages. In order to be able to include
// external_sort.hpp and compile it even if no such macors are defined
// in the client program, we have to define them empty here

#if !defined(LOG_FAT) || !defined(LOG_ERR) || !defined(LOG_WRN) || \
    !defined(LOG_IMP) || !defined(LOG_INF) || !defined(LOG_LOW) || \
    !defined(LOG_DBG)

#define LOG_FAT(x)
#define LOG_ERR(x)
#define LOG_WRN(x)
#define LOG_IMP(x)
#define LOG_INF(x)
#define LOG_LOW(x)
#define LOG_DBG(x)

#endif

#if !defined(TRACE) || !defined(TRACE_SCOPE) || !defined(TRACE_FUNC) || \
    !defined(TRACEX) || !defined(TRACEX_SCOPE) || !defined(TRACEX_METHOD) || \
    !defined(TRACEX_NAME)

#define TRACE(x)
#define TRACE_SCOPE(scope)
#define TRACE_FUNC()

#define TRACEX(x)
#define TRACEX_SCOPE(scope)
#define TRACEX_METHOD()
#define TRACEX_NAME(channel)

#endif

#endif
