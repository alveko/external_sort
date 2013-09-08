#ifndef EXTERNAL_SORT_AUX_H
#define EXTERNAL_SORT_AUX_H

#include <boost/format.hpp>
#include <boost/timer/timer.hpp>

#define TIMER(x) boost::timer::auto_cpu_timer __x__timer(x);
#define TRACE(x) std::cout << boost::format x

#ifdef DEBUG
#define TDEBUG(x) TRACE(x)
#else
#define TDEBUG(x)
#endif

#endif
