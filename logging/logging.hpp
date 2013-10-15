#ifndef LOGGING_HPP
#define LOGGING_HPP

#include <boost/format.hpp>
#include <boost/timer/timer.hpp>

// NOLOG
// STDLOG
// BOOSTLOG

#define TIMER(x) boost::timer::auto_cpu_timer __x__timer(x);

enum severity_level
{
    NON = 0, // none
    FAT = 1, // fatal
    ERR = 2, // error
    WRN = 3, // warning
    IMP = 4, // important information
    INF = 5, // information
    LOW = 6, // information of low importance
    DBG = 7, // debug (only for debug builds)
};

extern severity_level log_lvl;

/// ----------------------------------------------------------------------------
/// LOG with boost.log
#ifdef BOOSTLOG
#include <boost/log/common.hpp>

typedef boost::log::sources::severity_channel_logger_mt<
    severity_level,     // the type of the severity level
    std::string         // the type of the channel name
> channel_logger_mt;

BOOST_LOG_INLINE_GLOBAL_LOGGER_INIT(my_global_logger, channel_logger_mt)
{
    return channel_logger_mt(boost::log::keywords::channel = "global");
}

#define LOG(lvl, x) \
    BOOST_LOG_SEV(my_global_logger::get(), lvl) << boost::format x
#else
/// ----------------------------------------------------------------------------
/// LOG with std::cout
#define LOG(lvl, x) \
    { if (lvl <= log_lvl) std::cout << boost::format x << std::endl; }
#endif

#define LOG_FAT(x) LOG(FAT, x)
#define LOG_ERR(x) LOG(ERR, x)
#define LOG_WRN(x) LOG(WRN, x)
#define LOG_IMP(x) LOG(IMP, x)
#define LOG_INF(x) LOG(INF, x)
#define LOG_LOW(x) LOG(LOW, x)
#define LOG_DBG(x) LOG(DBG, x)

#define LOG_INIT   log_init
void log_init(severity_level = IMP);

/// ----------------------------------------------------------------------------
/// NO DEBUG => all TRACE* macros are empty
#if !defined(DEBUG) || !defined(BOOSTLOG)

#define TRACE(x)
#define TRACE_SCOPE(scope)
#define TRACE_FUNC()

#define TRACEX(x)
#define TRACEX_SCOPE(scope)
#define TRACEX_METHOD()
#define TRACEX_NAME(channel)

#else
/// ----------------------------------------------------------------------------
/// DEBUG TRACE with boost log

#define TRACE_WITH_LOGGER(logger, x) \
    BOOST_LOG_SEV(logger, DBG) << boost::format x

template<typename Logger>
class TraceScope {
  public:
    TraceScope(const Logger& logger, const std::string& scope)
        : scope_(scope), logger_(logger)
    {
        TRACE_WITH_LOGGER(logger_, ("--> Enter %s") % scope_);
    }
    ~TraceScope()
    {
        TRACE_WITH_LOGGER(logger_, ("<-- Exit %s")  % scope_);
    }
  private:
    std::string scope_;
    const Logger &logger_;
};

#define AUX_TRACE_SCOPE_CLASS(logger, scope) \
    TraceScope<decltype(logger)> aux_trace_scope(logger, scope)

/// TRACE_*() - global trace

#define TRACE(x) \
    TRACE_WITH_LOGGER(my_global_logger::get(), x)

#define TRACE_SCOPE(scope)        \
    BOOST_LOG_NAMED_SCOPE(scope); \
    AUX_TRACE_SCOPE_CLASS(my_global_logger::get(), scope);

#define TRACE_FUNC() \
    TRACE_SCOPE(__func__)

/// TRACEX_*() - trace with named channel (for example, in a class)

#define TRACEX(x) \
    TRACE_WITH_LOGGER(get_channel_logger(), x)

#define TRACEX_SCOPE(scope)       \
    BOOST_LOG_NAMED_SCOPE(scope); \
    AUX_TRACE_SCOPE_CLASS(get_channel_logger(), scope)

#define TRACEX_METHOD() \
    TRACEX_SCOPE(__func__)

#define TRACEX_NAME(channel_name)                                  \
    mutable std::shared_ptr<channel_logger_mt> channel_logger_;    \
    inline channel_logger_mt& get_channel_logger() const {         \
        if (!channel_logger_) {                                    \
            std::ostringstream ss;                                 \
            ss << boost::format("%014p:%s") % this % channel_name; \
            channel_logger_.reset(new channel_logger_mt(           \
                boost::log::keywords::channel = ss.str()));        \
        }                                                          \
        return *channel_logger_;                                   \
    }

#endif

#endif
