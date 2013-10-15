#include <thread>
#include <fstream>
#include <unordered_map>

#include "logging.hpp"

#ifdef BOOSTLOG
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>

namespace logging  = boost::log;
namespace expr     = boost::log::expressions;
namespace attrs    = boost::log::attributes;
namespace keywords = boost::log::keywords;

BOOST_LOG_ATTRIBUTE_KEYWORD(timestamp, "TimeStamp", boost::posix_time::ptime)
BOOST_LOG_ATTRIBUTE_KEYWORD(thread_id, "ThreadID", attrs::current_thread_id::value_type)
BOOST_LOG_ATTRIBUTE_KEYWORD(severity, "Severity", severity_level)
BOOST_LOG_ATTRIBUTE_KEYWORD(scope, "Scope", attrs::named_scope::value_type)
BOOST_LOG_ATTRIBUTE_KEYWORD(channel, "Channel", std::string)
#endif

// The operator is used for regular stream formatting
std::ostream& operator<< (std::ostream& strm, severity_level level)
{
    static const char* strings[] =
    {
        "NON",
        "FAT",
        "ERR",
        "WRN",
        "IMP",
        "INF",
        "LOW",
        "DBG"
    };
    if (static_cast<std::size_t>(level) < sizeof(strings) / sizeof(*strings)) {
        strm << strings[level];
    } else {
        strm << static_cast<int>(level);
    }
    return strm;
}

#ifdef BOOSTLOG
inline size_t tid2nid(const logging::aux::thread::native_type& tid)
{
    static std::unordered_map<logging::aux::thread::native_type,
                              size_t> tid_map;

    auto nid_iter = tid_map.find(tid);
    if (nid_iter == tid_map.end()) {
        tid_map[tid] = tid_map.size();
        nid_iter = tid_map.find(tid);
    }
    return nid_iter->second;
}

struct my_formatter
{
    void operator()(logging::record_view const& rec,
                    logging::formatting_ostream& strm)
    {
#ifdef DEBUG
        std::ostringstream ss_channel;
        ss_channel << "<" << rec[channel] << ">";

        std::ostringstream ss_scope;
        ss_scope << "(";
        if (rec[scope]->size() >= 2) {
            ss_scope << (*(++rec[scope]->rbegin())).scope_name << "->";
        }
        if (rec[scope]->size() >= 1) {
            ss_scope << (*(rec[scope]->rbegin())).scope_name;
        }
        ss_scope << ")";
#endif
        std::string fdt_str;
        logging::formatting_ostream fdt_stream(fdt_str);
        dt_formatter_(rec, fdt_stream);

        auto tid = rec[thread_id]->native_id();

        strm << bformat_
                % fdt_str % tid
#ifdef DEBUG
                % tid2nid(tid)
#endif
                % rec[severity]
#ifdef DEBUG
                % ss_channel.str() % ss_scope.str()
#endif
                % rec[expr::smessage];
    }

    my_formatter() :
#ifdef DEBUG
        bformat_(boost::format("[%s %012x:%03d %s] %-40s %-40s %s")),
#else
        bformat_(boost::format("[%s %012x %s] %s")),
#endif
        dt_formatter_(expr::stream <<
                      expr::format_date_time<boost::posix_time::ptime>(
                          "TimeStamp", "%Y-%m-%d %H:%M:%S.%f"))
    {
    }

  private:
    boost::format bformat_;
    std::function<void(logging::record_view const&,
                       logging::formatting_ostream&)> dt_formatter_;
};
#endif // BOOSTLOG

severity_level log_lvl = IMP;

void log_init(severity_level lvl)
{
    log_lvl = lvl;
#ifdef BOOSTLOG
    // Add attributes
    logging::add_common_attributes();
    logging::core::get()->add_global_attribute("Scope", attrs::named_scope());

    typedef logging::sinks::synchronous_sink<
        logging::sinks::text_ostream_backend> text_sink;

    // file sink
    boost::shared_ptr< text_sink > sinkFile = boost::make_shared< text_sink >();
    sinkFile->locked_backend()->add_stream(
        boost::make_shared< std::ofstream >("trace.log"));
    sinkFile->locked_backend()->auto_flush(true);
    logging::core::get()->add_sink(sinkFile);

    // console sink
    boost::shared_ptr<text_sink> sinkConsole = boost::make_shared<text_sink>();
    sinkConsole->locked_backend()->add_stream(
        boost::shared_ptr< std::ostream >(&std::cout, logging::empty_deleter()));
    sinkConsole->locked_backend()->auto_flush(true);
    logging::core::get()->add_sink(sinkConsole);

    sinkFile->set_formatter(my_formatter());
    sinkConsole->set_formatter(my_formatter());
#endif // BOOSTLOG
}
