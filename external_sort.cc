#include <iostream>
#include <sstream>
#include <memory>
#include <queue>
#include <boost/program_options.hpp>
#include <boost/format.hpp>

#include "logging.hpp"
#include "block_types.hpp"
#include "block_input_stream.hpp"
#include "block_output_stream.hpp"
#include "block_file_read_policy.hpp"
#include "block_file_write_policy.hpp"
#include "block_memory_policy.hpp"
#include "external_sort.hpp"

/// ----------------------------------------------------------------------------
/// type definitions

using Block = U32Block;

using InputStream = BlockInputStream<Block,
                                     BlockFileReadPolicy<Block>,
                                     BlockMemoryPolicy<Block>>;

using OutputStream = BlockOutputStream<Block,
                                       BlockFileWritePolicy<Block>,
                                       BlockMemoryPolicy<Block>>;

using InputStreamPtr = std::shared_ptr<InputStream>;
using OutputStreamPtr = std::shared_ptr<OutputStream>;

#define ACT_NONE (0x00)
#define ACT_ALL  (0xFF)
#define ACT_GEN  (1 << 0)
#define ACT_SRT  (1 << 1)
#define ACT_MRG  (1 << 2)
#define ACT_CHK  (1 << 3)

#define DEF_TMP_DIR           "/tmp"
#define DEF_GEN_OUT_FILENAME  "generated"
#define DEF_SRT_OUT_FILENAME  "ph1_sorted"
#define DEF_MRG_OUT_FILENAME  "ph2_merged"
#define DEF_MRG_RES_SFX       ".sorted"

namespace po = boost::program_options;

/// ----------------------------------------------------------------------------
/// auxiliary logging functions

std::string any2str(const boost::any x)
{
    std::ostringstream ss;
    if (x.type() == typeid(std::string)) {
        ss << boost::any_cast<std::string>(x);
    } else if (x.type() == typeid(size_t)) {
        ss << boost::any_cast<size_t>(x);
    } else if (x.type() == typeid(int)) {
        ss << boost::any_cast<int>(x);
    } else {
        ss << "UNKNOWN_TYPE";
    }
    return ss.str();
}

void log_params(const po::variables_map& params,
                const std::string& section = {})
{
    for (auto it = params.begin(); it != params.end(); ++it) {
        if ((section.empty() && it->first.find(".") == std::string::npos) ||
            (!section.empty() &&
             section.compare(0, section.length(),
                             it->first, 0, section.length()) == 0)) {
            LOG_LOW(("%-15s = %s") % it->first % any2str(it->second.value()));
        }
    }
}

/// ----------------------------------------------------------------------------
/// action: split/sort

std::queue<std::string> act_split(const po::variables_map& vm)
{
    TRACE_FUNC();
    LOG_IMP(("*** Phase 1: Splitting/Sorting"));
    log_params(vm, "srt");
    TIMER("Done in %t sec CPU, %w sec real\n");

    // create a pool of blocks shared between input and output streams
    auto mem_pool =
        std::make_shared<typename BlockMemoryPolicy<Block>::BlockPool>(
            vm["srt.block_size"].as<size_t>(),
            vm["srt.blocks"].as<size_t>());

    // input stream factory
    auto istream_factory = [ &mem_pool ] (const std::string& ifn) {
        InputStreamPtr sin(new InputStream());
        sin->set_mem_pool(mem_pool);
        sin->set_input_filename(ifn);
        return sin;
    };

    // output stream factory
    auto ostream_factory = [ &mem_pool, &vm ] () {
        static int file_cnt = 0;
        std::stringstream filename;
        filename << (boost::format("%s.%02d")
                     % vm["srt.ofile"].as<std::string>() % (++file_cnt));

        OutputStreamPtr sout(new OutputStream());
        sout->set_mem_pool(mem_pool);
        sout->set_output_filename(filename.str());
        sout->set_output_blocks_per_file(0);
        return sout;
    };

    // output stream to filename conversion
    auto ostream2file = [] (const OutputStreamPtr& sout) {
        return sout->output_filenames().front();
    };

    // split and sort
    return split(vm["srt.ifile"].as<std::string>(),
                 istream_factory, ostream_factory, ostream2file);
}

/// ----------------------------------------------------------------------------
/// action: merge

void act_merge(const po::variables_map& vm, std::queue<std::string>& files)
{
    LOG_IMP(("*** Phase 2: Merging"));
    log_params(vm, "mrg");
    TIMER("Done in %t sec CPU, %w sec real\n");

    // input stream factory
    auto istream_factory = [ &vm ] (const std::string& ifn) {
        InputStreamPtr sin(new InputStream());
        sin->set_mem_pool(vm["mrg.iblock_size"].as<size_t>(),
                          vm["mrg.blocks"].as<size_t>());
        sin->set_input_filename(ifn);
        return sin;
    };

    // onput stream factory
    auto ostream_factory = [ &vm ] () {
        static int file_cnt = 0;
        std::stringstream filename;
        filename << (boost::format("%s.%02d")
                     % vm["mrg.ofile"].as<std::string>() % (++file_cnt));

        OutputStreamPtr sout(new OutputStream());
        sout->set_mem_pool(vm["mrg.oblock_size"].as<size_t>(),
                           vm["mrg.blocks"].as<size_t>());
        sout->set_output_filename(filename.str());
        sout->set_output_blocks_per_file(0);
        return sout;
    };

    // output stream to filename conversion
    auto ostream2file = [] (const OutputStreamPtr& sout) {
        return sout->output_filenames().front();
    };

    // merge the files
    merge(files, vm["mrg.tasks"].as<size_t>(), vm["mrg.nmerge"].as<size_t>(),
          istream_factory, ostream_factory, ostream2file);

    if (rename(files.front().c_str(),
               vm["mrg.rfile"].as<std::string>().c_str()) == 0) {
        LOG_IMP(("*** Result: %s") % vm["mrg.rfile"].as<std::string>());
    } else {
        LOG_ERR(("Cannot rename %s to %s")
                % files.front() % vm["mrg.rfile"].as<std::string>());
    }
}

/// ----------------------------------------------------------------------------
/// action: generate

// functor is inlined and is faster than a callback function
template <typename T>
struct Generator
{
    T operator()()
    {
        union {
            T data;
            uint8_t bytes[sizeof(T)];
        } u;
        for (auto& b : u.bytes) {
            b = rand() & 0xFF;
        }
        return u.data;
    }
};

void act_generate(const po::variables_map& vm)
{
    LOG_INF(("*** Generating random data"));
    log_params(vm, "gen");
    TIMER("Done in %t sec CPU, %w sec real\n");

    // output stream factory
    auto ostream_factory = [ &vm ] () {
        OutputStreamPtr sout(new OutputStream());
        sout->set_mem_pool(vm["gen.block_size"].as<size_t>(),
                           vm["gen.blocks"].as<size_t>());
        sout->set_output_filename(vm["gen.ofile"].as<std::string>());
        sout->set_output_blocks_per_file(0);
        return sout;
    };

    generate(vm["gen.size"].as<size_t>(),
             ostream_factory,
             Generator<typename BlockTraits<Block>::ValueType>());
}

/// ----------------------------------------------------------------------------
/// action: check

void act_check(const po::variables_map& vm)
{
    LOG_IMP(("*** Checking data"));
    log_params(vm, "chk");
    TIMER("Done in %t sec CPU, %w sec real\n");

    auto istream_factory = [ &vm ] () {
        InputStreamPtr sin(new InputStream());
        sin->set_mem_pool(vm["chk.block_size"].as<size_t>(),
                          vm["chk.blocks"].as<size_t>());
        sin->set_input_filename(vm["chk.ifile"].as<std::string>());
        return sin;
    };

    check(istream_factory);
}

/// ----------------------------------------------------------------------------
/// main

int main(int argc, char *argv[])
{
    std::ostringstream ss;
    ss << boost::format("Usage: %s [options]\n\n"
                        "General options") % argv[0];

    po::options_description desc(ss.str());
    desc.add_options()
        ("help,h",
         "Display this information")

        ("act",
         po::value<std::string>()->default_value("all"),
         "Action to perform. Possible values:\n"
         "<gen | srt | mrg | chk | all | ext>\n"
         "gen - Generates random data\n"
         "srt - Splits and sorts the input\n"
         "mrg - Merges the input\n"
         "chk - Checks if the input is sorted\n"
         "all - All of the above\n"
         "ext = srt + mrg\n")

        ("msize",
         po::value<size_t>()->default_value(1),
         "Memory size")

        ("munit",
         po::value<std::string>()->default_value("M"),
         "Memory unit: <B | K | M>")

        ("dir",
         po::value<std::string>()->default_value(DEF_TMP_DIR),
         "Directory for temporary files")

        ("log",
         po::value<int>()->default_value(4),
         "Log level: [0-6]");

    po::options_description gen_desc("Options for act = gen (generate)");
    gen_desc.add_options()
        ("gen.ofile",
         po::value<std::string>()->default_value(DEF_GEN_OUT_FILENAME),
         "Output filename")

        ("gen.fsize",
         po::value<size_t>(),
         "File size to generate, in memory units.\n"
         "By default: gen.fsize = 16 * msize")

        ("gen.blocks",
         po::value<size_t>()->default_value(2),
         "Number of blocks in memory");

    po::options_description srt_desc(
        "Options for act = srt (phase 1: split and sort)");
    srt_desc.add_options()
        ("srt.ifile",
         po::value<std::string>()->default_value("<gen.ofile>"),
         "Input filename")

        ("srt.ofile",
         po::value<std::string>()->default_value(DEF_SRT_OUT_FILENAME),
         "Output filename (prefix)")

        ("srt.blocks",
         po::value<size_t>()->default_value(2),
         "Number of blocks in memory");

    po::options_description mrg_desc("Options for act = mrg (phase 2: merge)");
    mrg_desc.add_options()
        ("mrg.ifile",
         po::value<std::string>()->default_value("<srt.ofile>"),
         "Input filename")

        ("mrg.ofile",
         po::value<std::string>()->default_value(DEF_MRG_OUT_FILENAME),
         "Output filename (prefix)")

        ("mrg.rfile",
         po::value<std::string>()->default_value("<srt.ifile>" DEF_MRG_RES_SFX),
         "Result filename")

        ("mrg.tasks",
         po::value<size_t>()->default_value(4),
         "Number of simultaneous merge tasks")

        ("mrg.nmerge",
         po::value<size_t>()->default_value(4),
         "Number of streams merged at a time")

        ("mrg.blocks",
         po::value<size_t>()->default_value(2),
         "Number of blocks per stream");

    po::options_description chk_desc("Options for act = chk (check)");
    chk_desc.add_options()
        ("chk.ifile",
         po::value<std::string>()->default_value("<mrg.rfile>"),
         "       Input filename")

        ("chk.blocks",
         po::value<size_t>()->default_value(2),
         "       Number of blocks in memory");

    //desc.add(gen_desc).add(srt_desc).add(mrg_desc).add(chk_desc);

    srt_desc.add(mrg_desc);
    gen_desc.add(srt_desc);
    desc.add(gen_desc);
    desc.add(chk_desc);

    // parse command line arguments
    po::variables_map vm;

    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        if (vm.count("help")) {
            std::cout << desc << std::endl;
            return 1;
        }
        po::notify(vm);
    } catch (std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        std::cout << desc << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown error!" << "\n";
        return 1;
    }

    severity_level lvl = IMP;
    if (vm["log"].as<int>() >= 0 &&
        vm["log"].as<int>() <= 6) {
        lvl = static_cast<severity_level>(vm["log"].as<int>());
    }

    LOG_INIT(lvl);
    TRACE_FUNC();
    srand(time(NULL));

    size_t esize = sizeof(typename BlockTraits<Block>::ValueType);
    vm.insert(std::make_pair("esize",
                             po::variable_value(esize, false)));
    log_params(vm);

    // po::variables_map does not allow to modify variable_values,
    // so cast it to std::map to be able to modify the content
    auto& mr = static_cast<std::map<std::string, po::variable_value>&>(vm);

    // set the default value for fsize
    if (!vm.count("gen.fsize")) {
        mr["gen.fsize"].value() = mr["msize"].as<size_t>() * 16;
    }

    // get memory unit coefficient
    size_t munit = 1;
    if (vm["munit"].as<std::string>() == "M") {
        munit = 1024 * 1024;
    } else if (vm["munit"].as<std::string>() == "K") {
        munit = 1024;
    } else if (vm["munit"].as<std::string>() != "B") {
        LOG_INF(("Unknown munit: %s") % mr["munit"].as<std::string>());
        std::cout << desc << std::endl;
        return 1;
    }

    // adjust msize and gen.fsize according to munit
    mr["msize"].as<size_t>() *= munit;
    mr["gen.fsize"].as<size_t>() *= munit;
    LOG_INF(("%-15s = %s bytes") % "msize" % vm["msize"].as<std::size_t>());

    if (vm["srt.ifile"].defaulted()) {
        mr["srt.ifile"].value() = mr["gen.ofile"].value();
    }
    if (vm["mrg.ifile"].defaulted()) {
        mr["mrg.ifile"].value() = mr["srt.ofile"].value();
    }
    if (vm["chk.ifile"].defaulted()) {
        mr["chk.ifile"].value() = mr["mrg.rfile"].value();
    }

    // create internal parameters based on the given configuration
    auto v0 = po::variable_value(size_t(0), false);
    vm.insert(std::make_pair("gen.size", v0));
    vm.insert(std::make_pair("gen.block_mem", v0));
    vm.insert(std::make_pair("gen.block_size", v0));
    vm.insert(std::make_pair("srt.block_mem", v0));
    vm.insert(std::make_pair("srt.block_size", v0));
    vm.insert(std::make_pair("mrg.task_mem", v0));
    vm.insert(std::make_pair("mrg.istream_mem", v0));
    vm.insert(std::make_pair("mrg.ostream_mem", v0));
    vm.insert(std::make_pair("mrg.iblock_mem", v0));
    vm.insert(std::make_pair("mrg.oblock_mem", v0));
    vm.insert(std::make_pair("mrg.iblock_size", v0));
    vm.insert(std::make_pair("mrg.oblock_size", v0));
    vm.insert(std::make_pair("chk.block_mem", v0));
    vm.insert(std::make_pair("chk.block_size", v0));

    mr["gen.size"].as<size_t>() = (mr["gen.fsize"].as<size_t>() /
                                   mr["esize"].as<size_t>());
    mr["gen.block_mem"].as<size_t>() = (mr["msize"].as<size_t>() /
                                        mr["gen.blocks"].as<size_t>());
    mr["gen.block_size"].as<size_t>() = (mr["gen.block_mem"].as<size_t>() /
                                         mr["esize"].as<size_t>());

    mr["srt.block_mem"].as<size_t>() = (mr["msize"].as<size_t>() /
                                        mr["srt.blocks"].as<size_t>());
    mr["srt.block_size"].as<size_t>() = (mr["srt.block_mem"].as<size_t>() /
                                         mr["esize"].as<size_t>());

    mr["mrg.task_mem"].as<size_t>() = mr["msize"].as<size_t>() /
                                      mr["mrg.tasks"].as<size_t>();

    mr["mrg.ostream_mem"].as<size_t>() = mr["mrg.task_mem"].as<size_t>() / 2;
    mr["mrg.istream_mem"].as<size_t>() = (mr["mrg.task_mem"].as<size_t>() -
                                          mr["mrg.ostream_mem"].as<size_t>()) /
                                         mr["mrg.nmerge"].as<size_t>();

    mr["mrg.iblock_mem"].as<size_t>()  = mr["mrg.istream_mem"].as<size_t>() /
                                         mr["mrg.blocks"].as<size_t>();
    mr["mrg.oblock_mem"].as<size_t>()  = mr["mrg.ostream_mem"].as<size_t>() /
                                         mr["mrg.blocks"].as<size_t>();
    mr["mrg.iblock_size"].as<size_t>() = mr["mrg.iblock_mem"].as<size_t>() /
                                         mr["esize"].as<size_t>();
    mr["mrg.oblock_size"].as<size_t>() = mr["mrg.oblock_mem"].as<size_t>() /
                                         mr["esize"].as<size_t>();

    mr["chk.block_mem"].as<size_t>() = (mr["msize"].as<size_t>() /
                                        mr["chk.blocks"].as<size_t>());
    mr["chk.block_size"].as<size_t>() = (mr["chk.block_mem"].as<size_t>() /
                                         mr["esize"].as<size_t>());

    uint8_t act = ACT_NONE;
    std::string action = vm["act"].as<std::string>();
    if (action == "all") {
        act = ACT_ALL;
    } else if (action == "gen") {
        act = ACT_GEN;
    } else if (action == "srt") {
        act = ACT_SRT;
    } else if (action == "mrg") {
        act = ACT_MRG;
    } else if (action == "chk") {
        act = ACT_CHK;
    } else if (action == "ext") {
        act = ACT_SRT | ACT_MRG;
    } else {
        LOG_INF(("Unknown action: %s") % action);
        std::cout << desc << std::endl;
        return 1;
    }

    std::queue<std::string> files;
    // if no split/sort phase, merge what is given in command line
    // FIX IT: files.push(vm["mrg.file_in"].as<std::string>());

    TIMER("Done in %t sec CPU, %w sec real\n");

    if (act & ACT_GEN) {
        act_generate(vm);
    }
    if (act & ACT_SRT) {
        files = act_split(vm);
    }
    if (act & ACT_MRG) {
        act_merge(vm, files);
    }
    if (act & ACT_CHK) {
        act_check(vm);
    }

    return 0;
}
