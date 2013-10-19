#include <iostream>
#include <sstream>
#include <memory>
#include <list>
#include <boost/program_options.hpp>
#include <boost/format.hpp>

#include "logging.hpp"

#include "external_sort.hpp"

/// ----------------------------------------------------------------------------
/// type definitions

#define ACT_NONE (0x00)
#define ACT_ALL  (0xFF)
#define ACT_GEN  (1 << 0)
#define ACT_SRT  (1 << 1)
#define ACT_MRG  (1 << 2)
#define ACT_CHK  (1 << 3)

#define DEF_MRG_RES_SFX  ".sorted"
#define DEF_GEN_OFILE    "generated"
#define DEF_MRG_OFILE    DEF_GEN_OFILE DEF_MRG_RES_SFX

namespace po = boost::program_options;

using ValueType = uint32_t;



/// ----------------------------------------------------------------------------
/// auxiliary functions

std::string any2str(const boost::any x)
{
    std::ostringstream ss;
    if (x.type() == typeid(std::string)) {
        ss << boost::any_cast<std::string>(x);
    } else if (x.type() == typeid(size_t)) {
        ss << boost::any_cast<size_t>(x);
    } else if (x.type() == typeid(int)) {
        ss << boost::any_cast<int>(x);
    } else if (x.type() == typeid(bool)) {
        ss << (boost::any_cast<bool>(x) ? "true" : "false" );
    } else {
        ss << "...";
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

std::string basename(const std::string& pathname)
{
    return {std::find_if(pathname.rbegin(), pathname.rend(),
                         [](char c) { return c == '/'; }).base(),
            pathname.end()};
}

std::string replace_dirname(const std::string& pathname,
                            const std::string& dirname)
{
    if (dirname.size()) {
        return dirname + '/' + basename(pathname);
    }
    return pathname;
}

/// ----------------------------------------------------------------------------
/// action: split/sort

std::list<std::string> act_split(const po::variables_map& vm)
{
    LOG_IMP(("\n*** Phase 1: Splitting and Sorting"));
    LOG_IMP(("Input file: %s") % vm["srt.ifile"].as<std::string>());
    log_params(vm, "srt");
    TIMER("Done in %t sec CPU, %w sec real\n");

    external_sort::SplitParams params;
    params.mem.size    = vm["msize"].as<size_t>();
    params.mem.unit    = vm["memunit"].as<external_sort::MemUnit>();
    params.mem.blocks  = vm["srt.blocks"].as<size_t>();
    params.spl.ifile   = vm["srt.ifile"].as<std::string>();
    params.spl.oprefix = vm["tfile"].as<std::string>();

    external_sort::split<ValueType>(params);
    return params.out.ofiles;
}

/// ----------------------------------------------------------------------------
/// action: merge

void act_merge(const po::variables_map& vm, std::list<std::string>& files)
{
    LOG_IMP(("\n*** Phase 2: Merging"));
    log_params(vm, "mrg");
    TIMER("Done in %t sec CPU, %w sec real\n");

    external_sort::MergeParams params;
    params.mem.size      = vm["msize"].as<size_t>();
    params.mem.unit      = vm["memunit"].as<external_sort::MemUnit>();
    params.mrg.merges    = vm["mrg.tasks"].as<size_t>();
    params.mrg.nmerge    = vm["mrg.nmerge"].as<size_t>();
    params.mrg.stmblocks = vm["mrg.blocks"].as<size_t>();
    params.mrg.ifiles    = files;
    params.mrg.ofile     = vm["mrg.ofile"].as<std::string>();
    params.mrg.rm_input  = !vm["no_rm"].as<bool>();

    external_sort::merge<ValueType>(params);
}

/// ----------------------------------------------------------------------------
/// action: generate

void act_generate(const po::variables_map& vm)
{
    LOG_IMP(("\n*** Generating random data"));
    LOG_IMP(("Output file: %s") % vm["gen.ofile"].as<std::string>());
    log_params(vm, "gen");
    TIMER("Done in %t sec CPU, %w sec real\n");

    external_sort::GenerateParams params;
    params.mem.size   = vm["msize"].as<size_t>();
    params.mem.unit   = vm["memunit"].as<external_sort::MemUnit>();
    params.mem.blocks = vm["gen.blocks"].as<size_t>();
    params.gen.ofile  = vm["gen.ofile"].as<std::string>();
    params.gen.size   = vm["gen.fsize"].as<size_t>();

    external_sort::generate<ValueType>(params);
}

/// ----------------------------------------------------------------------------
/// action: check

void act_check(const po::variables_map& vm)
{
    LOG_IMP(("\n*** Checking data"));
    LOG_IMP(("Input file: %s") % vm["chk.ifile"].as<std::string>());
    log_params(vm, "chk");
    TIMER("Done in %t sec CPU, %w sec real\n");

    external_sort::CheckParams params;
    params.mem.size   = vm["msize"].as<size_t>();
    params.mem.unit   = vm["memunit"].as<external_sort::MemUnit>();
    params.mem.blocks = vm["chk.blocks"].as<size_t>();
    params.chk.ifile     = vm["chk.ifile"].as<std::string>();

    external_sort::check<ValueType>(params);
}

/// ----------------------------------------------------------------------------
/// main

int main(int argc, char *argv[])
{
    std::ostringstream ss;
    ss << boost::format("\nUsage: %s [options]\n\n"
                        "General options") % basename(argv[0]);

    po::options_description desc(ss.str());
    desc.add_options()
        ("help,h",
         "Display this information\n")

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

        ("log",
         po::value<int>()->default_value(4),
         "Log level: [0-6]")

        ("no_rm",
         po::value<bool>()->
             zero_tokens()->default_value(false)->implicit_value(true),
         "Do not remove temporary files")

        ("tmpdir",
         po::value<std::string>()->default_value("", "<same as input files>"),
         "Directory for temporary files");

    po::options_description gen_desc("Options for act=gen (generate)");
    gen_desc.add_options()
        ("gen.ofile",
         po::value<std::string>()->default_value(DEF_GEN_OFILE),
         "Output file")

        ("gen.fsize",
         po::value<size_t>(),
         "File size to generate, in memory units.\n"
         "By default: gen.fsize = 16 * msize")

        ("gen.blocks",
         po::value<size_t>()->default_value(2),
         "Number of blocks in memory");

    po::options_description srt_desc(
        "Options for act=srt (phase 1: split and sort)");
    srt_desc.add_options()
        ("srt.ifile",
         po::value<std::string>()->default_value("<gen.ofile>"),
         "Input file")

        ("srt.blocks",
         po::value<size_t>()->default_value(2),
         "Number of blocks in memory");

    po::options_description mrg_desc("Options for act=mrg (phase 2: merge)");
    mrg_desc.add_options()
        ("mrg.ifiles",
         po::value<std::vector<std::string>>()->default_value(
             std::vector<std::string>(), "<sorted splits>")->multitoken(),
         "Input files to be merged into one\n"
         "(required and only relevant if act=mrg, otherwise the list of files, "
         "i.e. sorted splits, is passed over from phase 1)")

        ("mrg.ofile",
         po::value<std::string>()->default_value("<srt.ifile>" DEF_MRG_RES_SFX),
         "Output file (required if act=mrg)")

        ("mrg.tasks",
         po::value<size_t>()->default_value(4),
         "Number of simultaneous merge tasks")

        ("mrg.nmerge",
         po::value<size_t>()->default_value(4),
         "Number of streams merged at a time")

        ("mrg.blocks",
         po::value<size_t>()->default_value(2),
         "Number of memory blocks per stream");

    po::options_description chk_desc("Options for act=chk (check)");
    chk_desc.add_options()
        ("chk.ifile",
         po::value<std::string>()->default_value("<mrg.ofile>"),
         "       Input file")

        ("chk.blocks",
         po::value<size_t>()->default_value(2),
         "       Number of blocks in memory");

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
    log_params(vm);

    // po::variables_map does not allow to modify variable_values,
    // so cast it to std::map to be able to modify the content
    auto& mr = static_cast<std::map<std::string, po::variable_value>&>(vm);

    // set the default value for fsize
    if (!vm.count("gen.fsize")) {
        mr["gen.fsize"].value() = mr["msize"].as<size_t>() * 16;
    }

    // get memory unit coefficient
    if (vm["munit"].as<std::string>() == "M") {
        vm.insert(std::make_pair("memunit",
                                 po::variable_value(external_sort::MB, false)));
    } else if (vm["munit"].as<std::string>() == "K") {
        vm.insert(std::make_pair("memunit",
                                 po::variable_value(external_sort::KB, false)));
    } else if (vm["munit"].as<std::string>() == "B") {
        vm.insert(std::make_pair("memunit",
                                 po::variable_value(external_sort::B, false)));
    } else {
        LOG_INF(("Unknown munit: %s") % vm["munit"].as<std::string>());
        std::cout << desc << std::endl;
        return 1;
    }

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

    std::list<std::string> files;

    // adjust filename variables according to the provided options
    if (vm["srt.ifile"].defaulted()) {
        mr["srt.ifile"].value() = mr["gen.ofile"].value();
    }
    if (!(act & ACT_SRT) && (act & ACT_MRG)) {
        // no split/sort phase, only the merge phase
        // check for mandatory parameters
        for (auto param : {"mrg.ifiles", "mrg.ofile"}){
            if (vm[param].defaulted()) {
                LOG_ERR(("Missing mandatory parameter: %s\n"
                         "For more information, run: %s --help")
                        % param % argv[0]);
                return 1;
            }
        }
        // copy the provided files into the queue
        for (const auto& x :
                     vm["mrg.ifiles"].as<std::vector<std::string>>()) {
            files.push_back(x);
        }
    }
    if (vm["mrg.ofile"].defaulted()) {
        mr["mrg.ofile"].value() = mr["srt.ifile"].value();
        mr["mrg.ofile"].as<std::string>() += DEF_MRG_RES_SFX;
    }
    if (vm["chk.ifile"].defaulted()) {
        mr["chk.ifile"].value() = mr["mrg.ofile"].value();
    }

    // make a prefix for temporary files
    vm.insert(std::make_pair("tfile", po::variable_value(std::string(), false)));
    mr["tfile"].as<std::string>() = replace_dirname(
        vm["mrg.ofile"].defaulted() ? vm["srt.ifile"].as<std::string>()
                                    : vm["mrg.ofile"].as<std::string>(),
        vm["tmpdir"].as<std::string>());

    TIMER("\nOverall %t sec CPU, %w sec real\n");

    // action!
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
