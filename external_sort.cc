#include <string>
#include <queue>
#include <iostream>
#include <sstream>
#include <memory>
#include <boost/program_options.hpp>
#include <boost/format.hpp>

#include "block_types.hpp"
#include "block_input_stream.hpp"
#include "block_output_stream.hpp"
#include "in_process_out.hpp"
#include "policies.hpp"
#include "stream_merge.hpp"
#include "async_tasks.hpp"
#include "async_funcs.hpp"

#include "logging.hpp"

/// ----------------------------------------------------------------------------

namespace po = boost::program_options;

using Block = U32Block;

using InputStream = BlockInputStream<Block,
                                     BlockInFilePolicy<Block>,
                                     BlockMemoryPolicy<Block>>;

using OutputStream = BlockOutputStream<Block,
                                       BlockOutFilePolicy<Block>,
                                       BlockMemoryPolicy<Block>>;

using InputStreamPtr = std::shared_ptr<InputStream>;
using OutputStreamPtr = std::shared_ptr<OutputStream>;

#define ACT_NONE (0x00)
#define ACT_ALL  (0xFF)
#define ACT_GEN  (1 << 0)
#define ACT_SRT  (1 << 1)
#define ACT_MRG  (1 << 2)
#define ACT_CHK  (1 << 3)

#define DEF_GEN_OUT_FILENAME "./external/generated"
#define DEF_SRT_OUT_FILENAME "./external/ph1_sorted"
#define DEF_MRG_OUT_FILENAME "./external/ph2_merged"
#define DEF_MRG_RES_FILENAME "./external/generated.sorted"

/// ----------------------------------------------------------------------------
/// misc

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
/// action: generate

// functor is inlined and is faster than a callback function
template <typename T>
struct generate_data
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
        //TRACE(("data  = %s") % u.data);
        return u.data;
    }
};

template <typename T, typename Generator = generate_data<T> >
void generate_vector(std::vector<T>& v, size_t n, Generator gen = Generator())
{
    TRACE(("Generating data n = %d...") % n);
    TIMER("Done in %t sec CPU, %w sec real\n");

    std::generate(v.begin(), v.end(), gen);

    TRACE(("min = %s, max = %s")
          % *std::min_element(v.begin(), v.end())
          % *std::max_element(v.begin(), v.end()));
}

void act_generate(const po::variables_map& vm)
{
    LOG_INF(("*** Generating random data"));
    log_params(vm, "gen");
    TIMER("Done in %t sec CPU, %w sec real\n");

    // create and open the output stream
    OutputStreamPtr sout(new OutputStream());
    sout->set_mem_block_size(vm["gen.block_size"].as<size_t>());
    sout->set_mem_max_blocks(vm["gen.blocks"].as<size_t>());
    sout->set_output_filename(vm["gen.file_out"].as<std::string>());
    sout->set_output_blocks_per_file(0);
    sout->Open();

    // generate data
    generate_data<Block::value_type> generator;
    for (size_t i = 0; i < vm["gen.size"].as<size_t>(); i++) {
        sout->Push(generator());
    }

    // close the stream
    sout->Close();
}

/// ----------------------------------------------------------------------------
/// action: split/sort

std::queue<std::string> act_split(const po::variables_map& vm)
{
    LOG_IMP(("*** Phase 1: Splitting/Sorting"));
    log_params(vm, "srt");
    TIMER("Done in %t sec CPU, %w sec real\n");

    using SplitPolicies = InProcessOutPolicies<Block,
                                               BlockMemoryPolicy,
                                               BlockInFilePolicy,
                                               BlockOutFilePolicy,
                                               BlockSortPolicy>;
    using Split = InProcessOut<Block, SplitPolicies>;

    InProcessOut<Block, SplitPolicies> split;
    split.set_mem_block_size(vm["srt.block_size"].as<size_t>());
    split.set_mem_max_blocks(vm["srt.blocks"].as<size_t>());
    split.set_input_filename(vm["srt.file_in"].as<std::string>());
    split.set_output_filename(vm["srt.file_out"].as<std::string>());
    split.set_output_blocks_per_file(1);
    split.Run();
    return split.output_filenames();

    /* TBD: mutiple split tasks
    std::ifstream file_in(vm["srt.file_in"].as<std::string>(),
                          std::ios::binary | std::ios::ate);

    size_t file_size = file_in.tellg();
    size_t splits = 4;
    size_t split_task_mem = vm["msize"].as<size_t>() / splits;
    size_t split_task_block_size = split_task_mem / vm["esize"].as<size_t>();
    size_t split_task_size = (file_size / split_task_mem / splits) *
                             split_task_mem;
    LOG_INF(("file_size \t= %s") % file_size);
    LOG_INF(("split_task_mem \t= %s") % split_task_mem);
    LOG_INF(("split_task_size \t= %s") % split_task_size);

    AsyncTasks<Split> split_tasks;
    for (size_t i = 1; i <= splits; i++) {

        std::ostringstream srt_file_out;
        srt_file_out << (boost::format("%s_%02d")
                         % vm["srt.file_out"].as<std::string>() % i);

        auto split = split_tasks.NewTask();
        split->set_mem_max_blocks(1);
        split->set_mem_block_size(split_task_block_size);
        split->set_input_offset((i - 1) * split_task_size);
        split->set_input_limit((i < splits) ? split_task_size : 0);
        split->set_input_filename(vm["srt.file_in"].as<std::string>());
        split->set_output_filename(srt_file_out.str());
        split->set_output_blocks_per_file(1);
        split_tasks.AddTask(split);
    }

    std::queue<std::string> files;

    split_tasks.StartAll();
    while (!split_tasks.Empty()) {
        auto task = split_tasks.GetAny();

        //auto task_files = task->output_filenames();
        //while (!task_files.empty()) {
        //    files.push(task_files.front());
        //    task_files.pop();
        //}
    }
    return files;
    */
}

/// ----------------------------------------------------------------------------
/// action: merge

InputStreamPtr create_istream(const po::variables_map& vm,
                              const std::string& ifn)
{
    InputStreamPtr sin(new InputStream());
    sin->set_mem_block_size(vm["mrg.iblock_size"].as<size_t>());
    sin->set_mem_max_blocks(vm["mrg.blocks"].as<size_t>());
    sin->set_input_filename(ifn);
    return sin;
}

OutputStreamPtr create_ostream(const po::variables_map& vm)
{
    static int file_cnt = 0;
    std::stringstream filename;
    filename << (boost::format("%s.%02d")
                 % vm["mrg.file_out"].as<std::string>() % (++file_cnt));

    OutputStreamPtr sout(new OutputStream());
    sout->set_mem_block_size(vm["mrg.oblock_size"].as<size_t>());
    sout->set_mem_max_blocks(vm["mrg.blocks"].as<size_t>());
    sout->set_output_filename(filename.str());
    sout->set_output_blocks_per_file(0);
    return sout;
}

std::string ostream2filename(const OutputStreamPtr& sout) {
    return sout->output_filenames().front();
};

void act_merge(const po::variables_map& vm, std::queue<std::string>&& files)
{
    LOG_IMP(("*** Phase 2: Merging"));
    log_params(vm, "mrg");
    TIMER("Done in %t sec CPU, %w sec real\n");

    AsyncFuncs<OutputStreamPtr> merges;
    size_t nmerge = vm["mrg.nmerge"].as<size_t>();
    size_t tasks = vm["mrg.tasks"].as<size_t>();

    // Merge files. Stop when only one file left and no ongoing merges
    while (!(files.size() == 1 && merges.Empty())) {
        LOG_INF(("* files left to merge %d") % files.size());

        // create input streams (nmerge streams from the queue)
        std::unordered_set<InputStreamPtr> sin;
        while (sin.size() < nmerge && !files.empty()) {
            sin.insert(create_istream(vm, files.front()));
            files.pop();
        }

        // create the output stream
        auto sout = create_ostream(vm);

        // run async merge task
        merges.Async(&merge_streams<InputStreamPtr, OutputStreamPtr>,
                     std::move(sin), std::move(sout));

        // Wait/get results of asynchroniously running merges if:
        // 1) Too few files ready to be merged, while still running merges.
        //    In other words, more files can be merged at once than
        //    currently available. So wait for more files.
        // 2) There are completed (ready) merges; results shall be collected
        // 3) There are simple too many already ongoing merges
        while ((files.size() < nmerge && !merges.Empty()) ||
               (merges.Ready() > 0) || (merges.Running() >= tasks)) {
            files.push(ostream2filename(merges.GetAny()));
        }
    }

    if (rename(files.front().c_str(),
               vm["mrg.file_res"].as<std::string>().c_str()) == 0) {
        LOG_IMP(("*** Result: %s") % vm["mrg.file_res"].as<std::string>());
    } else {
        LOG_ERR(("Cannot rename %s to %s")
                % files.front() % vm["mrg.file_res"].as<std::string>());
    }
}

/// ----------------------------------------------------------------------------
/// action: check

void act_check(const po::variables_map& vm)
{
    LOG_IMP(("*** Checking data"));
    log_params(vm, "chk");
    TIMER("Done in %t sec CPU, %w sec real\n");

    InputStreamPtr sin(new InputStream());
    sin->set_mem_block_size(vm["chk.block_size"].as<size_t>());
    sin->set_mem_max_blocks(vm["chk.blocks"].as<size_t>());
    sin->set_input_filename(vm["chk.file_in"].as<std::string>());
    sin->Open();

    size_t cnt = 0, bad = 0;
    bool sorted = true;
    if (!sin->Empty()) {
        auto v_curr  = sin->Front();
        auto v_prev  = v_curr;
        auto v_first = v_prev;
        auto v_min   = v_prev;
        auto v_max   = v_prev;
        sin->Pop();
        ++cnt;

        while (!sin->Empty()) {
            v_curr = sin->Front();
            if (v_curr < v_prev && bad < 10) {
                sorted = false;
                LOG_WRN(("OUT OF ORDER: cnt = %s, prev = %s, curr = %s")
                        % cnt % v_prev % v_curr);
                bad++;
            }
            if (v_curr < v_min) {
                v_min = v_curr;
            }
            if (v_max < v_curr) {
                v_max = v_curr;
            }
            v_prev = v_curr;
            sin->Pop();
            ++cnt;
        }
        LOG_IMP(("min = %s, max = %s") % v_min % v_max);
        LOG_IMP(("first = %s, last = %s") % v_first % v_prev);
    }
    LOG_IMP(("Data checked: sorted = %s, elements = %s") % sorted % cnt);
    sin->Close();
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
         "\nDisplay this information")

        ("act",
         po::value<std::string>()->default_value("all"),
         "\nAction to perform. Possible values:"
         "\n<gen | srt | mrg | chk | all | ext>"
         "\ngen - Generates random data"
         "\nsrt - Splits and sorts the input"
         "\nmrg - Merges the input"
         "\nchk - Checks if the input is sorted"
         "\nall - All of the above"
         "\next = srt + mrg")

        ("msize",
         po::value<size_t>()->default_value(1),
         "\nMemory size")

        ("munit",
         po::value<std::string>()->default_value("M"),
         "\nMemory unit: <B | K | M>")

        ("log",
         po::value<int>()->default_value(4),
         "\nLog level: [0-6]");

    po::options_description gen_desc("Options for act = gen (generate)");
    gen_desc.add_options()
        ("gen.file_out",
         po::value<std::string>()->default_value(DEF_GEN_OUT_FILENAME),
         "Output filename")

        ("gen.fsize",
         po::value<size_t>(),
         "\nFile size to generate, in memory units.\n"
         "By default: gen.fsize = 16 * msize")

        ("gen.blocks",
         po::value<size_t>()->default_value(2),
         "\nNumber of blocks in memory");

    po::options_description srt_desc(
        "Options for act = srt (phase 1: split and sort)");
    srt_desc.add_options()
        ("srt.file_in",
         po::value<std::string>()->default_value("<gen.file_out>"),
         "\nInput filename")

        ("srt.file_out",
         po::value<std::string>()->default_value(DEF_SRT_OUT_FILENAME),
         "Output filename (prefix)")

        ("srt.blocks",
         po::value<size_t>()->default_value(2),
         "\nNumber of blocks in memory");

    po::options_description mrg_desc("Options for act = mrg (phase 2: merge)");
    mrg_desc.add_options()
        ("mrg.file_in",
         po::value<std::string>()->default_value("<srt.file_out>"),
         "\nInput filename")

        ("mrg.file_out",
         po::value<std::string>()->default_value(DEF_MRG_OUT_FILENAME),
         "Output filename (prefix)")

        ("mrg.file_res",
         po::value<std::string>()->default_value(DEF_MRG_RES_FILENAME),
         "Result filename")

        ("mrg.tasks",
         po::value<size_t>()->default_value(4),
         "\nNumber of simultaneous merge tasks")

        ("mrg.nmerge",
         po::value<size_t>()->default_value(4),
         "\nNumber of streams merged at a time")

        ("mrg.blocks",
         po::value<size_t>()->default_value(2),
         "\nNumber of blocks per stream");

    po::options_description chk_desc("Options for act = chk (check)");
    chk_desc.add_options()
        ("chk.file_in",
         po::value<std::string>()->default_value("<mrg.file_res>"),
         "\n  Input filename")

        ("chk.blocks",
         po::value<size_t>()->default_value(2),
         "\n  Number of blocks in memory");

    desc.add(gen_desc).add(srt_desc).add(mrg_desc).add(chk_desc);

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

    if (vm["srt.file_in"].defaulted()) {
        mr["srt.file_in"].value() = mr["gen.file_out"].value();
    }
    if (vm["mrg.file_in"].defaulted()) {
        mr["mrg.file_in"].value() = mr["srt.file_out"].value();
    }
    if (vm["chk.file_in"].defaulted()) {
        mr["chk.file_in"].value() = mr["mrg.file_res"].value();
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
        act_merge(vm, std::move(files));
    }
    if (act & ACT_CHK) {
        act_check(vm);
    }

    return 0;
}
