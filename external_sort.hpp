#include <algorithm>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <memory>
#include <list>

#include "async_funcs.hpp"
#include "external_sort_types.hpp"
#include "external_sort_merge.hpp"

namespace external_sort {

const char* DEF_SRT_TMP_SFX = "split";
const char* DEF_MRG_TMP_SFX = "merge";

enum MemUnit { MB, KB, B };

/// ----------------------------------------------------------------------------
/// Parameter objects

struct MemoryParams
{
    size_t  size   = 10;
    MemUnit unit   = MB;
    size_t  blocks = 2;
};

struct SplitParams
{
    MemoryParams mem;
    struct {
        std::string ifile;
        std::string oprefix;
        bool rm_input = false;
    } spl;
    struct {
        std::list<std::string> ofiles;
    } out;
};

struct MergeParams
{
    MemoryParams mem;
    struct {
        size_t merges    = 4;
        size_t nmerge    = 4;
        size_t stmblocks = 2;
        std::list<std::string> ifiles;
        std::string ofile;
        bool rm_input = true;
    } mrg;
};

struct CheckParams
{
    MemoryParams mem;
    struct {
        std::string ifile;
    } chk;
};

struct GenerateParams
{
    MemoryParams mem;
    struct {
        size_t size = 0;
        std::string ofile;
    } gen;
};

/// ----------------------------------------------------------------------------
/// auxiliary functions

template <typename SizeType>
SizeType memsize_in_bytes(const SizeType& memsize, const MemUnit& u)
{
    if (u == KB) {
        return memsize << 10;
    }
    if (u == MB) {
        return memsize << 20;
    }
    return memsize;
}

template <typename IndexType>
std::string make_tmp_filename(const std::string& prefix,
                              const std::string& suffix,
                              const IndexType& index)
{
    std::ostringstream filename;
    filename << prefix << "." << suffix << "."
             << std::setfill ('0') << std::setw(3) << index;
    return filename.str();
}

template <typename ValueType>
typename Types<ValueType>::OStreamPtr
sort_and_write(typename Types<ValueType>::BlockPtr block,
               typename Types<ValueType>::OStreamPtr ostream)
{
    // sort the block
    std::sort(block->begin(), block->end(),
              typename Types<ValueType>::Comparator());
    TRACE(("block %014p sorted") %
          Types<ValueType>::BlockTraits::RawPtr(block));

    // write the block to the output stream
    ostream->Open();
    ostream->WriteBlock(block);
    ostream->Close();
    return ostream;
}

/// ----------------------------------------------------------------------------
/// main external sorting functions

template <typename ValueType>
void split(SplitParams& params)
{
    TRACE_FUNC();
    size_t file_cnt = 0;

    aux::AsyncFuncs<typename Types<ValueType>::OStreamPtr> splits;

    // create memory pool to be shared between input and output streams
    auto mem_pool = std::make_shared<typename Types<ValueType>::BlockPool>(
        memsize_in_bytes(params.mem.size, params.mem.unit), params.mem.blocks);

    // create the input stream
    auto istream = std::make_shared<typename Types<ValueType>::IStream>();
    istream->set_mem_pool(mem_pool);
    istream->set_input_filename(params.spl.ifile);
    istream->set_input_rm_file(params.spl.rm_input);
    istream->Open();

    while (!istream->Empty()) {
        // read a block from the input stream
        auto block = istream->FrontBlock();
        istream->PopBlock();

        // create an output stream
        auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
        ostream->set_mem_pool(mem_pool);
        ostream->set_output_filename(
            make_tmp_filename(params.spl.oprefix, DEF_SRT_TMP_SFX, ++file_cnt));

        // asynchronously sort the block and write it to the output stream
        splits.Async(&sort_and_write<ValueType>,
                     std::move(block), std::move(ostream));

        // collect the results
        while ((splits.Ready() > 0) || (splits.Running() && istream->Empty())) {
            // wait for any split and get its output filename
            auto ostream_ready = splits.GetAny();
            params.out.ofiles.push_back(ostream_ready->output_filename());
        }
    }
    istream->Close();
}

template <typename ValueType>
void merge(MergeParams& params)
{
    TRACE_FUNC();
    size_t file_cnt = 0;

    aux::AsyncFuncs<typename Types<ValueType>::OStreamPtr> merges;

    // Merge files. Stop when one file left and no ongoing merges
    auto files = params.mrg.ifiles;
    while (!(files.size() == 1 && merges.Empty())) {
        LOG_INF(("* files left to merge %d") % files.size());

        // create a set of input streams with next nmerge files from the queue
        std::unordered_set<typename Types<ValueType>::IStreamPtr> istreams;
        while (istreams.size() < params.mrg.nmerge && !files.empty()) {

            // create input stream
            auto is = std::make_shared<typename Types<ValueType>::IStream>();
            is->set_mem_pool(memsize_in_bytes(params.mem.size, params.mem.unit),
                             params.mrg.stmblocks);
            is->set_input_filename(files.front());
            is->set_input_rm_file(params.mrg.rm_input);

            // add to the set
            istreams.insert(is);
            files.pop_front();
        }

        // create an output stream
        auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
        ostream->set_mem_pool(memsize_in_bytes(params.mem.size,
                                               params.mem.unit),
                              params.mrg.stmblocks);
        ostream->set_output_filename(
            make_tmp_filename(params.mrg.ofile, DEF_MRG_TMP_SFX, ++file_cnt));

        // asynchronously merge and write to the output stream
        merges.Async(&merge_streams<typename Types<ValueType>::IStreamPtr,
                                    typename Types<ValueType>::OStreamPtr>,
                     std::move(istreams), std::move(ostream));

        // Wait/get results of asynchroniously running merges if:
        // 1) Too few files ready to be merged, while still running merges.
        //    In other words, more files can be merged at once than
        //    currently available. So wait for more files.
        // 2) There are completed (ready) merges; results shall be collected
        // 3) There are simple too many already ongoing merges
        while ((files.size() < params.mrg.nmerge && !merges.Empty()) ||
               (merges.Ready() > 0) || (merges.Running() >= params.mrg.merges)) {
            auto ostream_ready = merges.GetAny();
            files.push_back(ostream_ready->output_filename());
        }
    }

    if (rename(files.front().c_str(), params.mrg.ofile.c_str()) == 0) {
        LOG_IMP(("Output file: %s") % params.mrg.ofile);
    } else {
        LOG_ERR(("Cannot rename %s to %s") % files.front() % params.mrg.ofile);
    }
}

template <typename ValueType>
bool check(CheckParams& params)
{
    TRACE_FUNC();
    auto comp = typename Types<ValueType>::Comparator();

    auto istream = std::make_shared<typename Types<ValueType>::IStream>();
    istream->set_mem_pool(memsize_in_bytes(params.mem.size, params.mem.unit),
                          params.mem.blocks);
    istream->set_input_filename(params.chk.ifile);
    istream->Open();

    size_t cnt = 0, bad = 0;
    bool sorted = true;
    if (!istream->Empty()) {
        auto v_curr  = istream->Front();
        auto v_prev  = v_curr;
        auto v_first = v_prev;
        auto v_min   = v_prev;
        auto v_max   = v_prev;
        istream->Pop();
        ++cnt;

        while (!istream->Empty()) {
            v_curr = istream->Front();
            if (comp(v_curr, v_prev) && bad < 10) {
                sorted = false;
                LOG_WRN(("OUT OF ORDER: cnt = %s, prev = %s, curr = %s")
                        % cnt % v_prev % v_curr);
                bad++;
            }
            if (comp(v_curr, v_min)) {
                v_min = v_curr;
            }
            if (comp(v_max, v_curr)) {
                v_max = v_curr;
            }
            v_prev = v_curr;
            istream->Pop();
            ++cnt;
        }
        LOG_IMP(("\tmin = %s, max = %s") % v_min % v_max);
        LOG_IMP(("\tfirst = %s, last = %s") % v_first % v_prev);
    }
    LOG_IMP(("\tsorted = %s, elements = %s") % sorted % cnt);
    istream->Close();
    return sorted;
}

template <typename ValueType>
void generate(const GenerateParams& params)
{
    TRACE_FUNC();

    auto generator = typename Types<ValueType>::Generator();
    size_t gen_elements = memsize_in_bytes(params.gen.size, params.mem.unit) /
                          sizeof(ValueType);

    auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
    ostream->set_mem_pool(memsize_in_bytes(params.mem.size, params.mem.unit),
                          params.mem.blocks);
    ostream->set_output_filename(params.gen.ofile);
    ostream->Open();

    for (size_t i = 0; i < gen_elements; i++) {
        ostream->Push(generator());
    }

    ostream->Close();
}

} // namespace external_sort
