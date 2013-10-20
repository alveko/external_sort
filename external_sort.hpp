#include <algorithm>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <memory>
#include <list>

#include "external_sort_nolog.hpp"
#include "external_sort_types.hpp"
#include "external_sort_merge.hpp"
#include "async_funcs.hpp"

namespace external_sort {

const char* DEF_SPL_TMP_SFX = "split";
const char* DEF_MRG_TMP_SFX = "merge";

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
    ostream->WriteBlock(block);
    return ostream;
}

/// ----------------------------------------------------------------------------
/// main external sorting functions

//! External Split
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

    if (params.spl.ofile.empty()) {
        // if no output prefix given, use input filename as a prefix
        params.spl.ofile = params.spl.ifile;
    }

    while (!istream->Empty()) {
        // read a block from the input stream
        auto block = istream->FrontBlock();
        istream->PopBlock();

        // create an output stream
        auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
        ostream->set_mem_pool(mem_pool);
        ostream->set_output_filename(
            make_tmp_filename(params.spl.ofile, DEF_SPL_TMP_SFX, ++file_cnt));
        ostream->Open();

        // asynchronously sort the block and write it to the output stream
        splits.Async(&sort_and_write<ValueType>,
                     std::move(block), std::move(ostream));

        // collect the results
        while ((splits.Ready() > 0) || (splits.Running() && istream->Empty())) {
            // wait for any split and get its output filename
            auto ostream_ready = splits.GetAny();
            if (ostream_ready) {
                ostream_ready->Close();
                params.out.ofiles.push_back(ostream_ready->output_filename());
            }
        }
    }
    istream->Close();
}

//! External Merge
template <typename ValueType>
void merge(MergeParams& params)
{
    TRACE_FUNC();
    size_t file_cnt = 0;

    aux::AsyncFuncs<typename Types<ValueType>::OStreamPtr> merges;

    size_t mem_merge = memsize_in_bytes(params.mem.size, params.mem.unit) /
                       params.mrg.merges;
    size_t mem_ostream = mem_merge / 2;
    size_t mem_istream = mem_merge - mem_ostream;

    // Merge files while something to merge or there are ongoing merges
    auto files = params.mrg.ifiles;
    while (files.size() > 1 || !merges.Empty()) {
        LOG_INF(("* files left to merge %d") % files.size());

        // create a set of input streams with next kmerge files from the queue
        std::unordered_set<typename Types<ValueType>::IStreamPtr> istreams;
        while (istreams.size() < params.mrg.kmerge && !files.empty()) {
            // create input stream
            auto is = std::make_shared<typename Types<ValueType>::IStream>();
            is->set_mem_pool(mem_istream, params.mrg.stmblocks);
            is->set_input_filename(files.front());
            is->set_input_rm_file(params.mrg.rm_input);
            // add to the set
            istreams.insert(is);
            files.pop_front();
        }

        // create an output stream
        auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
        ostream->set_mem_pool(mem_ostream, params.mrg.stmblocks);
        ostream->set_output_filename(
            make_tmp_filename(params.mrg.tfile, DEF_MRG_TMP_SFX, ++file_cnt));

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
        while ((files.size() < params.mrg.kmerge && !merges.Empty()) ||
               (merges.Ready() > 0) || (merges.Running() >= params.mrg.merges)) {
            auto ostream_ready = merges.GetAny();
            if (ostream_ready) {
                files.push_back(ostream_ready->output_filename());
            }
        }
    }

    if (files.size()) {
        if (rename(files.front().c_str(), params.mrg.ofile.c_str()) == 0) {
            LOG_IMP(("Output file: %s") % params.mrg.ofile);
        } else {
            params.err.none = false;
            params.err.stream << "Cannot rename " << files.front()
                              << " to " << params.mrg.ofile;
        }
    } else {
        params.err.none = false;
        params.err.stream << "Merge failed. No input";
    }
}

//! External Sort (= Split + Merge)
template <typename ValueType>
void sort(SplitParams& sp, MergeParams& mp)
{
    split<ValueType>(sp);

    if (sp.err.none) {
        mp.mrg.ifiles = sp.out.ofiles;
        merge<ValueType>(mp);
    }
}

//! External Check
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
    if (!istream->Empty()) {
        auto vcurr  = istream->Front();
        auto vprev  = vcurr;
        auto vfirst = vprev;
        auto vmin   = vfirst;
        auto vmax   = vfirst;
        istream->Pop();
        ++cnt;

        while (!istream->Empty()) {
            vcurr = istream->Front();
            if (comp(vcurr, vprev)) {
                if (bad < 10) {
                    params.err.stream << "Out of order! cnt = " << cnt
                                      << " prev = " << vprev
                                      << " curr = " << vcurr << "\n";
                }
                bad++;
            }
            if (comp(vcurr, vmin)) {
                vmin = vcurr;
            }
            if (comp(vmax, vcurr)) {
                vmax = vcurr;
            }
            vprev = vcurr;
            istream->Pop();
            ++cnt;
        }
        if (bad) {
            params.err.none = false;
            params.err.stream << "Total elements out of order: " << bad << "\n";
        }
        params.err.stream << "\tmin = " << vmin << ", max = " << vmax << "\n";
        params.err.stream << "\tfirst = " << vfirst << ", last = " << vprev
                          << "\n";
    }
    params.err.stream << "\tsorted = " << ((bad) ? "false" : "true")
                      << ", elems = " << cnt << ", bad = " << bad;
    istream->Close();
    return bad == 0;
}

//! External Generate
template <typename ValueType>
void generate(const GenerateParams& params)
{
    TRACE_FUNC();

    auto generator = typename Types<ValueType>::Generator();
    size_t gen_elements = memsize_in_bytes(params.gen.fsize, params.mem.unit) /
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
