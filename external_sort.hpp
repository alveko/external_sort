#include <algorithm>
#include <iostream>
#include <sstream>
#include <memory>
#include <queue>
#include <boost/format.hpp>

#include "logging.hpp"
#include "async_funcs.hpp"
#include "stream_merge.hpp"

/// ----------------------------------------------------------------------------
/// split and sort

template <typename Block, typename OStreamPtr>
OStreamPtr sort_and_write(typename BlockTraits<Block>::BlockPtr block,
                          OStreamPtr ostream)
{
    TRACE_FUNC();

    // sort the block
    std::sort(block->begin(), block->end(),
              typename BlockTraits<Block>::Comparator());
    TRACE(("block %014p sorted") % BlockTraits<Block>::RawPtr(block));

    // write the block to the output stream
    ostream->Open();
    ostream->WriteBlock(block);
    ostream->Close();
    return ostream;
}

template <typename IStrmFactory, typename OStrmFactory, typename OStrm2File>
std::queue<std::string> split(const std::string& ifile,
                              IStrmFactory create_istream,
                              OStrmFactory create_ostream,
                              OStrm2File ostream2file)
{
    TRACE_FUNC();
    using IStreamPtr = typename std::result_of<IStrmFactory(std::string)>::type;
    using OStreamPtr = typename std::result_of<OStrmFactory()>::type;
    using Block = typename IStreamPtr::element_type::BlockType;

    AsyncFuncs<OStreamPtr> splits;
    std::queue<std::string> ofiles;

    IStreamPtr istream = create_istream(ifile);
    istream->Open();
    while (!istream->Empty()) {

        // read a block from the input stream
        auto block = istream->FrontBlock();
        istream->PopBlock();

        // asynchronously sort it and write to the output stream
        splits.Async(&sort_and_write<Block, OStreamPtr>,
                     std::move(block), std::move(create_ostream()));

        // collect the results
        while ((splits.Ready() > 0) ||
               (splits.Running() && istream->Empty())) {
            ofiles.push(ostream2file(splits.GetAny()));
        }
    }
    istream->Close();
    assert(splits.Empty());
    return ofiles;
}

/// ----------------------------------------------------------------------------
/// merge

template <typename IStrmFactory, typename OStrmFactory, typename OStrm2File>
void merge(std::queue<std::string>& files, size_t mrg_tasks, size_t mrg_nmerge,
           IStrmFactory create_istream, OStrmFactory create_ostream,
           OStrm2File ostream2file)
{
    TRACE_FUNC();
    using IStreamPtr = typename std::result_of<IStrmFactory(std::string)>::type;
    using OStreamPtr = typename std::result_of<OStrmFactory()>::type;

    AsyncFuncs<OStreamPtr> merges;

    // Merge files. Stop when one file left and no ongoing merges
    while (!(files.size() == 1 && merges.Empty())) {
        LOG_INF(("* files left to merge %d") % files.size());

        // create input streams (nmerge streams from the queue)
        std::unordered_set<IStreamPtr> sin;
        while (sin.size() < mrg_nmerge && !files.empty()) {
            sin.insert(create_istream(files.front()));
            files.pop();
        }

        // asynchronously merge and write to the output stream
        merges.Async(&merge_streams<IStreamPtr, OStreamPtr>,
                     std::move(sin), std::move(create_ostream()));

        // Wait/get results of asynchroniously running merges if:
        // 1) Too few files ready to be merged, while still running merges.
        //    In other words, more files can be merged at once than
        //    currently available. So wait for more files.
        // 2) There are completed (ready) merges; results shall be collected
        // 3) There are simple too many already ongoing merges
        while ((files.size() < mrg_nmerge && !merges.Empty()) ||
               (merges.Ready() > 0) || (merges.Running() >= mrg_tasks)) {
            files.push(ostream2file(merges.GetAny()));
        }
    }
    assert(merges.Empty());
}

/// ----------------------------------------------------------------------------
/// check

template <typename IStrmFactory>
void check(IStrmFactory create_istream)
{
    TRACE_FUNC();
    using IStreamPtr = typename std::result_of<IStrmFactory()>::type;
    using Block = typename IStreamPtr::element_type::BlockType;
    auto comp = typename BlockTraits<Block>::Comparator();

    IStreamPtr istream = create_istream();
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
        LOG_IMP(("min = %s, max = %s") % v_min % v_max);
        LOG_IMP(("first = %s, last = %s") % v_first % v_prev);
    }
    LOG_IMP(("Data checked: sorted = %s, elements = %s") % sorted % cnt);
    istream->Close();
}

/// ----------------------------------------------------------------------------
/// generate

template <typename OStrmFactory , typename Generator>
void generate(size_t gen_size, OStrmFactory create_ostream, Generator generator)
{
    TRACE_FUNC();
    auto ostream = create_ostream();
    ostream->Open();
    for (size_t i = 0; i < gen_size; i++) {
        ostream->Push(generator());
    }
    ostream->Close();
}
