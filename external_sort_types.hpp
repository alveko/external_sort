#ifndef EXTERNAL_SORT_TYPES_HPP
#define EXTERNAL_SORT_TYPES_HPP

#include <memory>
#include <vector>
#include <unordered_set>

#include "block_types.hpp"
#include "block_input_stream.hpp"
#include "block_output_stream.hpp"
#include "block_file_read_policy.hpp"
#include "block_file_write_policy.hpp"
#include "block_memory_policy.hpp"

namespace external_sort {

/// ----------------------------------------------------------------------------
/// Parameter objects

enum MemUnit { MB, KB, B };

struct MemParams
{
    size_t  size   = 10;                // memory size
    MemUnit unit   = MB;                // memory unit
    size_t  blocks = 2;                 // number of blocks memory is divided by
};

struct ErrParams
{
    bool none = true;                   // error status
    std::ostringstream stream;          // error stream

    operator bool () const { return !none; }
    operator std::string () const { return stream.str(); }
    std::string msg() const { return stream.str(); }

};

struct SplitParams
{
    MemParams mem;                      // memory params
    ErrParams err;                      // error params
    struct {
        std::string ifile;              // input file to split
        std::string ofile;              // output file prefix (prefix of splits)
        bool rm_input = false;          // ifile should be removed when done?
    } spl;
    struct {
        std::list<std::string> ofiles;  // list of output files (splits)
    } out;
};

struct MergeParams
{
    MemParams mem;                      // memory params
    ErrParams err;                      // error params
    struct {
        size_t merges    = 4;           // number of simultaneous merges
        size_t kmerge    = 4;           // number of streams to merge at a time
        size_t stmblocks = 2;           // number of memory blocks per stream
        std::list<std::string> ifiles;  // list of input files to merge
        std::string tfile;              // prefix for temporary files
        std::string ofile;              // output file (the merge result)
        bool rm_input = true;           // ifile should be removed when done?
    } mrg;
};

struct CheckParams
{
    MemParams mem;                      // memory params
    ErrParams err;                      // error params
    struct {
        std::string ifile;              // input file to check it it's sorted
    } chk;
};

struct GenerateParams
{
    MemParams mem;                      // memory params
    ErrParams err;                      // error params
    struct {
        size_t fsize = 0;               // file size to generate (in mem.units)
        std::string ofile;              // output file
    } gen;
};

/// ----------------------------------------------------------------------------
/// Types

//! Default generator
template <typename T>
struct DefaultValueGenerator
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

//! Default value-to-string convertor
template <typename ValueType>
struct DefaultValue2Str
{
    std::string operator()(const ValueType& value)
    {
        std::ostringstream ss;
        ss << value;
        return ss.str();
    }
};

//! Default ValueType traits
template <typename ValueType>
struct ValueTraits
{
    using Comparator = std::less<ValueType>;
    using Generator = DefaultValueGenerator<ValueType>;
    using Value2Str = DefaultValue2Str<ValueType>;

    // It can be extended to support non-POD types:
    // static const size_t ValueSize = sizeof(ValueType);
    // static inline int Serialize(...);
    // static inline int Deserialize(...);
};

//! Stream set
template <typename T>
using StreamSet = std::unordered_set<T>;

//! All types in one place
template <typename ValueType>
struct Types
{
    // Value trait shortcuts
    using Comparator = typename ValueTraits<ValueType>::Comparator;

    // Block Types
    using Block = block::VectorBlock<ValueType>;
    using BlockPtr = typename block::BlockTraits<Block>::BlockPtr;
    using BlockPool = typename block::BlockMemoryPolicy<Block>::BlockPool;
    using BlockTraits = block::BlockTraits<Block>;

    // Stream Types
    using IStream = block::BlockInputStream<Block,
                                            block::BlockFileReadPolicy<Block>,
                                            block::BlockMemoryPolicy<Block>>;

    using OStream = block::BlockOutputStream<Block,
                                             block::BlockFileWritePolicy<Block>,
                                             block::BlockMemoryPolicy<Block>>;

    using IStreamPtr = std::shared_ptr<IStream>;
    using OStreamPtr = std::shared_ptr<OStream>;
};

} // namespace external_sort

#endif
