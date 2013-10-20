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
    size_t  size   = 10;
    MemUnit unit   = MB;
    size_t  blocks = 2;
};

struct ErrParams
{
    bool none = true;
    std::ostringstream stream;

    operator bool () const { return !none; }
    operator std::string () const { return stream.str(); }
    std::string msg() const { return stream.str(); }

};

struct SplitParams
{
    MemParams mem;
    ErrParams err;
    struct {
        std::string ifile;
        std::string ofile;
        bool rm_input = false;
    } spl;
    struct {
        std::list<std::string> ofiles;
    } out;
};

struct MergeParams
{
    MemParams mem;
    ErrParams err;
    struct {
        size_t merges    = 4;
        size_t nmerge    = 4;
        size_t stmblocks = 2;
        std::list<std::string> ifiles;
        std::string tfile;
        std::string ofile;
        bool rm_input = true;
    } mrg;
};

struct CheckParams
{
    MemParams mem;
    ErrParams err;
    struct {
        std::string ifile;
    } chk;
};

struct GenerateParams
{
    MemParams mem;
    ErrParams err;
    struct {
        size_t fsize = 0;
        std::string ofile;
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
    using Generator = typename ValueTraits<ValueType>::Generator;

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
