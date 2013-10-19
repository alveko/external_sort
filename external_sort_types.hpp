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

// Default generator
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

// Default value-to-string convertor
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

// Default ValueType traits
template <typename ValueType>
struct ValueTraits
{
    using Comparator = std::less<ValueType>;
    using Generator = DefaultValueGenerator<ValueType>;
    using Value2Str = DefaultValue2Str<ValueType>;

    static const size_t ValueSize = sizeof(ValueType);
};

template <typename T>
using StreamSet = std::unordered_set<T>;

// All types in one plase
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
