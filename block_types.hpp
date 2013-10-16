#ifndef BLOCK_TYPES_HPP
#define BLOCK_TYPES_HPP

#include <memory>
#include <vector>

template <typename T>
using VectorBlock = std::vector<T>;

using U8Block  = VectorBlock<uint8_t>;
using U16Block = VectorBlock<uint16_t>;
using U32Block = VectorBlock<uint32_t>;

template <typename Block>
struct BlockTraits {};

template <>
struct BlockTraits<U32Block>
{
    using Block = U32Block;

    using BlockPtr = Block*;
    inline static void* RawPtr(BlockPtr block) { return block; };
    inline static void DeletePtr(BlockPtr block) { delete block; };

    // Alternatively BlockPtr can be a shared pointer (but it's slower):
    // using BlockPtr = std::shared_ptr<Block>;
    // inline static void* RawPtr(BlockPtr block) { return block.get(); };
    // inline static void DeletePtr(BlockPtr block) { };

    using Container = Block;
    using ValueType = typename Container::value_type;
    using Iterator  = typename Container::iterator;

    using Comparator = std::less<ValueType>;
};

#endif
