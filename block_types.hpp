#ifndef BLOCK_TYPES_HPP
#define BLOCK_TYPES_HPP

#include <vector>
#include <memory>

namespace external_sort {
namespace block {

template <typename T>
using VectorBlock = std::vector<T>;

template <typename BlockType>
struct BlockTraits
{
    using Block = BlockType;

    using BlockPtr = Block*;
    inline static void* RawPtr(BlockPtr block) { return block; };
    inline static void DeletePtr(BlockPtr block) { delete block; };

    // Alternatively BlockPtr can be a shared pointer (but it's slower):
    // using BlockPtr = std::shared_ptr<Block>;
    // inline static void* RawPtr(BlockPtr block) { return block.get(); };
    // inline static void DeletePtr(BlockPtr block) { };

    using Container = Block;
    using Iterator  = typename Container::iterator;
    using ValueType = typename Container::value_type;
};

} // namespace block
} // namespace external_sort

#endif
