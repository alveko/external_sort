#ifndef BLOCK_SORT_HPP
#define BLOCK_SORT_HPP

#include <algorithm>

#include "logging.hpp"
#include "block_types.hpp"

template <typename Block>
class BlockSortPolicy {
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;

    void Process(BlockPtr block) {
        std::sort(block->begin(), block->end());
        TRACE(("block %014p sorted")
              % BlockTraits<Block>::RawPtr(block));
    }
};

#endif
