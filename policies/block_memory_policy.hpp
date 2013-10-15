#ifndef BLOCK_MEMORY_HPP
#define BLOCK_MEMORY_HPP

#include <condition_variable>
#include <mutex>
#include <atomic>
#include <stack>

#include "logging.hpp"
#include "block_types.hpp"

#include <boost/lockfree/stack.hpp>

template <typename Block>
class BlockMemoryPolicy
{
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;

    BlockMemoryPolicy();
    ~BlockMemoryPolicy();

    BlockPtr Allocate();
    void Free(BlockPtr block);

    size_t Allocated() const;

    size_t mem_max_blocks() const { return mem_max_blocks_; }
    void set_mem_max_blocks(size_t n);

    size_t mem_block_size() const { return mem_block_size_; }
    void set_mem_block_size(size_t s) { mem_block_size_ = s; }

  private:
    TRACEX_NAME("BlockMemoryPolicy");

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    size_t blocks_allocated_ = {0};
    size_t blocks_cnt_       = {0};
    size_t mem_block_size_   = {4};
    size_t mem_max_blocks_   = {4};
    std::stack<BlockPtr> pool_;
};

template <typename Block>
BlockMemoryPolicy<Block>::BlockMemoryPolicy()
{
}

template <typename Block>
BlockMemoryPolicy<Block>::~BlockMemoryPolicy()
{
    while (!pool_.empty()) {
        BlockTraits<Block>::DeletePtr(pool_.top());
        pool_.pop();
    }
}

template <typename Block>
void BlockMemoryPolicy<Block>::set_mem_max_blocks(size_t n)
{
    mem_max_blocks_ = n;

    while (pool_.size() < n) {
        BlockPtr block(new Block);
        block->reserve(mem_block_size_);
        pool_.push(block);
    }
}

template <typename Block>
size_t BlockMemoryPolicy<Block>::Allocated() const
{
    std::unique_lock<std::mutex> lck(mtx_);
    return blocks_allocated_;
}

template <typename Block>
auto BlockMemoryPolicy<Block>::Allocate() -> BlockPtr
{
    std::unique_lock<std::mutex> lck(mtx_);
    blocks_cnt_++;
    TRACEX(("allocating block (%d)...") % blocks_cnt_);

    // get (wait if necesssary) a block from the pre-allocated pool
    while (pool_.empty()) {
        cv_.wait(lck);
    }
    BlockPtr block = pool_.top();
    pool_.pop();

    blocks_allocated_++;
    TRACEX(("block %014p allocated!  (%s/%s), (%d), cap = %s")
           % BlockTraits<Block>::RawPtr(block) % blocks_allocated_
           % mem_max_blocks_ % blocks_cnt_ % block->capacity());
    return block;
}

template <typename Block>
void BlockMemoryPolicy<Block>::Free(BlockPtr block)
{
    std::unique_lock<std::mutex> lck(mtx_);
    blocks_allocated_--;

    // return the block back to the pool
    block->resize(0);
    pool_.push(block);

    TRACEX(("block %014p deallocated (%s/%s)")
           % BlockTraits<Block>::RawPtr(block)
           % blocks_allocated_ % mem_max_blocks_);
    cv_.notify_one();
}

////////////////////////////
/* TODO: lockfree alternative
template <typename Block>
class BlockMemoryPolicy2
{
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;

    BlockPtr Allocate();
    void Free(BlockPtr block);

    size_t Allocated() const { return blocks_allocated_; }

    size_t mem_max_blocks() const { return mem_max_blocks_; }
    void set_mem_max_blocks(size_t n);// { mem_max_blocks_ = n; }

    size_t mem_block_size() const { return mem_block_size_; }
    void set_mem_block_size(size_t s) { mem_block_size_ = s; }

    BlockMemoryPolicy2();
    ~BlockMemoryPolicy2();

  private:
    TRACEX_NAME("BlockMemoryPolicy");

    std::mutex mtx_;
    std::condition_variable cv_;
    std::atomic<size_t> blocks_allocated_= {0};
    std::atomic<size_t> blocks_cnt_ = {0};
    std::atomic<size_t> mem_max_blocks_ = {4};
    std::atomic<size_t> mem_block_size_ = {4};

    boost::lockfree::stack<BlockPtr> blocks_;
};

template <typename Block>
BlockMemoryPolicy2<Block>::BlockMemoryPolicy2()
    : blocks_(16)
{
}

template <typename Block>
BlockMemoryPolicy2<Block>::~BlockMemoryPolicy2()
{
    BlockPtr block = nullptr;
    while (blocks_.pop(block)) {
        BlockTraits<Block>::DeletePtr(block);
    }
}

template <typename Block>
void BlockMemoryPolicy2<Block>::set_mem_max_blocks(size_t n)
{
    mem_max_blocks_ = n;

    //blocks_.reserve(n);
    for (size_t i = 0; i < n; i++) {
        BlockPtr block(new Block);
        block->reserve(mem_block_size_);
        blocks_.push(block);
    }
}

template <typename Block>
auto BlockMemoryPolicy2<Block>::Allocate() -> BlockPtr
{
    blocks_cnt_++;
    TRACEX(("allocating block (%d)...") % blocks_cnt_);

    BlockPtr block = nullptr;
    while (!blocks_.pop(block)) {
        std::unique_lock<std::mutex> lck(mtx_);
        cv_.wait(lck);
    }

    blocks_allocated_++;
    TRACEX(("block %014p allocated!  (%s/%s), (%d), cap = %s")
           % BlockTraits<Block>::RawPtr(block) % blocks_allocated_
           % mem_max_blocks_ % blocks_cnt_ % block->capacity());
    return block;
}

template <typename Block>
void BlockMemoryPolicy2<Block>::Free(BlockPtr block)
{
    blocks_allocated_--;

    block->resize(0);
    blocks_.push(block);

    TRACEX(("block %014p deallocated (%s/%s)")
           % BlockTraits<Block>::RawPtr(block)
           % blocks_allocated_ % mem_max_blocks_);
    cv_.notify_one();
}
*/
#endif
