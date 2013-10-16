#ifndef BLOCK_INPUT_STREAM_HPP
#define BLOCK_INPUT_STREAM_HPP

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

#include "logging.hpp"
#include "block_types.hpp"

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
class BlockInputStream : public ReadPolicy, public MemoryPolicy
{
  public:
    using BlockType = Block;
    using BlockPtr  = typename BlockTraits<Block>::BlockPtr;
    using Iterator  = typename BlockTraits<Block>::Iterator;
    using ValueType = typename BlockTraits<Block>::ValueType;

    void Open();
    void Close();
    bool Empty();

    ValueType& Front();     // get a single value
    BlockPtr FrontBlock();  // get entire block
    BlockPtr ReadBlock();   // read a block right from the file

    void Pop();
    void PopBlock();

  private:
    void InputLoop();
    void WaitForBlock();

  private:
    TRACEX_NAME("BlockInputStream");

    mutable std::condition_variable cv_;
    mutable std::mutex mtx_;
    std::queue<BlockPtr> blocks_queue_;

    BlockPtr block_ = {nullptr};
    Iterator block_iter_;

    std::thread tinput_;
    std::atomic<bool> empty_ = {false};
};

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
void BlockInputStream<Block, ReadPolicy, MemoryPolicy>::Open()
{
    TRACEX_METHOD();
    ReadPolicy::Open();
    empty_ = false;
    tinput_ = std::thread(&BlockInputStream::InputLoop, this);
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
void BlockInputStream<Block, ReadPolicy, MemoryPolicy>::Close()
{
    TRACEX_METHOD();
    ReadPolicy::Close();
    tinput_.join();
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
bool BlockInputStream<Block, ReadPolicy, MemoryPolicy>::Empty()
{
    if (!block_) {
        WaitForBlock();
    }
    return empty_ && !block_;
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
auto BlockInputStream<Block, ReadPolicy, MemoryPolicy>::Front()
    -> ValueType&
{
    // Empty() must be called first!

    return *block_iter_;
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
void BlockInputStream<Block, ReadPolicy, MemoryPolicy>::Pop()
{
    // Empty() must be called first!

    ++block_iter_;
    if (block_iter_ == block_->end()) {
        // block is over, free it
        auto tmp = block_;
        PopBlock();
        MemoryPolicy::Free(tmp);
    }
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
auto BlockInputStream<Block, ReadPolicy, MemoryPolicy>::FrontBlock()
    -> BlockPtr
{
    TRACEX(("block %014p front block") % BlockTraits<Block>::RawPtr(block_));
    return block_;
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
void BlockInputStream<Block, ReadPolicy, MemoryPolicy>::PopBlock()
{
    // No MemoryPolicy::Free! The caller has to free the block
    block_ = nullptr;
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
void BlockInputStream<Block, ReadPolicy, MemoryPolicy>::InputLoop()
{
    TRACEX_METHOD();

    while (!ReadPolicy::Empty()) {
        // Allocate and read the block from the file (blocking!)
        BlockPtr block = ReadBlock();

        // push the block to the queue
        if (block) {
            std::unique_lock<std::mutex> lck(mtx_);
            blocks_queue_.push(block);
            TRACEX(("block %014p => input queue (%d)")
                   % BlockTraits<Block>::RawPtr(block) % blocks_queue_.size());
            cv_.notify_one();
        }
    }

    // empty_ needed, since ReadPolicy::Empty() becomes true before
    // the last block pushed into the queue
    // (hence it can be intercepted by the other thread)
    std::unique_lock<std::mutex> lck(mtx_);
    empty_ = true;
    cv_.notify_one();
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
auto BlockInputStream<Block, ReadPolicy, MemoryPolicy>::ReadBlock()
    -> BlockPtr
{
    // allocate a new block; supposed to be a blocking call!
    // waits for chunks to be released if needed
    BlockPtr block = MemoryPolicy::Allocate();

    // read (fill in) the block from the input source
    ReadPolicy::Read(block);
    if (block->empty()) {
        // this happens when the previous block ended right before EOF
        TRACEX(("block %014p is empty, ignoring")
               % BlockTraits<Block>::RawPtr(block));
        MemoryPolicy::Free(block);
        block = nullptr;
    }

    return block;
}

template <typename Block, typename ReadPolicy, typename MemoryPolicy>
void BlockInputStream<Block, ReadPolicy, MemoryPolicy>::WaitForBlock()
{
    TRACEX_METHOD();

    std::unique_lock<std::mutex> lck(mtx_);
    while (blocks_queue_.empty() && !empty_) {
        cv_.wait(lck);
    }

    if (!blocks_queue_.empty()) {
        block_ = blocks_queue_.front();
        blocks_queue_.pop();
        block_iter_ = block_->begin();
        TRACEX(("block %014p <= input queue (%d)")
               % BlockTraits<Block>::RawPtr(block_) % blocks_queue_.size());
    }
}

#endif
