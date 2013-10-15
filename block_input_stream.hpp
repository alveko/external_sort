#ifndef BLOCK_INPUT_STREAM_HPP
#define BLOCK_INPUT_STREAM_HPP

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

#include "logging.hpp"
#include "block_types.hpp"

template <typename Block, typename InputPolicy, typename MemoryPolicy>
class BlockInputStream : public InputPolicy,
                         public MemoryPolicy
{
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    using Iterator = typename BlockTraits<Block>::Iterator;
    using ValueType = typename BlockTraits<Block>::ValueType;
    using QueueType = std::queue<BlockPtr>;

    BlockInputStream();
    ~BlockInputStream();

    void Open();
    void Close();
    bool Empty();

    ValueType& Front();
    void Pop();

  private:
    void InputLoop();
    void WaitForBlock();

  private:
    TRACEX_NAME("BlockInputStream");

    mutable std::condition_variable cv_;
    mutable std::mutex mtx_;
    QueueType blocks_queue_;

    BlockPtr block_ = {nullptr};
    Iterator block_iter_;

    std::thread tinput_;
    std::atomic<bool> is_over_ = {false};
    std::atomic<bool> opened_ = {false};
};

template <typename Block, typename InputPolicy, typename MemoryPolicy>
BlockInputStream<Block, InputPolicy, MemoryPolicy>::BlockInputStream()
{
}

template <typename Block, typename InputPolicy, typename MemoryPolicy>
BlockInputStream<Block, InputPolicy, MemoryPolicy>::~BlockInputStream()
{
    if (opened_) {
        Close();
    }
}

template <typename Block, typename InputPolicy, typename MemoryPolicy>
void BlockInputStream<Block, InputPolicy, MemoryPolicy>::Open()
{
    TRACEX_METHOD();
    InputPolicy::Open();
    is_over_ = false;
    tinput_ = std::thread(&BlockInputStream::InputLoop, this);
    opened_ = true;
}

template <typename Block, typename InputPolicy, typename MemoryPolicy>
void BlockInputStream<Block, InputPolicy, MemoryPolicy>::Close()
{
    TRACEX_METHOD();
    InputPolicy::Close();
    tinput_.join();
    opened_ = false;
}

template <typename Block, typename InputPolicy, typename MemoryPolicy>
bool BlockInputStream<Block, InputPolicy, MemoryPolicy>::Empty()
{
    if (!block_) {
        WaitForBlock();
    }
    return is_over_ && !block_;
}

template <typename Block, typename InputPolicy, typename MemoryPolicy>
auto BlockInputStream<Block, InputPolicy, MemoryPolicy>::Front() -> ValueType&
{
    // Empty() must be called first

    return *block_iter_;
}

template <typename Block, typename InputPolicy, typename MemoryPolicy>
void BlockInputStream<Block, InputPolicy, MemoryPolicy>::Pop()
{
    // Empty() must be called first

    ++block_iter_;
    if (block_iter_ == block_->end()) {
        // block is over, free it
        MemoryPolicy::Free(block_);
        block_ = nullptr;
    }
}

template <typename Block, typename InputPolicy, typename MemoryPolicy>
void BlockInputStream<Block, InputPolicy, MemoryPolicy>::InputLoop()
{
    TRACEX_METHOD();

    while (!InputPolicy::IsOver()) {
        // allocate a new block; supposed to be a blocking call!
        // waits for chunks to be released if needed
        BlockPtr block = MemoryPolicy::Allocate();

        // read (fill in) the block from the input source
        InputPolicy::Read(block);

        // push the block to the queue
        if (!block->empty()) {
            std::unique_lock<std::mutex> lck(mtx_);
            blocks_queue_.push(block);
            TRACEX(("block %014p => input queue (%d)")
                   % BlockTraits<Block>::RawPtr(block) % blocks_queue_.size());
            cv_.notify_one();
        } else {
            TRACEX(("block %014p is empty, ignoring")
                   % BlockTraits<Block>::RawPtr(block));
            MemoryPolicy::Free(block);
        }
    }

    // is_over_ needed, since InputPolicy::IsOver() becomes true before
    // the last block pushed into the queue
    // (hence it can be intercepted by another thread)
    std::unique_lock<std::mutex> lck(mtx_);
    is_over_ = true;
    cv_.notify_one();
}

template <typename Block, typename InputPolicy, typename MemoryPolicy>
void BlockInputStream<Block, InputPolicy, MemoryPolicy>::WaitForBlock()
{
    std::unique_lock<std::mutex> lck(mtx_);
    while (blocks_queue_.empty() && !is_over_) {
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
