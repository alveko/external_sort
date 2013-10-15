#ifndef BLOCK_OUTPUT_STREAM_HPP
#define BLOCK_OUTPUT_STREAM_HPP

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

#include "logging.hpp"
#include "block_types.hpp"

template <typename Block, typename OutputPolicy, typename MemoryPolicy>
class BlockOutputStream : public OutputPolicy,
                          public MemoryPolicy
{
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    using Iterator = typename BlockTraits<Block>::Iterator;
    using ValueType = typename BlockTraits<Block>::ValueType;
    using QueueType = std::queue<BlockPtr>;

    BlockOutputStream();
    ~BlockOutputStream();

    void Open();
    void Close();

    void Push(const ValueType& value);

  private:
    void PushBlock();

    void OutputLoop();

  private:
    TRACEX_NAME("BlockOutputStream");

    mutable std::condition_variable cv_;
    mutable std::mutex mtx_;
    QueueType blocks_queue_;

    BlockPtr block_ = {nullptr};

    std::thread toutput_;
    std::atomic<bool> stopped_ = {false};
    std::atomic<bool> opened_ = {false};
};

template <typename Block, typename OutputPolicy, typename MemoryPolicy>
BlockOutputStream<Block, OutputPolicy, MemoryPolicy>::BlockOutputStream()
{
}

template <typename Block, typename OutputPolicy, typename MemoryPolicy>
BlockOutputStream<Block, OutputPolicy, MemoryPolicy>::~BlockOutputStream()
{
    if (opened_) {
        Close();
    }
}

template <typename Block, typename OutputPolicy, typename MemoryPolicy>
void BlockOutputStream<Block, OutputPolicy, MemoryPolicy>::Open()
{
    TRACEX_METHOD();

    OutputPolicy::Open();
    stopped_ = false;
    toutput_ = std::thread(&BlockOutputStream::OutputLoop, this);
    opened_ = true;
}

template <typename Block, typename OutputPolicy, typename MemoryPolicy>
void BlockOutputStream<Block, OutputPolicy, MemoryPolicy>::Close()
{
    TRACEX_METHOD();

    PushBlock();
    stopped_ = true;
    cv_.notify_one();
    toutput_.join();
    OutputPolicy::Close();
    opened_ = false;
}

template <typename Block, typename OutputPolicy, typename MemoryPolicy>
void BlockOutputStream<Block, OutputPolicy, MemoryPolicy>::Push(
    const ValueType& value)
{
    if (!block_) {
        block_ = MemoryPolicy::Allocate();
    }
    block_->push_back(value);

    if (block_->size() == block_->capacity()) {
        // block is full, push it to the output queue
        PushBlock();
    }
}

template <typename Block, typename OutputPolicy, typename MemoryPolicy>
void BlockOutputStream<Block, OutputPolicy, MemoryPolicy>::PushBlock()
{
    TRACEX_METHOD();

    if (block_) {
        std::unique_lock<std::mutex> lck(mtx_);
        blocks_queue_.push(block_);
        TRACEX(("block %014p => output queue (%d)")
               % BlockTraits<Block>::RawPtr(block_) % blocks_queue_.size());
        block_ = nullptr;
        cv_.notify_one();
    }
}

template <typename Block, typename OutputPolicy, typename MemoryPolicy>
void BlockOutputStream<Block, OutputPolicy, MemoryPolicy>::OutputLoop()
{
    TRACEX_METHOD();

    while (!stopped_ || MemoryPolicy::Allocated()) {

        // wait for a block in the queue or the stop-flag
        std::unique_lock<std::mutex> lck(mtx_);
        while (blocks_queue_.empty() && !stopped_) {
            cv_.wait(lck);
        }

        if (!blocks_queue_.empty()) {
            BlockPtr block = blocks_queue_.front();
            blocks_queue_.pop();
            TRACEX(("block %014p <= output queue (%d)")
                   % BlockTraits<Block>::RawPtr(block) % blocks_queue_.size());
            lck.unlock();

            OutputPolicy::Write(block);
            MemoryPolicy::Free(block);
        }
    }
}

#endif
