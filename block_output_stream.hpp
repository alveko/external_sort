#ifndef BLOCK_OUTPUT_STREAM_HPP
#define BLOCK_OUTPUT_STREAM_HPP

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

#include "logging.hpp"
#include "block_types.hpp"

template <typename Block, typename WritePolicy, typename MemoryPolicy>
class BlockOutputStream : public WritePolicy, public MemoryPolicy
{
  public:
    using BlockType = Block;
    using BlockPtr  = typename BlockTraits<Block>::BlockPtr;
    using Iterator  = typename BlockTraits<Block>::Iterator;
    using ValueType = typename BlockTraits<Block>::ValueType;

    void Open();
    void Close();

    void Push(const ValueType& value);  // push a single value
    void PushBlock(BlockPtr block);     // push entire block
    void WriteBlock(BlockPtr block);    // write a block directly into a file

  private:
    void OutputLoop();

  private:
    TRACEX_NAME("BlockOutputStream");

    mutable std::condition_variable cv_;
    mutable std::mutex mtx_;
    std::queue<BlockPtr> blocks_queue_;

    BlockPtr block_ = {nullptr};

    std::thread toutput_;
    std::atomic<bool> stopped_ = {false};
};

template <typename Block, typename WritePolicy, typename MemoryPolicy>
void BlockOutputStream<Block, WritePolicy, MemoryPolicy>::Open()
{
    TRACEX_METHOD();

    WritePolicy::Open();
    stopped_ = false;
    toutput_ = std::thread(&BlockOutputStream::OutputLoop, this);
}

template <typename Block, typename WritePolicy, typename MemoryPolicy>
void BlockOutputStream<Block, WritePolicy, MemoryPolicy>::Close()
{
    TRACEX_METHOD();

    PushBlock(block_);
    stopped_ = true;
    cv_.notify_one();
    toutput_.join();
    WritePolicy::Close();
}

template <typename Block, typename WritePolicy, typename MemoryPolicy>
void BlockOutputStream<Block, WritePolicy, MemoryPolicy>::Push(
    const ValueType& value)
{
    if (!block_) {
        block_ = MemoryPolicy::Allocate();
    }
    block_->push_back(value);

    if (block_->size() == block_->capacity()) {
        // block is full, push it to the output queue
        PushBlock(block_);
        block_ = nullptr;
    }
}

template <typename Block, typename WritePolicy, typename MemoryPolicy>
void BlockOutputStream<Block, WritePolicy, MemoryPolicy>::PushBlock(
    BlockPtr block)
{
    if (block) {
        std::unique_lock<std::mutex> lck(mtx_);
        blocks_queue_.push(block);
        TRACEX(("block %014p => output queue (%d)")
               % BlockTraits<Block>::RawPtr(block) % blocks_queue_.size());
        cv_.notify_one();
    }
}

template <typename Block, typename WritePolicy, typename MemoryPolicy>
void BlockOutputStream<Block, WritePolicy, MemoryPolicy>::OutputLoop()
{
    TRACEX_METHOD();
    for (;;) {
    //while (!stopped_ || MemoryPolicy::Allocated()) {

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

            WriteBlock(block);
        } else if (stopped_) {
            // nothing left in the queue and
            // the stop flag is set => quit
            break;
        }
    }
}

template <typename Block, typename WritePolicy, typename MemoryPolicy>
void BlockOutputStream<Block, WritePolicy, MemoryPolicy>::WriteBlock(
    BlockPtr block)
{
    WritePolicy::Write(block);
    MemoryPolicy::Free(block);
}

#endif
