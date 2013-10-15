#ifndef IN_PROCESS_OUT_HPP
#define IN_PROCESS_OUT_HPP

#include <condition_variable>
#include <mutex>
#include <thread>
#include <queue>
#include <vector>

#include "logging.hpp"
#include "block_types.hpp"

/// InProcessOut:
///   1) input thread   : a) allocate memory for a new block
///                       b) read the block from input
///                       c) push the block to the input queue
///
///   2) process thread : a) pop a block from the input queue
///                       b) process the block in a worker thread,
///                       c) push the processed block to the output queue
///
///   3) output thread  : a) pop a block from the output queue
///                       b) write the block to output
///                       c) free memory of the block
///
/// [input]  -> InputPolicy  -> (InputQueue)  ---
///                 |                            |
///                 v                            v
///      --------------------------
///     | MemoryPolicy::Allocate() |      ProcessPolicy ==> [workers]
///     | MemoryPolicy::Free()     |                    ==> [workers]
///      --------------------------              |
///                 ^                            |
///                 |                            |
/// [output] <- OutputPolicy <- (OutputQueue) <--
///
///

template <typename Block,
          template <typename> class MemoryPolicy,
          template <typename> class InputPolicy,
          template <typename> class OutputPolicy,
          template <typename> class ProcessPolicy>
struct InProcessOutPolicies
{
    using Memory  = MemoryPolicy<Block>;
    using Input   = InputPolicy<Block>;
    using Output  = OutputPolicy<Block>;
    using Process = ProcessPolicy<Block>;
};

template <typename Block, typename Policies>
class InProcessOut2 : public Policies::Memory,
                      public Policies::Input,
                      public Policies::Output,
                      public Policies::Process
{
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;

    void Run()
    {
        TRACEX_METHOD();

        Policies::Input::Open();
        Policies::Output::Open();

        // allocate a new block
        BlockPtr block = Policies::Memory::Allocate();

        while (!Policies::Input::IsOver()) {
            // read (fill in) the block from the input source
            Policies::Input::Read(block);

            Policies::Process::Process(block);

            // write the block to the output
            Policies::Output::Write(block);

        }
        // destroy the block and free memory
        Policies::Memory::Free(block);

        Policies::Input::Close();
        Policies::Output::Close();
    }

  private:
    TRACEX_NAME("InProcessOut");
};

template <typename Block, typename Policies>
class InProcessOut : public Policies::Memory,
                     public Policies::Input,
                     public Policies::Output,
                     public Policies::Process
{
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;

    void Run();

  private:
    void InputLoop();
    void ProcessLoop();
    void OutputLoop();

    void Process(BlockPtr block);
    void Output(BlockPtr block);

  private:
    TRACEX_NAME("InProcessOut");

    std::mutex mtx_in_, mtx_out_;
    std::condition_variable cv_in_, cv_out_;

    std::queue<BlockPtr> queue_in_;
    std::queue<BlockPtr> queue_out_;
};

template <typename Block, typename Policies>
void InProcessOut<Block, Policies>::Run()
{
    TRACEX_METHOD();
    Policies::Input::Open();
    Policies::Output::Open();

    std::thread tinput(&InProcessOut::InputLoop, this);
    std::thread tprocess(&InProcessOut::ProcessLoop, this);
    std::thread toutput(&InProcessOut::OutputLoop, this);
    tinput.join();
    tprocess.join();
    toutput.join();

    Policies::Input::Close();
    Policies::Output::Close();
}

template <typename Block, typename Policies>
void InProcessOut<Block, Policies>::InputLoop()
{
    TRACEX_METHOD();
    for (;;) {
        BlockPtr block = nullptr;

        if (!Policies::Input::IsOver()) {
            // allocate a new block; supposed to be a blocking call!
            // waits for blocks to be released if needed
            block = Policies::Memory::Allocate();

            // read (fill in) the block from the input source
            Policies::Input::Read(block);
        }

        // push the block to the input queue
        TRACEX(("block %014p => input queue")
               % BlockTraits<Block>::RawPtr(block));
        std::unique_lock<std::mutex> lck(mtx_in_);
        queue_in_.push(block);
        cv_in_.notify_one();

        // if input is over and null-block marker has been pushed, exit
        if (Policies::Input::IsOver() && !block) {
            break;
        }
    }
}

template <typename Block, typename Policies>
void InProcessOut<Block, Policies>::ProcessLoop()
{
    TRACEX_METHOD();
    for (;;) {
        std::unique_lock<std::mutex> lck(mtx_in_);

        // wait for a block in the input queue
        cv_in_.wait(lck, [ this ] () {
                return !(this->queue_in_.empty());
            });

        // pop the front block from the input queue
        BlockPtr block = queue_in_.front();
        queue_in_.pop();

        if (block) {
            // spawn a thread to sort the block
            std::thread tprocess(&InProcessOut::Process, this, block);
            tprocess.detach();
        } else {
            // null block means end of input
            break;
        }
    }
}

template <typename Block, typename Policies>
void InProcessOut<Block, Policies>::Process(BlockPtr block)
{
    TRACEX(("block %014p processing")
           % BlockTraits<Block>::RawPtr(block));

    Policies::Process::Process(block);

    // push to the output queue
    std::unique_lock<std::mutex> lck(mtx_out_);
    TRACEX(("block %014p => output queue")
           % BlockTraits<Block>::RawPtr(block));
    queue_out_.push(block);
    cv_out_.notify_one();
}

template <typename Block, typename Policies>
void InProcessOut<Block, Policies>::OutputLoop()
{
    TRACEX_METHOD();

    // stop when the input is over and no more pending allocated blocks
    while (!(Policies::Input::IsOver() && Policies::Memory::Allocated() == 0)) {

        // pop a block from the output queue
        std::unique_lock<std::mutex> lck(mtx_out_, std::defer_lock);
        lck.lock();
        cv_out_.wait(lck, [ this ] () {
                return !(this->queue_out_.empty());
            });
        BlockPtr block = queue_out_.front();
        queue_out_.pop();
        lck.unlock();

        InProcessOut::Output(block);
    }
}

template <typename Block, typename Policies>
void InProcessOut<Block, Policies>::Output(BlockPtr block)
{
    // write the block to the output
    Policies::Output::Write(block);

    // destroy the block and free memory
    Policies::Memory::Free(block);
}

#endif
