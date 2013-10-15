#ifndef BLOCK_INFILE_HPP
#define BLOCK_INFILE_HPP

#include <string>
#include <fstream>

#include "logging.hpp"
#include "block_types.hpp"

/// ----------------------------------------------------------------------------
/// BlockInFilePolicy

template <typename Block>
class BlockInFilePolicy
{
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    using ValueType = typename BlockTraits<Block>::ValueType;

    /// Policy interface
    void Open();
    void Close();
    void Read(BlockPtr& block);
    bool IsOver() const;

    /// Set/get properties
    void set_input_filename(const std::string& ifn) { input_filename_ = ifn; }
    const std::string& input_filename() const { return input_filename_; }

  private:
    void FileOpen();
    void FileRead(BlockPtr& block);
    void FileClose();

  private:
    TRACEX_NAME("BlockInFilePolicy");

    size_t block_cnt_ = 0;
    std::string input_filename_;
    std::ifstream ifs_;
};

/// ----------------------------------------------------------------------------
/// Policy interface methods

template <typename Block>
void BlockInFilePolicy<Block>::Open()
{
    TRACEX_METHOD();
    FileOpen();
}

template <typename Block>
void BlockInFilePolicy<Block>::Close()
{
    TRACEX_METHOD();
    FileClose();
}

template <typename Block>
void BlockInFilePolicy<Block>::Read(BlockPtr& block)
{
    FileRead(block);
    block_cnt_++;
}

template <typename Block>
bool BlockInFilePolicy<Block>::IsOver() const
{
    return !(ifs_.is_open() && ifs_.good());
}

/// ----------------------------------------------------------------------------
/// File operations

template <typename Block>
void BlockInFilePolicy<Block>::FileOpen()
{
    LOG_INF(("opening r %s") % input_filename_);
    ifs_.open(input_filename_, std::ifstream::in | std::ifstream::binary);
    if (!ifs_) {
        LOG_ERR(("Failed to open input file: %s") % input_filename_);
    }
}

template <typename Block>
void BlockInFilePolicy<Block>::FileRead(BlockPtr& block)
{
    block->resize(block->capacity());
    std::streamsize bsize = block->size() * sizeof(ValueType);

    ifs_.read(reinterpret_cast<char*>(block->data()), bsize);
    if (ifs_.gcount() < bsize) {
        block->resize(ifs_.gcount() / sizeof(ValueType));
    }
    TRACEX(("block %014p <= file (%s), is_over = %s, size = %s")
           % BlockTraits<Block>::RawPtr(block)
           % block_cnt_ % IsOver() % block->size());
}

template <typename Block>
void BlockInFilePolicy<Block>::FileClose()
{
    ifs_.close();
}

#endif
