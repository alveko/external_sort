#ifndef BLOCK_FILE_WRITE_HPP
#define BLOCK_FILE_WRITE_HPP

#include <string>
#include <queue>
#include <fstream>

#include "block_types.hpp"

namespace external_sort {
namespace block {

/// ----------------------------------------------------------------------------
/// BlockFileWritePolicy

template <typename Block>
class BlockFileWritePolicy
{
  public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    using ValueType = typename BlockTraits<Block>::ValueType;

    /// Policy interface
    void Open();
    void Close();
    void Write(const BlockPtr& block);

    /// Set/get properties
    void set_output_filename(const std::string& ofn) { output_filename_ = ofn; }
    const std::string& output_filename() const { return output_filename_; }

  private:
    void FileOpen();
    void FileWrite(const BlockPtr& block);
    void FileClose();

  private:
    TRACEX_NAME("BlockFileWritePolicy");

    size_t block_cnt_ = 0;
    std::string output_filename_;
    std::ofstream ofs_;
};

/// ----------------------------------------------------------------------------
/// Policy interface methods

template <typename Block>
void BlockFileWritePolicy<Block>::Open()
{
    TRACEX_METHOD();
    FileOpen();
}

template <typename Block>
void BlockFileWritePolicy<Block>::Close()
{
    TRACEX_METHOD();
    FileClose();
}

template <typename Block>
void BlockFileWritePolicy<Block>::Write(const BlockPtr& block)
{
    // egnore empty blocks
    if (!block || block->empty()) {
        return;
    }

    // write the block
    FileWrite(block);
    block_cnt_++;
}

/// ----------------------------------------------------------------------------
/// File operations

template <typename Block>
void BlockFileWritePolicy<Block>::FileOpen()
{
    LOG_INF(("opening file w %s") % output_filename_);
    TRACEX(("output file %s") % output_filename_);
    ofs_.open(output_filename_, std::ofstream::out | std::ofstream::binary);
    if (!ofs_) {
        LOG_ERR(("Failed to open output file: %s") % output_filename_);
    }
}

template <typename Block>
void BlockFileWritePolicy<Block>::FileWrite(const BlockPtr& block)
{
    ofs_.write((const char*)block->data(), block->size() * sizeof(ValueType));
    TRACEX(("block %014p => file (%s), bsize = %d")
           % BlockTraits<Block>::RawPtr(block) % block_cnt_ % block->size());
}

template <typename Block>
void BlockFileWritePolicy<Block>::FileClose()
{
    if (ofs_.is_open()) {
        ofs_.close();
    }
}

} // namespace block
} // namespace external_sort

#endif
