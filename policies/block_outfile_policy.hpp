#ifndef BLOCK_OUTFILE_HPP
#define BLOCK_OUTFILE_HPP

#include <string>
#include <queue>
#include <fstream>

#include "logging.hpp"
#include "block_types.hpp"

/// ----------------------------------------------------------------------------
/// BlockOutFilePolicy

template <typename Block>
class BlockOutFilePolicy
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

    void set_output_blocks_per_file(const size_t& n) {
        output_blocks_per_file_ = n;
    }
    const size_t& output_blocks_per_file() const {
        return output_blocks_per_file_;
    }

    const std::queue<std::string>& output_filenames() const {
        return output_filenames_;
    }

  private:
    void FileOpen();
    void FileWrite(const BlockPtr& block);
    void FileClose();

  private:
    TRACEX_NAME("BlockOutFilePolicy");

    size_t block_cnt_ = 0;
    size_t block_cnt_file_ = 0;
    size_t output_blocks_per_file_ = 0;
    std::queue<std::string> output_filenames_;
    std::string output_filename_;
    std::ofstream ofs_;
};

/// ----------------------------------------------------------------------------
/// Policy interface methods

template <typename Block>
void BlockOutFilePolicy<Block>::Open()
{
    TRACEX_METHOD();
    FileOpen();
}

template <typename Block>
void BlockOutFilePolicy<Block>::Close()
{
    TRACEX_METHOD();
    FileClose();
}

template <typename Block>
void BlockOutFilePolicy<Block>::Write(const BlockPtr& block)
{
    // egnore empty blocks
    if (!block || block->empty()) {
        return;
    }

    if (output_blocks_per_file_ &&
        output_blocks_per_file_ <= block_cnt_file_) {
        FileClose();
        FileOpen();
    }

    // write the block
    FileWrite(block);
    block_cnt_++;
    block_cnt_file_++;
}

/// ----------------------------------------------------------------------------
/// File operations

template <typename Block>
void BlockOutFilePolicy<Block>::FileOpen()
{
    std::stringstream filename;
    filename << output_filename_;
    if (output_blocks_per_file_) {
        filename << (boost::format(".%02d") %
                     (1 + block_cnt_ / output_blocks_per_file_));
    }
    LOG_INF(("opening w %s") % filename.str());
    ofs_.open(filename.str(), std::ofstream::out | std::ofstream::binary);
    output_filenames_.push(filename.str());
    block_cnt_file_ = 0;
}

template <typename Block>
void BlockOutFilePolicy<Block>::FileWrite(const BlockPtr& block)
{
    ofs_.write((const char*)block->data(), block->size() * sizeof(ValueType));
    TRACEX(("block %014p => file (%s/%s), bsize = %d")
           % BlockTraits<Block>::RawPtr(block)
           % block_cnt_file_ % block_cnt_ % block->size());
}

template <typename Block>
void BlockOutFilePolicy<Block>::FileClose()
{
    ofs_.close();
}

#endif
