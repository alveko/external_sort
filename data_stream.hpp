#ifndef DATA_STREAM_HPP
#define DATA_STREAM_HPP

#include <algorithm>
#include <iterator>
#include <vector>
#include <queue>

#include <boost/utility.hpp>

#include "external_sort_aux.h"

template<typename DataStream>
using DataStreamPtr = std::shared_ptr<DataStream>;

template <typename T, T SentinelValue, typename Comparator>
class DataStream : boost::noncopyable
{
public:
    using ValueType = T;
    using ContainerType = std::vector<ValueType>;
    using size_type = typename ContainerType::size_type;

    DataStream(size_type size) {
        // reserve capacity, extra element for sentinel value
        data_.reserve(size + 1);
    }

    bool is_sentinel() const { return front() ==  SentinelValue; }
    bool comp_with(const DataStreamPtr<DataStream>& s) const {
        return Comparator()(front(), s->front());
    }

    // direct access to the data container
    ContainerType& data() { return data_; }
    const ContainerType& data() const { return data_; }

    // next data element in the stream
    const ValueType& front() const { return *iter_; }

    // push/pop data elements
    void pop() { ++iter_; }
    void push(const ValueType& val) { data_.push_back(val); }
    void push(ValueType&& val) { data_.push_back(std::move(val)); }

    // push/pop sentinel to mark end of stream
    void push_sentinel() {
        data_.push_back(SentinelValue);
        iter_ = data_.begin();
    }

private:
    ContainerType data_;
    typename ContainerType::iterator iter_;
};

template<typename DataStream>
class DataStreamsQueue : boost::noncopyable
{
public:
    using StreamPtr = DataStreamPtr<DataStream>;
    using QueueType = std::deque<StreamPtr>;
    using size_type = typename QueueType::size_type;

    QueueType& streams() { return streams_; }
    const QueueType& streams() const { return streams_; }

    const StreamPtr& front() const { return streams_.front(); }
    const StreamPtr& back() const { return streams_.back(); }

    void pop() { streams_.pop_front(); }
    void push(const StreamPtr& val) { streams_.push_back(val); }
    void push(StreamPtr&& val) { streams_.push_back(std::move(val)); }

    bool empty() const { return streams_.empty(); }
    size_type size() const { return streams_.size(); }

private:
    QueueType streams_;
};

#endif
