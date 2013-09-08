#ifndef DATA_STREAM_MERGE_HPP
#define DATA_STREAM_MERGE_HPP

#include "data_stream.hpp"

// merges 2 streams
template <typename DataStream>
void merge_2streams(const DataStreamsQueue<DataStream>& instreams,
                    std::shared_ptr<DataStream> out)
{
    auto it = instreams.streams().begin();
    std::shared_ptr<DataStream> s1 = *(it++);
    std::shared_ptr<DataStream> s2 = *(it++);
    std::shared_ptr<DataStream> smin;
    s1->push_sentinel();
    s2->push_sentinel();

    for (;;) {
        smin = s1->comp_with(s2) ? s1 : s2;

        if (smin->is_sentinel()) {
            break;
        }

        out->push(smin->front());
        smin->pop();
    }
}

// merges 3 streams
template <typename DataStream>
void merge_3streams(const DataStreamsQueue<DataStream>& instreams,
                    std::shared_ptr<DataStream> out)
{
    auto it = instreams.streams().begin();
    std::shared_ptr<DataStream> s1 = *(it++);
    std::shared_ptr<DataStream> s2 = *(it++);
    std::shared_ptr<DataStream> s3 = *(it++);
    std::shared_ptr<DataStream> smin;
    s1->push_sentinel();
    s2->push_sentinel();
    s3->push_sentinel();

    for (;;) {
        if (s1->comp_with(s2)) {
            smin = s1->comp_with(s3) ? s1 : s3;
        } else {
            smin = s2->comp_with(s3) ? s2 : s3;
        }

        if (smin->is_sentinel()) {
            break;
        }
        out->push(smin->front());
        smin->pop();
    }
}

// merges 4 streams
template <typename DataStream>
void merge_4streams(const DataStreamsQueue<DataStream>& instreams,
                    std::shared_ptr<DataStream> out)
{
    auto it = instreams.streams().begin();
    std::shared_ptr<DataStream> s1 = *(it++);
    std::shared_ptr<DataStream> s2 = *(it++);
    std::shared_ptr<DataStream> s3 = *(it++);
    std::shared_ptr<DataStream> s4 = *(it++);
    std::shared_ptr<DataStream> smin;
    s1->push_sentinel();
    s2->push_sentinel();
    s3->push_sentinel();
    s4->push_sentinel();

    for (;;) {
        if (s1->comp_with(s2)) {
            if (s3->front() < s4->front())
                smin = s1->comp_with(s3) ? s1 : s3;
            else
                smin = s1->comp_with(s4) ? s1 : s4;
        } else {
            if (s3->comp_with(s4))
                smin = s2->comp_with(s3) ? s2 : s3;
            else
                smin = s2->comp_with(s4) ? s2 : s4;
        }

        if (smin->is_sentinel()) {
            break;
        }
        out->push(smin->front());
        smin->pop();
    }
}

template <typename DataStream>
void merge_streams(const DataStreamsQueue<DataStream>& instreams,
                    std::shared_ptr<DataStream> out)
{
    std::shared_ptr<DataStream> smin;
    auto hcomp = [] (const std::shared_ptr<DataStream>& s1,
                     const std::shared_ptr<DataStream>& s2) {
        //return std::greater<T>(s1->front(), s2->front());
        return s2->comp_with(s1);
    };

    //
    std::vector< std::shared_ptr<DataStream> > heap;
    for (auto& s : instreams.streams()) {
        s->push_sentinel();
        heap.push_back(s);
    }
    std::make_heap(heap.begin(), heap.end(), hcomp);

    while (heap.size() > 0) {
        // find minimum element in the input streams
        smin = heap.front();
        std::pop_heap(heap.begin(), heap.end(), hcomp);

        // output the minumum element
        out->push(smin->front());
        smin->pop();

        if (smin->is_sentinel()) {
            // end of this stream
            heap.pop_back();
        } else {
            // there is more data in the stream,
            // push it back to the heap
            heap.back() = smin;
            std::push_heap(heap.begin(), heap.end(), hcomp);
        }
    }
}

template <typename DataStream>
void merge_all(DataStreamsQueue<DataStream>& streams, size_t nmerge)
{
    TRACE(("\n*** merge_all: streams.size = %s, nmerge = %s\n")
          % streams.size() % nmerge);
    TIMER("Done in %t sec CPU, %w sec real\n");

    // merge streams until only one left
    while (streams.size() > 1) {
        TRACE(("streams left %s, next stream size = %s\n")
              % streams.size() % (streams.front()->data().size()));

        size_t round_size = 0;

        // take nmerge streams from the queue to merge them into one
        DataStreamsQueue<DataStream> roundset;
        while (roundset.size() < nmerge && !streams.empty()) {

            std::shared_ptr<DataStream> next_stream = streams.front();
            streams.pop();

            // prepare the next stream to be merged
            round_size += next_stream->data().size();
            roundset.push(next_stream);
        }

        // merge selected streams for this round
        // and put the result back into the queue
        std::shared_ptr<DataStream> s(new DataStream(round_size));

        if (roundset.size() > 4) {
            merge_streams(roundset, s);
        } else if (roundset.size() == 2) {
            merge_2streams(roundset, s);
        } else if (roundset.size() == 4) {
            merge_4streams(roundset, s);
        } else {
            merge_3streams(roundset, s);
        }
        streams.push(s);
    }

    TRACE(("all streams merged : result_size = %s\n"
           "min = %s\nmax = %s\n")
           % streams.front()->data().size()
           % *std::min_element(streams.front()->data().begin(),
                               streams.front()->data().end())
           % *std::max_element(streams.front()->data().begin(),
                               streams.front()->data().end()));
}

#endif
