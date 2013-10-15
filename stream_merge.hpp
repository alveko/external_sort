#ifndef STREAM_MERGE_HPP
#define STREAM_MERGE_HPP

#include <unordered_set>
#include "logging.hpp"

template <typename T>
using StreamSet = std::unordered_set<T>;

// merges 1 stream (simple copy)
template <typename InputStream, typename OutputStream>
void copy_stream(InputStream* sin, OutputStream* sout)
{
    TRACE_FUNC();
    while (!sin->Empty()) {
        sout->Push(sin->Front());
        sin->Pop();
    }
}

// merges 2 streams
template <typename InputStream, typename OutputStream>
void merge_2streams(StreamSet<InputStream*>& sin, OutputStream* sout)
{
    TRACE_FUNC();
    if (sin.size() != 2) {
        LOG_ERR(("Internal error: mismatch in number of streams %d/%d")
                % sin.size() % 2);
        return;
    }
    auto it = sin.begin();
    InputStream* s1 = *(it++);
    InputStream* s2 = *(it++);
    InputStream* smin = s1;

    for (;;) {
        smin = (s1->Front() < s2->Front()) ? s1 : s2;
        sout->Push(smin->Front());
        smin->Pop();
        if (smin->Empty()) {
            sin.erase(smin);
            break;
        }
    }
    copy_stream(*sin.begin(), sout);
}

// merges 3 streams
template <typename InputStream, typename OutputStream>
void merge_3streams(StreamSet<InputStream*>& sin, OutputStream* sout)
{
    TRACE_FUNC();
    if (sin.size() != 3) {
        LOG_ERR(("Internal error: mismatch in number of streams %d/%d")
                % sin.size() % 3);
        return;
    }
    auto it = sin.begin();
    InputStream* s1 = *(it++);
    InputStream* s2 = *(it++);
    InputStream* s3 = *(it++);
    InputStream* smin = s1;

    for (;;) {
        if (s1->Front() < s2->Front()) {
            smin = (s1->Front() < s3->Front()) ? s1 : s3;
        } else {
            smin = (s2->Front() < s3->Front()) ? s2 : s3;
        }
        sout->Push(smin->Front());
        smin->Pop();
        if (smin->Empty()) {
            sin.erase(smin);
            break;
        }
    }
    merge_2streams(sin, sout);
}

// merges 4 streams
template <typename InputStream, typename OutputStream>
void merge_4streams(StreamSet<InputStream*>& sin, OutputStream* sout)
{
    TRACE_FUNC();
    if (sin.size() != 4) {
        LOG_ERR(("Internal error: mismatch in number of streams %d/%d")
                % sin.size() % 4);
        return;
    }
    auto it = sin.begin();
    InputStream* s1 = *(it++);
    InputStream* s2 = *(it++);
    InputStream* s3 = *(it++);
    InputStream* s4 = *(it++);
    InputStream* smin = s1;

    for (;;) {
        if (s1->Front() < s2->Front()) {
            if (s3->Front() < s4->Front())
                smin = (s1->Front() < s3->Front()) ? s1 : s3;
            else
                smin = (s1->Front() < s4->Front()) ? s1 : s4;
        } else {
            if (s3->Front() < s4->Front())
                smin = (s2->Front() < s3->Front()) ? s2 : s3;
            else
                smin = (s2->Front() < s4->Front()) ? s2 : s4;
        }
        sout->Push(smin->Front());
        smin->Pop();
        if (smin->Empty()) {
            sin.erase(smin);
            break;
        }
    }
    merge_3streams(sin, sout);
}

template <typename InputStream, typename OutputStream>
void merge_nstreams(StreamSet<InputStream*>& sin, OutputStream* sout)
{
    TRACE_FUNC();
    if (sin.size() <= 4) {
        LOG_ERR(("Internal error: too few streams for heap-based merge %d")
                % sin.size());
        return;
    }

    InputStream* smin;

    std::vector<InputStream*> heap;
    for (auto& s : sin) {
        if (!s->Empty()) {
            heap.push_back(s);
        }
    }
    auto hcomp = [] (const InputStream*& s1,
                     const InputStream*& s2) {
        return s2->Front() < s1->Front();
    };
    std::make_heap(heap.begin(), heap.end(), hcomp);

    while (heap.size() > 4) {
        // find minimum element in the input streams
        smin = heap.front();
        std::pop_heap(heap.begin(), heap.end(), hcomp);

        // output the minumum element
        sout->Push(smin->Front());
        smin->Pop();

        if (smin->Empty()) {
            // end of this stream
            heap.pop_back();
            sin.erase(smin);
        } else {
            // there is more data in the stream,
            // push it back to the heap
            heap.back() = smin;
            std::push_heap(heap.begin(), heap.end(), hcomp);
        }
    }
    merge_4streams(sin, sout);
}

template <typename InputStreamPtr, typename OutputStreamPtr>
OutputStreamPtr merge_streams(StreamSet<InputStreamPtr> sin,
                              OutputStreamPtr sout)
{
    TRACE_FUNC();
    // Make a new StreamSet with raw pointers to pass to the merge functions:
    // 1) Raw pointers are faster
    // 2) The merge functions will shrink the set as streams get exhausted
    // 3) The original StreamSet is needed to close all streams, when it's done
    using InputStream = typename InputStreamPtr::element_type;
    using OutputStream = typename OutputStreamPtr::element_type;
    StreamSet<InputStream*> sinp;
    OutputStream* soutp = sout.get();

    for (const auto& s : sin) {
        s->Open();
        if (!s->Empty()) {
            sinp.insert(s.get());
        }
    }
    sout->Open();

    if (sinp.size() > 4) {
        //merge_nstreams(sinp, sout);
    } else if (sinp.size() == 4) {
        merge_4streams(sinp, soutp);
    } else if (sinp.size() == 3) {
        merge_3streams(sinp, soutp);
    } else if (sinp.size() == 2) {
        merge_2streams(sinp, soutp);
    } else if (sinp.size() == 1) {
        copy_stream(*sinp.begin(), soutp);
    } else {
        LOG_ERR(("Unexpected number of streams to merge: %d") % sin.size());
    }

    for (const auto& s : sin) {
        s->Close();
    }
    sout->Close();
    return sout;
}

#endif
