#pragma once

#include <bits/stdc++.h>
#include <boost/atomic.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
using namespace std;

namespace DELTAKV_NAMESPACE {

template <class T>
class messageQueue {
    boost::lockfree::queue<T, boost::lockfree::capacity<5000>> lockFreeQueue_;

public:
    messageQueue() = default;
    ~messageQueue() = default;
    bool push(T& data);
    bool pop(T& data);
    bool isEmpty();
};

} // namespace DELTAKV_NAMESPACE