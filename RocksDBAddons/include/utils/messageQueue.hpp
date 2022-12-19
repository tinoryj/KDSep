#pragma once

#include <bits/stdc++.h>
#include <boost/atomic.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
using namespace std;

namespace DELTAKV_NAMESPACE {

template <typename T>
class messageQueue {
public:
    messageQueue();
    ~messageQueue() = default;
    boost::atomic<bool> done_;
    bool push(T& data);
    bool pop(T& data);
    bool isEmpty();

private:
    boost::lockfree::queue<T, boost::lockfree::capacity<500>> lockFreeQueue_;
};

template <typename T>
messageQueue<T>::messageQueue()
{
    done_ = false;
}

template <typename T>
bool messageQueue<T>::push(T& data)
{
    bool status = lockFreeQueue_.bounded_push(data);
    return status;
}

template <typename T>
bool messageQueue<T>::pop(T& data)
{
    return lockFreeQueue_.pop(data);
}

template <typename T>
bool messageQueue<T>::isEmpty()
{
    return lockFreeQueue_.empty();
}

} // namespace DELTAKV_NAMESPACE