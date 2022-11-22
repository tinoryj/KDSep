#include "utils/messageQueue.hpp"

namespace DELTAKV_NAMESPACE {

template <typename T>
bool messageQueue<T>::push(T& data)
{
    while (!lockFreeQueue_.push(data))
        ;
    return true;
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