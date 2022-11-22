#pragma once

#include <bits/stdc++.h>
#include <chrono>
#include <ctime>

using namespace std;

namespace DELTAKV_NAMESPACE {

class Timer {
public:
    Timer();
    ~Timer();

    void triggerTimer(bool start = true, const char* label = 0);
    void startTimer();
    void stopTimer(const char* label);
    void restartTimer();
    void pauseTimer();

private:
    std::chrono::system_clock::time_point _startTime;
    size_t _prev;
};

} // namespace DELTAKV_NAMESPACE