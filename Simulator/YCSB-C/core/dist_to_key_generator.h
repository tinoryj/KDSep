#ifndef YCSB_C_PARETOGENERATOR_H
#define YCSB_C_PARETOGENERATOR_H

#include <random>

#include "generator.h"
#include "iostream"

namespace ycsbc {

class DistToKeyGenerator : public Generator<uint64_t> {
   public:
    DistToKeyGenerator(double theta, double k, double sigma)
        : theta_(theta), k_(k), sigma_(sigma) {
	num_ = 10000;
    }
    DistToKeyGenerator() : theta_(0), k_(0.92), sigma_(226) {
	num_ = 10000;
    }
    uint64_t Next(std::string& key) {
	int64_t rand_v = 0;
	for (int i = (key.size() > 4) ? key.size() - 4 : 0; i < key.size(); i++) {
	    rand_v = rand_v * 10 + (key[i] - '0');
	}
        rand_v = rand_v % num_ + 1;
        double u = static_cast<double>(rand_v) / num_;
        double ret;
        if (k_ == 0.0) {
            ret = theta_ - sigma_ * std::log(u);
        } else {
            ret = theta_ + sigma_ * (std::pow(u, -1 * k_) - 1) / k_;
        }
//	std::cout << key << " " << rand_v << " " << ret << std::endl;
        last_value_ = Value(static_cast<int64_t>(ceil(ret)), key);
        return last_value_;
    }

    uint64_t Last() { return last_value_; }

   private:
    uint64_t num_;
    double theta_;
    double k_;
    double sigma_;
    double last_value_;
    uint64_t max_value_{128 * 1024};

    uint64_t Value(uint64_t v, std::string& key) {
	if (v < 0) {
	    return 10;
	}
	if (v > max_value_) {
	    return max_value_;
	}
	return v;
//        return (v < 0 || v > max_value_) ? Next(key) : v;
        // return v < 0 ? 10 : (v > max_value_ ? v % max_value_ : v);
    }
};

}  // namespace ycsbc

#endif  // YCSB_C_PARETOGENERATOR_H
