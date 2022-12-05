#include "./include/utils/prefixTree.hpp"
#include <bits/stdc++.h>

using namespace std;

int main()
{
    DELTAKV_NAMESPACE::PrefixTree<uint64_t> testTree(2, 16);
    uint64_t number1 = 2;
    uint64_t number2 = 3;
    testTree.insert("0000", number1);
    testTree.insert("0010", number2);
    testTree.insert("0100", number1);
    testTree.insert("1000", number1);
    testTree.insert("0110", number2);
    testTree.insert("1010", number2);
    uint64_t newContent;
    bool status = testTree.get("0000", newContent);
    if (status == true) {
        cerr << "Find value = " << newContent << endl;
    }
    status = testTree.get("1010", newContent);
    if (status == true) {
        cerr << "Find value = " << newContent << endl;
    }
    uint64_t targetLevelID = 0;
    testTree.find("1010", targetLevelID);
    testTree.remove("1010", targetLevelID);
    status = testTree.get("1010", newContent);
    if (status == true) {
        cerr << "Find value = " << newContent << endl;
    }
    vector<pair<string, uint64_t>> validObjVec;
    testTree.getCurrentValidNodes(validObjVec);
    for (auto it : validObjVec) {
        cerr << "Valid object prefix = " << it.first << ", content = " << it.second << endl;
    }
    testTree.insertWithFixedBitNumber("111111111111", 8, number1);
    testTree.insertWithFixedBitNumber("111100000", 8, number1);
    return 0;
}
