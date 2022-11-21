#pragma once
#include <string>

#include "_trie_iterator.h"
#include "_trie_util.h"

template <typename T>
class Trie {
public:
    using iterator = trie_iterator<T>;
    using reverse_iterator = std::reverse_iterator<iterator>;

    Trie();
    void insert(std::string, T);
    bool exist(std::string);
    bool empty();
    int size();
    iterator begin();
    iterator end();
    reverse_iterator rbegin();
    reverse_iterator rend();
    iterator find(std::string);

private:
    tnode<T>* root_;
    int size_;
};
