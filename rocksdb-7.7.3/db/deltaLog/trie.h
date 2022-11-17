#include <string>
// #include <iterator>

#include "_trie_iterator.h"

template <typename T>
class Trie {
 public:
  using iterator = trie_iterator<T>;
  using reverse_iterator = std::reverse_iterator<iterator>;

  Trie();
  void insert(std::string, T);
  bool exist(std::string);
  bool empty();
  iterator begin();
  iterator end();
  reverse_iterator rbegin();
  reverse_iterator rend();
  iterator find(std::string);

 private:
  tnode<T>* root;
  int size;
};

#include "trie.hpp"
