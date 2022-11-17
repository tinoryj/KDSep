#include <string>

#include "_trie_util.h"
#include "_trie_iterator.h"

template <typename T>
Trie<T>::Trie(): size(0) {
    T flag;
    root = new tnode<T>(flag, nullptr, -1);
    size = 0;
}

template <typename T>
void Trie<T>::insert(std::string key, T val) {
  tnode<T>* node = this->root;
  int ascii;
  sIter(key, it) {
    ascii = (int)*it;
    if (node->getChild(ascii) != nullptr) {
      node = node->getChild(ascii);
    } else {
      T flag;
      tnode<T>* _node = new tnode<T>(flag, node, ascii);
      node->addChild(_node, ascii);
      node = node->getChild(ascii);
    }
  }
  if (!node->isEnd()) {
    this->size += 1;
  }
  node->update(val);
  node->markEnd(key);
}

template <typename T>
bool Trie<T>::exist(std::string key) {
  int ascii;
  bool res = true;
  tnode<T>* node = this->root;
  sIter(key, it) {
    ascii = (int)*it;
    if (node->getChild(ascii) == nullptr) {
      res = false;
      break;
    } else {
      node = node->getChild(ascii);
    }
  }
  if (!node->isEnd()) {
    res = false;
  }
  return res;
}

template <typename T>
bool Trie<T>::empty() {
  return this->size == 0;
}

template <typename T>
typename Trie<T>::iterator Trie<T>::begin() {
  trie_iterator<T> it = *(new trie_iterator<T>(this->root));
  return ++it;
}

template <typename T>
tnode<T>* rbrecur(tnode<T>* n, int offset = 127, tnode<T>* r = nullptr);

template <typename T>
typename Trie<T>::iterator Trie<T>::end() {
  T flag;
  tnode<T>* r = nullptr;
  if (!this->empty()) {
    r = rbrecur(this->root);
  }
  tnode<T>* t = new tnode<T>(flag, r, 1516);
  return *(new trie_iterator<T>(t));
}

template <typename T>
tnode<T>* rbrecur(tnode<T>* n, int offset, tnode<T>* r) {
  tnode<T>* it = nullptr;
  for (int i = offset; i > -1; i--) {
    it = n->getChild(i);
    if (it == nullptr) {
      if (i == 0) {
        return r;
      }
      continue;
    }
    if (it->isEnd()) {
      r = it;
    }
    return rbrecur(it, 127, r);
  }
}

template <typename T>
typename Trie<T>::reverse_iterator Trie<T>::rbegin() {
  return *(new Trie<T>::reverse_iterator(Trie<T>::end()));
}

template <typename T>
typename Trie<T>::reverse_iterator Trie<T>::rend() {
  return *(new Trie<T>::reverse_iterator(Trie<T>::begin()));
}

template <typename T>
typename Trie<T>::iterator Trie<T>::find(std::string key) {
  tnode<T>* n = this->root;
  sIter(key, it) {
    n = n->getChild(*it);
    if (n == nullptr) {
      return this->end();
    }
  }
  if (!n->isEnd()) {
    return this->end();
  }
  trie_iterator<T> it = *(new trie_iterator<T>(n));
  return it;
}
