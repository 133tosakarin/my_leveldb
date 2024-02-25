

#pragma once


namespace my_leveldb {

class Comparator;
class Iterator;

/**
 * Return an iterator that provided the union of the data in
 * children[0,n-1]. Takes ownership of the child iterators and
 * will delete them when the result iterator is deleted.
 * 
 * The result does no duplicate supression. I.e, If a particular
 * key is present in K child iterator, it will be yields K times.
 * 
*/
Iterator* NewMergingIterator(const Comparator *comparator, Iterator **children,
                            int n);
} // namespace my_leveldb