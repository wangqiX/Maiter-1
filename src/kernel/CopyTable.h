/* 
 * File:   CopyTable.h
 * Author: wangq
 *
 * Created on 2016年11月15日, 下午7:53
 */
#include "util/common.h"
#include "worker/worker.pb.h"
#include "kernel/table.h"
#include "kernel/local-table.h"
#include <boost/noncopyable.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/lognormal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <algorithm>

#ifndef COPYTABLE_H
#define	COPYTABLE_H

namespace dsm {

    template<class K, class V1>
    class CopyTable :
    public LocalTable,
    public CTypedTable<K, V1>,
    private boost::noncopyable {
    private:
#pragma pack(push, 1)

        struct Bucket {
            K k;
            V1 v1;
            vector<int> vec;
            bool in_use;
        };
#pragma pack(pop)
    private:

        uint32_t bucket_idx(K k) {
            return hashobj_(k) % size_;
        }

        int bucket_for_key(const K& k) {
            int start = bucket_idx(k);
            int b = start;

            do {
                if (buckets_[b].in_use) {
                    if (buckets_[b].k == k) {
                        return b;
                    }
                } else {
                    return -1;
                }

                b = (b + 1) % size_;
            } while (b != start);

            return -1;
        }

        std::vector<Bucket> buckets_;

        int64_t entries_;
        int64_t size_;
        double total_curr;
        int64_t total_updates;

        std::tr1::hash<K> hashobj_;

    public:

        struct Iterator : public CTypedTableIterator<K, V1> {

            Iterator(CopyTable<K, V1> &parent) : pos(-1), parent_(parent) {
            }

            virtual ~Iterator() {
            }

            Marshal<K>* kmarshal() {
                return parent_.kmarshal();
            }

            Marshal<V1>* v1marshal() {
                return parent_.v1marshal();
            }

            bool Next() {
                do {
                    ++pos;
                } while (pos < parent_.size_ && !parent_.buckets_[pos].in_use);

                if (pos >= parent_.size_) {
                    return false;
                } else {
                    return true;
                }

            }

            bool done() {
                return pos + 1 == parent_.size_;
            }

            const K& key() {
                return parent_.buckets_[pos].k;
            }

            V1& value1() {
                return parent_.buckets_[pos].v1;
            }

            vector<int>& vec() {
                return parent_.buckets_[pos].vec;
            }

            int pos;
            CopyTable<K, V1> &parent_;
        };

        struct Factory : public TableFactory {

            TableBase* New() {
                return new CopyTable<K, V1>();
            }
        };

        CopyTable(int size = 1);

        ~CopyTable() {

        }

        void resize(int64_t size);

        bool empty() {
            return size() == 0;
        }

        int64_t size() {
            return entries_;
        }

        void clear() {
            for (int i = 0; i < size_; ++i) {
                buckets_[i].in_use = 0;
            }
            entries_ = 0;
        }

        void reset() {
        }

        bool contains(const K &k);
        V1 get(const K &k);
        vector<int> getV(const K&k);
        void put(const K &k, const V1 &v1, vector<int> &vec);
        void update(const K &k, const V1 &v);
        void accumulate(const K &k, const V1 &v);
        bool remove(const K &k);


        void serializeToFile(TableCoder *out);
        void serializeToNet(KVPairCoder *out);
        void deserializeFromFile(TableCoder *in, DecodeIteratorBase *itbase);
        void deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *itbase);
        void serializeToSnapshot(const string& f, long *updates, double *totalF2);

        TableIterator *get_iterator(TableHelper* helper, bool bfilter) {
            return new Iterator(*this);
        }

        TableIterator *schedule_iterator(TableHelper* helper, bool bfilter) {
            return NULL;
        }

        TableIterator *entirepass_iterator(TableHelper* helper) {
            return NULL;
        }

        Marshal<K>* kmarshal() {
            return ((Marshal<K>*)info_.key_marshal);
        }

        Marshal<V1>* v1marshal() {
            return ((Marshal<V1>*)info_.value1_marshal);
        }

    };

    template <class K, class V1>
    CopyTable<K, V1>::CopyTable(int size)
    : buckets_(0), entries_(0), size_(0), total_curr(0), total_updates(0) {
        clear();

        VLOG(1) << "new CopyTable size " << size;
        resize(size);
    }

    template <class K, class V1>
    void CopyTable<K, V1>::resize(int64_t size) {
        CHECK_GT(size, 0);
        if (size_ == size)
            return;

        std::vector<Bucket> old_b = buckets_;

        buckets_.resize(size);
        size_ = size;
        clear();

        for (int i = 0; i < old_b.size(); ++i) {
            if (old_b[i].in_use) {
                put(old_b[i].k, old_b[i].v1, old_b[i].vec);
            }
        }

    }

    template<class K, class V1>
    bool CopyTable<K, V1>::contains(const K&k) {
        int start = bucket_for_key(k);
        if (start >= 0)
            return true;
        else
            return false;
    }

    template<class K, class V1>
    V1 CopyTable<K, V1>::get(const K &k) {
        int b = bucket_for_key(k);
        return buckets_[b].v1;
    }

    template<class K, class V1>
    vector<int> CopyTable<K, V1>::getV(const K &k) {
        int b = bucket_for_key(k);
        return buckets_[b].vec;
    }

    template<class K, class V1>
    void CopyTable<K, V1>::put(const K &k, const V1 &v1, vector<int> &vec) {
        int start = bucket_idx(k);
        int b = start;
        bool found = false;

        do {
            if (!buckets_[b].in_use) {
                break;
            }

            if (buckets_[b].k == k) {
                found = true;
                break;
            }

            b = (b + 1) % size_;
        } while (b != start);

        // Inserting a new entry:
        if (!found) {
            if (entries_ > size_ /** kLoadFactor*/) { //doesn't consider loadfactor, the tablesize is pre-defined 
                //VLOG(0) << "resizing... " << size_ << " : " << (int)(1 + size_ * 2) << " entries "<< entries_;
                //entries_-=1;
                resize((int) (1 + size_ * 2));
                put(k, v1, vec);
                ++entries_;
                //VLOG(0) << "if entries_: " << entries_<<"  key: "<<k;
            } else {
                buckets_[b].in_use = 1;
                buckets_[b].k = k;
                buckets_[b].v1 = v1;
                buckets_[b].vec = vec;
                ++entries_;
            }
        } else {
            // Replacing an existing entry
            buckets_[b].v1 = v1;
            buckets_[b].vec = vec;
        }
    }

    template<class K, class V1>
    void CopyTable<K, V1>::update(const K &k, const V1 &v) {
        int start = bucket_for_key(k);
        buckets_[start].v1 = v;
    }

    template<class K, class V1>
    void CopyTable<K, V1>::accumulate(const K &k, const V1 &v) {
        int start = bucket_for_key(k);
        buckets_[start].v1 += v;
    }

    template<class K, class V1>
    bool CopyTable<K, V1>::remove(const K &k) {

    }

    template <class K, class V1>
    void CopyTable<K, V1>::serializeToFile(TableCoder *out) {

    }

    template <class K, class V1>
    void CopyTable<K, V1>::serializeToNet(KVPairCoder *out) {

    }

    template <class K, class V1>
    void CopyTable<K, V1>::deserializeFromFile(TableCoder *in, DecodeIteratorBase *itbase) {

    }

    template <class K, class V1>
    void CopyTable<K, V1>::deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *itbase) {

    }

    //it can also be used to generate snapshot, but currently in order to measure the performance we skip this step, 
    //but focus on termination check

    template <class K, class V1>
    void CopyTable<K, V1>::serializeToSnapshot(const string& f, long* updates, double* totalF2) {

    }

}

#endif	/* COPYTABLE_H */

