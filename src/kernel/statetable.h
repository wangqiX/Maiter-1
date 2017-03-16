#ifndef SPARSE_MAP_H_
#define SPARSE_MAP_H_

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

namespace dsm {

    static const int sample_size = 1000;

    template <class K, class V1, class V2, class V3>
    struct ClutterRecord;

    template <class K, class V1, class V2, class V3>
    class StateTable :
    public LocalTable,
    public TypedTable<K, V1, V2, V3>,
    private boost::noncopyable { //1
    private:
#pragma pack(push, 1)

        struct Bucket {
            K k;
            V1 v1;
            V2 v2;
            V3 v3;
            V1 priority;
            bool in_use;
            bool is_copy;
        };
#pragma pack(pop)

    public:
        typedef FileDecodeIterator<K, V1, V2, V3> FileUpdateDecoder;
        typedef NetDecodeIterator<K, V1> NetUpdateDecoder;

        struct Iterator : public TypedTableIterator<K, V1, V2, V3> {//2

            Iterator(StateTable<K, V1, V2, V3>& parent, bool bfilter) : pos(-1), parent_(parent) {
                pos = -1; //pos(-1) doesn't work
                /*
                 this filter is very important in large-scale experiment, if there is no such control,
                 * many useless parsing will occur.,degrading the performance a lot
             
                 */
                if (bfilter) {
                    //check if there is a change
                    b_no_change = true;

                    //random number generator
                    boost::mt19937 gen(time(0));
                    boost::uniform_int<> dist(0, parent_.buckets_.size() - 1);
                    boost::variate_generator<boost::mt19937&, boost::uniform_int<> > rand_num(gen, dist);

                    defaultv = ((IterateKernel<K, V1, V3>*)parent_.info_.iterkernel)->default_v();
                    int i;
                    for (i = 0; i < sample_size && b_no_change; i++) {
                        int rand_pos = rand_num();
                        while (!parent_.buckets_[rand_pos].in_use) {
                            rand_pos = rand_num();
                        }

                        b_no_change = b_no_change && parent_.buckets_[rand_pos].v1 != defaultv;
                    }
                } else {
                    b_no_change = false;
                }

                //Next();
            }

            virtual ~Iterator() {
            }

            Marshal<K>* kmarshal() {
                return parent_.kmarshal();
            }

            Marshal<V1>* v1marshal() {
                return parent_.v1marshal();
            }

            Marshal<V2>* v2marshal() {
                return parent_.v2marshal();
            }

            Marshal<V3>* v3marshal() {
                return parent_.v3marshal();
            }

            bool Next() {
                do {
                    ++pos;
                    //cout << "pos now is " << pos << " v1 " << parent_.buckets_[pos].v1 << endl;
                } while (pos < parent_.size_ && (/*parent_.buckets_[pos].v1 == defaultv || */!parent_.buckets_[pos].in_use));

                if (pos >= parent_.size_) {
                    return false;
                } else {
                    return true;
                }
            }

            bool done() {
                //cout<< "pos " << pos << "\tsize" << parent_.size_ << endl;
                return pos + 1 == parent_.size_;
            }

            const K& key() {
                return parent_.buckets_[pos].k;
            }

            V1& value1() {
                return parent_.buckets_[pos].v1;
            }

            V2& value2() {
                return parent_.buckets_[pos].v2;
            }

            V3& value3() {
                return parent_.buckets_[pos].v3;
            }

            bool is_copy() {
                return parent_.buckets_[pos].is_copy;
            }

            int pos;
            StateTable<K, V1, V2, V3> &parent_;
            bool b_no_change;
            V1 defaultv;
        }; //2

        struct ScheduledIterator : public TypedTableIterator<K, V1, V2, V3> {//3

            ScheduledIterator(StateTable<K, V1, V2, V3>& parent, bool bfilter) : pos(-1), parent_(parent) {

                pos = -1;
                b_no_change = true;

                //random number generator
                boost::mt19937 gen(time(0));
                boost::uniform_int<> dist(0, parent_.buckets_.size() - 1);
                boost::variate_generator<boost::mt19937&, boost::uniform_int<> > rand_num(gen, dist);

                V1 defaultv = ((IterateKernel<K, V1, V3>*)parent_.info_.iterkernel)->default_v();

                if (parent_.entries_ <= sample_size) {
                    //if table size is less than the sample set size, schedule them all
                    int i;
                    for (i = 0; i < parent_.size_; i++) {
                        if (parent_.buckets_[i].in_use) {
                            scheduled_pos.push_back(i);
                            b_no_change = b_no_change && parent_.buckets_[i].v1 != defaultv;
                        }
                    }
                    if (b_no_change && bfilter) return;
                    if (!bfilter) b_no_change = false;
                } else {
                    //sample random pos, the sample reflect the whole data set more or less
                    vector<int> sampled_pos;
                    int i;
                    int trials = 0;
                    for (i = 0; i < sample_size; i++) {
                        int rand_pos = rand_num();
                        trials++;
                        while (!parent_.buckets_[rand_pos].in_use) {
                            rand_pos = rand_num();
                            trials++;
                        }
                        sampled_pos.push_back(rand_pos);

                        b_no_change = b_no_change && parent_.buckets_[rand_pos].v1 == defaultv;
                    }

                    if (b_no_change && bfilter) return;
                    if (!bfilter) b_no_change = false;

                    /*
                    //determine priority
                    for(i=0; i<parent_.size_; i++){
                        if(parent_.buckets_[i].v1 == defaultv) continue;
                        V sum = ((IterateKernel<V1>*)parent.info_.iterkernel)->accumulate(&parent_.buckets_[i].v1, parent_.buckets_[i].v2);
                        parent_.buckets_[i].priority = abs(sum - parent_.buckets_[i].v2);
                    }
                     */

                    //get the cut index, everything larger than the cut will be scheduled
                    sort(sampled_pos.begin(), sampled_pos.end(), compare_priority(parent_));
                    int cut_index = sample_size * parent_.info_.schedule_portion;
                    V1 threshold = parent_.buckets_[sampled_pos[cut_index]].priority;
                    //V1 threshold = ((Scheduler<K, V1>*)parent_.info_.scheduler)->priority(parent_.buckets_[sampled_pos[cut_index]].k, parent_.buckets_[sampled_pos[cut_index]].v1);

                    VLOG(1) << "cut index " << cut_index << " theshold " << threshold << " pos " << sampled_pos[cut_index] << " max " << parent_.buckets_[sampled_pos[0]].v1;

                    if (cut_index == 0 || parent_.buckets_[sampled_pos[0]].priority == threshold) {
                        //to avoid non eligible records
                        int i;
                        for (i = 0; i < parent_.size_; i++) {
                            if (!parent_.buckets_[i].in_use) continue;
                            if (parent_.buckets_[i].v1 == defaultv) continue;

                            if (parent_.buckets_[i].priority >= threshold) {
                                scheduled_pos.push_back(i);
                            }
                        }
                    } else {
                        int i;
                        for (i = 0; i < parent_.size_; i++) {
                            if (!parent_.buckets_[i].in_use) continue;
                            if (parent_.buckets_[i].v1 == defaultv) continue;

                            if (parent_.buckets_[i].priority > threshold) {
                                scheduled_pos.push_back(i);
                            }
                        }
                    }
                }

                VLOG(1) << "table size " << parent_.buckets_.size() << " workerid " << parent_.id() << " scheduled " << scheduled_pos.size();
                //Next();
            }

            virtual ~ScheduledIterator() {
            }

            Marshal<K>* kmarshal() {
                return parent_.kmarshal();
            }

            Marshal<V1>* v1marshal() {
                return parent_.v1marshal();
            }

            Marshal<V2>* v2marshal() {
                return parent_.v2marshal();
            }

            Marshal<V3>* v3marshal() {
                return parent_.v3marshal();
            }

            bool Next() {
                ++pos;
                return true;
            }

            bool done() {
                return pos + 1 == scheduled_pos.size();
            }

            const K& key() {
                return parent_.buckets_[scheduled_pos[pos]].k;
            }

            V1& value1() {
                return parent_.buckets_[scheduled_pos[pos]].v1;
            }

            V2& value2() {
                return parent_.buckets_[scheduled_pos[pos]].v2;
            }

            V3& value3() {
                return parent_.buckets_[scheduled_pos[pos]].v3;
            }

            bool is_copy() {
                return parent_.buckets_[scheduled_pos[pos]].is_copy;
            }

            class compare_priority {
            public:
                StateTable<K, V1, V2, V3> &parent;

                compare_priority(StateTable<K, V1, V2, V3> &inparent) : parent(inparent) {
                }

                bool operator()(const int a, const int b) {
                    return parent.buckets_[a].priority > parent.buckets_[b].priority;
                    //return ((Accumulator<V1>*)parent.info_.accum)->priority(parent.buckets_[a].v1, parent.buckets_[a].v2)
                    //> ((Accumulator<V1>*)parent.info_.accum)->priority(parent.buckets_[b].v1, parent.buckets_[b].v2);
                }
            };

            int pos;
            StateTable<K, V1, V2, V3> &parent_;
            double portion;
            vector<int> scheduled_pos;
            bool b_no_change;
        }; //3

        //for termination check

        struct EntirePassIterator : public TypedTableIterator<K, V1, V2, V3>, public LocalTableIterator<K, V2> {//4

            EntirePassIterator(StateTable<K, V1, V2, V3>& parent) : pos(-1), parent_(parent) {
                //Next();
                total = 0;
                pos = -1;
                defaultv = ((IterateKernel<K, V1, V3>*)parent_.info_.iterkernel)->default_v();
            }

            virtual ~EntirePassIterator() {
            }

            Marshal<K>* kmarshal() {
                return parent_.kmarshal();
            }

            Marshal<V1>* v1marshal() {
                return parent_.v1marshal();
            }

            Marshal<V2>* v2marshal() {
                return parent_.v2marshal();
            }

            Marshal<V3>* v3marshal() {
                return parent_.v3marshal();
            }

            bool Next() {
                do {
                    ++pos;
                } while (pos < parent_.size_ && !parent_.buckets_[pos].in_use);
                total++;

                if (pos >= parent_.size_) {
                    return false;
                } else {
                    return true;
                }
            }

            bool done() {
                //cout<< "entire pos " << pos << "\tsize" << parent_.size_ << endl;
                return pos + 1 == parent_.size_;
            }

            V1 defaultV() {
                return defaultv;
            }

            const K& key() {
                return parent_.buckets_[pos].k;
            }

            V1& value1() {
                return parent_.buckets_[pos].v1;
            }

            V2& value2() {
                return parent_.buckets_[pos].v2;
            }

            V3& value3() {
                return parent_.buckets_[pos].v3;
            }

            bool is_copy() {
                return parent_.buckets_[pos].is_copy;
            }

            int pos;
            StateTable<K, V1, V2, V3> &parent_;
            int total;
            V1 defaultv;
        }; //4

        struct Factory : public TableFactory {

            TableBase* New() {
                return new StateTable<K, V1, V2, V3>();
            }
        };

        // Construct a StateTable with the given initial size; it will be expanded as necessary.
        StateTable(int size = 1);

        ~StateTable() {
        }

        void Init(const TableDescriptor* td) {
            TableBase::Init(td);
        }

        V1 getF1(const K& k);
        V2 getF2(const K& k);
        V3 getF3(const K& k);
        ClutterRecord<K, V1, V2, V3> get(const K& k);
        bool contains(const K& k);
        void putc(const K &k, const V1 &v1, V3 &v3, int shard);
        void put(const K& k, const V1& v1, const V2& v2, const V3& v3);
        void put2(const K &k, const V1 &v1, const V2 &v2, const V3 &v3);
        void setCopy(const K&k);
        void setNoCopy(const K&k);
        void updateF1(const K& k, const V1& v);
        void updateF2(const K& k, const V2& v);
        void updateF3(const K& k, const V3& v);
        void accumulateFFF(const K &k, const V1 &v, int shard);
        void accumulateFF(const K &k, const V1 &v, int shard);
        void accumulateF1(const K& k, const V1& v);
        void accumulateF2(const K& k, const V2& v);
        void accumulateF3(const K& k, const V3& v);

        bool remove(const K& k) {
            LOG(FATAL) << "Not implemented.";
            return false;
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

        bool compare_priority(int i, int j) {
            return buckets_[i].priority > buckets_[j].priority;
            //return ((Scheduler<K, V1>*)info_.scheduler)->priority(buckets_[i].k, buckets_[i].v1)
            //> ((Scheduler<K, V1>*)info_.scheduler)->priority(buckets_[j].k, buckets_[j].v1);
        }

        TableIterator *get_iterator(TableHelper* helper, bool bfilter) {
            if (terminated_) return NULL; //if get term signal, return null to tell program terminate
            helper->FlushUpdates();
            boost::this_thread::sleep(boost::posix_time::seconds(0.1));
            helper->HandlePutRequest();

            Iterator* iter = new Iterator(*this, true);
            int trial = 0;
            while (iter->b_no_change) {
                VLOG(1) << "wait for put";
                delete iter;
                helper->FlushUpdates();
                boost::this_thread::sleep(boost::posix_time::seconds(1));
                helper->HandlePutRequest();

                if (terminated_) return NULL; //if get term signal, return null to tell program terminate

                iter = new Iterator(*this, bfilter);

                trial++;
                if (trial >= 10) {
                    delete iter;
                    EntirePassIterator* entireIter = new EntirePassIterator(*this);

                    total_curr = 0;
                    while (!entireIter->done()) {
                        bool cont = entireIter->Next();
                        if (!cont) break;

                        total_curr += entireIter->value2();
                    }

                    VLOG(1) << "send term check since many times trials " << total_curr << "and perform a pass of the current table";
                    helper->SendTermcheck(-1, total_updates, total_curr);

                    return entireIter;
                }

            }

            return iter;
        }

        TableIterator *schedule_iterator(TableHelper* helper, bool bfilter) {
            if (terminated_) return NULL;
            helper->FlushUpdates();
            boost::this_thread::sleep(boost::posix_time::seconds(0.1));
            helper->HandlePutRequest();

            ScheduledIterator* iter = new ScheduledIterator(*this, bfilter);
            int trial = 0;
            while (iter->b_no_change) {
                VLOG(1) << "wait for put, send buffered updates";
                delete iter;
                helper->FlushUpdates();
                boost::this_thread::sleep(boost::posix_time::seconds(1));
                helper->HandlePutRequest();

                if (terminated_) return NULL; //if get term signal, return null to tell program terminate

                iter = new ScheduledIterator(*this, bfilter);

                trial++;
                if (trial >= 10) {
                    delete iter;
                    EntirePassIterator* entireIter = new EntirePassIterator(*this);

                    total_curr = 0;
                    while (!entireIter->done()) {
                        entireIter->Next();
                        total_curr += entireIter->value2();
                    }

                    VLOG(1) << "send term check since many times trials " << total_curr << "and perform a pass of the current table";
                    helper->SendTermcheck(-1, total_updates, total_curr);

                    return entireIter;
                }
            }

            return iter;
        }

        TableIterator *entirepass_iterator(TableHelper* helper) {
            return new EntirePassIterator(*this);
        }


        void serializeToFile(TableCoder *out);
        void serializeToNet(KVPairCoder *out);
        void deserializeFromFile(TableCoder *in, DecodeIteratorBase *itbase);
        void deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *itbase);
        void serializeToSnapshot(const string& f, long *updates, double *totalF2);

        Marshal<K>* kmarshal() {
            return ((Marshal<K>*)info_.key_marshal);
        }

        Marshal<V1>* v1marshal() {
            return ((Marshal<V1>*)info_.value1_marshal);
        }

        Marshal<V2>* v2marshal() {
            return ((Marshal<V2>*)info_.value2_marshal);
        }

        Marshal<V3>* v3marshal() {
            return ((Marshal<V3>*)info_.value3_marshal);
        }

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

    }; //1

    template <class K, class V1, class V2, class V3>
    StateTable<K, V1, V2, V3>::StateTable(int size)
    : buckets_(0), entries_(0), size_(0), total_curr(0), total_updates(0) {
        clear();

        VLOG(1) << "new statetable size " << size;
        resize(size);
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::serializeToFile(TableCoder *out) {
        Iterator *i = (Iterator*) get_iterator(NULL, false);
        string k, v1, v2, v3;
        while (!i->done()) {
            k.clear();
            v1.clear();
            v2.clear();
            v3.clear();
            ((Marshal<K>*)info_.key_marshal)->marshal(i->key(), &k);
            ((Marshal<V1>*)info_.value1_marshal)->marshal(i->value1(), &v1);
            ((Marshal<V2>*)info_.value2_marshal)->marshal(i->value2(), &v2);
            ((Marshal<V3>*)info_.value3_marshal)->marshal(i->value3(), &v3);
            out->WriteEntryToFile(k, v1, v2, v3);
            i->Next();
        }
        delete i;
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::serializeToNet(KVPairCoder *out) {
        Iterator *i = (Iterator*) get_iterator(NULL, false);
        string k, v1;
        while (!i->done()) {
            k.clear();
            v1.clear();
            ;
            ((Marshal<K>*)info_.key_marshal)->marshal(i->key(), &k);
            ((Marshal<V1>*)info_.value1_marshal)->marshal(i->value1(), &v1);
            out->WriteEntryToNet(k, v1);
            i->Next();
        }
        delete i;
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::deserializeFromFile(TableCoder *in, DecodeIteratorBase *itbase) {
        FileUpdateDecoder* it = static_cast<FileUpdateDecoder*> (itbase);
        K k;
        V1 v1;
        V2 v2;
        V3 v3;
        string kt, v1t, v2t, v3t;

        it->clear();
        while (in->ReadEntryFromFile(&kt, &v1t, &v2t, &v3t)) {
            ((Marshal<K>*)info_.key_marshal)->unmarshal(kt, &k);
            ((Marshal<V1>*)info_.value1_marshal)->unmarshal(v1t, &v1);
            ((Marshal<V2>*)info_.value2_marshal)->unmarshal(v2t, &v2);
            ((Marshal<V3>*)info_.value3_marshal)->unmarshal(v3t, &v3);
            it->append(k, v1, v2, v3);
        }
        it->rewind();
        return;
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *itbase) {
        NetUpdateDecoder* it = static_cast<NetUpdateDecoder*> (itbase);
        K k;
        V1 v1;
        string kt, v1t;

        it->clear();
        while (in->ReadEntryFromNet(&kt, &v1t)) {
            ((Marshal<K>*)info_.key_marshal)->unmarshal(kt, &k);
            ((Marshal<V1>*)info_.value1_marshal)->unmarshal(v1t, &v1);
            it->append(k, v1);
        }
        it->rewind();
        return;
    }

    //it can also be used to generate snapshot, but currently in order to measure the performance we skip this step, 
    //but focus on termination check

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::serializeToSnapshot(const string& f, long* updates, double* totalF2) {
        total_curr = 0;
        EntirePassIterator* entireIter = new EntirePassIterator(*this);
        total_curr = static_cast<double> (((TermChecker<K, V2>*)info_.termchecker)->estimate_prog(entireIter));
        delete entireIter;
        *updates = total_updates;
        *totalF2 = total_curr;
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::resize(int64_t size) {
        CHECK_GT(size, 0);
        if (size_ == size)
            return;

        std::vector<Bucket> old_b = buckets_;
        //int old_entries = entries_;

        // LOG(INFO) << "Rehashing.vd.. " << entries_ << " : " << size_ << " -> " << size;

        buckets_.resize(size);
        size_ = size;
        clear();
        //LOG(INFO) << "Rehashing... " << entries_ << " : " << size_ << " -> " << size;
        for (int i = 0; i < old_b.size(); ++i) {
            if (old_b[i].in_use) {
                put(old_b[i].k, old_b[i].v1, old_b[i].v2, old_b[i].v3);
                // LOG(INFO) << "copy: " << old_b[i].k;
            }
        }

        //CHECK_EQ(old_entries, entries_); 
    }

    template <class K, class V1, class V2, class V3>
    bool StateTable<K, V1, V2, V3>::contains(const K& k) {
        return bucket_for_key(k) != -1;
    }

    template <class K, class V1, class V2, class V3>
    V1 StateTable<K, V1, V2, V3>::getF1(const K& k) {
        int b = bucket_for_key(k);
        //The following key display is a hack hack hack and only yields valid
        //results for ints.  It will display nonsense for other types.
        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">";

        return buckets_[b].v1;
    }

    template <class K, class V1, class V2, class V3>
    V2 StateTable<K, V1, V2, V3>::getF2(const K& k) {
        int b = bucket_for_key(k);
        //The following key display is a hack hack hack and only yields valid
        //results for ints.  It will display nonsense for other types.
        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">";

        return buckets_[b].v2;
    }

    template <class K, class V1, class V2, class V3>
    V3 StateTable<K, V1, V2, V3>::getF3(const K& k) {
        int b = bucket_for_key(k);
        //The following key display is a hack hack hack and only yields valid
        //results for ints.  It will display nonsense for other types.
        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">";

        return buckets_[b].v3;
    }

    template <class K, class V1, class V2, class V3>
    ClutterRecord<K, V1, V2, V3> StateTable<K, V1, V2, V3>::get(const K& k) {
        int b = bucket_for_key(k);
        //The following key display is a hack hack hack and only yields valid
        //results for ints.  It will display nonsense for other types.
        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">";

        return ClutterRecord<K, V1, V2, V3>(k, buckets_[b].v1, buckets_[b].v2, buckets_[b].v3);
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::updateF1(const K& k, const V1& v) {
        int b = bucket_for_key(k);

        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">";

        buckets_[b].v1 = v;
        buckets_[b].priority = 0; //didn't use priority function, assume the smallest priority is 0
        total_updates++;
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::updateF2(const K& k, const V2& v) {
        int b = bucket_for_key(k);

        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">";

        buckets_[b].v2 = v;
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::updateF3(const K& k, const V3& v) {
        int b = bucket_for_key(k);

        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">";

        buckets_[b].v3 = v;
    }

    template<class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::accumulateFFF(const K& k, const V1& v, int shard) {

    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::accumulateFF(const K& k, const V1& v, int shard) {

    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::accumulateF1(const K& k, const V1& v) {
        int b = bucket_for_key(k);

        //cout << "accumulate " << k << "\t" << v << endl;
        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">" << "key: " << k;
        ((IterateKernel<K, V1, V3>*)info_.iterkernel)->accumulate(buckets_[b].v1, v);
        ((IterateKernel<K, V1, V3>*)info_.iterkernel)->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1);

    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::accumulateF2(const K& k, const V2& v) {
        int b = bucket_for_key(k);

        CHECK_NE(b, -1) << "No entry for requested key <" << *((int*) &k) << ">";
        ((IterateKernel<K, V1, V3>*)info_.iterkernel)->accumulate(buckets_[b].v2, v);
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::accumulateF3(const K& k, const V3& v) {

    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::setCopy(const K& k) {
        int start = bucket_idx(k);
        buckets_[start].is_copy = 1;
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::setNoCopy(const K& k) {
        int start = bucket_idx(k);
        buckets_[start].is_copy = 0;
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::putc(const K &k, const V1 &v1, V3 &v3, int shard) {

    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::put(const K& k, const V1& v1, const V2& v2, const V3& v3) {
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
                put(k, v1, v2, v3);
                ++entries_;
                //VLOG(0) << "if entries_: " << entries_<<"  key: "<<k;
            } else {
                buckets_[b].in_use = 1;
                buckets_[b].k = k;
                buckets_[b].v1 = v1;
                buckets_[b].v2 = v2;
                buckets_[b].v3 = v3;
                ((IterateKernel<K, V1, V3>*)info_.iterkernel)->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1);
                ++entries_;
                //VLOG(0) << "else entries_: " << entries_<<"  key: "<<k;
            }
        } else {
            // Replacing an existing entry
            buckets_[b].v1 = v1;
            buckets_[b].v2 = v2;
            buckets_[b].v3 = v3;
            ((IterateKernel<K, V1, V3>*)info_.iterkernel)->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1);
        }
    }

    template <class K, class V1, class V2, class V3>
    void StateTable<K, V1, V2, V3>::put2(const K& k, const V1& v1, const V2& v2, const V3& v3) {
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
                put(k, v1, v2, v3);
                ++entries_;
                //VLOG(0) << "if entries_: " << entries_<<"  key: "<<k;
            } else {
                buckets_[b].in_use = 1;
                buckets_[b].k = k;
                buckets_[b].v1 = v1;
                buckets_[b].v2 = v2;
                buckets_[b].v3 = v3;
                ((IterateKernel<K, V1, V3>*)info_.iterkernel)->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1);
                ++entries_;
                //VLOG(0) << "else entries_: " << entries_<<"  key: "<<k;
            }
        } else {
            // Replacing an existing entry
            buckets_[b].v1 = v1;
            buckets_[b].v2 = v2;
            buckets_[b].v3 = v3;
            ((IterateKernel<K, V1, V3>*)info_.iterkernel)->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1);
        }
    }
}
#endif /* SPARSE_MAP_H_ */
