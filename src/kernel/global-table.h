#ifndef GLOBALTABLE_H_
#define GLOBALTABLE_H_

#include "table.h"
#include "local-table.h"

#include "util/file.h"
#include "util/rpc.h"

#include <queue>

//#define GLOBAL_TABLE_USE_SCOPEDLOCK

DECLARE_int32(bufmsg);
DECLARE_int32(bufmsg2);

namespace dsm {

    class Worker;
    class Master;

    // Encodes table entries using the passed in TableData protocol buffer.

    struct ProtoTableCoder : public TableCoder {
        ProtoTableCoder(const TableData* in);
        virtual void WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3);
        virtual bool ReadEntryFromFile(string* k, string *v1, string *v2, string *v3);

        int read_pos_;
        TableData *t_;
    };

    // Encodes table entries using the passed in TableData protocol buffer.

    struct ProtoKVPairCoder : public KVPairCoder {
        ProtoKVPairCoder(const KVPairData* in);
        virtual void WriteEntryToNet(StringPiece k, StringPiece v1);
        virtual bool ReadEntryFromNet(string* k, string *v1);

        int read_pos_;
        KVPairData *t_;
    };

    struct PartitionInfo {

        PartitionInfo() : dirty(false), tainted(false) {
        }
        bool dirty;
        bool tainted;
        ShardInfo sinfo;
    };

    class GlobalTable : virtual public TableBase {
    public:
        virtual void UpdatePartitions(const ShardInfo& sinfo) = 0;
        virtual TableIterator* get_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM) = 0;
        virtual TableIterator* get_iterator2(int shard, bool bfilter) = 0;
        virtual bool is_local_shard(int shard) = 0;
        virtual bool is_local_key(const StringPiece &k) = 0;

        virtual PartitionInfo* get_partition_info(int shard) = 0;
        virtual LocalTable* get_partition(int shard) = 0;

        virtual bool tainted(int shard) = 0;
        virtual int owner(int shard) = 0;

    protected:
        friend class Worker;
        friend class Master;

        virtual int64_t shard_size(int shard) = 0;
    };

    class MutableGlobalTable : virtual public GlobalTable {
    public:
        // Handle updates from the master or other workers.
        virtual void SendUpdates() = 0;
        virtual void SendUpdates2() = 0;
        virtual void ApplyUpdates(const KVPairData& req) = 0;
        virtual void HandlePutRequests() = 0;
        virtual void TermCheck() = 0;

        virtual int pending_write_bytes() = 0;

        virtual void clear() = 0;
        virtual void resize(int64_t new_size) = 0;

        // Exchange the content of this table with that of table 'b'.
        virtual void swap(GlobalTable *b) = 0;
    protected:
        friend class Worker;
        virtual void local_swap(GlobalTable *b) = 0;
    };

    class GlobalTableBase : virtual public GlobalTable {
    public:
        virtual ~GlobalTableBase();

        void Init(const TableDescriptor *tinfo);

        void UpdatePartitions(const ShardInfo& sinfo);

        virtual TableIterator* get_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM) = 0;
        virtual TableIterator* get_iterator2(int shard, bool bfilter) = 0;
        virtual bool is_local_shard(int shard);
        virtual bool is_local_key(const StringPiece &k);

        int64_t shard_size(int shard);

        PartitionInfo* get_partition_info(int shard) {
            return &partinfo_[shard];
        }

        LocalTable* get_partition(int shard) {
            return partitions_[shard];
        }

        bool tainted(int shard) {
            return get_partition_info(shard)->tainted;
        }

        int owner(int shard) {
            return get_partition_info(shard)->sinfo.owner();
        }
    protected:
        virtual int shard_for_key_str(const StringPiece& k) = 0;

        int worker_id_;

        vector<LocalTable*> partitions_;
        vector<LocalTable*> cpartitions_;
        vector<LocalTable*> cache_;

        boost::recursive_mutex& mutex() {
            return m_;
        }
        boost::recursive_mutex m_;

        boost::mutex& trigger_mutex() {
            return m_trig_;
        }
        boost::mutex m_trig_;

        vector<PartitionInfo> partinfo_;

        struct CacheEntry {
            double last_read_time;
            string value;
        };

        unordered_map<StringPiece, CacheEntry> remote_cache_;
    };

    class MutableGlobalTableBase :
    virtual public GlobalTableBase,
    virtual public MutableGlobalTable,
    virtual public Checkpointable {
    public:

        MutableGlobalTableBase() {
            pending_writes_ = 0;
            cpending_writes_ = 0;
            snapshot_index = 0;
            sent_bytes_ = 0;
            sendtime = 0;
        }

        void BufSend();
        void SendUpdates();
        void SendUpdates2();
        virtual void ApplyUpdates(const KVPairData& req) = 0;
        void HandlePutRequests();
        void TermCheck();

        int pending_write_bytes();

        void clear();
        void resize(int64_t new_size);

        void start_checkpoint(const string& f);
        void write_delta(const KVPairData& d);
        void finish_checkpoint();
        void restore(const string& f);

        void swap(GlobalTable *b);

        int64_t sent_bytes_;
        Timer timer;
        int timerindex;

    protected:
        int64_t pending_writes_;
        int64_t cpending_writes_;
        int snapshot_index;
        void local_swap(GlobalTable *b);
        void termcheck();


        //double send_overhead;
        //double objectcreate_overhead;
        int sendtime;
    };

    template <class K, class V1, class V2, class V3>
    class TypedGlobalTable :
    virtual public GlobalTable,
    public MutableGlobalTableBase,
    public TypedTable<K, V1, V2, V3>,
    private boost::noncopyable {
    public:

        virtual ~TypedGlobalTable() {
        }

        bool initialized() {
            return binit;
        }

        typedef pair<K, V1> KVPair;
        typedef TypedTableIterator<K, V1, V2, V3> Iterator;
        typedef CTypedTableIterator<K, V1> iterator2;
        typedef NetDecodeIterator<K, V1> NetUpdateDecoder;

        virtual void Init(const TableDescriptor *tinfo) {
            GlobalTableBase::Init(tinfo);

            for (int i = 0; i < partitions_.size(); ++i) {
                partitions_[i] = create_deltaT(i);
            }

            for (int i = 0; i < cpartitions_.size(); ++i) {
                cpartitions_[i] = create_deltaT(i);
            }

            //Clear the update queue, just in case
            update_queue.clear();
            binit = false;
        }

        void InitStateTable() {
            for (int i = 0; i < partitions_.size(); ++i) {
                if (is_local_shard(i)) {
                    partitions_[i] = create_localT(i);
                }
            }
            for (int i = 0; i < cpartitions_.size(); ++i) {
                if (is_local_shard(i)) {
                    cpartitions_[i] = create_copyT(i);
                }
            }
            binit = true;
        }

        int get_shard(const K& k);
        V1 get_localF1(const K& k);
        V2 get_localF2(const K& k);
        V3 get_localF3(const K& k);

        // Store the given key-value pair in this hash. If 'k' has affinity for a
        // remote thread, the application occurs immediately on the local host,
        // and the update is queued for transmission to the owner.
        void putc(const K &k, const V1 &v1, V3 &v3, int shard);
        void put(const K &k, const V1 &v1, const V2 &v2, const V3 &v3);
        void put2(const K &k, const V1 &v1, const V2 &v2, const V3 &v3);
        void setCopy(const K&k);
        void setNoCopy(const K&k);
        void updateF1(const K &k, const V1 &v);
        void updateF2(const K &k, const V2 &v);
        void updateF3(const K &k, const V3 &v);
        void enqueue_updateF1(K k, V1 v);
        void accumulateFFF(const K &k, const V1 &v, int shard);
        void accumulateFF(const K &k, const V1 &v, int shard);
        void accumulateF1(const K &k, const V1 &v); // 2 TypeGloobleTable :TypeTable
        void accumulateF2(const K &k, const V2 &v);
        void accumulateF3(const K &k, const V3 &v);

        // Return the value associated with 'k', possibly blocking for a remote fetch.
        ClutterRecord<K, V1, V2, V3> get(const K &k);
        V1 getF1(const K &k);
        V2 getF2(const K &k);
        V3 getF3(const K &k);
        bool contains(const K &k);
        bool remove(const K &k);

        TableIterator* get_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM);
        TableIterator* get_iterator2(int shard, bool bfilter);

        TypedTable<K, V1, V2, V3>* partition(int idx) {
            return dynamic_cast<TypedTable<K, V1, V2, V3>*> (partitions_[idx]);
        }

        PTypedTable<K, V1, V3>* deltaT(int idx) {
            return dynamic_cast<PTypedTable<K, V1, V3>*> (partitions_[idx]);
        }

        PTypedTable<K, V1, V3>* copyT(int idx) {
            return dynamic_cast<PTypedTable<K, V1, V3>*> (cpartitions_[idx]);
        }

        CTypedTable<K, V1>* cpartition(int idx) {
            return dynamic_cast<CTypedTable<K, V1>*> (cpartitions_[idx]);
        }

        virtual TypedTableIterator<K, V1, V2, V3>* get_typed_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM) {
            return static_cast<TypedTableIterator<K, V1, V2, V3>*> (get_iterator(shard, bfilter, fetch_num));
        }

        virtual CTypedTableIterator<K, V1>* get_copy_iterator(int shard, bool bfilter) {
            return static_cast<CTypedTableIterator<K, V1>*> (get_iterator2(shard, bfilter));
        }

        TypedTableIterator<K, V1, V2, V3>* get_entirepass_iterator(int shard) {
            return (TypedTableIterator<K, V1, V2, V3>*) partitions_[shard]->entirepass_iterator(this->helper());
        }

        void ApplyUpdates(const dsm::KVPairData& req) {
            boost::recursive_mutex::scoped_lock sl(mutex());

            //VLOG(2) << "applying updates, from " << req.source();

            if (!is_local_shard(req.shard())) {
                LOG_EVERY_N(INFO, 1000)
                        << "Forwarding push request from: " << MP(id(), req.shard())
                        << " to " << owner(req.shard());
            }

            // Changes to support centralized of triggers <CRM>
            ProtoKVPairCoder c(&req);

            NetUpdateDecoder it;
            partitions_[req.shard()]->deserializeFromNet(&c, &it);
            int shard = req.shard();
            if (is_local_shard(get_shard(it.key()))) {
                for (; !it.done(); it.Next()) {
                    //VLOG(2) << this->owner(req.shard()) << ":" << req.shard() << "read from remote " << it.key() << ";" << it.value1();
                    accumulateF1(it.key(), it.value1());
                }
            } else {
                for (; !it.done(); it.Next()) {
                    accumulateFFF(it.key(), it.value1(), shard);
                }
            }


            TermCheck();
        }

        Marshal<K> *kmarshal() {
            return ((Marshal<K>*)info_.key_marshal);
        }

        Marshal<V1> *v1marshal() {
            return ((Marshal<V1>*)info_.value1_marshal);
        }

        Marshal<V2> *v2marshal() {
            return ((Marshal<V2>*)info_.value2_marshal);
        }

        Marshal<V3> *v3marshal() {
            return ((Marshal<V3>*)info_.value3_marshal);
        }

    protected:
        int shard_for_key_str(const StringPiece& k);
        virtual LocalTable* create_localT(int shard);
        virtual LocalTable* create_deltaT(int shard);
        virtual LocalTable* create_copyT(int shard);
        deque<KVPair> update_queue;
        bool binit;
    };

    static const int kWriteFlushCount = 1000000;

    template<class K, class V1, class V2, class V3>
    int TypedGlobalTable<K, V1, V2, V3>::get_shard(const K& k) {
        DCHECK(this != NULL);
        DCHECK(this->info().sharder != NULL);

        Sharder<K> *sharder = (Sharder<K>*)(this->info().sharder);
        int shard = (*sharder)(k, this->info().num_shards);
        DCHECK_GE(shard, 0);
        DCHECK_LT(shard, this->num_shards());
        return shard;
    }

    template<class K, class V1, class V2, class V3>
    int TypedGlobalTable<K, V1, V2, V3>::shard_for_key_str(const StringPiece& k) {
        return get_shard(unmarshal(static_cast<Marshal<K>*> (this->info().key_marshal), k));
    }

    template<class K, class V1, class V2, class V3>
    V1 TypedGlobalTable<K, V1, V2, V3>::get_localF1(const K& k) {
        int shard = this->get_shard(k);

        CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

        return partition(shard)->getF1(k);
    }

    template<class K, class V1, class V2, class V3>
    V2 TypedGlobalTable<K, V1, V2, V3>::get_localF2(const K& k) {
        int shard = this->get_shard(k);

        CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

        return partition(shard)->getF2(k);
    }

    template<class K, class V1, class V2, class V3>
    V3 TypedGlobalTable<K, V1, V2, V3>::get_localF3(const K& k) {
        int shard = this->get_shard(k);

        CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

        return partition(shard)->getF3(k);
    }

    // Store the given key-value pair in this hash. If 'k' has affinity for a
    // remote thread, the application occurs immediately on the local host,
    // and the update is queued for transmission to the owner.

    template <class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::setCopy(const K& k) {

    }

    template <class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::setNoCopy(const K& k) {

    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::putc(const K &k, const V1 &v1, V3 &v3, int shard) {
        cpartition(shard)->put(k, v1, v3);
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::put(const K &k, const V1 &v1, const V2 &v2, const V3 &v3) {
        int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif

        if (is_local_shard(shard)) {
            partition(shard)->put(k, v1, v2, v3);
            partition(shard)->setCopy(k);
        } else {
            VLOG(1) << "not local put";
            ++pending_writes_;
        }

        //if (pending_writes_ > kWriteFlushCount) {
        //SendUpdates();
        //}
        //BufSend();

        PERIODIC(0.1,{this->HandlePutRequests();});
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::put2(const K &k, const V1 &v1, const V2 &v2, const V3 &v3) {
        int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif

        if (is_local_shard(shard)) {
            partition(shard)->put(k, v1, v2, v3);
            partition(shard)->setNoCopy(k);
        } else {
            VLOG(1) << "not local put";
            ++pending_writes_;
        }

        //if (pending_writes_ > kWriteFlushCount) {
        //SendUpdates();
        //}
        //BufSend();

        PERIODIC(0.1,{this->HandlePutRequests();});
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::updateF1(const K &k, const V1 &v) {
        int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::mutex::scoped_lock sl(trigger_mutex());
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif

        if (is_local_shard(shard)) {
            partition(shard)->updateF1(k, v);

            //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
        } else {
            deltaT(shard)->update(k, v);

        }

        PERIODIC(0.1,{this->HandlePutRequests();});
        /*
          //Deal with updates enqueued inside triggers
          while(!update_queue.empty()) {
            KVPair thispair(update_queue.front());
            update_queue.pop_front();
            update(thispair.first,thispair.second);
          }
         */
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::updateF2(const K &k, const V2 &v) {
        int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::mutex::scoped_lock sl(trigger_mutex());
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif

        if (is_local_shard(shard)) {
            partition(shard)->updateF2(k, v);

            //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
        } else {
            VLOG(2) << "update F2 shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
        }
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::updateF3(const K &k, const V3 &v) {
        int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::mutex::scoped_lock sl(trigger_mutex());
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif

        if (is_local_shard(shard)) {
            partition(shard)->updateF3(k, v);

            //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
        } else {
            VLOG(2) << "update F3 shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
        }
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::accumulateFFF(const K& k, const V1& v, int shard) {

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::mutex::scoped_lock sl(trigger_mutex());
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
        cpartition(shard)->accumulate(k, v);

    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::accumulateFF(const K& k, const V1& v, int shard) {

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::mutex::scoped_lock sl(trigger_mutex());
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
        copyT(shard)->accumulate(k, v);
        ++cpending_writes_;
        if (cpending_writes_ > FLAGS_bufmsg2) {
            SendUpdates2();
        }

    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::accumulateF1(const K &k, const V1 &v) { //3
        int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::mutex::scoped_lock sl(trigger_mutex());
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif

        if (is_local_shard(shard)) {
            //VLOG(1) << this->owner(shard) << ":" << shard << " accumulate " << v << " on local " << k;
            partition(shard)->accumulateF1(k, v); //TypeTable
        } else {
            //VLOG(1) << this->owner(shard) << ":" << shard << " accumulate " << v << " on remote " << k;
            deltaT(shard)->accumulate(k, v);

            ++pending_writes_;
            if (pending_writes_ > FLAGS_bufmsg) {
                SendUpdates();

            }
            //BufSend();

            //PERIODIC(0.1, {this->HandlePutRequests();});
        }
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::accumulateF2(const K &k, const V2 &v) { // 1
        int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::mutex::scoped_lock sl(trigger_mutex());
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif

        if (is_local_shard(shard)) {
            partition(shard)->accumulateF2(k, v); // Typetable

            //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
        } else {
            VLOG(2) << "accumulate F2 shard " << shard << " local? " << " : " << is_local_shard(shard);
        }
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::accumulateF3(const K &k, const V3 &v) {
        int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::mutex::scoped_lock sl(trigger_mutex());
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif

        if (is_local_shard(shard)) {
            partition(shard)->accumulateF3(k, v);

            //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
        } else {
            VLOG(2) << "accumulate F3 shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
        }
    }

    template<class K, class V1, class V2, class V3>
    void TypedGlobalTable<K, V1, V2, V3>::enqueue_updateF1(K k, V1 v) {
        const KVPair thispair(k, v);
        update_queue.push_back(thispair);
    }

    // Return the value associated with 'k', possibly blocking for a remote fetch.

    template<class K, class V1, class V2, class V3>
    ClutterRecord<K, V1, V2, V3> TypedGlobalTable<K, V1, V2, V3>::get(const K &k) {
        int shard = this->get_shard(k);

        CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
        return partition(shard)->get(k);
    }

    // Return the value associated with 'k', possibly blocking for a remote fetch.

    template<class K, class V1, class V2, class V3>
    V1 TypedGlobalTable<K, V1, V2, V3>::getF1(const K &k) {
        int shard = this->get_shard(k);

        CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
        return partition(shard)->getF1(k);
    }

    // Return the value associated with 'k', possibly blocking for a remote fetch.

    template<class K, class V1, class V2, class V3>
    V2 TypedGlobalTable<K, V1, V2, V3>::getF2(const K &k) {
        int shard = this->get_shard(k);

        CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
        return partition(shard)->getF2(k);
    }

    // Return the value associated with 'k', possibly blocking for a remote fetch.

    template<class K, class V1, class V2, class V3>
    V3 TypedGlobalTable<K, V1, V2, V3>::getF3(const K &k) {
        int shard = this->get_shard(k);

        CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
        return partition(shard)->getF3(k);
    }

    template<class K, class V1, class V2, class V3>
    bool TypedGlobalTable<K, V1, V2, V3>::contains(const K &k) {
        int shard = this->get_shard(k);

        if (is_local_shard(shard)) {
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
            boost::recursive_mutex::scoped_lock sl(mutex());
#endif
            return partition(shard)->contains(k);
        } else {
            return false;
        }
    }

    template<class K, class V1, class V2, class V3>
    bool TypedGlobalTable<K, V1, V2, V3>::remove(const K &k) {
        int shard = this->get_shard(k);

        if (is_local_shard(shard)) {
            return partition(shard)->remove(k);
            return true;
        } else {
            return false;
        }
    }

    template<class K, class V1, class V2, class V3>
    LocalTable* TypedGlobalTable<K, V1, V2, V3>::create_localT(int shard) {
        TableDescriptor *linfo = new TableDescriptor(info());
        linfo->shard = shard;
        VLOG(2) << "create local statetable " << shard;
        LocalTable* t = (LocalTable*) info_.partition_factory->New();
        t->Init(linfo);

        return t;
    }

    template<class K, class V1, class V2, class V3>
    LocalTable* TypedGlobalTable<K, V1, V2, V3>::create_deltaT(int shard) {
        TableDescriptor *linfo = new TableDescriptor(info());
        linfo->shard = shard;
        //VLOG(2) << "create local deltatable " << shard;
        LocalTable* t = (LocalTable*) info_.deltaT_factory->New();
        t->Init(linfo);

        return t;
    }

    template<class K, class V1, class V2, class V3>
    LocalTable* TypedGlobalTable<K, V1, V2, V3>::create_copyT(int shard) {
        TableDescriptor *linfo = new TableDescriptor(info());
        linfo->shard = shard;
        //VLOG(2) << "create local copytable " << shard;
        LocalTable* t = (LocalTable*) info_.CopyT_factory->New();
        t->Init(linfo);

        return t;
    }

    template<class K, class V1, class V2, class V3>
    TableIterator* TypedGlobalTable<K, V1, V2, V3>::get_iterator(int shard, bool bfilter, unsigned int fetch_num) {
        CHECK_EQ(this->is_local_shard(shard), true) << "should use local get_iterator";

        if (info().schedule_portion < 1) {
            ;
            return (TypedTableIterator<K, V1, V2, V3>*) partitions_[shard]->schedule_iterator(this->helper(), bfilter);
        } else {
            return (TypedTableIterator<K, V1, V2, V3>*) partitions_[shard]->get_iterator(this->helper(), bfilter);
        }
    }

    template<class K, class V1, class V2, class V3>
    TableIterator* TypedGlobalTable<K, V1, V2, V3>::get_iterator2(int shard, bool bfilter) {
        CHECK_EQ(this->is_local_shard(shard), true) << "should use local get_iterator2";

        return (CTypedTableIterator<K, V1>*) cpartitions_[shard]->get_iterator(this->helper(), bfilter);
    }

}

#endif /* GLOBALTABLE_H_ */
