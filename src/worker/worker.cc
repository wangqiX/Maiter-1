#include <boost/bind.hpp>
#include <signal.h>

#include "util/common.h"
#include "worker/worker.h"
#include "kernel/kernel.h"
#include "kernel/table-registry.h"

DEFINE_double(sleep_hack, 0.0, "");
DEFINE_double(sleep_time, 0.001, "");
DEFINE_string(checkpoint_write_dir, "/scratch/power/checkpoints", "");
DEFINE_string(checkpoint_read_dir, "/scratch/power/checkpoints", "");

namespace dsm {

    struct Worker::Stub : private boost::noncopyable {
        int32_t id;
        int32_t epoch;

        Stub(int id) : id(id), epoch(0) {
        }
    };

    Worker::Worker(const ConfigData &c) {
        epoch_ = 0;
        active_checkpoint_ = CP_NONE;

        network_ = NetworkThread::Get();

        config_.CopyFrom(c);
        config_.set_worker_id(network_->id() - 1);

        num_peers_ = config_.num_workers();
        peers_.resize(num_peers_);
        for (int i = 0; i < num_peers_; ++i) {
            peers_[i] = new Stub(i + 1);
        }

        running_ = true;
        iterator_id_ = 0;

        // HACKHACKHACK - register ourselves with any existing tables
        TableRegistry::Map &t = TableRegistry::Get()->tables();
        for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
            i->second->set_helper(this);
        }

        RegisterCallback(MTYPE_SHARD_ASSIGNMENT,
                new ShardAssignmentRequest, new EmptyMessage,
                &Worker::HandleShardAssignment, this);

        RegisterCallback(MTYPE_ITERATOR,
                new IteratorRequest, new IteratorResponse,
                &Worker::HandleIteratorRequest, this);

        RegisterCallback(MTYPE_CLEAR_TABLE,
                new ClearTable, new EmptyMessage,
                &Worker::HandleClearRequest, this);

        RegisterCallback(MTYPE_SWAP_TABLE,
                new SwapTable, new EmptyMessage,
                &Worker::HandleSwapRequest, this);

        RegisterCallback(MTYPE_WORKER_FLUSH,
                new EmptyMessage, new EmptyMessage,
                &Worker::HandleFlush, this);

        RegisterCallback(MTYPE_WORKER_APPLY,
                new EmptyMessage, new EmptyMessage,
                &Worker::HandleApply, this);

        RegisterCallback(MTYPE_ENABLE_TRIGGER,
                new EnableTrigger, new EmptyMessage,
                &Worker::HandleEnableTrigger, this);

        RegisterCallback(MTYPE_TERMINATION,
                new TerminationNotification, new EmptyMessage,
                &Worker::HandleTermNotification, this);

        NetworkThread::Get()->SpawnThreadFor(MTYPE_WORKER_FLUSH);
    }

    int Worker::peer_for_shard(int table, int shard) const {
        return TableRegistry::Get()->tables()[table]->owner(shard);
    }

    void Worker::Run() {
        KernelLoop();
    }

    Worker::~Worker() {
        running_ = false;

        for (int i = 0; i < peers_.size(); ++i) {
            delete peers_[i];
        }
    }

    void Worker::KernelLoop() {
        VLOG(1) << "Worker " << config_.worker_id() << " registering...";
        RegisterWorkerRequest req;
        req.set_id(id());
        network_->Send(0, MTYPE_REGISTER_WORKER, req);

        KernelRequest kreq;
        
        VLOG(0)<<"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        while (running_) {
            Timer idle;

            while (!network_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &kreq)) {
                CheckNetwork();
                Sleep(FLAGS_sleep_time);

                if (!running_) {
                    return;
                }
            }
            stats_["idle_time"] += idle.elapsed();

            // VLOG(1) << "Received run request for " << kreq;

            if (peer_for_shard(kreq.table(), kreq.shard()) != config_.worker_id()) {
                LOG(FATAL) << "Received a shard I can't work on! : " << kreq.shard()
                        << " : " << peer_for_shard(kreq.table(), kreq.shard());
            }

            KernelInfo *helper = KernelRegistry::Get()->kernel(kreq.kernel());
            KernelId id(kreq.kernel(), kreq.table(), kreq.shard());
            DSMKernel* d = kernels_[id];

            if (!d) {
                d = helper->create();
                kernels_[id] = d;
                d->initialize_internal(this, kreq.table(), kreq.shard());
                d->InitKernel();
            }

            MarshalledMap args;
            args.FromMessage(kreq.args());
            d->set_args(args);

            if (this->id() == 1 && FLAGS_sleep_hack > 0) {
                Sleep(FLAGS_sleep_hack);
            }

            // Run the user kernel
            helper->Run(d, kreq.method());

            KernelDone kd;
            kd.mutable_kernel()->CopyFrom(kreq);
            TableRegistry::Map &tmap = TableRegistry::Get()->tables();
            for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
                GlobalTable* t = i->second;
                HandlePutRequest();
                for (int j = 0; j < t->num_shards(); ++j) {
                    if (t->is_local_shard(j)) {
                        ShardInfo *si = kd.add_shards();
                        si->set_entries(t->shard_size(j));
                        si->set_owner(this->id());
                        si->set_table(i->first);
                        si->set_shard(j);
                    }
                }
            }
            network_->Send(config_.master_id(), MTYPE_KERNEL_DONE, kd);

            VLOG(1) << "Kernel finished: " << kreq;
            DumpProfile();
        }
    }

    void Worker::CheckNetwork() {
        Timer net;
        CheckForMasterUpdates();
        HandlePutRequest();

        // Flush any tables we no longer own.
        for (unordered_set<GlobalTable*>::iterator i = dirty_tables_.begin(); i != dirty_tables_.end(); ++i) {
            MutableGlobalTable *mg = dynamic_cast<MutableGlobalTable*> (*i);
            if (mg) {
                mg->SendUpdates();
            }
        }

        dirty_tables_.clear();
        stats_["network_time"] += net.elapsed();
    }

    int64_t Worker::pending_kernel_bytes() const {
        int64_t t = 0;

        TableRegistry::Map &tmap = TableRegistry::Get()->tables();
        for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
            MutableGlobalTable *mg = dynamic_cast<MutableGlobalTable*> (i->second);
            if (mg) {
                t += mg->pending_write_bytes();
            }
        }

        return t;
    }

    bool Worker::network_idle() const {
        return network_->pending_bytes() == 0;
    }

    bool Worker::has_incoming_data() const {
        return true;
    }

    void Worker::UpdateEpoch(int peer, int peer_epoch) {
        boost::recursive_mutex::scoped_lock sl(state_lock_);
        VLOG(1) << "Got peer marker: " << MP(peer, MP(epoch_, peer_epoch));
        if (epoch_ < peer_epoch) {
            LOG(INFO) << "Received new epoch marker from peer:" << MP(epoch_, peer_epoch);

            checkpoint_tables_.clear();
            TableRegistry::Map &t = TableRegistry::Get()->tables();
            for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
                checkpoint_tables_.insert(make_pair(i->first, true));
            }

            StartCheckpoint(peer_epoch, CP_ROLLING);
        }

        peers_[peer]->epoch = peer_epoch;

        bool checkpoint_done = true;
        for (int i = 0; i < peers_.size(); ++i) {
            if (peers_[i]->epoch != epoch_) {
                checkpoint_done = false;
                VLOG(1) << "Channel is out of date: " << i << " : " << MP(peers_[i]->epoch, epoch_);
            }
        }

        if (checkpoint_done) {
            FinishCheckpoint();
        }
    }

    void Worker::StartCheckpoint(int epoch, CheckpointType type) {
        boost::recursive_mutex::scoped_lock sl(state_lock_);

        if (epoch_ >= epoch) {
            LOG(INFO) << "Skipping checkpoint; " << MP(epoch_, epoch);
            return;
        }

        epoch_ = epoch;

        File::Mkdirs(StringPrintf("%s/epoch_%05d/",
                FLAGS_checkpoint_write_dir.c_str(), epoch_));

        TableRegistry::Map &t = TableRegistry::Get()->tables();
        for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
            if (checkpoint_tables_.find(i->first) != checkpoint_tables_.end()) {
                VLOG(1) << "Starting checkpoint... " << MP(id(), epoch_, epoch) << " : " << i->first;
                Checkpointable *t = dynamic_cast<Checkpointable*> (i->second);
                CHECK(t != NULL) << "Tried to checkpoint a read-only table?";

                t->start_checkpoint(StringPrintf("%s/epoch_%05d/checkpoint.table-%d",
                        FLAGS_checkpoint_write_dir.c_str(),
                        epoch_, i->first));
            }
        }

        active_checkpoint_ = type;

        // For rolling checkpoints, send out a marker to other workers indicating
        // that we have switched epochs.
        if (type == CP_ROLLING) {
            TableData epoch_marker;
            epoch_marker.set_source(id());
            epoch_marker.set_table(-1);
            epoch_marker.set_shard(-1);
            epoch_marker.set_done(true);
            epoch_marker.set_marker(epoch_);
            for (int i = 0; i < peers_.size(); ++i) {
                network_->Send(i + 1, MTYPE_PUT_REQUEST, epoch_marker);
            }
        }

        VLOG(1) << "Starting delta logging... " << MP(id(), epoch_, epoch);
    }

    void Worker::FinishCheckpoint() {
        VLOG(1) << "Worker " << id() << " flushing checkpoint.";
        boost::recursive_mutex::scoped_lock sl(state_lock_);

        active_checkpoint_ = CP_NONE;
        TableRegistry::Map &t = TableRegistry::Get()->tables();

        for (int i = 0; i < peers_.size(); ++i) {
            peers_[i]->epoch = epoch_;
        }

        for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
            Checkpointable *t = dynamic_cast<Checkpointable*> (i->second);
            if (t) {
                t->finish_checkpoint();
            }
        }

        EmptyMessage req;
        network_->Send(config_.master_id(), MTYPE_CHECKPOINT_DONE, req);
    }

    void Worker::SendTermcheck(int snapshot, long updates, double current) {
        boost::recursive_mutex::scoped_lock sl(state_lock_);

        TermcheckDelta req;
        req.set_index(snapshot);
        req.set_delta(current);
        req.set_updates(updates);
        network_->Send(config_.master_id(), MTYPE_TERMCHECK_DONE, req);

        VLOG(1) << "termination condition of subpass " << snapshot << " worker " << network_->id() << " sent to master... with total current " << StringPrintf("%.05f", current);
        ;
    }

    void Worker::Restore(int epoch) {
        boost::recursive_mutex::scoped_lock sl(state_lock_);
        LOG(INFO) << "Worker restoring state from epoch: " << epoch;
        epoch_ = epoch;

        TableRegistry::Map &t = TableRegistry::Get()->tables();
        for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
            Checkpointable* t = dynamic_cast<Checkpointable*> (i->second);
            if (t) {
                t->restore(StringPrintf("%s/epoch_%05d/checkpoint.table-%d",
                        FLAGS_checkpoint_read_dir.c_str(), epoch_, i->first));
            }
        }

        EmptyMessage req;
        network_->Send(config_.master_id(), MTYPE_RESTORE_DONE, req);
    }

    void Worker::HandlePutRequest() {
        boost::recursive_mutex::scoped_lock sl(state_lock_);

        KVPairData put;
        while (network_->TryRead(MPI::ANY_SOURCE, MTYPE_PUT_REQUEST, &put)) {
            if (put.marker() != -1) {
                UpdateEpoch(put.source(), put.marker());
                continue;
            }

            //VLOG(2) << "Read put request of size: "<< put.kv_data_size() << " for " << MP(put.table(), put.shard());

            MutableGlobalTable *t = TableRegistry::Get()->mutable_table(put.table());
            t->ApplyUpdates(put);

            // Record messages from our peer channel up until they checkpointed.
            if (active_checkpoint_ == CP_MASTER_CONTROLLED ||
                    (active_checkpoint_ == CP_ROLLING && put.epoch() < epoch_)) {
                if (checkpoint_tables_.find(t->id()) != checkpoint_tables_.end()) {
                    Checkpointable *ct = dynamic_cast<Checkpointable*> (t);
                    ct->write_delta(put);
                }
            }

            if (put.done() && t->tainted(put.shard())) {
                VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
                t->get_partition_info(put.shard())->tainted = false;
            }
        }
    }

    void Worker::HandleSwapRequest(const SwapTable& req, EmptyMessage *resp, const RPCInfo& rpc) {
        MutableGlobalTable *ta = TableRegistry::Get()->mutable_table(req.table_a());
        MutableGlobalTable *tb = TableRegistry::Get()->mutable_table(req.table_b());

        ta->local_swap(tb);
    }

    void Worker::HandleClearRequest(const ClearTable& req, EmptyMessage *resp, const RPCInfo& rpc) {
        MutableGlobalTable *ta = TableRegistry::Get()->mutable_table(req.table());

        for (int i = 0; i < ta->num_shards(); ++i) {
            if (ta->is_local_shard(i)) {
                ta->get_partition(i)->clear();
            }
        }
    }

    void Worker::HandleIteratorRequest(const IteratorRequest& iterator_req, IteratorResponse *iterator_resp, const RPCInfo& rpc) {
        int table = iterator_req.table();
        int shard = iterator_req.shard();

        GlobalTable * t = TableRegistry::Get()->table(table);
        TableIterator* it = NULL;
        if (iterator_req.id() == -1) {
            it = t->get_iterator(shard, false);
            uint32_t id = iterator_id_++;
            iterators_[id] = it;
            iterator_resp->set_id(id);
        } else {
            it = iterators_[iterator_req.id()];
            iterator_resp->set_id(iterator_req.id());
            CHECK_NE(it, (void *) NULL);
            it->Next();
        }

        iterator_resp->set_row_count(0);
        iterator_resp->clear_key();
        iterator_resp->clear_value();
        for (int i = 1; i <= iterator_req.row_count(); i++) {
            iterator_resp->set_done(it->done());
            if (!it->done()) {
                std::string* respkey = iterator_resp->add_key();
                it->key_str(respkey);
                std::string* respvalue = iterator_resp->add_value();
                it->value1_str(respvalue);
                iterator_resp->set_row_count(i);
                if (i < iterator_req.row_count())
                    it->Next();
            } else break;
        }
        VLOG(2) << "[PREFETCH] Returning " << iterator_resp->row_count()
                << " rows in response to request for " << iterator_req.row_count()
                << " rows in table " << table << ", shard " << shard << endl;
    }

    void Worker::HandleShardAssignment(const ShardAssignmentRequest& shard_req, EmptyMessage *resp, const RPCInfo& rpc) {
        //  LOG(INFO) << "Shard assignment: " << shard_req.DebugString();
        for (int i = 0; i < shard_req.assign_size(); ++i) {
            const ShardAssignment &a = shard_req.assign(i);
            GlobalTable *t = TableRegistry::Get()->table(a.table());
            int old_owner = t->owner(a.shard());
            t->get_partition_info(a.shard())->sinfo.set_owner(a.new_worker());

            //VLOG(3) << "Setting owner: " << MP(a.shard(), a.new_worker());

            if (a.new_worker() == id() && old_owner != id()) {
                // VLOG(1)  << "Setting self as owner of " << MP(a.table(), a.shard());

                // Don't consider ourselves canonical for this shard until we receive updates
                // from the old owner.
                if (old_owner != -1) {
                    LOG(INFO) << "Setting " << MP(a.table(), a.shard())
                            << " as tainted.  Old owner was: " << old_owner
                            << " new owner is :  " << id();
                    t->get_partition_info(a.shard())->tainted = true;
                }
            } else if (old_owner == id() && a.new_worker() != id()) {
                VLOG(1) << "Lost ownership of " << MP(a.table(), a.shard()) << " to " << a.new_worker();
                // A new worker has taken ownership of this shard.  Flush our data out.
                t->get_partition_info(a.shard())->dirty = true;
                dirty_tables_.insert(t);
            }
        }
    }

    void Worker::HandleFlush(const EmptyMessage& req, EmptyMessage *resp, const RPCInfo& rpc) {
        Timer net;

        TableRegistry::Map &tmap = TableRegistry::Get()->tables();
        for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
            MutableGlobalTable* t = dynamic_cast<MutableGlobalTable*> (i->second);
            if (t) {
                t->SendUpdates();
            }
        }

        network_->Flush();
        stats_["network_time"] += net.elapsed();
    }

    void Worker::FlushUpdates() {
        //VLOG(2) << "finish one pass";
        TableRegistry::Map &tmap = TableRegistry::Get()->tables();
        for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
            MutableGlobalTable* t = dynamic_cast<MutableGlobalTable*> (i->second);
            if (t) {
                t->SendUpdates();
                t->TermCheck();
            }
        }
    }

    void Worker::HandleApply(const EmptyMessage& req, EmptyMessage *resp, const RPCInfo& rpc) {
        HandlePutRequest();
    }

    void Worker::HandleEnableTrigger(const EnableTrigger& req, EmptyMessage *resp, const RPCInfo& rpc) {

        TableRegistry::Get()->tables()[req.table()]
                ->trigger(req.trigger_id())
                ->enable(req.enable());
    }

    void Worker::HandleTermNotification(const TerminationNotification& req, EmptyMessage* resp, const RPCInfo& rpc) {
        GlobalTable *ta = TableRegistry::Get()->table(0); //we have only 1 table, index 0

        for (int i = 0; i < ta->num_shards(); ++i) {
            if (ta->is_local_shard(i)) {
                ta->get_partition(i)->terminate();
            }
        }
    }

    void Worker::CheckForMasterUpdates() {
        boost::recursive_mutex::scoped_lock sl(state_lock_);
        // Check for shutdown.
        EmptyMessage empty;
        KernelRequest k;

        if (network_->TryRead(config_.master_id(), MTYPE_WORKER_SHUTDOWN, &empty)) {
            VLOG(1) << "Shutting down worker " << config_.worker_id();
            running_ = false;
            return;
        }

        CheckpointRequest checkpoint_msg;
        while (network_->TryRead(config_.master_id(), MTYPE_START_CHECKPOINT, &checkpoint_msg)) {
            for (int i = 0; i < checkpoint_msg.table_size(); ++i) {
                checkpoint_tables_.insert(make_pair(checkpoint_msg.table(i), true));
            }

            StartCheckpoint(checkpoint_msg.epoch(),
                    (CheckpointType) checkpoint_msg.checkpoint_type());
        }

        while (network_->TryRead(config_.master_id(), MTYPE_FINISH_CHECKPOINT, &empty)) {
            FinishCheckpoint();
        }

        StartRestore restore_msg;
        while (network_->TryRead(config_.master_id(), MTYPE_RESTORE, &restore_msg)) {
            Restore(restore_msg.epoch());
        }
    }

    bool StartWorker(const ConfigData& conf) {
        if (NetworkThread::Get()->id() == 0)
            return false;

        Worker w(conf);
        w.Run();
        Stats s = w.get_stats();
        s.Merge(NetworkThread::Get()->stats);
        VLOG(1) << "Worker stats: \n" << s.ToString(StringPrintf("[W%d]", conf.worker_id()));
        exit(0);
    }

} // end namespace
