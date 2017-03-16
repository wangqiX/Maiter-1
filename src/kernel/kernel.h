#ifndef KERNELREGISTRY_H_
#define KERNELREGISTRY_H_

#include "kernel/table.h"
#include "kernel/global-table.h"
#include "kernel/local-table.h"
#include "kernel/table-registry.h"

#include "util/common.h"
#include <boost/function.hpp>
#include <boost/lexical_cast.hpp>
#include <time.h>
#include <algorithm>
DECLARE_string(graph_dir);
DECLARE_int32(degree);
DECLARE_int32(shard);

namespace dsm {

    template <class K, class V1, class V2, class V3>
    class TypedGlobalTable;

    class TableBase;
    class Worker;

    template <class K, class V, class D>
    class MaiterKernel;

#ifndef SWIG

    class MarshalledMap {
    public:

        struct MarshalledValue {
            virtual string ToString() const = 0;
            virtual void FromString(const string& s) = 0;
            virtual void set(const void* nv) = 0;
            virtual void* get() const = 0;
        };

        template <class T>
        struct MarshalledValueT : public MarshalledValue { // MarshalledValueT

            MarshalledValueT() : v(new T) {
            }

            ~MarshalledValueT() {
                delete v;
            }

            string ToString() const {
                string tmp;
                m_.marshal(*v, &tmp);
                return tmp;
            }

            void FromString(const string& s) {
                m_.unmarshal(s, v);
            }

            void* get() const {
                return v;
            }

            void set(const void *nv) {
                *v = *(T*) nv;
            }

            mutable Marshal<T> m_;
            T *v;
        }; // MarshalledValueT

        template <class T>
        void put(const string& k, const T& v) {
            if (serialized_.find(k) != serialized_.end()) {
                serialized_.erase(serialized_.find(k));
            }

            if (p_.find(k) == p_.end()) {
                p_[k] = new MarshalledValueT<T>;
            }

            p_[k]->set(&v);
        }

        template <class T>
        T& get(const string& k) const {
            if (serialized_.find(k) != serialized_.end()) {
                p_[k] = new MarshalledValueT<T>;
                p_[k]->FromString(serialized_[k]);
                serialized_.erase(serialized_.find(k));
            }

            return *(T*) p_.find(k)->second->get();
        }

        bool contains(const string& key) const {
            return p_.find(key) != p_.end() ||
                    serialized_.find(key) != serialized_.end();
        }

        Args* ToMessage() const {
            Args* out = new Args;
            for (unordered_map<string, MarshalledValue*>::const_iterator i = p_.begin(); i != p_.end(); ++i) {
                Arg *p = out->add_param();
                p->set_key(i->first);
                p->set_value(i->second->ToString());
            }
            return out;
        }

        // We can't immediately deserialize the parameters passed in, since sadly we don't
        // know the type yet.  Instead, save the string values on the side, and de-serialize
        // on request.

        void FromMessage(const Args& p) {
            for (int i = 0; i < p.param_size(); ++i) {
                serialized_[p.param(i).key()] = p.param(i).value();
            }
        }

    private:
        mutable unordered_map<string, MarshalledValue*> p_;
        mutable unordered_map<string, string> serialized_;
    };
#endif

    class DSMKernel {
    public:
        // Called upon creation of this kernel by a worker.

        virtual void InitKernel() {
        }

        // The table and shard being processed.

        int current_shard() const {
            return shard_;
        }

        int current_table() const {
            return table_id_;
        }

        template <class T>
        T& get_arg(const string& key) const {
            return args_.get<T>(key);
        }

        template <class T>
        T& get_cp_var(const string& key, T defval = T()) {
            if (!cp_.contains(key)) {
                cp_.put(key, defval);
            }
            return cp_.get<T>(key);
        }

        GlobalTable* get_table(int id);

        template <class K, class V1, class V2, class V3>
        TypedGlobalTable<K, V1, V2, V3>* get_table(int id) {
            return dynamic_cast<TypedGlobalTable<K, V1, V2, V3>*> (get_table(id));
        }

        template <class K, class V, class D>
        void set_maiter(MaiterKernel<K, V, D> maiter) {
        }

    private:
        friend class Worker;
        friend class Master;

        void initialize_internal(Worker* w,
                int table_id, int shard);

        void set_args(const MarshalledMap& args);
        void set_checkpoint(const MarshalledMap& args);

        Worker *w_;
        int shard_;
        int table_id_;
        MarshalledMap args_;
        MarshalledMap cp_;
    };

    struct KernelInfo {

        KernelInfo(const char* name) : name_(name) {
        }

        virtual DSMKernel* create() = 0;
        virtual void Run(DSMKernel* obj, const string& method_name) = 0;
        virtual bool has_method(const string& method_name) = 0;

        string name_;
    };

    template <class C, class K, class V, class D>
    struct KernelInfoT : public KernelInfo {
        typedef void (C::*Method)();
        map<string, Method> methods_;
        MaiterKernel<K, V, D>* maiter;

        KernelInfoT(const char* name, MaiterKernel<K, V, D>* inmaiter) : KernelInfo(name) {
            maiter = inmaiter;
        }

        DSMKernel* create() {
            return new C;
        }

        void Run(DSMKernel* obj, const string& method_id) {
            ((C*) obj)->set_maiter(maiter);
            boost::function<void (C*) > m(methods_[method_id]);
            m((C*) obj);
        }

        bool has_method(const string& name) {
            return methods_.find(name) != methods_.end();
        }

        void register_method(const char* mname, Method m, MaiterKernel<K, V, D>* inmaiter) {
            methods_[mname] = m;
        }
    };

    class ConfigData;

    class KernelRegistry {
    public:
        typedef map<string, KernelInfo*> Map;

        Map& kernels() {
            return m_;
        }

        KernelInfo* kernel(const string& name) {
            return m_[name];
        }

        static KernelRegistry* Get();
    private:

        KernelRegistry() {
        }
        Map m_;
    };

    template <class C, class K, class V, class D>
    struct KernelRegistrationHelper {

        KernelRegistrationHelper(const char* name, MaiterKernel<K, V, D>* maiter) {
            KernelRegistry::Map& kreg = KernelRegistry::Get()->kernels();

            CHECK(kreg.find(name) == kreg.end()); //map.find(name)返回name所对应的迭代器，找不到则返回end()迭代器
            kreg.insert(make_pair(name, new KernelInfoT<C, K, V, D>(name, maiter)));
        }
    };

    template <class C, class K, class V, class D>
    struct MethodRegistrationHelper {

        MethodRegistrationHelper(const char* klass, const char* mname, void (C::*m)(), MaiterKernel<K, V, D>* maiter) {
            ((KernelInfoT<C, K, V, D>*)KernelRegistry::Get()->kernel(klass))->register_method(mname, m, maiter);
        }
    };

    template <class K, class V, class D>
    class MaiterKernel0 : public DSMKernel {
    private:
        MaiterKernel<K, V, D>* maiter;
    public:

        void run() {
            VLOG(0) << "initializing table ";
            init_table(maiter->table);
        }
    };

    template <class K, class V, class D>
    class MaiterKernel1 : public DSMKernel { //the first phase: initialize the local state table
    private:
        MaiterKernel<K, V, D>* maiter; //user-defined iteratekernel
    public:

        void set_maiter(MaiterKernel<K, V, D>* inmaiter) {
            maiter = inmaiter;
        }

        void read_file(TypedGlobalTable<K, V, V, D>* table) {
            string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());
            //cout<<"Unable to open file: " << patition_file<<endl;
            ifstream inFile;
            inFile.open(patition_file.c_str());
            if (!inFile) {
                //cout<<"Unable to open file: " << patition_file;
                cerr << "Unable to open file" << patition_file;
                cerr << system("ifconfig -a | grep 192.168.*") << endl;
                exit(1); // terminate with error
            }

            char linechr[2024000];
            while (inFile.getline(linechr, 2024000)) { //read a line of the input file, ensure the buffer is large enough
                K key;
                V delta;
                D data;
                V value;
                int size;
                string line(linechr);
                maiter->iterkernel->read_data(line, key, data, size); //invoke api, get the value of key field and data field
                maiter->iterkernel->init_v(key, value, data); //invoke api, get the initial v field value
                maiter->iterkernel->init_c(key, delta, data); //invoke api, get the initial delta v field value
                //cout<<"key: "<<key<<"delta: "<<delta<<"value: "<<value<<"   "<<data[0][0]<<"  "<<data[1][0]<<"   "<<data[2][0]<<endl;
                if (size >= FLAGS_degree) {
                    table->put(key, delta, value, data); //initialize a row of the state table (a node)
                } else {
                    table->put2(key, delta, value, data);
                }
            }

            patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard() + FLAGS_shard);
            ifstream inFile2;
            inFile2.open(patition_file.c_str());
            if (!inFile2) {
                //cout<<"Unable to open file: " << patition_file;
                cerr << "Unable to open file" << patition_file;
                cerr << system("ifconfig -a | grep 192.168.*") << endl;
                exit(1); // terminate with error
            }

            char linechr2[2024000];
            while (inFile.getline(linechr2, 2024000)) { //read a line of the input file, ensure the buffer is large enough
                K key;
                D data;
                int size;
                string line(linechr2);
                maiter->iterkernel->read_data(line, key, data, size); //invoke api, get the value of key field and data field
                table->putc(key, 0, data, this->current_shard());
            }
        }

        void init_table(TypedGlobalTable<K, V, V, D>* a) {
            if (!a->initialized()) {
                a->InitStateTable(); //initialize the local state table
            }
            a->resize(maiter->num_nodes); //create local state table based on the input size

            read_file(a); //initialize the state table fields based on the input data file
        }

        void show_data(TypedGlobalTable<K, V, V, D>* a) {
            typename TypedGlobalTable<K, V, V, D>::Iterator *it2 = a->get_typed_iterator(current_shard(), false);
            //should not use for(;!it->done();it->Next()), that will skip some entry
            while (!it2->done()) {
                bool cont = it2->Next(); //if we have more in the state table, we continue
                if (!cont) break;
                VLOG(0) << "processing " << it2->key() << " " << it2->value1() << " " << it2->value2();
            }
            delete it2; //delete the table iterator
        }

        void run() {
            VLOG(0) << "initializing table ";
            init_table(maiter->table);
            // show_data(maiter->table);
        }
    };

    template <class K, class V, class D>
    class MaiterKernel2 : public DSMKernel { //the second phase: iterative processing of the local state table
    private:
        MaiterKernel<K, V, D>* maiter; //user-defined iteratekernel
        vector<pair<K, V> >* output; //the output buffer          

    public:

        void set_maiter(MaiterKernel<K, V, D>* inmaiter) {
            maiter = inmaiter;
        }

        void run_iter(const K& k, V &v1, V &v2, D &v3) {
            //cout<<"delta:"<<v1<<endl;
            maiter->iterkernel->process_delta_v(k, v1, v2, v3);

            maiter->table->accumulateF2(k, v1); //perform v=v+delta_v     
            // process delta_v before accumulate
            maiter->iterkernel->g_func(k, v1, v2, v3, output); //invoke api, perform g(delta_v) and send messages to out-neighbors
            //cout << " key " << k << endl;
            maiter->table->updateF1(k, maiter->iterkernel->default_v()); //perform delta_v=0, reset delta_v after delta_v has been spread out

            typename vector<pair<K, V> >::iterator iter;
            for (iter = output->begin(); iter != output->end(); iter++) { //send the buffered messages to remote state table
                pair<K, V> kvpair = *iter;
                //cout << "accumulating " << kvpair.first << " with " <<kvpair.second << endl;
                maiter->table->accumulateF1(kvpair.first, kvpair.second); //apply the output messages to remote state table
            }
            output->clear(); //clear the output buffer
        }

        void run_iter2(const K& k, V &v1, V &v2, D &v3) {
            //cout<<"delta:"<<v1<<endl;
            maiter->iterkernel->process_delta_v(k, v1, v2, v3);

            maiter->table->accumulateF2(k, v1); //perform v=v+delta_v     
            // process delta_v before accumulate
            maiter->iterkernel->g_func(k, v1, v2, v3, output); //invoke api, perform g(delta_v) and send messages to out-neighbors
            //cout << " key " << k << endl;
            maiter->table->updateF1(k, maiter->iterkernel->default_v()); //perform delta_v=0, reset delta_v after delta_v has been spread out
            int shard = maiter->table->num_shards();
            vector<int> vec;
            vector<int>::iterator it;
            V v = output->begin()->second;
            typename vector<pair<K, V> >::iterator iter;
            for (iter = output->begin(); iter != output->end(); iter++) { //send the buffered messages to remote state table
                pair<K, V> kvpair = *iter;
                if ((kvpair.first % shard) == this->current_shard()) {
                    maiter->table->accumulateF1(kvpair.first, kvpair.second);
                } else {
                    int sh = kvpair.first % shard;
                    it = find(vec.begin(), vec.end(), sh);
                    if (it == vec.end()) {
                        vec.push_back(sh);
                    }
                }
            }
            for (vector<int>::iterator it = vec.begin(); it != vec.end(); ++it) {
                maiter->table->accumulateFF(k, v, *it);
            }
            vec.clear();
            output->clear(); //clear the output buffer
        }
        //    void dump(TypedGlobalTable<K, V, V, D>* a){
        //        static int id=0;
        //        ++id;
        //        double totalF1 = 0;             //the sum of delta_v, it should be smaller enough when iteration converges  
        //        double totalF2 = 0;             //the sum of v, it should be larger enough when iteration converges
        //        fstream File;                   //the output file containing the local state table infomation
        //    
        //        string file = StringPrintf("%s/part--%d%d", maiter->output.c_str(), current_shard(),id);  //the output path
        //        File.open(file.c_str(), ios::out);
        //        if(!File)  VLOG(0) << "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        //        //get the iterator of the local state table
        //        typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_entirepass_iterator(current_shard());
        //
        //        while(!it->done()) {
        //                bool cont = it->Next();
        //                if(!cont) break;
        //                
        //                totalF1 += it->value1();
        //                totalF2 += it->value2();
        //                File << it->key() << "\t"  << it->value2() << "\n";
        //                //File << it->key() << "\t" << it->value1() << ":" << it->value2() << "\n";
        //        }
        //        delete it;
        //
        //        File.close();
        //
        //        cout << "total F1 : " << totalF1 << endl;
        //        cout << "total F2 : " << totalF2 << endl;
        //    }

        void run_loop(TypedGlobalTable<K, V, V, D>* a) {
            Timer timer; //for experiment, time recording
            //double totalF1 = 0;                 //the sum of delta_v, it should be smaller and smaller as iterations go on
            double totalF2 = 0; //the sum of v, it should be larger and larger as iterations go on
            long updates = 0; //for experiment, recording number of update operations
            output = new vector<pair<K, V> >;
            //        double now=Now();
            //        double last=now;
            //the main loop for iterative update
            while (true) {
                //set false, no inteligient stop scheme, which can check whether there are changes in statetable
                //            now=Now();
                //            if(now-last>5){dump(maiter->table);last=now;}
                //get the iterator of the local state table
                typename TypedGlobalTable<K, V, V, D>::Iterator *it2 = a->get_typed_iterator(current_shard(), false);
                if (it2 == NULL) break;

                //should not use for(;!it->done();it->Next()), that will skip some entry
                while (!it2->done()) {

                    bool cont = it2->Next(); //if we have more in the state table, we continue
                    if (!cont) break;
                    totalF2 += it2->value2(); //for experiment, recording the sum of v
                    updates++; //for experiment, recording the number of updates

                    //cout << "processing " << it2->key() << " " << it2->value1() << " " << it2->value2() << endl;
                    if (it2->is_copy() == 0) {
                        run_iter(it2->key(), it2->value1(), it2->value2(), it2->value3());
                    } else {
                        run_iter2(it2->key(), it2->value1(), it2->value2(), it2->value3());
                    }
                }
                delete it2; //delete the table iterator

                typename TypedGlobalTable<K, V, V, D>::iterator2 *it3 = a->get_copy_iterator(current_shard(), false);
                if (it3 == NULL) break;

                while (!it3->done()) {
                    bool cont = it3->Next();
                    if (!cont) break;
                    if (it3->value1() != 0) {
                        for (vector<int>::iterator it = it3->vec().begin(); it != it3->vec().end(); it++) {
                            maiter->table->accumulateF1(*it, it3->value1());
                        }
                        it3->value1() = 0;
                    }
                }
                delete it3;
            }

        }

        void map() {
            VLOG(0) << "start performing iterative update";
            run_loop(maiter->table);
        }
    };

    template <class K, class V, class D>
    class MaiterKernel3 : public DSMKernel { //the third phase: dumping the result, write the in-memory table to disk
    private:
        MaiterKernel<K, V, D>* maiter; //user-defined iteratekernel
    public:

        void set_maiter(MaiterKernel<K, V, D>* inmaiter) {
            maiter = inmaiter;
        }

        void dump(TypedGlobalTable<K, V, V, D>* a) {
            double totalF1 = 0; //the sum of delta_v, it should be smaller enough when iteration converges  
            double totalF2 = 0; //the sum of v, it should be larger enough when iteration converges
            fstream File; //the output file containing the local state table infomation

            string file = StringPrintf("%s/part-%d", maiter->output.c_str(), current_shard()); //the output path
            File.open(file.c_str(), ios::out);

            //get the iterator of the local state table
            typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_entirepass_iterator(current_shard());

            while (!it->done()) {
                bool cont = it->Next();
                if (!cont) break;

                totalF1 += it->value1();
                totalF2 += it->value2();
                File << it->key() << "\t" << it->value2() << "\n";
                //File << it->key() << "\t" << it->value1() << ":" << it->value2() << "\n";
            }
            delete it;

            File.close();

            cout << "total F1 : " << totalF1 << endl;
            cout << "total F2 : " << totalF2 << endl;
        }

        void run() {
            VLOG(0) << "dumping result";
            dump(maiter->table);
        }
    };

    template <class K, class V, class D>
    class MaiterKernel {
    public:

        int64_t num_nodes;
        double schedule_portion;
        ConfigData conf;
        string output;
        Sharder<K> *sharder;
        IterateKernel<K, V, D> *iterkernel;
        TermChecker<K, V> *termchecker;

        TypedGlobalTable<K, V, V, D> *table;

        MaiterKernel() {
            Reset();
        }

        MaiterKernel(ConfigData& inconf, int64_t nodes, double portion, string outdir,
                Sharder<K>* insharder, //the user-defined partitioner
                IterateKernel<K, V, D>* initerkernel, //the user-defined iterate kernel
                TermChecker<K, V>* intermchecker) { //the user-defined terminate checker
            Reset();

            conf = inconf; //configuration
            num_nodes = nodes; //local state table size
            schedule_portion = portion; //priority scheduling, scheduled portion
            output = outdir; //output dir
            sharder = insharder; //the user-defined partitioner
            iterkernel = initerkernel; //the user-defined iterate kernel
            termchecker = intermchecker; //the user-defined terminate checker
        }

        ~MaiterKernel() {
        }

        void Reset() {
            num_nodes = 0;
            schedule_portion = 1;
            output = "result";
            sharder = NULL;
            iterkernel = NULL;
            termchecker = NULL;
        }


    public:

        int registerMaiter() {
            VLOG(0) << "shards " << conf.num_workers();
            table = CreateTable<K, V, V, D >(0, conf.num_workers(), schedule_portion,
                    sharder, iterkernel, termchecker);

            //initialize table job
            KernelRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", this);
            MethodRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", "run", &MaiterKernel1<K, V, D>::run, this);

            //iterative update job
            if (iterkernel != NULL) {
                KernelRegistrationHelper<MaiterKernel2<K, V, D>, K, V, D>("MaiterKernel2", this);
                MethodRegistrationHelper<MaiterKernel2<K, V, D>, K, V, D>("MaiterKernel2", "map", &MaiterKernel2<K, V, D>::map, this);
            }

            //dumping result to disk job
            if (termchecker != NULL) {
                KernelRegistrationHelper<MaiterKernel3<K, V, D>, K, V, D>("MaiterKernel3", this);
                MethodRegistrationHelper<MaiterKernel3<K, V, D>, K, V, D>("MaiterKernel3", "run", &MaiterKernel3<K, V, D>::run, this);
            }

            return 0;
        }
    };

    class RunnerRegistry {
    public:
        typedef int (*KernelRunner)(ConfigData&);
        typedef map<string, KernelRunner> Map;

        KernelRunner runner(const string& name) {
            return m_[name];
        }

        Map& runners() {
            return m_;
        }

        static RunnerRegistry* Get();
    private:

        RunnerRegistry() {
        }
        Map m_;
    };

    struct RunnerRegistrationHelper {

        RunnerRegistrationHelper(RunnerRegistry::KernelRunner k, const char* name) {
            RunnerRegistry::Get()->runners().insert(make_pair(name, k));
        }
    };

#define REGISTER_KERNEL(klass)\
  static KernelRegistrationHelper<klass> k_helper_ ## klass(#klass);

#define REGISTER_METHOD(klass, method)\
  static MethodRegistrationHelper<klass> m_helper_ ## klass ## _ ## method(#klass, #method, &klass::method);

#define REGISTER_RUNNER(r)\
  static RunnerRegistrationHelper r_helper_ ## r ## _(&r, #r);

}
#endif /* KERNELREGISTRY_H_ */
