#include "client/client.h"


using namespace dsm;

DEFINE_int32(graph_size, 100, "");
DEFINE_bool(weighted, false, "");
DEFINE_double(logn_degree_m, 0, "");
DEFINE_double(logn_degree_s, 2, "");
DEFINE_double(logn_weight_m, 0, "");
DEFINE_double(logn_weight_s, 1, "");
DEFINE_int32(weightgen_method, 1, "");
DEFINE_bool(uploadDFS, false, "");
DEFINE_string(dfs_path, "", "");
DEFINE_string(hadoop_path, "/home/yzhang/hadoop-priter-0.1/bin/hadoop", "");
DECLARE_string(graph_dir);

static TypedGlobalTable<int, double, double, vector<int> > *graph;

static int getSeed()
{
    ifstream rand("/dev/urandom");
    char tmp[sizeof(int)];
    rand.read(tmp,sizeof(int));
    rand.close();
    int* number = reinterpret_cast<int*>(tmp);
    return (*number);
}

static boost::mt19937 gen(time(0)+getSeed());

static double _mean_degree=FLAGS_logn_degree_m;
static double _sigma_degree=FLAGS_logn_degree_s;
static double mu_degree = sqrt(exp(2*_mean_degree)*exp(_sigma_degree*_sigma_degree));
static double sigma_degree = exp(_mean_degree)*sqrt(exp(2*_sigma_degree*_sigma_degree)-exp(_sigma_degree*_sigma_degree));

static double _mean_weight=FLAGS_logn_weight_m;
static double _sigma_weight=FLAGS_logn_weight_s;
static double mu_weight = sqrt(exp(2*_mean_weight)*exp(_sigma_weight*_sigma_weight));
static double sigma_weight = exp(_mean_weight)*sqrt(exp(2*_sigma_weight*_sigma_weight)-exp(_sigma_weight*_sigma_weight));

static int rand_target() {
    boost::uniform_int<> dist(0, FLAGS_graph_size-1);
    boost::variate_generator<boost::mt19937&, boost::uniform_int<> > die(gen, dist);
    return die();
}

static int rand_degree() {
    boost::lognormal_distribution<double> lnd(mu_degree, sigma_degree);
    boost::variate_generator<boost::mt19937&, boost::lognormal_distribution<double> > lnd_generator(gen,lnd);
    return ceil(lnd_generator());
}

static float rand_weight() {
    boost::lognormal_distribution<float> lnd(mu_weight, sigma_weight);
    boost::variate_generator<boost::mt19937&, boost::lognormal_distribution<float> > lnd_generator(gen,lnd);
    return lnd_generator();
}

static vector<int> InitLinks(const int &key) {
  vector<int> links;
  int degree = rand_degree();
  if(degree > 10000) {
	  VLOG(1) << "degree is " << degree;
	  degree = 10000;
  }

  for (int n = 0; n < degree; n++) {
        int p = rand_target();
        while(p == key){
            p = rand_target();
        }
        links.push_back(p);
  }
  return links;
}

//d = log(1+logn(m,s))
static vector<Link> InitLinks2(int key) {
  vector<Link> links;
  int degree = rand_degree();
  if(degree > 10000) {
	  VLOG(1) << "degree is " << degree;
	  degree = 10000;
  }

  for (int n = 0; n < degree; n++) {
      int target = rand_target();
      while(target == key){
          target = rand_target();
      }
              
      float weight = log(1+rand_weight());
      
      links.push_back(Link(target, weight));
  }
  return links;
}

//d = 1/logn(m,s)
static vector<Link> InitLinks3(int key) {
  vector<Link> links;
  int degree = rand_degree();
  if(degree > 10000) {
	  VLOG(1) << "degree is " << degree;
	  degree = 10000;
  }

  for (int n = 0; n < degree; n++) {
      int target = rand_target();
      while(target == key){
          target = rand_target();
      }
              
      float weight = 1 / rand_weight();
      
      links.push_back(Link(target, weight));
  }
  return links;
}


static int GenGraph(ConfigData& conf) {
	int shards = conf.num_workers();
	VLOG(0) << "shards " << shards;
        if(FLAGS_weighted){
            graph = CreateTable<int, double, double, vector<int> >(0, shards, 1, new Sharding::Mod,
                        new Accumulators<double>::Sum, NULL, NULL);
        }else{
            graph = CreateTable<int, double, double, vector<int> >(0, shards, 1, new Sharding::Mod,
                        new Accumulators<double>::Sum, NULL, NULL);
        }
  

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("gengraph_RunKernel1", "run", graph);
  }
  return 0;
}
REGISTER_RUNNER(GenGraph);

class gengraph_RunKernel1 : public DSMKernel {
public:
	  template <class TableA>
	  void gen_unweightgraph(TableA* a){  
              ofstream partition;
              string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());
              partition.open(patition_file.c_str());

	      const int num_shards = graph->num_shards();
	      for (int i = current_shard(); i < FLAGS_graph_size; i += num_shards) {
                  partition << i << "\t";
                  vector<int> links = InitLinks(i);
                  vector<int>::iterator it;
                  for(it=links.begin(); it!=links.end(); it++){
                      int target = *it;
                      partition << target << " ";
		  }
	          partition << "\n";
  
	      }
              partition.close();
	  }

          template <class TableA>
	  void gen_hadoop_unweightgraph(TableA* a){  
              ofstream partition;
              ofstream hadooppartition;
              string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());
              string patition_file_hadoop = StringPrintf("%shadoop/part%d", FLAGS_graph_dir.c_str(), current_shard());
              partition.open(patition_file.c_str());
              hadooppartition.open(patition_file_hadoop.c_str());

	      const int num_shards = graph->num_shards();
	      for (int i = current_shard(); i < FLAGS_graph_size; i += num_shards) {
                  partition << i << "\t";
                  hadooppartition << i << "\t1:";
                  vector<int> links = InitLinks(i);
                  vector<int>::iterator it;
                  for(it=links.begin(); it!=links.end(); it++){
                      int target = *it;
                      partition << target << " ";
                      hadooppartition << target << " ";
		  }
	          partition << "\n";
                  hadooppartition << "\n";
	      }
              partition.close();
              hadooppartition.close();
              

              string delete_cmd = StringPrintf("%s dfs -rmr %s", FLAGS_hadoop_path.c_str(), patition_file.c_str());
              string put_cmd = StringPrintf("%s dfs -put %s %s", FLAGS_hadoop_path.c_str(), patition_file.c_str(), patition_file.c_str());
              VLOG(1) << "hadoop cmd is " << endl << delete_cmd << endl << put_cmd << endl;
              system(delete_cmd.c_str());
              system(put_cmd.c_str());
              
              string delete_cmd2 = StringPrintf("%s dfs -rmr %s", FLAGS_hadoop_path.c_str(), patition_file_hadoop.c_str());
              string put_cmd2 = StringPrintf("%s dfs -put %s %s", FLAGS_hadoop_path.c_str(), patition_file_hadoop.c_str(), patition_file_hadoop.c_str());
              VLOG(1) << "hadoop cmd is " << endl << delete_cmd2 << endl << put_cmd2 << endl;
              system(delete_cmd2.c_str());
              system(put_cmd2.c_str());
	  }
                  
          template <class TableA>
	  void gen_weightgraph(TableA* a){  
              ofstream partition;
              string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());

              partition.open(patition_file.c_str());

	      const int num_shards = graph->num_shards();
	      for (int i = current_shard(); i < FLAGS_graph_size; i += num_shards) {
                  partition << i << "\t";
                  vector<Link> links = InitLinks2(i);
                  vector<Link>::iterator it;
                  for(it=links.begin(); it!=links.end(); it++){
                      Link target = *it;
                      partition << target.end << "," << target.weight << " ";
		  }
	          partition << "\n";
	      }
              partition.close();
	  }
                
          
          template <class TableA>
	  void gen_hadoop_weightgraph(TableA* a){  
              ofstream partition, hadoop_partition;
              string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());
              string hadoop_patition_file = StringPrintf("%shadoop/part%d", FLAGS_graph_dir.c_str(), current_shard());
              partition.open(patition_file.c_str());
              hadoop_partition.open(hadoop_patition_file.c_str());

	      const int num_shards = graph->num_shards();
	      for (int i = current_shard(); i < FLAGS_graph_size; i += num_shards) {
                  partition << i << "\t";
                  if(i == 0){
                      hadoop_partition << i << "\tf0:";
                  }else{
                      hadoop_partition << i << "\tp:";
                  }
                  
                  vector<Link> links = InitLinks2(i);
                  vector<Link>::iterator it;
                  for(it=links.begin(); it!=links.end(); it++){
                      Link target = *it;
                      partition << target.end << "," << target.weight << " ";
                      hadoop_partition << target.end << "," << target.weight << " ";
		  }
	          partition << "\n";
                  hadoop_partition << "\n";
	      }
              partition.close();
              hadoop_partition.close();

              string delete_cmd = StringPrintf("%s dfs -rmr %s", FLAGS_hadoop_path.c_str(), patition_file.c_str());
              string put_cmd = StringPrintf("%s dfs -put %s %s", FLAGS_hadoop_path.c_str(), patition_file.c_str(), patition_file.c_str());
              VLOG(1) << "hadoop cmd is " << endl << delete_cmd << endl << put_cmd << endl;
              system(delete_cmd.c_str());
              system(put_cmd.c_str());
              
              string delete_cmd2 = StringPrintf("%s dfs -rmr %s", FLAGS_hadoop_path.c_str(), hadoop_patition_file.c_str());
              string put_cmd2 = StringPrintf("%s dfs -put %s %s", FLAGS_hadoop_path.c_str(), hadoop_patition_file.c_str(), hadoop_patition_file.c_str());
              VLOG(1) << "hadoop cmd is " << endl << delete_cmd2 << endl << put_cmd2 << endl;
              system(delete_cmd2.c_str());
              system(put_cmd2.c_str());
	  }
                    
                    
	  void run() {
		  VLOG(1) << "generating synthetic graph";
                  graph->InitStateTable();
                  if(FLAGS_uploadDFS){
                      if(FLAGS_weighted){
                          gen_hadoop_weightgraph(graph);
                      }else{
                          gen_hadoop_unweightgraph(graph);
                      }
                  }else{
                      if(FLAGS_weighted){
                          gen_weightgraph(graph);
                      }else{
                          gen_unweightgraph(graph);
                      }
                  }

	  }
};

REGISTER_KERNEL(gengraph_RunKernel1);
REGISTER_METHOD(gengraph_RunKernel1, run);
