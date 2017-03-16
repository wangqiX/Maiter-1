//#include "client/client.h"
//
//
//using namespace dsm;
//
//DECLARE_string(result_dir);
//DECLARE_int64(num_nodes);
//DECLARE_double(portion);
//DECLARE_int64(shortestpath_source);
//
//
//struct ShortestpathIterateKernel : public IterateKernel<int, float, vector<Link> > {
//    float imax;
//
//    ShortestpathIterateKernel(){
//        imax = std::numeric_limits<float>::max();
//    }
//
//    void read_data(string& line, int& k, vector<Link>& data){
//        string linestr(line);
//        int pos = linestr.find("\t");
//        int source = boost::lexical_cast<int>(linestr.substr(0, pos));
//
//        vector<Link> linkvec;
//        int spacepos = 0;
//        string links = linestr.substr(pos+1);
//        while((spacepos = links.find_first_of(" ")) != links.npos){
//            Link to(0, 0);
//
//            if(spacepos > 0){
//                string link = links.substr(0, spacepos);
//                int cut = links.find_first_of(",");
//                to.end = boost::lexical_cast<int>(link.substr(0, cut));
//                to.weight = boost::lexical_cast<float>(link.substr(cut+1));
//            }
//            links = links.substr(spacepos+1);
//            linkvec.push_back(to);
//        }
//
//        k = source;
//        data = linkvec;
//    }
//
//    void init_c(const int& k, float& delta,vector<Link>& data){
//        if(k == FLAGS_shortestpath_source){
//            delta = 0;
//        }else{
//            delta = imax;
//        }
//    }
//    void init_v(const int& k,float& v,vector<Link>& data){
//        v = imax;  
//    }
//        
//    void accumulate(float& a, const float& b){
//        a = std::min(a, b); 
//    }
//
//    void priority(float& pri, const float& value, const float& delta){
//        pri = value - std::min(value, delta); 
//    }
//
//    void g_func(const int &k, const float& delta,const float& value, const vector<Link>& data, vector<pair<int, float> >* output){
//        for(vector<Link>::const_iterator it=data.begin(); it!=data.end(); it++){
//            Link target = *it;
//            float outv = delta + target.weight;
//            output->push_back(make_pair(target.end, outv));
//        }
//    }
//
//    const float& default_v() const {
//        return imax;
//    }
//};
//
//
//static int Shortestpath(ConfigData& conf) {
//    MaiterKernel<int, float, vector<Link> >* kernel = new MaiterKernel<int, float, vector<Link> >(
//                                        conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
//                                        new Sharding::Mod,
//                                        new ShortestpathIterateKernel,
//                                        new TermCheckers<int, float>::Diff);
//    
//    
//    kernel->registerMaiter();
//
//    if (!StartWorker(conf)) {
//        Master m(conf);
//        m.run_maiter(kernel);
//    }
//    
//    delete kernel;
//    return 0;
//}
//
//REGISTER_RUNNER(Shortestpath);

