//#include "client/client.h"
//
//
//using namespace dsm;
//
////DECLARE_string(graph_dir);
//DECLARE_string(result_dir);
//DECLARE_int64(num_nodes);
//DECLARE_double(portion);
//
//struct Simrankiterate : public IterateKernel<string, double, vector<vector<int> > > {
//    double zero;
//    int count;
//    Simrankiterate() : zero(0){count = 0;}
//
//    void read_data(string& line, string& k, vector<vector<int> >& data){
//        string linestr(line);
//        //cout<<"line:"<<line<<endl;
//        int pos = linestr.find("\t");
//        if(pos == -1) return;
//        k=linestr.substr(0,pos);
//        vector<vector<int> > linkvec;
//        string remain=linestr.substr(pos+1);
//        //cout<<remain<<endl;
//        int pos1=remain.find(" ");
//        string I_ab=remain.substr(0,pos1);
//        //cout<<I_ab<<endl;
//        int i_ab=boost::lexical_cast<int>(I_ab);
//        //cout<<i_ab<<endl;
//        vector<int> tmp;
//        tmp.push_back(i_ab);
//        linkvec.push_back(tmp);
//        //cout<<linkvec[0][0]<<endl;
//        pos=pos1;
//        remain=remain.substr(pos+1);
//        //cout<<remain<<endl;
//        pos1=remain.find("\t");
//        string links_a=remain.substr(0,pos1);
//        string links_b=remain.substr(pos1+1);
//        //cout<<links_a<<endl;
//        //cout<<links_b<<endl;
//        if(links_a==""||links_b==""||links_a==" "||links_b==" "){
//            //cout<<"全空"<<endl;
//            data = linkvec;
//            return;
//        }
//        int spacepos = 0;
//        vector<int> tmp_a;
//        //cout<<"link_a:"<<links_a<<endl;
//        while((spacepos = links_a.find_first_of(" ")) != links_a.npos){
//            int to;
//            if(spacepos > 0){
//                to = boost::lexical_cast<int>(links_a.substr(0, spacepos));
//                //cout<<"to:"<<to<<endl;
//            }
//            links_a= links_a.substr(spacepos+1);
//            tmp_a.push_back(to);
//        }
//        linkvec.push_back(tmp_a);
//        spacepos = 0;
//        vector<int> tmp_b;
//        while((spacepos = links_b.find_first_of(" ")) != links_b.npos){
//            int to;
//            if(spacepos > 0){
//                to = boost::lexical_cast<int>(links_b.substr(0, spacepos));
//               // cout<<"to:"<<to<<endl;
//            }
//            links_b = links_b.substr(spacepos+1);
//            tmp_b.push_back(to);
//        }
//        linkvec.push_back(tmp_b);
//        data = linkvec;
//        //cout<<"read_data"<<endl;
//    }
//
//    void init_c(const string& k, double& delta,vector<vector<int> >& data){
//         // cout<<"int_c1"<<endl;
//            string key=k;
//            int pos=key.find("_");
//            string key_a=key.substr(0,pos);
//            string key_b=key.substr(pos+1);
//            if(key_a==key_b){
//                delta=data[0][0]/0.8; 
//            }else{
//                delta=0;
//            }
//           // cout<<"int_c"<<endl;
// 
//                
//    } 
//    void init_v(const string& k,double& v,vector<vector<int> >& data){
//            v=0.0;
//    }
//    void process_delta_v(const string& k, double& delta, double& value, vector<vector<int> >& data){
//        //if(count==0) return;
//        if(delta==0) return;
//        int I_ab=boost::lexical_cast<int>(data[0][0]);
//        if(I_ab==0)return;  
//        delta=(delta*0.8)/I_ab;
//       // cout<<k<<"  process_delta_V:  "<<delta<<endl;
//        
//    }
//
//    void accumulate(double& a, const double& b){
//            a = a + b;
//    }
//
//    void priority(double& pri, const double& value, const double& delta){
//            pri = delta;
//    }
//
//    void g_func(const string& k, const double& delta,const double&value, const vector<vector<int> >& data, vector<pair<string, double> >* output){
//            if(data.size()<3){
//                return;
//            }
//            string key=k;
//            count++;
//            //cout<<"v:"<<value<<endl;
//            if(delta==0) return;
//            //count++;
//            int I_ab=data[0][0];
//            if(I_ab==0)return;
//            //cout<<"g_fuc_key"<<key<<endl;
//            double outv = delta;
//            int size_a=data[1].size();
//            int size_b=data[2].size();
//            if(size_a==0||size_b==0) return;
//            vector<string> list;
//            //list.push_back(" ");
//            for(vector<int>::const_iterator it_a=data[1].begin(); it_a!=data[1].end(); it_a++){
//                for(vector<int>::const_iterator it_b=data[2].begin(); it_b!=data[2].end(); it_b++){
//                    int a= *it_a;
//                    int b= *it_b;
//                    //cout<<"a:"<<a<<"    "<<"b:"<<b<<endl;
//                    string key_a=boost::lexical_cast<string>(a);
//                    string key_b=boost::lexical_cast<string>(b);
//                    string key;
//                    if(a==b) continue;
//                    if(a<b){
//                        key=key_a+"_"+key_b;
//                    }else {
//                        key=key_b+"_"+key_a;
//                    }
//                    int pos=1;
//                    for(vector<string>::const_iterator it=list.begin(); it!=list.end(); it++){
//                        string k=*it;
//                        if(k==key){
//                            pos=0;
//                            break;
//                        }
//                    }
//                    if(pos==1){
//                        list.push_back(key);
//                        output->push_back(make_pair(key,outv));
//                       // cout<<k <<"  "<<key<<"  "<<outv<<endl;
//                    }     
//                }
//            } 
//           // cout<<"g_fun"<<endl;
//    }
//   
//    const double& default_v() const {
//            return zero;
//    }
//};
//  struct SUM : public TermChecker<string, double> {
//    double last;
//    double curr;
//    
//    SUM(){
//        last = -std::numeric_limits<double>::max();
//        curr = 0;
//    }
//
//    double set_curr(){
//        return curr;
//    }
//    
//    double estimate_prog(LocalTableIterator<string, double>* statetable){
//        double partial_curr = 0;
//        double defaultv = statetable->defaultV();
//        while(!statetable->done()){
//            bool cont = statetable->Next();
//            if(!cont) break;
//            //statetable->Next();
//            //cout << statetable->key() << "\t" << statetable->value2() << endl;
//            if(statetable->value2() != defaultv){
//                partial_curr += static_cast<double>(statetable->value2());
//            }
//        }
//        return partial_curr;
//    }
//    
//    bool terminate(vector<double> local_reports){
//        curr = 0;
//        vector<double>::iterator it;
//        for(it=local_reports.begin(); it!=local_reports.end(); it++){
//                curr += *it;
//        }
//        
//        VLOG(0) << "terminate check : last progress " << last << " current progress " << curr << " difference " << abs(curr-last);
//        VLOG(0)<<"FLAGS_termcheck_threshold: "<<FLAGS_termcheck_threshold;
//        if(abs(curr) >= FLAGS_termcheck_threshold){
//            //VLOG(0)<<"FLAGS_termcheck_threshold: "<<FLAGS_termcheck_threshold << "currtrue: "<< curr;
//            return true;
//        }else{
//            last = curr;
//            //VLOG(0)<<"FLAGS_termcheck_threshold: "<<FLAGS_termcheck_threshold << "curr: "<< curr;
//            return false;
//        }
//    }
//  };
//static int Simrank(ConfigData& conf) {
//    MaiterKernel<string, double, vector<vector<int> > >* kernel = new MaiterKernel<string, double, vector<vector<int> > >(
//                                        conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
//                                        new Sharding::Mod_str,
//                                        new Simrankiterate,
//                                        new SUM);
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
//REGISTER_RUNNER(Simrank);
//
//
