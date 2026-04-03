#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    for (int i=1; i<argc; i++) {
        std::string a = argv[i];

        if (a == "--r_name" && i + 1 < argc) {
            i++;
            r_name = argv[i];
        } 
        else if (a == "--start_date" && i + 1 < argc) {
            i++;
            start_date = argv[i];
        } 
        else if (a == "--end_date" && i + 1 < argc) {
            i++;
            end_date = argv[i];
        } 
        else if (a == "--threads" && i + 1 < argc) {
            i++;
            num_threads = std::stoi(argv[i]);
        } 
        else if (a == "--table_path" && i + 1 < argc) {
            i++;
            table_path = argv[i];
        } 
        else if (a == "--result_path" && i + 1 < argc) {
            i++;
            result_path = argv[i];
        }
    }

    // checking all req arguments are provided or not...
    if (r_name.length() == 0 || start_date.length()==0 || end_date.length()==0 || table_path.length()==0 || result_path.length()==0 || num_threads <= 0) {
        return false;
    }

    return true;
}




// Function to read TPCH data from the specified paths
std::vector<std::string> split(const std::string &line, char delimiter) {
    std::vector<std::string> parts;
    std::stringstream ss(line);
    std::string item = "";
    while(getline(ss,item, delimiter)) {
        parts.push_back(item);
    }
    return parts;
}
bool readingFile(const std::string& path, std::vector<std::map<std::string, std::string>>& data, const std::vector<std::string> &key){
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr <<"Error in opening file: "<<path<<std::endl;
        return false;
    }

    std::string line;
    while (getline(file, line)) {
        auto parts = split(line, '|');
        std::map<std::string, std::string> r;


        for (int i=0; i<key.size(); i++) {
            r[key[i]] = parts[i];
        }
        data.push_back(r);
    }
    return true;
}





bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    // TODO: Implement reading TPCH data from files
    return readingFile(table_path + "/customer.tbl", customer_data, {"c_custkey", "c_nationkey"})  &&
        readingFile(table_path + "/orders.tbl", orders_data, {"o_orderkey", "o_custkey", "o_orderdate"}) &&
        readingFile(table_path + "/lineitem.tbl", lineitem_data,{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"}) &&
        readingFile(table_path + "/supplier.tbl", supplier_data,{"s_suppkey", "s_nationkey"}) &&
        readingFile(table_path + "/nation.tbl", nation_data,{"n_nationkey", "n_name", "n_regionkey"}) &&
        readingFile(table_path + "/region.tbl", region_data,{"r_regionkey", "r_name"});
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    // TODO: Implement TPCH Query 5 using multithreading
    std::string regionKey;
    for(auto &r : region_data){ // finding region key
        if (r.at("r_name") == r_name){
            regionKey = r.at("r_regionkey");
        }
    }
    std::map<std::string, std::string>  nationMpp; // nation to region mapping
    for(auto &n : nation_data){
        if(n.at("n_regionkey")==regionKey){
            nationMpp[n.at("n_nationkey")] =n.at("n_name");

        }
    }

    std::map<std::string, std::string>supplierMpp; //supplier to nation mapping
    for(auto &s :supplier_data){
        if(nationMpp.count(s.at("s_nationkey"))){
            supplierMpp[s.at("s_suppkey")]= s.at("s_nationkey");
        }
    }
    std::map<std::string, std::string> customerMap; //customer to nation mapping
    for(auto &c :customer_data){
        if(nationMpp.count(c.at("c_nationkey"))){
            customerMap[c.at("c_custkey")]=c.at("c_nationkey");
        }
    }
    std::map<std:: string, std:: string> orderMap; //orders filter
    for(auto& o : orders_data){
        if (o.at("o_orderdate") >= start_date  && o.at("o_orderdate")<end_date){
            orderMap[o.at("o_orderkey")]  =o.at("o_custkey");
        }
    }

    // THREAD FUNCTION
    auto worker= [&](int s,int e){
        std:: map<std:: string,double>r; // temp. result map

        for(int i=s; i<e ;i++){
            auto &l = lineitem_data[i];

            std::string orderKey = l.at("l_orderkey");
            std::string supplierKey = l.at("l_suppkey");
            
            if (supplierMpp.count(supplierKey)==0 || orderMap.count(orderKey)==0)  continue;
        

            std::string customerKey=orderMap[orderKey];
            if (customerMap.count(customerKey)==0) continue;

            std::string nationKey= supplierMpp[supplierKey];
            std::string nation= nationMpp[nationKey];

            double pr= std::stod(l.at("l_extendedprice")); //price
            double dis =std::stod(l.at("l_discount")); // discount
            double rev= pr*(1-dis); // revenue

            r[nation]+=rev; //nationName
        }

        // Merging
            std::mutex mtx;
        std::lock_guard<std::mutex> lock(mtx);
        for(auto& p:r){
            results[p.first]+= p.second;
        }
    };


    // Creating Threads

    std::vector<std::thread>  threads;
    int size= lineitem_data.size()/num_threads; //chunk's size

    for (int i=0; i<num_threads; i++){
        int s= i*size;

        int e=0;
        if(i == num_threads-1){
            e = lineitem_data.size();
        }
        else{
            e = s+size;
        }

        threads.emplace_back(worker, s, e);
    }

    for (auto &t :threads){
         t.join();         
    }

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream file(result_path + "/result.txt"); //ofstream is Used to write data to a file

    if(!file.is_open()){ 
        std::cerr<<"Error: Could not open result file."<<std::endl;
        return false;
    }
    for(const auto& e :results){
        file<<e.first <<" : "<<e.second<< std::endl;
    }

    file.close();

    return true;
}