#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <atomic>
#include "libcuckoo/cuckoohash_map.hh"

#include "tracer1.h"

#define LOCAL true

#define UPDATE_ONLY false
#define THREAD_NUM thread_num
#define LOAD_THREAD_NUM 1
#define TEST_TIME test_time
#define LOADER_NUM 100000000
#define RUNNER_NUM runner_num

#if(LOCAL)
string load_path = "/home/czl/CLionProjects/test_cuckoo/load-a.dat";
string run_path = "/home/czl/CLionProjects/test_cuckoo/run-a.dat";
#else
string load_path = "/mnt/nvme/czl/YCSB/load-a-200m-8B.dat";
string run_path = "/mnt/nvme/czl/YCSB/run-a-200m-8B.dat";
#endif


using namespace ycsb;

libcuckoo::cuckoohash_map<string, string> Table;

typedef struct {
    string key;
    string value;
    bool r;
} ITEM;

int thread_num = 4;
int test_time = 0;
atomic<int> stopMeasure(0);\

uint64_t runner_num;

unsigned long *runtimelist;

std::vector<YCSB_request *> loads;

std::vector<YCSB_request *> runs;

void loadert(int tid);

void runnert(int tid);

void show_info(bool load) {
    double throughput = 0.0;
    unsigned long runtime = 0;
    for (int i = 0; i < THREAD_NUM; i++) {
        runtime += runtimelist[i];
    }
    if (load) {
        runtime /= (LOAD_THREAD_NUM);
        throughput = LOADER_NUM * 1.0 / runtime;
        cout << "load runtime: " << runtime << endl;
        cout << "load thorughput :" << throughput << endl;
    } else {
        runtime /= (THREAD_NUM);
        throughput = RUNNER_NUM * 1.0 / runtime;
        cout << "run runtime: " << runtime << endl;
        cout << "**********run thorughput :" << throughput << endl;
    }

}

int main(int argc, char **argv) {
    char *path;
    if (argc == 3) {
        THREAD_NUM = std::atol(argv[1]);
        TEST_TIME = std::atol(argv[2]);
        runtimelist = (unsigned long *) calloc(THREAD_NUM , sizeof(unsigned long));
    } else {
        printf("./ycsb <thread_num>  <test_time>\n");
        return 0;
    }

    cout << "load file:" << load_path << endl;
    cout << "run  file:" << run_path << endl;
    cout << "thread_num:" << THREAD_NUM << endl;
    cout << "test_time:" << TEST_TIME <<endl;

    {
        YCSBLoader loader(load_path.c_str());

        loads = loader.load();

        cout << "finish load load file" << endl;
        std::vector<std::thread> threads;

        for (int i = 0; i < LOAD_THREAD_NUM; i++) {
            threads.push_back(std::thread(loadert, i));
        }
        cout << "finish create thread ==>" << LOAD_THREAD_NUM << endl;
        for (int i = 0; i < LOAD_THREAD_NUM; i++) {
            threads[i].join();
        }
        cout << "load stop" << endl;

        show_info(true);

    }
    for (int i = 0; i < THREAD_NUM; i++) {
        runtimelist[i] = 0;
    }
    printf("---\nfinish load start runing\n---\n");
    {
        YCSBLoader loader1(run_path.c_str());

        runs = loader1.load();

        cout << "finish load run file" << endl;

        std::vector<std::thread> threads;

        for (int i = 0; i < THREAD_NUM; i++) {
            threads.push_back(std::thread(runnert, i));
        }
        cout << "finish create thread ==>" << THREAD_NUM << endl;
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].join();
        }
        cout << "run stop" << endl;

        show_info(false);
    }
    cout <<endl<<endl;
    return 0;
}

void loadert(int tid) {
    std::vector<YCSB_request *> *work_load;
    work_load = &loads;

    int send_num = work_load->size() / LOAD_THREAD_NUM;
    int start_index = tid * send_num;
    int p = tid == LOAD_THREAD_NUM - 1 ? send_num + work_load->size() % LOAD_THREAD_NUM : send_num;

    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < p; i++) {
        YCSB_request *it = work_load->at(start_index + i);
        if (!it->getOp()) {
            try {
                string s;
                //string res = Table.find(it->getKey());
                Table.find(it->getKey(), s);
                //Table.find_KV(it->getKey(),s);
            } catch (const std::out_of_range &e) {
                std::cout << e.what() << endl;
                exit(-1);
            }
        } else {
            Table.insert_or_assign(it->getKey(), it->getVal());
        }
    }
    runtimelist[tid] += tracer.getRunTime();
}


void runnert(int tid) {
    std::vector<YCSB_request *> *work_load;
    work_load = &runs;

    int send_num = work_load->size() / THREAD_NUM;
    int start_index = tid * send_num;

    Tracer tracer;
    tracer.startTime();

    while(stopMeasure.load(memory_order_relaxed) == 0){
        uint64_t opcount = 0;
        for (int i = 0; i < send_num; i++) {
            YCSB_request *it = work_load->at(start_index + i);
            if (!it->getOp()) {
#if (!UPDATE_ONLY)
                try {
                    string s;
                    //string res = Table.find(it->getKey());
                    bool b = Table.find(it->getKey(), s);
                    if(!b) {printf("key not found error\n");exit(-1);}
                    ++opcount ;
                    //Table.find_KV(it->getKey(),s);
                } catch (const std::out_of_range &e) {
                    std::cout << e.what() << endl;
                    exit(-1);
                }
#endif
            } else {
                Table.insert_or_assign_u(it->getKey(), it->getVal());
                ++opcount;
            }
        }

        __sync_fetch_and_add(&runner_num,opcount);
        uint64_t tmptruntime = tracer.fetchTime();
        if(tmptruntime / 1000000 > TEST_TIME){
            stopMeasure.store(1, memory_order_relaxed);
        }
    }

    runtimelist[tid] += tracer.getRunTime();
}







