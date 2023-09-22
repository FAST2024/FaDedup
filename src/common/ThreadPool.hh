#ifndef _THREAD_POOL_HH_
#define _THREAD_POOL_HH_

#include "../inc/include.hh"

using namespace std;
class ThreadPool {
public:
    typedef function<void()> Task;

    ThreadPool();
    ~ThreadPool();
    
public:
    size_t initnum;
    // thread vector
    vector<thread>threads ;
    
    // task queue
    queue<Task>task ;
    
    mutex _mutex ;
    condition_variable cond ;
    
    // when the thread pool finished all tasks, done is true
    bool done ;
    
    // task queue is empty
    bool isEmpty ;
    // task queue is full
    bool isFull;

public:
    void addTask(const Task&f);
    void start(int num);
    void setSize(int num);
    void runTask();
    void finish();
};


#endif