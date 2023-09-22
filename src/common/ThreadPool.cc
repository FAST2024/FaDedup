#include "ThreadPool.hh"

ThreadPool::ThreadPool():done(false),isEmpty(true),isFull(false){
}

ThreadPool::~ThreadPool(){   
}

void ThreadPool::setSize(int num){
        (*this).initnum = num ;
}

void ThreadPool::addTask(const Task&f){
   
    if(!done){ 
        unique_lock<mutex>lk(_mutex);

        while(isFull && !done){
            cond.wait(lk);
        }

        if(done) {
            return;
        }

        task.push(f);
        
        if(task.size() == initnum)
            isFull = true;
        
        isEmpty = false ;
        cond.notify_one();
    }
}

void ThreadPool::finish(){
    done = true;
    cond.notify_all();
    for(size_t i = 0; i < threads.size(); i++){
        if(threads[i].joinable())
            threads[i].join();
    }
}

void ThreadPool::runTask(){
    while(!done){
        unique_lock<mutex>lk(_mutex);
    
        while(isEmpty && !done){
            cond.wait(lk);
        }

        if (done) {
            return;
        }
        
        Task ta;
        ta = move(task.front());  
        task.pop();
        
        if(task.empty()){
            isEmpty = true;    
        }    
        
        isFull = false;
        ta();
        cond.notify_one();
    }
}

void ThreadPool::start(int num){
    setSize(num);
    for(int i = 0; i < num; i++){        
        threads.push_back(thread(&ThreadPool::runTask, this));
    }
}

