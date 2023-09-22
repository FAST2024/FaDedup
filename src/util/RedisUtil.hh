#ifndef _REDIS_UTIL_HH_
#define _REDIS_UTIL_HH_

#include "../inc/include.hh"

enum {
    REDIS_CREATION_FAILURE=1, REDIS_BLPOP_FAILURE, REDIS_RPUSH_FAILURE
};

using namespace std;

class RedisUtil {
  public:
    static string ip2Str(unsigned int ip);

    static redisContext* createContext(unsigned int ip);
    static redisContext* createContext(string ip);
    static redisContext* createContext(string ip, int port);

    // return length of the pop'ed content
    static int blpopContent(redisContext*, const char* key, char* dst, int length);
    static void rpushContent(redisContext*, const char* key, const char* src, int length);
    static double duration(struct timeval t1, struct timeval t2);
    static vector<string> str2container(string line);
    static void AskRedisContext(unsigned int ip) {
      // _redis_context_cnt_map[ip2Str(ip)]++;
      if(ip2Str(ip) != "192.168.0.7") {
        return ;
      }
      while(true) {
        _redis_cnt_mutex.lock();
        if(_redis_context_cnt < _MAX_REDIS_CONTEXT_NUM) {
          _redis_context_cnt++;
          _redis_cnt_mutex.unlock();
          return;
        } else {
          cout << "[RedisUtil] connect to 192.168.0.7 blocked" << endl;
          _redis_cnt_mutex.unlock();
          usleep(100000);
        }
      }
    }
    static void FreeRedisContext(unsigned int ip) {
      if(ip2Str(ip) != "192.168.0.7") {
        return ;
      }
      _redis_cnt_mutex.lock();
      _redis_context_cnt--;
      _redis_cnt_mutex.unlock();
    }
    static std::mutex _redis_cnt_mutex;
    static const int _MAX_REDIS_CONTEXT_NUM = 1;
    static int _redis_context_cnt;
    static unordered_map<string, int> _redis_context_cnt_map;
};

#endif