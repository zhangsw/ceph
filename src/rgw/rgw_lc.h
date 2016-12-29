#ifndef CEPH_RGW_LC_H
#define CEPH_RGW_LC_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "common/debug.h"

#include "include/types.h"
#include "include/atomic.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "cls/rgw/cls_rgw_types.h"

using namespace std;
#define HASH_PRIME 7877
#define MAX_ID_LEN 255
static string lc_oid_prefix = "lc";
static string lc_index_lock_name = "lc_process";

extern const char* LC_STATUS[];

typedef enum {
  lc_uninitial = 0,
  lc_processing,
  lc_failed,
  lc_complete,
}LC_BUCKET_STATUS;

class LCExpiration
{
protected:
  string days;
public:
  LCExpiration() {}
  ~LCExpiration() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(days, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(days, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
//  static void generate_test_instances(list<ACLOwner*>& o);
  void set_days(const string& _days) { days = _days; }
  int get_days() {return atoi(days.c_str()); }
};
WRITE_CLASS_ENCODER(LCExpiration)

class LCRule
{
protected:
  string id;
  string prefix;
  string status;
  LCExpiration expiration;

public:

  LCRule(){};
  ~LCRule(){};

  string get_id() const{
      return id;
  }

  string get_status() const{
      return status;
  }
  
  string get_prefix() const{
      return prefix;
  }

  LCExpiration get_expiration() const{
    return expiration;
  }

  void set_id(string*_id) {
    id = *_id;
  }

  void set_prefix(string*_prefix) {
    prefix = *_prefix;
  }

  void set_status(string*_status) {
    status = *_status;
  }

  void set_expiration(LCExpiration*_expiration) {
    expiration = *_expiration;
  }

  bool validate() const;
  
  void encode(bufferlist& bl) const {
     ENCODE_START(1, 1, bl);
     ::encode(id, bl);
     ::encode(prefix, bl);
     ::encode(status, bl);
     ::encode(expiration, bl);
     ENCODE_FINISH(bl);
   }
   void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
     ::decode(id, bl);
     ::decode(prefix, bl);
     ::decode(status, bl);
     ::decode(expiration, bl);
     DECODE_FINISH(bl);
   }

};
WRITE_CLASS_ENCODER(LCRule)

struct rule_comp 
{
  bool operator() (const LCRule& lhs, const LCRule& rhs) const {
    if (lhs.get_id().compare(rhs.get_id()) == 0) {
      return true;
    } else {
      string l_prefix = lhs.get_prefix();
      string r_prefix = lhs.get_prefix();
      if (l_prefix.length() > r_prefix.length()) {
        if (l_prefix.compare(0, r_prefix.length(), r_prefix) == 0) {
          return true;
        }
      } else {
        if (r_prefix.compare(0, l_prefix.length(), l_prefix) == 0) {
          return true;
        }
      }
      return lhs.get_id().compare(rhs.get_id()) < 0;
    }
  }
};

class RGWLifecycleConfiguration
{
protected:
  CephContext *cct;
  map<string, int> prefix_map;
  set<LCRule, rule_comp> rule_set;
  void _add_rule(LCRule *rule);
public:
  RGWLifecycleConfiguration(CephContext *_cct) : cct(_cct) {}
  RGWLifecycleConfiguration() : cct(NULL) {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
  }

  virtual ~RGWLifecycleConfiguration() {}

//  int get_perm(string& id, int perm_mask);
//  int get_group_perm(ACLGroupTypeEnum group, int perm_mask);
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(rule_set, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(rule_set, bl);
    for (auto iter = rule_set.begin(); iter != rule_set.end(); ++iter) {
      LCRule& rule = iter->second;
      _add_rule(&rule);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
//  static void generate_test_instances(list<RGWAccessControlList*>& o);

  bool add_rule(LCRule* rule);
  
  set<LCRule, rule_comp>& get_rule_set() { return rule_set; }
  map<string, int>& get_prefix_map() { return prefix_map; }
/*
  void create_default(string id, string name) {
    ACLGrant grant;
    grant.set_canon(id, name, RGW_PERM_FULL_CONTROL);
    add_grant(&grant);
  }
*/
};
WRITE_CLASS_ENCODER(RGWLifecycleConfiguration)

class RGWLC {
  CephContext *cct;
  RGWRados *store;
  int max_objs;
  string *obj_names;
  atomic_t down_flag;
  string cookie;

  class LCWorker : public Thread {
    CephContext *cct;
    RGWLC *lc;
    Mutex lock;
    Cond cond;

  public:
    LCWorker(CephContext *_cct, RGWLC *_lc) : cct(_cct), lc(_lc), lock("LCWorker") {}
    void *entry();
    void stop();
    bool should_work(utime_t& now);
    int schedule_next_start_time(utime_t& start, utime_t& now);
  };
  
  public:
  LCWorker *worker;
  RGWLC() : cct(NULL), store(NULL), worker(NULL) {}
  ~RGWLC() {
    stop_processor();
    finalize();
  }

  void initialize(CephContext *_cct, RGWRados *_store);
  void finalize();

  int process();
  int process(int index, int max_secs);
  bool if_already_run_today(time_t& start_date);
  int list_lc_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map);
  int bucket_lc_prepare(int index);
  int bucket_lc_process(string& shard_id);
  int bucket_lc_post(int index, int max_lock_sec, cls_rgw_lc_obj_head& head, 
                                                              pair<string, int >& entry, int& result);
  bool going_down();
  void start_processor();
  void stop_processor();

  private:
  bool obj_has_expired(double timediff, int days);
};



#endif
