#ifndef CEPH_RGW_LC_S3_H
#define CEPH_RGW_LC_S3_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include <expat.h>

#include "include/str_list.h"
#include "rgw_lc.h"
#include "rgw_xml.h"



using namespace std;

class LCID_S3 : public XMLObj
{
public:
  LCID_S3() {}
  ~LCID_S3() {}
  string& to_str() { return data; }
};

class LCPrefix_S3 : public XMLObj
{
public:
  LCPrefix_S3() {}
  ~LCPrefix_S3() {}
  string& to_str() { return data; }
};

class LCStatus_S3 : public XMLObj
{
public:
  LCStatus_S3() {}
  ~LCStatus_S3() {}
  string& to_str() { return data; }
};

class LCDays_S3 : public XMLObj
{
public:
  LCDays_S3() {}
  ~LCDays_S3() {}
  string& to_str() { return data; }
};

class LCExpiration_S3 : public LCExpiration, public XMLObj
{
public:
  LCExpiration_S3() {}
  ~LCExpiration_S3() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out) {
    out << "<Expiration>" << "<Days>" << days << "</Days>"<< "</Expiration>";
  }
  void dump_xml(Formatter *f) const {
	  f->open_object_section("Expiration");
	  encode_xml("Days", days, f);
	  f->close_section(); // Expiration
  }
};

class LCRule_S3 : public LCRule, public XMLObj
{
public:
  LCRule_S3() {}
  ~LCRule_S3() {}

  void to_xml(CephContext *cct, ostream& out);
  bool xml_end(const char *el);
  bool xml_start(const char *el, const char **attr);
  void dump_xml(Formatter *f) const {
	  const LCExpiration_S3& expir = static_cast<const LCExpiration_S3&>(expiration);

	  f->open_object_section("Rule");
	  encode_xml("ID", id, f);
	  encode_xml("Prefix", prefix, f);
	  encode_xml("Status", status, f);
	  expir.dump_xml(f);
	  f->close_section(); // Rule
  }
};

class RGWLCXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el);
public:
  RGWLCXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};

class RGWLifecycleConfiguration_S3 : public RGWLifecycleConfiguration, public XMLObj
{
public:
  RGWLifecycleConfiguration_S3(CephContext *_cct) : RGWLifecycleConfiguration(_cct) {}
  RGWLifecycleConfiguration_S3() : RGWLifecycleConfiguration(NULL) {}
  ~RGWLifecycleConfiguration_S3() {}

  bool xml_end(const char *el);

  void to_xml(ostream& out) {
    out << "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
    for (auto iter = rule_set.begin(); iter != rule_set.end(); ++iter) {
      LCRule_S3& rule = static_cast<LCRule_S3&>(iter->second);
      rule.to_xml(cct, out);
    }
    out << "</LifecycleConfiguration>";
  }
  int rebuild(RGWRados *store, RGWLifecycleConfiguration& dest);
  void dump_xml(Formatter *f) const;
};


#endif
