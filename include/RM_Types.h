#ifndef __RECORD_MODEL_TYPES__HEADER__
#define __RECORD_MODEL_TYPES__HEADER__

#include <stdint.h>  // uint32_t...
#include <strings.h> // bzero, memset
#include <string.h>  // memcpy
#include <assert.h>  // assert
#include <limits>    // std::numeric_limits

class RM_Type
{
  uint16_t _offset;
  uint8_t _size;

  inline uint16_t offset() { return _offset; } 
  inline uint8_t size() { return _size; } 

  virtual void set_min(void *a) = 0;
  virtual void set_max(void *a) = 0;
  virtual void add(void *a, const void *b) = 0;
  virtual void inc(void *a) = 0;
  virtual void copy(void *a, const void *b) = 0;
  virtual int between(const void *c, const void *l, const void *r) = 0;
  virtual int compare(const void *a, const void *b) = 0;
};

template <class NT>
struct RM_Numeric : RM_Type
{
  inline NT *element_ptr(void *data) { return (NT*) (((char*)data)+offset()); }
  inline NT &element(void *data) { return *element_ptr(data); }
  inline NT element(const void *data) { return *element_ptr((void*)data); }

  virtual void add(void *a, const void *b)
  {
    element(a) += element(b) 
  }

  virtual void inc(void *a)
  {
    ++element(a);
  }
 
  virtual void copy(void *a, const void *b)
  {
    element(a) = element(b) 
  }

  virtual int between(const void *c, const void *l, const void *r)
  {
    if (element(c) < element(l)) return -1;
    if (element(c) > element(r)) return 1;
    return 0;
  }

  virtual int compare(const void *a, const void *b)
  {
    if (element(a) < element(b)) return -1;
    if (element(a) > element(b)) return 1;
    return 0;
  }

  virtual void set_min(void *a)
  {
    element(a) = std::numeric_limits<NT>::min();
  }

  virtual void set_max(void *a)
  {
    element(a) = std::numeric_limits<NT>::max();
  }

};

struct RM_UINT8 : RM_Numeric<uint8_t>
{
  //static const int TYPE = 0x0001;

};

struct RM_UINT16 : RM_Numeric<uint16_t>
{
  //static const int TYPE = 0x0002;
};

struct RM_UINT32 : RM_Numeric<uint32_t>
{
  //static const int TYPE = 0x0004;
};

struct RM_UINT64 : RM_Numeric<uint64_t>
{
  //static const int TYPE = 0x0008;
};

struct RM_DOUBLE : RM_Numeric<double>
{
  //static const int TYPE = 0x0108;

  virtual void inc(void *a)
  {
    // DO NOTHING here!
  }
};

struct RM_HEXSTRING 
{
  inline uint8_t *element_ptr(void *data) { return (uint8_t*) (((char*)data)+offset()); }
  inline const uint8_t *element_ptr(const void *data) { return (const uint8_t*) (((const char*)data)+offset()); }

  virtual void add(void *a, const void *b)
  {
    // Makes no sense for HEXSTRING
    assert(false);
  }

  virtual void inc(void *a)
  {
    uint8_t *str = element_ptr(a);
    for (int i=(int)size()-1; i >= 0; --i)
    {
      if (str[i] < 0xFF)
      {
        // no overflow
        ++str[i];
        break;
      }
      else
      {
        // overflow
        str[i] = 0;
      }
    }
  }
 
  virtual void copy(void *a, const void *b)
  {
    memcpy(element_ptr(a), element_ptr(b), size());
  }

  virtual int between(const void *c, const void *l, const void *r)
  {
    const uint8_t *cp = element_ptr(c);
    const uint8_t *lp = element_ptr(l);
    const uint8_t *rp = element_ptr(r);

    // XXX: Check correctness
    for (int k=0; k < size(); ++k)
    {
      if (cp[k] < lp[k]) return -1;
      if (cp[k] > lp[k]) break;
    }
    for (int k=0; k < size(); ++k)
    {
      if (cp[k] > rp[k]) return 1;
      if (cp[k] < rp[k]) break;
    }
    return 0;
  }

  virtual int compare(const void *a, const void *b)
  {
    const uint8_t *ap = element_ptr(a);
    const uint8_t *bp = element_ptr(b);
 
    for (int i=0; i < size(); ++i)
    {
      if (a[i] < b[i]) return -1;
      if (a[i] > b[i]) return 1;
    }
    return 0;
  }

  virtual void set_min(void *a)
  {
    memset(element_ptr(a), 0, size());
  }
  
  virtual void set_max(void *a)
  {
    memset(element_ptr(a), 0xFF, size());
  }

};

#endif
