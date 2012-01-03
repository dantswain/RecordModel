#ifndef __RECORD_MODEL_TYPES__HEADER__
#define __RECORD_MODEL_TYPES__HEADER__

#include <stdint.h>  // uint32_t...
#include <strings.h> // bzero, memset
#include <string.h>  // memcpy
#include <assert.h>  // assert
#include <limits>    // std::numeric_limits
#include "ruby.h"    // Ruby

class RM_Type
{
  uint16_t _offset;
  uint8_t _size;

  inline uint16_t offset() { return _offset; } 
  inline uint8_t size() { return _size; } 

  virtual VALUE to_ruby(const void *a) = 0;
  virtual void set_from_ruby(void *a, VALUE val) = 0;

  virtual void set_min(void *a) = 0;
  virtual void set_max(void *a) = 0;

  virtual void add(void *a, const void *b) = 0;
  virtual void inc(void *a) = 0;
  virtual void copy(void *a, const void *b) = 0;
  virtual int between(const void *c, const void *l, const void *r) = 0;
  virtual int compare(const void *a, const void *b) = 0;
};

template <typename NT>
struct RM_UInt : RM_Type
{
  inline NT *element_ptr(void *data) { return (NT*) (((char*)data)+offset()); }
  inline NT &element(void *data) { return *element_ptr(data); }
  inline NT element(const void *data) { return *element_ptr((void*)data); }

  virtual VALUE to_ruby(const void *a)
  {
    return ULONG2NUM(element(a));
  }

  virtual void set_from_ruby(void *a, VALUE val)
  {
    uint64_t v = NUM2ULONG(val);

    if (!(v >= std::numeric_limits<NT>::min() &&
          v <= std::numeric_limits<NT>::max()))
      rb_raise(rb_eArgError, "Integer out of range: %ld", v);

    element(a) = (NT)v;
  }

  virtual void set_min(void *a)
  {
    element(a) = std::numeric_limits<NT>::min();
  }

  virtual void set_max(void *a)
  {
    element(a) = std::numeric_limits<NT>::max();
  }

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

};

struct RM_UINT8 : RM_UInt<uint8_t>
{
  //static const int TYPE = 0x0001;
};

struct RM_UINT16 : RM_UInt<uint16_t>
{
  //static const int TYPE = 0x0002;
};

struct RM_UINT32 : RM_UInt<uint32_t>
{
  //static const int TYPE = 0x0004;
};

struct RM_UINT64 : RM_UInt<uint64_t>
{
  //static const int TYPE = 0x0008;
};

struct RM_DOUBLE : RM_Type 
{
  //static const int TYPE = 0x0108;

  inline double *element_ptr(void *data) { return (double*) (((char*)data)+offset()); }
  inline double &element(void *data) { return *element_ptr(data); }
  inline double element(const void *data) { return *element_ptr((void*)data); }

  virtual VALUE to_ruby(const void *a)
  {
    return rb_float_new(element(a));
  }

  virtual void set_from_ruby(void *a, VALUE val)
  {
    element(a) = (double)NUM2DBL(val);
  }

  virtual void set_min(void *a)
  {
    // XXX: -INF
    element(a) = std::numeric_limits<double>::min();
  }

  virtual void set_max(void *a)
  {
    // XXX: +INF
    element(a) = std::numeric_limits<double>::max();
  }

  virtual void add(void *a, const void *b)
  {
    element(a) += element(b) 
  }

  virtual void inc(void *a)
  {
    // DO NOTHING here!
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
};


struct RM_HEXSTRING 
{
  inline uint8_t *element_ptr(void *data) { return (uint8_t*) (((char*)data)+offset()); }
  inline const uint8_t *element_ptr(const void *data) { return (const uint8_t*) (((const char*)data)+offset()); }

  char to_hex_digit(uint8_t v)
  {
    if (/*v >= 0 && */v <= 9) return '0' + v;
    if (v >= 10 && v <= 15) return 'A' + v - 10;
    return '#';
  }

  virtual VALUE to_ruby(const void *a)
  {
    const uint8_t *ptr = element_ptr(a);

    VALUE strbuf = rb_str_buf_new(2*size());
    char cbuf[3];
    cbuf[2] = 0;
    for (int i = 0; i < size(); ++i)
    {
      cbuf[0] = to_hex_digit((ptr[i]) >> 4);
      cbuf[1] = to_hex_digit((ptr[i]) & 0x0F);
      rb_str_buf_cat_ascii(strbuf, cbuf);
    }
    return strbuf;
  }

  int from_hex_digit(char c)
  {
    if (c >= '0' && c <= '9') return c-'0';
    if (c >= 'a' && c <= 'f') return c-'a'+10;
    if (c >= 'A' && c <= 'F') return c-'A'+10;
    return -1;
  }

  void parse_hexstring(uint8_t *v, int strlen, const char *str)
  {
    const int max_sz = 2*size();
    const int i_off = max_sz - strlen;

    if (strlen > max_sz)
    {
      rb_raise(rb_eArgError, "Invalid string size. Was: %d, Max: %d",
               strlen, max_sz);
    }

    bzero(v, size());

    for (int i = 0; i < strlen; ++i)
    {
      int digit = from_hex_digit(str[i]);
      if (digit < 0)
        rb_raise(rb_eArgError, "Invalid hex digit at %s", &str[i]);

      v[(i+i_off)/2] = (v[(i+i_off)/2] << 4) | (uint8_t)digit;
    }
  }

  virtual void set_from_ruby(void *a, VALUE val)
  {
    Check_Type(val, T_STRING);
    parse_hexstring(element_ptr(a), RSTRING_LEN(val), RSTRING_PTR(val));
  }

  virtual void set_min(void *a)
  {
    memset(element_ptr(a), 0, size());
  }
  
  virtual void set_max(void *a)
  {
    memset(element_ptr(a), 0xFF, size());
  }

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

};

#endif
