#ifndef __RECORD_MODEL_TYPES__HEADER__
#define __RECORD_MODEL_TYPES__HEADER__

#include <stdint.h>  // uint32_t...
#include <strings.h> // bzero, memset
#include <string.h>  // memcpy
#include <assert.h>  // assert
#include <limits>    // std::numeric_limits
#include <stdlib.h>  // atof
#include "ruby.h"    // Ruby

struct RM_Type
{
  uint16_t _offset;

  inline uint16_t offset() { return _offset; } 

  virtual uint8_t size() = 0;

  virtual VALUE to_ruby(const void *a) = 0;
  virtual void set_from_ruby(void *a, VALUE val) = 0;
  virtual void set_from_string(void *a, const char *s, const char *e) = 0;
  virtual void set_from_memory(void *a, const void *ptr) = 0;

  // ptr must point to a valid memory frame of size()
  virtual void copy_to_memory(const void *a, void *ptr) = 0;

  virtual void set_min(void *a) = 0;
  virtual void set_max(void *a) = 0;

  virtual void add(void *a, const void *b) = 0;
  virtual void inc(void *a) = 0;
  virtual void copy(void *a, const void *b) = 0;
  virtual int between(const void *c, const void *l, const void *r) = 0;
  virtual int memory_between(const void *mem, const void *l, const void *r) = 0;

  virtual int compare(const void *a, const void *b) = 0;

  // mem is not a pointer to a record, but to the field itself
  virtual int compare_with_memory(const void *a, const void *mem) = 0;
};

template <typename NT>
struct RM_UInt : RM_Type
{
  inline NT *element_ptr(void *data) { return (NT*) (((char*)data)+offset()); }
  inline NT &element(void *data) { return *element_ptr(data); }
  inline NT element(const void *data) { return *element_ptr((void*)data); }

  virtual uint8_t size() { return sizeof(NT); }

  virtual VALUE to_ruby(const void *a)
  {
    return ULONG2NUM(element(a));
  }

  int8_t order; // -1 means ascending, +1 means descending

  RM_UInt()
  {
    order = -1;
  }

  void ascending()
  {
    order = -1;
  }

  void descending()
  {
    order = 1
  }

  void _set_uint(void *a, uint64_t v)
  {
    if (!(v >= std::numeric_limits<NT>::min() &&
          v <= std::numeric_limits<NT>::max()))
      rb_raise(rb_eArgError, "Integer out of range: %ld", v);

    element(a) = (NT)v;
  }

  virtual void set_from_ruby(void *a, VALUE val)
  {
    _set_uint(a, (uint64_t)NUM2ULONG(val));
  }

  // XXX: handle conversion failures
  static uint64_t conv_str_to_uint(const char *s, const char *e)
  {
    uint64_t v = 0;
    for (; s != e; ++s)
    {
      char c = *s;
      if (c >= '0' && c <= '9')
      {
        v *= 10;
        v += (c-'0');
      }
      else
      {
        return 0; // invalid
      }
    }
    return v;
  }

  virtual void set_from_string(void *a, const char *s, const char *e)
  {
    _set_uint(a, conv_str_to_uint(s, e));
  }

  virtual void set_from_memory(void *a, const void *ptr)
  {
    element(a) = *((const NT*)ptr);
  }

  virtual void copy_to_memory(const void *a, void *ptr)
  {
    *((NT*)ptr) = element(a); 
  }

  virtual void set_min(void *a)
  {
    element(a) = (order < 0) ? std::numeric_limits<NT>::min() : std::numeric_limits<NT>::max();
  }

  virtual void set_max(void *a)
  {
    element(a) = (order < 0) ? std::numeric_limits<NT>::max() : std::numeric_limits<NT>::min();
  }

  virtual void add(void *a, const void *b)
  {
    element(a) += element(b);
  }

  virtual void inc(void *a)
  {
    element(a) += (-order);
  }
 
  virtual void copy(void *a, const void *b)
  {
    element(a) = element(b);
  }

  virtual int between(const void *c, const void *l, const void *r)
  {
    if (element(c) < element(l)) return order;
    if (element(c) > element(r)) return -order;
    return 0;
  }

  virtual int memory_between(const void *mem, const void *l, const void *r)
  {
    NT c = *((const NT*)mem);
    if (c < element(l)) return order;
    if (c > element(r)) return -order;
    return 0;
  }

  virtual int compare(const void *a, const void *b)
  {
    if (element(a) < element(b)) return order;
    if (element(a) > element(b)) return -order;
    return 0;
  }

  virtual int compare_with_memory(const void *a, const void *mem)
  {
    NT b = *((const NT*)mem);
    if (element(a) < b) return order;
    if (element(a) > b) return -order;
    return 0;
  }
};

struct RM_UINT8 : RM_UInt<uint8_t> {}; 
struct RM_UINT16 : RM_UInt<uint16_t> {};
struct RM_UINT32 : RM_UInt<uint32_t> {};
struct RM_UINT64 : RM_UInt<uint64_t> {};

// with millisecond precision
struct RM_TIMESTAMP : RM_UInt<uint64_t>
{
  // XXX: handle conversion failures
  static uint64_t conv_str_to_uint2(const char *s, const char *e, int precision)
  {
    uint64_t v = 0;
    int post_digits = -1; 
    for (; s != e; ++s)
    {
      char c = *s;
      if (c >= '0' && c <= '9')
      {
        v *= 10;
        v += (c-'0');
        if (post_digits >= 0)
          ++post_digits;
      }
      else if (c == '.')
      {
        if (post_digits >= 0)
        {
          return 0; // invalid
        }
        // ignore
        post_digits = 0;
      }
      else
      {
        return 0; // invalid
      }
    }

    for (; post_digits < precision; ++post_digits)
    {
      v *= 10;
    }

    for (; post_digits > precision; --post_digits)
    {
      v /= 10;
    }
 
    return v;
  }

  virtual void set_from_string(void *a, const char *s, const char *e)
  {
    _set_uint(a, conv_str_to_uint2(s, e, 3));
  }
};

struct RM_DOUBLE : RM_Type 
{
  typedef double NT;

  inline NT *element_ptr(void *data) { return (NT*) (((char*)data)+offset()); }
  inline NT &element(void *data) { return *element_ptr(data); }
  inline NT element(const void *data) { return *element_ptr((void*)data); }

  virtual uint8_t size() { return sizeof(NT); }

  virtual VALUE to_ruby(const void *a)
  {
    return rb_float_new(element(a));
  }

  virtual void set_from_ruby(void *a, VALUE val)
  {
    element(a) = (NT)NUM2DBL(val);
  }

  // XXX: remove char* cast and string modification!
  static double conv_str_to_double(const char *s, const char *e)
  {
    char c = *e;
    *((char*)e) = '\0';
    double v = atof(s);
    *((char*)e) = c;
    return v;
  }

  virtual void set_from_string(void *a, const char *s, const char *e)
  {
    element(a) = conv_str_to_double(s, e);
  }

  virtual void set_from_memory(void *a, const void *ptr)
  {
    element(a) = *((const NT*)ptr);
  }

  virtual void copy_to_memory(const void *a, void *ptr)
  {
    *((NT*)ptr) = element(a); 
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
    element(a) += element(b); 
  }

  virtual void inc(void *a)
  {
    // DO NOTHING here!
  }

  virtual void copy(void *a, const void *b)
  {
    element(a) = element(b);
  }

  virtual int between(const void *c, const void *l, const void *r)
  {
    if (element(c) < element(l)) return -1;
    if (element(c) > element(r)) return 1;
    return 0;
  }

  virtual int memory_between(const void *mem, const void *l, const void *r)
  {
    double c = *((const double*)mem);
    if (c < element(l)) return -1;
    if (c > element(r)) return 1;
    return 0;
  }

  virtual int compare(const void *a, const void *b)
  {
    if (element(a) < element(b)) return -1;
    if (element(a) > element(b)) return 1;
    return 0;
  }

  virtual int compare_with_memory(const void *a, const void *mem)
  {
    NT b = *((const NT*)mem);
    if (element(a) < b) return -1;
    if (element(a) > b) return 1;
    return 0;
  }
};

struct RM_HEXSTR : RM_Type
{
  uint8_t _size;

  RM_HEXSTR(uint8_t size) : _size(size) {}

  virtual uint8_t size() { return _size; }

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

  virtual void set_from_string(void *a, const char *s, const char *e)
  {
    parse_hexstring(element_ptr(a), (int)(e-s), s);
  }

  virtual void set_from_memory(void *a, const void *ptr)
  {
    memcpy(element_ptr(a), ptr, size());
  }

  virtual void copy_to_memory(const void *a, void *ptr)
  {
    memcpy(ptr, element_ptr(a), size());
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

  inline int between_pointers(const uint8_t *cp, const uint8_t *lp, const uint8_t *rp)
  {
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

  virtual int between(const void *c, const void *l, const void *r)
  {
    return between_pointers(element_ptr(c), element_ptr(l), element_ptr(r));
  }

  virtual int memory_between(const void *mem, const void *l, const void *r)
  {
    return between_pointers((const uint8_t*)mem, element_ptr(l), element_ptr(r));
  }

  inline int compare_pointers(const uint8_t *ap, const uint8_t *bp)
  {
    for (int i=0; i < size(); ++i)
    {
      if (ap[i] < bp[i]) return -1;
      if (ap[i] > bp[i]) return 1;
    }
    return 0;
  }

  virtual int compare(const void *a, const void *b)
  {
    return compare_pointers(element_ptr(a), element_ptr(b));
  }

  virtual int compare_with_memory(const void *a, const void *mem)
  {
    return compare_pointers(element_ptr(a), (const uint8_t*)mem);
  }

};

#endif
