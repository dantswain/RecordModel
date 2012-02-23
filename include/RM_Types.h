#ifndef __RECORD_MODEL_TYPES__HEADER__
#define __RECORD_MODEL_TYPES__HEADER__

#include <stdint.h>  // uint32_t...
#include <strings.h> // bzero, memset
#include <string.h>  // memcpy
#include <assert.h>  // assert
#include <limits>    // std::numeric_limits
#include <stdlib.h>  // atof
#include <endian.h>  // htole64
#include <math.h>    // pow
#include "ruby.h"    // Ruby

#define RM_ERR_OK 0
#define RM_ERR_INT_RANGE 1
#define RM_ERR_INT_INV 2
#define RM_ERR_HEX_INV_SIZE 10
#define RM_ERR_HEX_INV_DIGIT 11
#define RM_ERR_STR_TOO_LONG 20

struct RM_Conversion
{
  static uint32_t ipstr_to_uint(const char *s, const char *e, int &err)
  {
    uint32_t octets[4] = {0,0,0,0};
    int octet = 0;
    err = RM_ERR_OK;

    for (; s != e; ++s)
    {
      char c = *s;
      if (c >= '0' && c <= '9')
      {
        octets[octet] *= 10;
        octets[octet] += (c-'0');
      }
      else if (c == '.')
      {
        ++octet;
	if (octet >= 4)
	{
          err = RM_ERR_INT_INV; // invalid 
	  return 0;
	}
      }
      else
      {
        err = RM_ERR_INT_INV; // invalid 
        return 0;
      }
    }

    if (octet != 3)
    {
      err = RM_ERR_INT_INV; // invalid 
      return 0;
    }

    uint32_t ip = 0;
    for (int i=0; i<4; ++i)
    {
      if (octets[i] >= 256)
      {
        err = RM_ERR_INT_INV; // invalid 
        return 0;
      }
      ip = (ip << 8) | octets[i];
    }

    return ip;
  }

  static uint64_t str_to_uint(const char *s, const char *e, int &err)
  {
    uint64_t v = 0;

    err = RM_ERR_OK;
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
        err = RM_ERR_INT_INV; // invalid 
        return 0;
      }
    }
    return v;
  }

  static uint64_t str_to_uint2(const char *s, const char *e, int precision, int &err)
  {
    uint64_t v = 0;
    int post_digits = -1; 

    err = RM_ERR_OK;
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
        if (post_digits < 0)
        {
          post_digits = 0;
        }
        else
        {
          err = RM_ERR_INT_INV; // invalid (duplicate ".")
          return 0;
        }
      }
      else
      {
        err = RM_ERR_INT_INV; // invalid character
        return 0;
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

  // XXX: remove char* cast and string modification!
  static double str_to_double(const char *s, const char *e)
  {
    return str_to_double2((char*)s, (char*)e);
  }

  static double str_to_double2(char *s, char *e)
  {
    char c = *e;
    *e = '\0';
    double v = atof(s);
    *e = c;
    return v;
  }

  static double str_to_double(const char *str)
  {
    return atof(str);
  }

  static double str_to_double2(const char *s, const char *e, int &err)
  {
    double v = 0.0;    
    err = RM_ERR_OK;

    bool positive = true;
    // parse sign
    for (; s != e; ++s)
    {
      char c = *s;
      if (c == '-')
      {
        positive = !positive;
      }
      else if (c == '+')
      {
        // just skip it
      }
      else
      {
        break;
      }
    }
      
    int exp = -1;
    for (; s != e; ++s)
    {
      char c = *s;
      if (c >= '0' && c <= '9')
      {
        v *= 10.0;
        v += (int)(c-'0');
        if (exp >= 0)
          ++exp;
      }
      else if (c == '.')
      {
        if (exp < 0)
        {
          exp = 0;
        }
        else
        {
          err = RM_ERR_INT_INV; // invalid (duplicate ".")
          return 0.0;
        }
      }
      else
      {
        err = RM_ERR_INT_INV; // invalid character
        return 0.0;
      }
    }

    if (exp > 0)
    {
      v /= pow(10, exp);
    }

    if (!positive)
    {
      v *= -1.0;
    }

    return v;
  }

  // Converts for example the integer (('3' << 8) | '4') into "34".
  // str has to point to at least sizeof(val)+1 bytes
  // returns the converted string in 'str'.
  static void int_encoded_str(uint64_t val, char *str)
  {
    val = htole64(val);
    const unsigned char *p = (const unsigned char*)&val;
    int i = sizeof(val)-1;
    while (i >= 0 && p[i] == 0) --i; // skip leading "zeros"
    for (; i >= 0; --i)
    {
      if (p[i] <= 127)
      {
        *str++ = p[i];
      }
    } 
    *str = '\0';
  }

  static char to_hex_digit(uint8_t v)
  {
    if (/*v >= 0 && */v <= 9) return '0' + v;
    if (v >= 10 && v <= 15) return 'A' + v - 10;
    return '#';
  }

  static int from_hex_digit(char c)
  {
    if (c >= '0' && c <= '9') return c-'0';
    if (c >= 'a' && c <= 'f') return c-'a'+10;
    if (c >= 'A' && c <= 'F') return c-'A'+10;
    return -1;
  }

  static int from_hex_byte(char c0, char c1)
  {
    int b0 = from_hex_digit(c0);
    int b1 = from_hex_digit(c1);
    if (b0 < 0 || b1 < 0) return -1;
    return (b0 << 4) | b1;
  }

};

struct RM_Type
{
  uint16_t _offset;

  inline uint16_t offset() { return _offset; } 

  virtual uint8_t size() = 0;

  // returns true if the internal value is equal to Ruby value "val"
  virtual bool equal_ruby(void *a, VALUE val) = 0;

  virtual void set_default(void *a) = 0;
  virtual VALUE to_ruby(const void *a) = 0;
  virtual int set_from_ruby(void *a, VALUE val) = 0;
  virtual int set_from_string(void *a, const char *s, const char *e) = 0;
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

// order=true ==> ascending, order=false descending
template <typename NT, bool order=true>
struct RM_UInt : RM_Type
{
  NT _default;

  RM_UInt(NT def) : _default(def) {}

  inline NT *element_ptr(void *data) { return (NT*) (((char*)data)+offset()); }
  inline NT &element(void *data) { return *element_ptr(data); }
  inline NT element(const void *data) { return *element_ptr((void*)data); }

  virtual uint8_t size() { return sizeof(NT); }

  virtual bool equal_ruby(void *a, VALUE val)
  {
    return element(a) == (NT)NUM2ULONG(val); 
  }

  virtual VALUE to_ruby(const void *a)
  {
    return ULONG2NUM(element(a));
  }

  int _set_uint(void *a, uint64_t v)
  {
    if (!(v >= std::numeric_limits<NT>::min() &&
          v <= std::numeric_limits<NT>::max()))
      return RM_ERR_INT_RANGE;

    element(a) = (NT)v;
    return RM_ERR_OK;
  }

  virtual void set_default(void *a)
  {
    element(a) = _default;
  }

  virtual int set_from_ruby(void *a, VALUE val)
  {
    return _set_uint(a, (uint64_t)NUM2ULONG(val));
  }

  virtual int set_from_string(void *a, const char *s, const char *e)
  {
    int err;
    uint64_t i = RM_Conversion::str_to_uint(s, e, err);
    if (err) return err;
    return _set_uint(a, i);
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
    element(a) = order ? std::numeric_limits<NT>::min() : std::numeric_limits<NT>::max();
  }

  virtual void set_max(void *a)
  {
    element(a) = order ? std::numeric_limits<NT>::max() : std::numeric_limits<NT>::min();
  }

  virtual void add(void *a, const void *b)
  {
    element(a) += element(b);
  }

  virtual void inc(void *a)
  {
    if (order) ++element(a);
    else --element(a);
  }
 
  virtual void copy(void *a, const void *b)
  {
    element(a) = element(b);
  }

  inline int betw(const NT &c, const NT &l, const NT &r)
  {
    if (order)
    {
      if (c < l) return -1;
      if (c > r) return 1;
    }
    else
    {
      if (c > l) return -1;
      if (c < r) return 1;
    }
    return 0;
  }

  inline int cmp(const NT &a, const NT &b)
  {
    if (order)
    {
      if (a < b) return -1;
      if (a > b) return 1;
    }
    else
    {
      if (a > b) return -1;
      if (a < b) return 1;
    }
    return 0;
  }

  virtual int between(const void *c, const void *l, const void *r)
  {
    return betw(element(c), element(l), element(r));
  }

  virtual int memory_between(const void *mem, const void *l, const void *r)
  {
    return betw(*((const NT*)mem), element(l), element(r));
  }

  virtual int compare(const void *a, const void *b)
  {
    return cmp(element(a), element(b));
  }

  virtual int compare_with_memory(const void *a, const void *mem)
  {
    return cmp(element(a), *((const NT*)mem));
  }
};

struct RM_UINT8 : RM_UInt<uint8_t> {
  RM_UINT8(uint8_t d) : RM_UInt<uint8_t>(d) {}
}; 
struct RM_UINT16 : RM_UInt<uint16_t> {
  RM_UINT16(uint16_t d) : RM_UInt<uint16_t>(d) {}
};
struct RM_UINT32 : RM_UInt<uint32_t> {
  RM_UINT32(uint32_t d) : RM_UInt<uint32_t>(d) {}
};
struct RM_UINT64 : RM_UInt<uint64_t> {
  RM_UINT64(uint64_t d) : RM_UInt<uint64_t>(d) {}
};

// with millisecond precision
struct RM_TIMESTAMP : RM_UInt<uint64_t>
{
  RM_TIMESTAMP(uint64_t d) : RM_UInt<uint64_t>(d) {}

  virtual int set_from_string(void *a, const char *s, const char *e)
  {
    int err;
    uint64_t i = RM_Conversion::str_to_uint2(s, e, 3, err);
    if (err) return err;
    return _set_uint(a, i);
  }
};

struct RM_TIMESTAMP_DESC : RM_UInt<uint64_t, false>
{
  RM_TIMESTAMP_DESC(uint64_t d) : RM_UInt<uint64_t, false>(d) {}

  virtual int set_from_string(void *a, const char *s, const char *e)
  {
    int err;
    uint64_t i = RM_Conversion::str_to_uint2(s, e, 3, err);
    if (err) return err;
    return _set_uint(a, i);
  }
};

// XXX: ascending, descending!
struct RM_DOUBLE : RM_Type 
{
  typedef double NT;

  inline NT *element_ptr(void *data) { return (NT*) (((char*)data)+offset()); }
  inline NT &element(void *data) { return *element_ptr(data); }
  inline NT element(const void *data) { return *element_ptr((void*)data); }

  virtual uint8_t size() { return sizeof(NT); }

  virtual bool equal_ruby(void *a, VALUE val)
  {
    return element(a) == (NT)NUM2DBL(val);
  }

  virtual VALUE to_ruby(const void *a)
  {
    return rb_float_new(element(a));
  }

  virtual void set_default(void *a)
  {
    element(a) = 0.0;
  }

  virtual int set_from_ruby(void *a, VALUE val)
  {
    element(a) = (NT)NUM2DBL(val);
    return RM_ERR_OK;
  }

  virtual int set_from_string(void *a, const char *s, const char *e)
  {
    element(a) = RM_Conversion::str_to_double(s, e);
    return RM_ERR_OK;
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

struct RM_IP : RM_UInt<uint32_t>
{
  RM_IP(uint32_t d) : RM_UInt<uint32_t>(d) {}

  virtual int set_from_ruby(void *a, VALUE val)
  {
    if (TYPE(val) == T_STRING)
    {
      return set_from_string(a, RSTRING_PTR(val), RSTRING_PTR(val)+RSTRING_LEN(val));
    }

    return RM_UInt<uint32_t>::set_from_ruby(a, val);
  }

  // TODO: return as IP string?
  #if 0
  virtual VALUE to_ruby(const void *a)
  {
  }
  #endif

  virtual int set_from_string(void *a, const char *s, const char *e)
  {
    int err;
    uint32_t i = RM_Conversion::ipstr_to_uint(s, e, err);
    if (err) return err;
    return _set_uint(a, i);
  }
};



struct RM_String : RM_Type
{
  uint8_t _size;

  RM_String(uint8_t size) : _size(size) {}

  virtual uint8_t size() { return _size; }

  inline uint8_t *element_ptr(void *data) { return (uint8_t*) (((char*)data)+offset()); }
  inline const uint8_t *element_ptr(const void *data) { return (const uint8_t*) (((const char*)data)+offset()); }

  // XXX: Not yet implemented
  virtual bool equal_ruby(void *a, VALUE val)
  {
    assert(false);
    return false;
  }

  virtual void set_default(void *a)
  {
    bzero(element_ptr(a), size()); 
  }

  //virtual VALUE to_ruby(const void *a) = 0;
  //virtual int set_from_ruby(void *a, VALUE val) = 0;
  //virtual int set_from_string(void *a, const char *s, const char *e) = 0;

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

struct RM_HEXSTR : RM_String
{
  RM_HEXSTR(uint8_t size) : RM_String(size) {}

  int parse_hexstring(uint8_t *v, int strlen, const char *str)
  {
    const int max_sz = 2*size();
    const int i_off = max_sz - strlen;

    if (strlen > max_sz)
    {
      return RM_ERR_HEX_INV_SIZE;
    }

    bzero(v, size());

    for (int i = 0; i < strlen; ++i)
    {
      int digit = RM_Conversion::from_hex_digit(str[i]);
      if (digit < 0)
      {
        // invalid hex digit at str[i]
        return RM_ERR_HEX_INV_DIGIT;
      }

      v[(i+i_off)/2] = (v[(i+i_off)/2] << 4) | (uint8_t)digit;
    }
    return RM_ERR_OK;
  }

  virtual VALUE to_ruby(const void *a)
  {
    const uint8_t *ptr = element_ptr(a);

    VALUE strbuf = rb_str_buf_new(2*size());
    char cbuf[3];
    cbuf[2] = 0;
    for (int i = 0; i < size(); ++i)
    {
      cbuf[0] = RM_Conversion::to_hex_digit((ptr[i]) >> 4);
      cbuf[1] = RM_Conversion::to_hex_digit((ptr[i]) & 0x0F);
      rb_str_buf_cat_ascii(strbuf, cbuf);
    }
    return strbuf;
  }

  virtual int set_from_ruby(void *a, VALUE val)
  {
    Check_Type(val, T_STRING);
    return parse_hexstring(element_ptr(a), RSTRING_LEN(val), RSTRING_PTR(val));
  }

  virtual int set_from_string(void *a, const char *s, const char *e)
  {
    return parse_hexstring(element_ptr(a), (int)(e-s), s);
  }

};

struct RM_STR : RM_String
{
  RM_STR(uint8_t size) : RM_String(size) {}

  int parse_string(uint8_t *v, int strlen, const char *str)
  {
    if (strlen > size())
      return RM_ERR_STR_TOO_LONG;

    for (int i=0; i < size(); ++i)
    {
      v[i] = (i < strlen) ? (uint8_t)str[i] : (uint8_t)0;
    }

    return RM_ERR_OK;
  }

  virtual VALUE to_ruby(const void *a)
  {
    return rb_str_new((const char*)element_ptr(a), size());
  }

  virtual int set_from_ruby(void *a, VALUE val)
  {
    Check_Type(val, T_STRING);
    return parse_string(element_ptr(a), RSTRING_LEN(val), RSTRING_PTR(val));
  }

  virtual int set_from_string(void *a, const char *s, const char *e)
  {
    return parse_string(element_ptr(a), (int)(e-s), s);
  }

};

#endif
