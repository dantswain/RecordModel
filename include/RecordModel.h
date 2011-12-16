#ifndef __RECORD_MODEL__HEADER__
#define __RECORD_MODEL__HEADER__

#include <stdint.h>  // uint32_t...
#include <stdlib.h>  // malloc
#include <strings.h> // bzero
#include <string.h>  // memcpy
#include <assert.h>  // assert

struct RecordModel;

#pragma pack(push, 1)
struct RecordModelInstance
{
  RecordModel *model;
  char *ptr; // points to the data
  int flags;
};
#pragma pack(pop)

struct RecordModelInstanceArray
{
  RecordModel *model;
  char *ptr;
  size_t _capacity;
  size_t _entries;

  size_t entries() const { return _entries; }

  RecordModelInstanceArray()
  {
    model = NULL;
    ptr = NULL;
    _capacity = 0;
    _entries = 0;
  }

  ~RecordModelInstanceArray()
  {
    if (ptr)
    {
      free(ptr);
      ptr = NULL;
    }
  }

  bool empty() const { return (_entries == 0); }
  bool full() const { return (_entries >= _capacity); }
};

//
// If the RecordModelInstance.ptr is allocated
// and as such has to be freed. Else it points
// to memory which will be freed otherwise.
//
#define FL_RecordModelInstance_PTR_ALLOCATED 1L

#define RMT_UINT8   0x0001
#define RMT_UINT16  0x0002
#define RMT_UINT32  0x0004
#define RMT_UINT64  0x0008
#define RMT_DOUBLE  0x0108
#define RMT_HEXSTR  0x0200

/*
 * Record Model description specifier
 * 32-bit unsigned int, hex notation:
 *   ooooTTss
 *   oooo: record offset
 *   TT: type
 *   ss: size of type
 */ 
#define RecordModelOffset(u) ((u) >> 16)
#define RecordModelType(u) ((u) & 0xFFFF)
#define RecordModelTypeNoSize(u) ((u) & 0xFF00)
#define RecordModelTypeSize(u) ((u) & 0xFF)

struct RecordModel
{
  uint32_t size;
  uint32_t _keysize;

  uint32_t *keys;
  uint32_t *values;
  uint32_t *items;

  RecordModel()
  {
    items = NULL;
    keys = NULL;
    values = NULL;
    size = 0;
    _keysize = 0;
  }

  uint32_t keysize() const { return _keysize; }
  uint32_t datasize() const { return size - _keysize; } 

  char *keyptr(const RecordModelInstance *mi) const
  {
    return mi->ptr;
  }

  char *dataptr(const RecordModelInstance *mi) const
  {
    return (mi->ptr + keysize());
  }

  char *keyptr(const RecordModelInstanceArray *mia, size_t i) const
  {
    assert(i < mia->entries());
    return mia->ptr + (i*size);
  }

  char *dataptr(const RecordModelInstanceArray *mia, size_t i) const
  {
    assert(i < mia->entries());
    return mia->ptr + (i*size) + keysize();
  }

  char *elemptr(const RecordModelInstanceArray *mia, size_t i) const
  {
    assert(i < mia->_capacity);
    return mia->ptr + (i*size);
  }

  ~RecordModel()
  {
    if (items)
    {
      delete[] items;
      items = NULL;
    }
  }

  RecordModelInstance *create_instance(bool zero=true)
  {
    uint32_t sz = this->size + sizeof(RecordModelInstance);
    RecordModelInstance *mi = (RecordModelInstance*) malloc(sz);
    if (mi)
    {
      mi->model = this;
      mi->ptr = ((char*)mi) + sizeof(RecordModelInstance);
      mi->flags = 0;

      if (zero)
      {
        zero_instance(mi);
      }
    }
    return mi;
  }

  void copy_instance(RecordModelInstance *dst, const RecordModelInstance *src)
  {
    assert(dst->model == this);
    assert(src->model == this);
    memcpy(dst->ptr, src->ptr, this->size);
    assert(dst->model == this);
  }

  RecordModelInstance *dup_instance(const RecordModelInstance *copy)
  {
    RecordModelInstance *mi = create_instance(false);
    if (mi)
    {
      copy_instance(mi, copy);
    }
    return mi;
  }

  void zero_instance(RecordModelInstance *mi)
  {
    bzero(mi->ptr, this->size);
  }

  inline const char *ptr_to_field(const RecordModelInstance *i, uint32_t desc) const
  {
    return (const char*)(i->ptr + RecordModelOffset(desc));
  }

  /*
   * Sums (adds) all numeric values: ra = ra + rb
   * Does not touch key attributes!
   *
   * XXX: NOT supported for RMT_HEXSTR.
   */
  void sum_instance(RecordModelInstance *ra, const RecordModelInstance *rb)
  {
    assert(ra->model == this && rb->model == this);

    for (uint32_t *v = this->values; *v != 0; ++v)
    {
      uint32_t desc = *v;

      const char *ptr_ra = ptr_to_field(ra, desc);
      const char *ptr_rb = ptr_to_field(rb, desc);

      if (RecordModelType(desc) == RMT_UINT64)
      {
        *((uint64_t*)ptr_ra) += *((const uint64_t*)ptr_rb);
      }
      else if (RecordModelType(desc) == RMT_UINT32)
      {
        *((uint32_t*)ptr_ra) += *((const uint32_t*)ptr_rb);
      }
      else if (RecordModelType(desc) == RMT_DOUBLE)
      {
        *((double*)ptr_ra) += *((const double*)ptr_rb);
      }
      else if (RecordModelType(desc) == RMT_UINT16)
      {
        *((uint16_t*)ptr_ra) += *((const uint16_t*)ptr_rb);
      }
      else if (RecordModelType(desc) == RMT_UINT8)
      {
        *((uint8_t*)ptr_ra) += *((const uint8_t*)ptr_rb);
      }
      else
      {
        assert(false);
      }
    }
  }

  /*
   * Returns i >= 0, if key is not in range, or -1 if it is in range. If i >= 0, then this is the key position
   * that is not in range.
   */
  int keys_in_range_pos(const RecordModelInstance *c, const RecordModelInstance *l, const RecordModelInstance *r)
  {
    assert(c->model == this && l->model == this && r->model == this);

    for (int i = 0; this->keys[i] != 0; ++i)
    {
      uint32_t desc = this->keys[i];

      if (RecordModelType(desc) == RMT_UINT64)
      {
        uint64_t cv = *((uint64_t*)ptr_to_field(c, desc));
        if (cv < *((uint64_t*)ptr_to_field(l, desc))) return -i-1;
        if (cv > *((uint64_t*)ptr_to_field(r, desc))) return i+1;
      }
      else if (RecordModelType(desc) == RMT_UINT32)
      {
        uint32_t cv = *((uint32_t*)ptr_to_field(c, desc));
        if (cv < *((uint32_t*)ptr_to_field(l, desc))) return -i-1;
        if (cv > *((uint32_t*)ptr_to_field(r, desc))) return i+1;
      }
      else if (RecordModelType(desc) == RMT_UINT16)
      {
        uint16_t cv = *((uint16_t*)ptr_to_field(c, desc));
        if (cv < *((uint16_t*)ptr_to_field(l, desc))) return -i-1;
        if (cv > *((uint16_t*)ptr_to_field(r, desc))) return i+1;
      }
      else if (RecordModelType(desc) == RMT_UINT8)
      {
        uint8_t cv = *((uint8_t*)ptr_to_field(c, desc));
        if (cv < *((uint8_t*)ptr_to_field(l, desc))) return -i-1;
        if (cv > *((uint8_t*)ptr_to_field(r, desc))) return i+1;
      }
      else if (RecordModelTypeNoSize(desc) == RMT_HEXSTR)
      {
        const uint8_t *cp = (const uint8_t*)ptr_to_field(c, desc);
        const uint8_t *lp = (const uint8_t*)ptr_to_field(l, desc);
        const uint8_t *rp = (const uint8_t*)ptr_to_field(r, desc);
        // XXX: Check correctness
        for (int k=0; k < RecordModelTypeSize(desc); ++k)
        {
          if (cp[k] < lp[k]) return -i-1;
          if (cp[k] > lp[k]) break;
        }
        for (int k=0; k < RecordModelTypeSize(desc); ++k)
        {
          if (cp[k] > rp[k]) return i+1;
          if (cp[k] < rp[k]) break;
        }
      }
      else if (RecordModelType(desc) == RMT_DOUBLE)
      {
        // XXX: Rarely used as keys!
        double cv = *((double*)ptr_to_field(c, desc));
        if (cv < *((double*)ptr_to_field(l, desc))) return -i-1;
        if (cv > *((double*)ptr_to_field(r, desc))) return i+1;
      }
      else
      {
        assert(false);
      }
    }
    return 0;
  }

  /*
   * Return true if all keys of "c" are within the ranges of the keys of "l" and "r".
   */
  bool keys_in_range(const RecordModelInstance *c, const RecordModelInstance *l, const RecordModelInstance *r)
  {
    return (keys_in_range_pos(c, l, r) == 0);
  }

  void copy_keys(RecordModelInstance *to, const RecordModelInstance *from, int i)
  {
    assert(to->model == this && from->model == this);
    for (; this->keys[i] != 0; ++i)
    {
      uint32_t desc = this->keys[i];

      if (RecordModelType(desc) == RMT_UINT64)
      {
        *((uint64_t*)ptr_to_field(to, desc)) = *((const uint64_t*)ptr_to_field(from, desc)); 
      }
      else if (RecordModelType(desc) == RMT_UINT32)
      {
        *((uint32_t*)ptr_to_field(to, desc)) = *((const uint32_t*)ptr_to_field(from, desc)); 
      }
      else if (RecordModelType(desc) == RMT_UINT16)
      {
        *((uint16_t*)ptr_to_field(to, desc)) = *((const uint16_t*)ptr_to_field(from, desc)); 
      }
      else if (RecordModelType(desc) == RMT_UINT8)
      {
        *((uint8_t*)ptr_to_field(to, desc)) = *((const uint8_t*)ptr_to_field(from, desc)); 
      }
      else if (RecordModelTypeNoSize(desc) == RMT_HEXSTR)
      {
        memcpy((char*)ptr_to_field(to, desc), (const char*)ptr_to_field(from, desc), RecordModelTypeSize(desc));
      }
      else if (RecordModelType(desc) == RMT_DOUBLE)
      {
        *((double*)ptr_to_field(to, desc)) = *((const double*)ptr_to_field(from, desc));
      }
      else
      {
        assert(false);
      }
    }
  }

  void increase_key(RecordModelInstance *to, int i)
  {
    assert(to->model == this);
    uint32_t desc = this->keys[i];

    if (RecordModelType(desc) == RMT_UINT64)
    {
      *((uint64_t*)ptr_to_field(to, desc)) += 1;
    }
    else if (RecordModelType(desc) == RMT_UINT32)
    {
      *((uint32_t*)ptr_to_field(to, desc)) += 1;
    }
    else if (RecordModelType(desc) == RMT_UINT16)
    {
      *((uint16_t*)ptr_to_field(to, desc)) += 1;
    }
    else if (RecordModelType(desc) == RMT_UINT8)
    {
      *((uint8_t*)ptr_to_field(to, desc)) += 1;
    }
    else if (RecordModelTypeNoSize(desc) == RMT_HEXSTR)
    {
      uint8_t *str = (uint8_t*)ptr_to_field(to, desc);
      for (int i=RecordModelTypeSize(desc)-1; i >= 0; --i)
      {
        if (str[i] < 0xFF)
	{
	  // no overflow
          str[i] += 1;
	  break;
        }
	else
	{
	  // overflow
	  str[i] = 0;
        }
      }
    }
    else if (RecordModelType(desc) == RMT_DOUBLE)
    {
      // cannot do anything for DOUBLE
    }
    else
    {
      assert(false);
    }
  }

  int compare_keys(const RecordModelInstance *a, const RecordModelInstance *b)
  {
    assert(a->model == this && b->model == this);
    return compare_keys(this->keyptr(a), this->keysize(), this->keyptr(b), this->keysize());
  }

  int compare_keys_buf(const char *akbuf, const char *bkbuf) const
  {
    for (uint32_t *k = this->keys; *k != 0; ++k)
    {
      uint32_t desc = *k;

      assert(RecordModelOffset(desc) + RecordModelTypeSize(desc) <= this->_keysize);

      if (RecordModelType(desc) == RMT_UINT64)
      {
        uint64_t a = *((const uint64_t*)(akbuf + RecordModelOffset(desc)));
        uint64_t b = *((const uint64_t*)(bkbuf + RecordModelOffset(desc)));
        if (a != b) return (a < b ? -1 : 1);
      }
      else if (RecordModelType(desc) == RMT_UINT32)
      {
        uint32_t a = *((const uint32_t*)(akbuf + RecordModelOffset(desc)));
        uint32_t b = *((const uint32_t*)(bkbuf + RecordModelOffset(desc)));
        if (a != b) return (a < b ? -1 : 1);
      }
      else if (RecordModelType(desc) == RMT_UINT16)
      {
        uint16_t a = *((const uint16_t*)(akbuf + RecordModelOffset(desc)));
        uint16_t b = *((const uint16_t*)(bkbuf + RecordModelOffset(desc)));
        if (a != b) return (a < b ? -1 : 1);
      }
      else if (RecordModelTypeNoSize(desc) == RMT_HEXSTR)
      {
        const uint8_t *a = (const uint8_t*)(akbuf + RecordModelOffset(desc));
        const uint8_t *b = (const uint8_t*)(bkbuf + RecordModelOffset(desc));
 
        // XXX: Check correctness
        for (int i=0; i < RecordModelTypeSize(desc); ++i)
        {
          if (a[i] < b[i]) return -1;
          if (a[i] > b[i]) return 1;
        }
      }
#if 0
      else if (RecordModelType(desc) == RMT_UINT128)
      {
        const uint64_t *a = (const uint64_t*)(akbuf + RecordModelOffset(desc));
        const uint64_t *b = (const uint64_t*)(bkbuf + RecordModelOffset(desc));

        if (a[0] != b[0] || a[1] != b[1]) // a != b
        {
          if (a[0] < b[0])
            return -1;
          else if (a[0] == b[0] && a[1] < b[1])
            return -1;
          else
            return 1;
        }
      }
#endif
      else if (RecordModelType(desc) == RMT_UINT8)
      {
        uint8_t a = *((const uint8_t*)(akbuf + RecordModelOffset(desc)));
        uint8_t b = *((const uint8_t*)(bkbuf + RecordModelOffset(desc)));
        if (a != b) return (a < b ? -1 : 1);
      }
      else if (RecordModelType(desc) == RMT_DOUBLE)
      {
        // XXX: Rarely used as keys!
        double a = *((const double*)(akbuf + RecordModelOffset(desc)));
        double b = *((const double*)(bkbuf + RecordModelOffset(desc)));
        if (a != b) return (a < b ? -1 : 1);
      }
      else
      {
        assert(false);
      }
    }
    return 0;
  }

  int compare_keys(const char *akbuf, size_t aksiz, const char *bkbuf, size_t bksiz) const
  {
    assert(aksiz == bksiz && aksiz == this->keysize());
    return compare_keys_buf(akbuf, bkbuf);
  }

};

#endif
