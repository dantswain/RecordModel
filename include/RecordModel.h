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
};
#pragma pack(pop)

#define RMT_UINT32 0x0004
#define RMT_UINT64 0x0008
#define RMT_DOUBLE 0x0108

#define RecordModelOffset(u) ((u) >> 16)
#define RecordModelType(u) ((u) & 0xFFFF)
#define RecordModelTypeSize(u) ((u) & 0xFF)

struct RecordModel
{
  uint32_t size;
  uint32_t keysize;

  uint32_t *keys;
  uint32_t *values;
  uint32_t *items;

  RecordModel()
  {
    items = NULL;
    keys = NULL;
    values = NULL;
    size = 0;
    keysize = 0;
  }

  ~RecordModel()
  {
    if (items)
    {
      delete[] items;
      items = NULL;
    }
  }

  RecordModelInstance *dup_instance(const RecordModelInstance *copy)
  {
    size_t sz = this->size + sizeof(RecordModelInstance);
    RecordModelInstance *mi = (RecordModelInstance*) malloc(sz);
    if (mi)
    {
      memcpy(mi, copy, sz);
      assert(mi->model == this);
    }
    return mi;
  }

  void copy_instance(RecordModelInstance *dst, const RecordModelInstance *src)
  {
    assert(dst->model == this);
    assert(src->model == this);
    memcpy(dst, src, this->size + sizeof(RecordModelInstance));
    assert(dst->model == this);
  }
 
  RecordModelInstance *create_instance()
  {
    uint32_t sz = this->size + sizeof(RecordModelInstance);
    RecordModelInstance *mi = (RecordModelInstance*) malloc(sz);
    if (mi)
    {
      bzero(mi, sz);
      mi->model = this;
    }
    return mi;
  }

  void zero_instance(RecordModelInstance *mi)
  {
    if (mi)
    {
      bzero(((char*)mi) + sizeof(RecordModelInstance), this->size);
    }
  }

  inline const char *ptr_to_field(const RecordModelInstance *i, uint32_t desc) const
  {
    return ((const char*)i) + sizeof(RecordModelInstance) + RecordModelOffset(desc);
  }

  /*
   * Sums (adds) all numeric values: ra = ra + rb
   * Does not touch key attributes!
   */
  void sum_instance(RecordModelInstance *ra, const RecordModelInstance *rb)
  {
    assert(ra->model == this && rb->model == this);

    for (uint32_t *v = this->values; *v != 0; ++v)
    {
      uint32_t desc = *v;

      if (RecordModelType(desc) == RMT_UINT64)
      {
        *((uint64_t*)ptr_to_field(ra, desc)) += *((const uint64_t*)ptr_to_field(rb, desc));
      }
      else if (RecordModelType(desc) == RMT_UINT32)
      {
        *((uint32_t*)ptr_to_field(ra, desc)) += *((const uint32_t*)ptr_to_field(rb, desc));
      }
      else if (RecordModelType(desc) == RMT_DOUBLE)
      {
        *((double*)ptr_to_field(ra, desc)) += *((const double*)ptr_to_field(rb, desc));
      }
      else
      {
        assert(false);
      }
    }
  }

  /*
   * Return true if all keys of "c" are within the ranges of the keys of "l" and "r".
   */
  bool keys_in_range(const RecordModelInstance *c, const RecordModelInstance *l, const RecordModelInstance *r)
  {
    assert(c->model == this && l->model == this && r->model == this);

    for (uint32_t *k = this->keys; *k != 0; ++k)
    {
      uint32_t desc = *k;

      if (RecordModelType(desc) == RMT_UINT64)
      {
        uint64_t cv = *((uint64_t*)ptr_to_field(c, desc));
        if (cv < *((uint64_t*)ptr_to_field(l, desc))) return false;
        if (cv > *((uint64_t*)ptr_to_field(r, desc))) return false;
      }
      else if (RecordModelType(desc) == RMT_UINT32)
      {
        uint64_t cv = *((uint32_t*)ptr_to_field(c, desc));
        if (cv < *((uint32_t*)ptr_to_field(l, desc))) return false;
        if (cv > *((uint32_t*)ptr_to_field(r, desc))) return false;
      }
      else if (RecordModelType(desc) == RMT_DOUBLE)
      {
        // XXX: Rarely used as keys!
        double cv = *((double*)ptr_to_field(c, desc));
        if (cv < *((double*)ptr_to_field(l, desc))) return false;
        if (cv > *((double*)ptr_to_field(r, desc))) return false;
      }
      else
      {
        assert(false);
      }
    }
    return true;
  }

  int compare_keys(const RecordModelInstance *a, const RecordModelInstance *b)
  {
    assert(a->model == this && b->model == this);
    return compare_keys(((char*)a) + sizeof(RecordModelInstance), this->keysize,
                        ((char*)b) + sizeof(RecordModelInstance), this->keysize);
  }

  int compare_keys(const char *akbuf, size_t aksiz, const char *bkbuf, size_t bksiz) const
  {
    assert(aksiz == bksiz && aksiz == this->keysize);

    for (uint32_t *k = this->keys; *k != 0; ++k)
    {
      uint32_t desc = *k;

      assert(RecordModelOffset(desc) + RecordModelTypeSize(desc) <= this->size); // XXX
      assert(RecordModelOffset(desc) + RecordModelTypeSize(desc) <= aksiz); // XXX
      assert(RecordModelOffset(desc) + RecordModelTypeSize(desc) <= bksiz); // XXX

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

};

#endif
