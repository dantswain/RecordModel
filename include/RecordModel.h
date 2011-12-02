#ifndef __RECORD_MODEL__HEADER__
#define __RECORD_MODEL__HEADER__

#include <stdint.h>  // uint32_t...
#include <stdlib.h>  // malloc
#include <strings.h> // bzero
#include <assert.h>  // assert

struct RecordModel;

#pragma pack(push, 1)
struct RecordModelInstance
{
  RecordModel *model;
};
#pragma pack(pop)

#define RMT_UINT64 0x0008

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
      else
      {
        assert(false);
      }
    }
    return 0;
  } 

};

#endif
