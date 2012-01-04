#ifndef __RECORD_MODEL__HEADER__
#define __RECORD_MODEL__HEADER__

#include <stdint.h>  // uint32_t...
#include <stdlib.h>  // malloc
#include <strings.h> // bzero, memset
#include <string.h>  // memcpy
#include <assert.h>  // assert
#include "RM_Types.h"

struct RecordModel
{
  RM_Type **_all_fields;
  RM_Type **_keys;
  RM_Type **_values;
  size_t _num_fields;
  size_t _size;
  VALUE _rm_obj; // corresponding Ruby object (needed for GC)

  inline size_t size() { return _size; }

  RecordModel()
  {
    _all_fields = NULL;
    _keys = NULL;
    _values = NULL;
    _num_fields = 0;
    _size = 0;
    _rm_obj = Qnil;
  }

  RM_Type *get_field(size_t idx)
  {
    if (idx >= _num_fields) return NULL;
    return _all_fields[idx];
  }

  bool is_virgin()
  {
    return (_all_fields == NULL && _keys == NULL && _values == NULL && _num_fields == 0 && _size == 0 /*&& _rm_obj == Qnil*/);
  }

  ~RecordModel()
  {
    if (_all_fields)
    {
      for (int i=0; _all_fields[i] != NULL; ++i)
      {
        delete(_all_fields[i]);
      }
      free(_all_fields);
      _all_fields = NULL;
    }
    if (_keys)
    {
      free(_keys);
      _keys = NULL;
    }
    if (_values)
    {
      free(_values);
      _values = NULL;
    }
    _rm_obj = Qnil;
  }
};

/*
 * An instance of a RecordModel
 */
struct RecordModelInstance
{
  RecordModel *model;
  void *_ptr; // points to the allocated data

  inline void *ptr() { return _ptr; }
  inline const void *ptr() const { return (const void*)_ptr; }

  inline int size() { return model->_size; }

  RecordModelInstance(RecordModel *model, void *ptr)
  {
    this->model = model;
    this->_ptr = ptr;
  }

  static void deallocate(RecordModelInstance *rec)
  {
    if (rec)
    {
      free(rec);
    }
  }

  static RecordModelInstance *allocate(RecordModel *model)
  {
    RecordModelInstance *rec = (RecordModelInstance*) malloc(model->size() + sizeof(RecordModelInstance));
    if (rec)
    {
      rec->model = model;
      rec->_ptr = ((char*)rec) + sizeof(RecordModelInstance);
    }
    return rec;
  }

  // ptr must be of _size
  static RecordModelInstance *allocate(RecordModel *model, void *ptr)
  {
    RecordModelInstance *rec = (RecordModelInstance*) malloc(sizeof(RecordModelInstance));
    if (rec)
    {
      rec->model = model;
      rec->_ptr = ptr;
    }
    return rec;
  }

  void zero()
  {
    bzero(ptr(), size());
  }

  void copy(const RecordModelInstance *src)
  {
    assert(src->model == model);
    memcpy(ptr(), src->ptr(), size());
  }

  RecordModelInstance *dup() const
  {
    RecordModelInstance *rec = allocate(model);
    if (rec)
    {
      rec->copy(this);
    }
    return rec;
  }
 
  /*
   * Sums (adds) all numeric values: this.x += other.x 
   * Does not touch key attributes!
   */
  void add_values(const RecordModelInstance *other)
  {
    assert(other->model == model);

    for (int i = 0; model->_values[i] != NULL; ++i)
    {
      model->_values[i]->add(ptr(), other->ptr());
    }
  }

  /*
   * Returns i >= 0, if key is not in range, or -1 if it is in range. If i >= 0, then this is the key position
   * that is not in range.
   */
  int keys_in_range_pos(const RecordModelInstance *l, const RecordModelInstance *r) const
  {
    assert(l->model == model && r->model == model);

    for (int i = 0; model->_keys[i] != NULL; ++i)
    {
      int cmp = model->_keys[i]->between(ptr(), l->ptr(), r->ptr());
      if (cmp < 0) return -i-1;
      if (cmp > 0) return i+1;
    }
    return 0;
  }

  /*
   * Return true if all keys are within the ranges of the keys of "l" and "r".
   */
  bool keys_in_range(const RecordModelInstance *l, const RecordModelInstance *r) const
  {
    return (keys_in_range_pos(l, r) == 0);
  }

  void copy_keys(const RecordModelInstance *from, int i)
  {
    assert(from->model == model);
    for (; model->_keys[i] != NULL; ++i)
    {
      model->_keys[i]->copy(ptr(), from->ptr());
    }
  }

  void increase_key(int i)
  {
    model->_keys[i]->inc(ptr());
  }

  int compare_keys(const RecordModelInstance *other)
  {
    assert(other->model == model);
    for (int i = 0; model->_keys[i] != NULL; ++i)
    {
      int cmp = model->_keys[i]->compare(ptr(), other->ptr());
      if (cmp != 0) return cmp;
    }
    return 0;
  }
};


/*
 * Represents a dynamic array of RecordModel instances
 */
struct RecordModelInstanceArray
{
  RecordModel *model;
  void *_ptr;
  size_t _capacity;
  size_t _entries;
  bool expandable;

  size_t entries() const { return _entries; }
  size_t capacity() const { return _capacity; }
  bool empty() const { return (_entries == 0); }
  bool full() const { return (_entries >= _capacity); }

  RecordModelInstanceArray()
  {
    model = NULL;
    _ptr = NULL;
    _capacity = 0;
    _entries = 0;
    expandable = false;
  }

  bool is_virgin()
  {
    return (model == NULL && _ptr == NULL && _capacity == 0 && _entries == 0 && expandable == false);
  }

  ~RecordModelInstanceArray()
  {
    if (_ptr)
    {
      free(_ptr);
      _ptr = NULL;
    }
  }

  bool _alloc(void *ptr, size_t capacity)
  {
    void *new_ptr = NULL;

    if (capacity < 8) capacity = 8;
    if (ptr == NULL)
      new_ptr = malloc(model->size() * capacity);
    else
      new_ptr = realloc(ptr, model->size() * capacity);

    if (new_ptr == NULL)
      return false;

    _capacity = capacity;
    _ptr = new_ptr;
    return true;
  } 

  bool allocate(size_t capacity)
  {
    if (_ptr) return false;
    return _alloc(_ptr, capacity);
  }

  bool expand()
  {
    return expand(_capacity * 2);
  }

  bool expand(size_t capacity)
  {
    if (!expandable) return false;
    return _alloc(_ptr, capacity);
  }

  void reset()
  {
    _entries = 0;
  }

  inline void *element_n(size_t n)
  {
    assert(n < _capacity);
    return ((char*)_ptr + (n * model->size()));
  }

  bool push(const RecordModelInstance *rec)
  {
    assert(model == rec->model);

    if (full() && expand(capacity() * 2) == false)
    {
      return false;
    }
    assert(!full());

    RecordModelInstance dst(this->model, element_n(_entries++));
    dst.copy(rec);
    return true;
  }

  void copy(RecordModelInstance *rec, size_t i)
  {
    assert(i < _entries);
    assert(model == rec->model);

    RecordModelInstance src(this->model, element_n(i));
    rec->copy(&src);
  }

};

#endif
