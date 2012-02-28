#ifndef __RECORD_MODEL__HEADER__
#define __RECORD_MODEL__HEADER__

#include <stdint.h>  // uint32_t...
#include <stdlib.h>  // malloc
#include <strings.h> // bzero, memset
#include <string.h>  // memcpy
#include <assert.h>  // assert
#include <vector>    // std::vector
#include <algorithm> // std::sort
#include "RM_Types.h"
#include "RM_Token.h"

struct RecordModel
{
  RM_Type **_all_fields;
  RM_Type **_keys;
  RM_Type **_values;
  size_t _num_fields;
  size_t _num_keys;
  size_t _num_values;
  size_t _size;
  size_t _size_keys;
  size_t _size_values;

  VALUE _rm_obj; // corresponding Ruby object (needed for GC)

  inline size_t size() { return _size; }
  inline size_t size_keys() { return _size_keys; }
  inline size_t size_values() { return _size_values; }

  inline size_t num_keys() { return _num_keys; }

  RecordModel()
  {
    _all_fields = NULL;
    _keys = NULL;
    _values = NULL;
    _num_fields = 0;
    _num_keys = 0;
    _num_values = 0;
    _size = 0;
    _size_keys = 0;
    _size_values = 0;
    _rm_obj = Qnil;
  }

  RM_Type *get_field(size_t idx)
  {
    if (idx >= _num_fields) return NULL;
    return _all_fields[idx];
  }

  /*
   * Returns true if any field in the range a0..a1 overlaps
   * with b0..b1.
   */
  bool overlap(const void *a0, const void *a1, const void *b0, const void *b1)
  {
    for (size_t k = 0; k < _num_fields; ++k)
    {
      if (_all_fields[k]->overlap(a0, a1, b0, b1)) return true;
    }

    return false;
  }

  bool is_virgin()
  {
    return (_all_fields == NULL && _keys == NULL && _values == NULL && _num_fields == 0 && _num_keys == 0 && _num_values == 0 &&
            _size == 0 && _size_keys == 0 && _size_values == 0 /*&& _rm_obj == Qnil*/);
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

  inline int size() { return model->size(); }

  RecordModelInstance()
  {
    this->model = NULL;
    this->_ptr = NULL;
  }

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

  // Set all values to their default values.
  void zero()
  {
    for (size_t i = 0; i < model->_num_fields; ++i)
    {
      model->_all_fields[i]->set_default(ptr());
    }
  }

  /*
   * Set all fields to their mininum value
   */
  void set_min()
  {
    for (size_t i = 0; i < model->_num_fields; ++i)
    {
      model->_all_fields[i]->set_min(ptr());
    }
  }

  /*
   * Set all fields to their maximum value
   */
  void set_max()
  {
    for (size_t i = 0; i < model->_num_fields; ++i)
    {
      model->_all_fields[i]->set_max(ptr());
    }
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
   * Returns 0 if all keys are between the corresponding keys of "l" and "r".
   *
   * Returns < 0 if record < "l".
   *
   * Returns > 0 if record > "r".
   *
   * The key which is out of range is returned in "int &i".
   */
  int keys_in_range_pos(const RecordModelInstance *l, const RecordModelInstance *r, int &i) const
  {
    assert(l->model == model && r->model == model);

    for (i = 0; model->_keys[i] != NULL; ++i)
    {
      int cmp = model->_keys[i]->between(ptr(), l->ptr(), r->ptr());
      if (cmp < 0) return -1;
      if (cmp > 0) return 1;
    }
    return 0;
  }

  /*
   * Return true if all keys are within the ranges of the keys of "l" and "r".
   */
  bool keys_in_range(const RecordModelInstance *l, const RecordModelInstance *r) const
  {
    int keypos;
    return (keys_in_range_pos(l, r, keypos) == 0);
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

  static int compare_keys_ptr2(RM_Type **keys, const void *a, const void *b)
  {
    for (int i = 0; keys[i] != NULL; ++i)
    {
      int cmp = keys[i]->compare(a, b);
      if (cmp != 0) return cmp;
    }
    return 0;
  }
 
  inline static int compare_keys_ptr(RecordModel *model, const void *a, const void *b)
  {
    return compare_keys_ptr2(model->_keys, a, b);
  }
  
  int compare_keys(const RecordModelInstance *other)
  {
    assert(other->model == model);
    return compare_keys_ptr(model, ptr(), other->ptr());
  }

  /*
   * Parses a line "str" separated by "sep" and stores each value into the corresponding field_arr item (index into fields).
   *
   * Returns the number of successfully parsed tokens.
   *
   * For example if it returns 0, it means that it could not parse the first token successfully.
   * If it returns field_arr_sz, then it could parse all tokens successfully. If it retunrns field_arr_sz+1, then
   * it could parse all tokens successfully, but there is more input available.
   */
  int parse_line(const char *str, const int *field_arr, int field_arr_sz, char sep, int &err)
  {
    RM_Token token;
    const char *next = str;
    err = RM_ERR_OK;

    int i;
    for (i=0; i < field_arr_sz; ++i)
    {
      err = RM_ERR_OK;

      next = token.parse(next, sep);
      if (token.empty())
	return i; // premature end

      if (field_arr[i] < 0)
	continue;

      RM_Type *field = this->model->get_field(field_arr[i]);
      assert(field);
      err = field->set_from_string(this->ptr(), token.beg, token.end);
      if (err)
      {
	return i;
      }
    }

    next = token.parse(next, sep);
    if (token.empty())
      return field_arr_sz; // means, OK
    else
      return field_arr_sz+1; // means, has additional items
  }
  
};

struct RecordModelInstanceArraySorter
{
  RM_Type **keys;
  void *base_ptr;
  size_t element_size;
 
  bool operator()(uint32_t ai, uint32_t bi)
  {
    return (RecordModelInstance::compare_keys_ptr2(keys, 
      ((char*)base_ptr) + element_size*ai, ((char*)base_ptr) + element_size*bi) < 0);
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

  // Allows max. 2**32-1 elements to be stored within an array.
  typedef uint32_t SORT_IDX;
  std::vector<SORT_IDX> *sort_arr; 

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
    sort_arr = NULL;
  }

  bool is_virgin()
  {
    return (model == NULL && _ptr == NULL && _capacity == 0 && _entries == 0 && expandable == false && sort_arr == NULL);
  }

  ~RecordModelInstanceArray()
  {
    if (_ptr)
    {
      free(_ptr);
      _ptr = NULL;
    }
    if (sort_arr)
    {
      delete sort_arr;
      sort_arr = NULL;
    }
  }

  bool _alloc(void *ptr, size_t capacity)
  {
    void *new_ptr = NULL;

    if (capacity < 8) capacity = 8;
    if (ptr == NULL)
      new_ptr = malloc(element_size() * capacity);
    else
      new_ptr = realloc(ptr, element_size() * capacity);

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
    if (sort_arr)
      sort_arr->clear();
  }

  bool push(const RecordModelInstance *rec)
  {
    assert(model == rec->model);

    if (full() && expand(capacity() * 2) == false)
    {
      return false;
    }
    assert(!full());

    if (sort_arr)
      sort_arr->push_back(_entries);
    RecordModelInstance dst(this->model, element_n(_entries)); // NOTE: we have to use element_n here, NOT ptr_at!
    dst.copy(rec);

    ++_entries;
    return true;
  }

  /*
   * Copies element at index 'i' (in sorted order) into 'rec'
   */
  void copy_out(RecordModelInstance *rec, size_t i)
  {
    assert(i < _entries);
    assert(model == rec->model);

    RecordModelInstance src(this->model, ptr_at(i));
    rec->copy(&src);
  }

  /*
   * Copies 'rec' into element at index 'i' (in sorted order)
   */
  void copy_in(const RecordModelInstance *rec, size_t i)
  {
    assert(i < _entries);
    assert(model == rec->model);

    RecordModelInstance dst(this->model, ptr_at(i));
    dst.copy(rec);
  }


  /*
   * Sorts the array. Does not move the entries around, but instead 
   * uses a separate sort array (sort_arr). Use idx_to_sort(i) to
   * retrieve the sorted index.
   */
  void sort(RM_Type **keys=NULL)
  {
    RecordModelInstanceArraySorter s;
    s.base_ptr = _ptr;
    s.element_size = element_size(); 

    if (keys)
    {
      s.keys = keys;
    }
    else
    {
      s.keys = model->_keys;
    }

    if (!sort_arr)
    {
      sort_arr = new std::vector<SORT_IDX>; 
      sort_arr->reserve(_entries);
      for (size_t i = 0; i < _entries; ++i)
      {
        sort_arr->push_back(i);
      }
    }
    std::sort(sort_arr->begin(), sort_arr->end(), s);
  }

  /*
   * 'i' is in sorted order
   */
  inline void *ptr_at(size_t i)
  {
    assert(i < _entries);
    SORT_IDX k = sort_arr ? (*sort_arr)[i] : i;
    assert(k < _entries);
    return element_n(k);
  }

  void *ptr_at_last()
  {
    if (_entries > 0)
    {
      return ptr_at(_entries-1);
    }
    else
    {
      return NULL;
    }
  }


private:

  inline size_t element_size()
  {
    return model->size();
  }

  /*
   * 'n' is in raw order (un-sorted)
   */
  inline void *element_n(size_t n)
  {
    assert(n < _capacity);
    return ((char*)_ptr + (n * element_size()));
  }
};

#endif
