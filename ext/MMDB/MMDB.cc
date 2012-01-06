#include <assert.h> // assert
#include <stdio.h> // snprintf 
#include <strings.h> // bzero
#include "../../include/RecordModel.h"
#include "MmapFile.h"
#include "ruby.h"

/*
 * A database consists of:
 *
 *   - One slices file
 *
 *   - One file for each key
 *
 *   - One data file
 *
 * The slices file simply records the size (number of entries) in each sorted
 * slice, i.e. it's just [uint32_t length_of_slice]*.
 *
 * The key files and the data file all have the same number of entries, while
 * the record size of each file might be different as they might be of
 * different types.
 *
 * A database (consisting of multiple files) is usually stored under it's own
 * directory. The naming for the slices file is just "slices", for the key
 * databases it is e.g. "k0_4" for the first (index 0) key and a record size of
 * 4, or "k1_16" for the second (index 1) key and a size of 16. The sizes are
 * just there to help a bit against unwanted schema changes. The data file is
 * named e.g. "data_40", i.e. again is the record size in the name. 
 * 
 */

struct MMDB
{
  RecordModel *model;
  MmapFile *db_slices;
  MmapFile *db_data;
  MmapFile **db_keys;
  size_t num_keys;
  bool readonly;
  size_t num_slices;
  size_t num_records;

  MMDB()
  {
    model = NULL;
    db_slices = NULL;
    db_data = NULL;
    db_keys = NULL;
    num_keys = 0;
    readonly = true;
    num_slices = 0;
    num_records = 0;
  }

  ~MMDB()
  {
    close();
  }

  /*
   * Note that path_prefix must include the trailing '/' if you want to store the databases under it's own directory.
   */
  bool open(RecordModel *_model, const char *path_prefix, size_t _num_slices, size_t _hint_slices, size_t _num_records, size_t _hint_records, bool _readonly)
  {
    using namespace std;

    num_slices = _num_slices;
    num_records = _num_records;
    readonly = _readonly;
    model = _model;
    num_keys = model->num_keys();
    assert(num_keys > 0);

    bool ok;
    size_t name_sz = strlen(path_prefix) + 32;
    char *name = (char*)malloc(name_sz);
    if (!name) goto fail;

    // open slices file
    snprintf(name, name_sz, "%sslices_%ld", path_prefix, sizeof(uint32_t));
    db_slices = new MmapFile();
    ok = db_slices->open(name, sizeof(uint32_t)*num_slices, sizeof(uint32_t)*_hint_slices, readonly);
    if (!ok) goto fail;

    // open data file
    snprintf(name, name_sz, "%sdata_%ld", path_prefix, model->size_values());
    db_data = new MmapFile();
    ok = db_data->open(name, model->size_values()*num_records, model->size_values()*_hint_records, readonly);
    if (!ok) goto fail;

    // open key files
    db_keys = (MmapFile**) malloc(sizeof(MmapFile*) * num_keys);
    if (!db_keys) goto fail;
    bzero(db_keys, sizeof(MmapFile*) * num_keys);

    for (size_t i = 0; i < num_keys; ++i)
    {
      RM_Type *field = model->_keys[i]; 
      assert(field);
      snprintf(name, name_sz, "%sk%ld_%d", path_prefix, i, field->size());
      db_keys[i] = new MmapFile();
      ok = db_keys[i]->open(name, field->size()*num_records, field->size()*_hint_records, readonly);
      if (!ok) goto fail;
    }

    return true;

  fail:
    if (name) free(name);
    close();
    return false;
  }

  void close()
  {
    model = NULL;
    if (db_slices)
    {
      db_slices->close();
      delete db_slices;
      db_slices = NULL;
    }
    if (db_data)
    {
      db_data->close();
      delete db_data;
      db_data = NULL;
    }
    if (db_keys)
    {
      for (size_t i = 0; i < num_keys; ++i)
      {
        MmapFile *f = db_keys[i];
        if (f)
        {
          f->close();
          delete f;
        }
        db_keys[i] = NULL;
      }
      free(db_keys);
      db_keys = NULL;
    }

    num_keys = 0;
    readonly = true;
    num_slices = 0;
    num_records = 0;
  }
};

static
void MMDB__mark(void *ptr)
{
  MMDB *mdb = (MMDB*)ptr;
  if (mdb && mdb->model)
  {
    rb_gc_mark(mdb->model->_rm_obj);
  }
}

static
void MMDB__free(void *ptr)
{
  MMDB *mdb = (MMDB*)ptr;
  if (mdb)
  {
    delete(mdb);
  }
}

static
MMDB* MMDB__get(VALUE self) {
  MMDB *ptr;
  Data_Get_Struct(self, MMDB, ptr);
  return ptr;
}

static
VALUE MMDB__open(VALUE klass, VALUE recordmodel, VALUE path_prefix, VALUE num_slices, VALUE hint_slices, VALUE num_records, VALUE hint_records, VALUE readonly)
{
  Check_Type(path_prefix, T_STRING);

  RecordModel *model;
  Data_Get_Struct(recordmodel, RecordModel, model);

  MMDB *mdb = new MMDB;

  bool ok = mdb->open(model, RSTRING_PTR(path_prefix), NUM2ULONG(num_slices), NUM2ULONG(hint_slices), NUM2ULONG(num_records), NUM2ULONG(hint_records), RTEST(readonly));
  if (!ok)
  {
    delete mdb;
    return Qnil;
  }

  VALUE obj = Data_Wrap_Struct(klass, MMDB__mark, MMDB__free, mdb);
  return obj;
}

static
VALUE MMDB_close(VALUE self)
{
  MMDB__get(self)->close();
  return Qnil;
}

/*
 * XXX: Do not mix size_t and uint32_t
 */
static
void _put_bulk(MMDB *db, RecordModelInstanceArray *arr, bool verify=false)
{
  assert(db);
  assert(arr);
  assert(db->model == arr->model);
  assert(!db->readonly);

  RecordModel *model = db->model;

  size_t n = arr->entries();

  if (n == 0)
  {
    return;
  }

  arr->sort();

  if (verify)
  {
    RecordModelInstance ia(model, NULL);
    RecordModelInstance ib(model, NULL);

    for (size_t i = 1; i < n; ++i)
    {
      ia._ptr = arr->ptr_at_sorted(i-1);
      ib._ptr = arr->ptr_at_sorted(i);
      assert(ia.compare_keys(&ib) <= 0);
    }
  }

  // store the slice length
  *((uint32_t*)db->db_slices->ptr_append(sizeof(uint32_t))) = n;

  for (size_t i = 0; i < n; ++i)
  {
    void *rec_ptr = arr->ptr_at_sorted(i);

    // copy data
    MmapFile *d = db->db_data;
    for (size_t k = 0; k < model->_num_values; ++k)
    {
      RM_Type *field = model->_values[k];
      assert(field);
      field->copy_to_memory(rec_ptr, d->ptr_append(field->size())); 
    }

    // copy keys
    for (size_t k = 0; k < model->_num_keys; ++k)
    {
      MmapFile *d = db->db_keys[k];
      assert(d);
      RM_Type *field = model->_keys[k];
      assert(field);
      field->copy_to_memory(rec_ptr, d->ptr_append(field->size())); 
    }
  }

  db->num_records += n;
  ++db->num_slices;
}

struct Params 
{
  MMDB *db;
  RecordModelInstanceArray *arr;
  bool verify;
};

static
VALUE put_bulk(void *ptr)
{
  Params *params = (Params*)ptr;
  _put_bulk(params->db, params->arr, params->verify);
  return Qnil;
}

static
VALUE MMDB_put_bulk(VALUE self, VALUE arr)
{
  Params p;

  Data_Get_Struct(self, MMDB, p.db);
  Data_Get_Struct(arr, RecordModelInstanceArray, p.arr);
  p.verify = false;

  return rb_thread_blocking_region(put_bulk, &p, NULL, NULL);
}

struct Search
{
  MMDB *db;
  RecordModel *model;

  Search(MMDB *_db, RecordModel *_model)
  {
    this->db = _db;
    this->model = _model;
  }

  /*
   * Compare the key we are looking for with the element at position 'index'.
   */
  inline int compare(const void *key_ptr, uint64_t index)
  {
    for (size_t i = 0; i < db->num_keys; ++i)
    {
      RM_Type *field = model->_keys[i];
      const void *b_ptr = db->db_keys[i]->ptr_read_element(index, field->size());

      int cmp = field->compare_with_memory(key_ptr, b_ptr);
      if (cmp != 0) return cmp;
    }
    return 0;
  }

  void copy_keys_in(RecordModelInstance *rec, uint64_t index)
  {
    for (size_t i = 0; i < db->num_keys; ++i)
    {
      RM_Type *field = model->_keys[i];
      const void *c = db->db_keys[i]->ptr_read_element(index, field->size());
      field->set_from_memory(rec->ptr(), c);
    }
  }

  void copy_values_in(RecordModelInstance *rec, uint64_t index)
  {
    const void *c = db->db_data->ptr_read_element(index, model->size_values());

    for (size_t i = 0; i < model->_num_values; ++i)
    {
      RM_Type *field = model->_values[i];
      field->set_from_memory(rec->ptr(), c);
      c = (const void*) (((const char*)c) + field->size());
    }
  }

  int64_t bin_search(int64_t l, int64_t r, const void *key_ptr)
  {
    int64_t m;

    while (l < r)
    {
      m = l + (r - l) / 2;

      assert(l < r);
      assert(l >= 0);
      assert(r > 0);
      assert(m >= 0);
      assert(m >= l);

      int c = compare(key_ptr, m);
      if (c > 0)
      {
        /*
         * key is > element[m]
         *
         * continue search in right half
         */
        l = m + 1;
      }
      else if (c < 0)
      {
        /*
         * key is < element[m]
         *
         * continue search in left half
         */
        r = m - 1;
      }
      else
      {
        assert (c == 0);
        /*
	 * key is == element[m]
         *
	 * as mulitple equal keys are allowed to exist, and we want to find the
	 * first one, we continue in the left half but include the current
	 * element.
         */ 
         r = m;
      }
    }
    return l;
  }

  bool query(uint64_t idx_from, uint64_t idx_to, const RecordModelInstance *range_from, const RecordModelInstance *range_to,
             RecordModelInstance *current,
             bool (*iterator)(RecordModelInstance*, void*), void *data)
  {
    assert(idx_from <= idx_to);

    /*
     * What we are looking for is completely out of bound.
     *
     * [range_from, range_to] ... [idx_from, idx_to]
     */
    if (compare(range_to->ptr(), idx_from) < 0)
      return true;

    /*
     * What we are looking for is completely out of bound.
     *
     * [idx_from, idx_to] ... [range_from, range_to]
     */
    if (compare(range_from->ptr(), idx_to) > 0)
      return true;

    /*
     * Position our cursor using binary search
     */ 
    uint64_t cursor = bin_search(idx_from, idx_to, range_from->ptr());

    /*
     * Linear scan from current position
     */
    while (cursor <= idx_to)
    {
      copy_keys_in(current, cursor);
     
      int keypos;
      int cmp = current->keys_in_range_pos(range_from, range_to, keypos);
      if (cmp == 0)
      {
        /*
         * all keys are within [range_from, range_to]
         */
        copy_values_in(current, cursor);

        if (iterator && !iterator(current, data))
        {
          return false;
        }

        ++cursor;
      }
      else if (cmp < 0)
      {
        /*
	 * The key at position "keypos" is less than the corresponding key of
	 * "range_from".  Set the keys starting from "keypos" to the end to the
	 * values from the keys from "range_from".
         */

        if (keypos == 0)
        {
          /*
	   * This only can happen when the initial bin_search positions cursor
	   * to before "range_from". So in this case, we continue to the next
	   * record, instead of calling again bin_search (which might lead to
	   * in infinite loop).
           */
           ++cursor;
           continue;
        }

        current->copy_keys(range_from, keypos);

        /*
         * Search forward
         */
        cursor = bin_search(cursor+1, idx_to, current->ptr());
      }
      else if (cmp > 0)
      {
        /*
	 * The key at "keypos" exceeds the corresponding key in "range_to".
	 * Reset all keys starting from "keypos" to the values from
	 * "range_from" and increase the previous key (basically a
	 * carry-forward).
         */

        if (keypos == 0)
        {
          /*
           * The first key exceeded "range_to" -> quit the search.
           */
          break;
        }

        current->copy_keys(range_from, keypos);
        current->increase_key(keypos-1); // XXX: check overflows

        /*
         * Search forward
         */
        cursor = bin_search(cursor+1, idx_to, current->ptr());
      }
    }

    return true; // continue with next slice
  }

  /*
   * Queries all slices
   */
  bool query_all(const RecordModelInstance *range_from, const RecordModelInstance *range_to,
             RecordModelInstance *current,
             bool (*iterator)(RecordModelInstance*, void*), void *data)
  {
    size_t offs = 0;
    for (size_t s = 0; s < db->num_slices; ++s)
    {
      uint32_t length = db->db_slices->ptr_read_element_at<uint32_t>(s);
      if (length == 0)
        continue;

      if (!query(offs, offs+length-1, range_from, range_to, current, iterator, data))
        return false;
    }
    return true;
  }
 
};

struct yield_iter_data
{
  VALUE _current;
};

static
bool yield_iter(RecordModelInstance *current, void *data)
{
  rb_yield(((yield_iter_data*)data)->_current);
  return true;
}

static
VALUE MMDB_query(VALUE self, VALUE _from, VALUE _to, VALUE _current)
{
  MMDB *db;
  Data_Get_Struct(self, MMDB, db);

  RecordModelInstance *from;
  RecordModelInstance *to;
  RecordModelInstance *current;

  Data_Get_Struct(_from, RecordModelInstance, from);
  Data_Get_Struct(_to, RecordModelInstance, to);
  Data_Get_Struct(_current, RecordModelInstance, current);

  assert(from->model == to->model);
  assert(from->model == current->model);

  struct yield_iter_data d;
  d._current = _current;

  Search s(db, from->model);
  bool ok = s.query_all(from, to, current, yield_iter, &d); 

  return (ok ? Qtrue : Qfalse);
}

struct array_fill_iter_data
{
  RecordModelInstanceArray *arr;
};

static
bool array_fill_iter(RecordModelInstance *current, void *data)
{
  return ((array_fill_iter_data*)data)->arr->push((const RecordModelInstance*)current);
}
 
struct Params_query_into
{
  MMDB *db;
  RecordModelInstance *from;
  RecordModelInstance *to;
  RecordModelInstance *current;
  RecordModelInstanceArray *arr;
};

static
VALUE query_into(void *a)
{
  Params_query_into *p = (Params_query_into*)a;
  struct array_fill_iter_data d;
  d.arr = p->arr;

  Search s(p->db, p->from->model);
  bool ok = s.query_all(p->from, p->to, p->current, array_fill_iter, &d); 

  return (ok ? Qtrue : Qfalse);
}

static
VALUE MMDB_query_into(VALUE self, VALUE _from, VALUE _to, VALUE _current, VALUE _arr)
{
  Params_query_into p;
  Data_Get_Struct(self, MMDB, p.db);

  Data_Get_Struct(_from, RecordModelInstance, p.from);
  Data_Get_Struct(_to, RecordModelInstance, p.to);
  Data_Get_Struct(_current, RecordModelInstance, p.current);
  Data_Get_Struct(_arr, RecordModelInstanceArray, p.arr);

  assert(p.arr->model == p.from->model);
  assert(p.from->model == p.to->model);
  assert(p.from->model == p.current->model);

  return rb_thread_blocking_region(query_into, &p, NULL, NULL);
}

extern "C"
void Init_RecordModelMMDBExt()
{
  VALUE cMMDB = rb_define_class("RecordModelMMDB", rb_cObject);
  rb_define_singleton_method(cMMDB, "open", (VALUE (*)(...)) MMDB__open, 7);
  rb_define_method(cMMDB, "close", (VALUE (*)(...)) MMDB_close, 0);
  rb_define_method(cMMDB, "put_bulk", (VALUE (*)(...)) MMDB_put_bulk, 1);
  rb_define_method(cMMDB, "query", (VALUE (*)(...)) MMDB_query, 3);
  rb_define_method(cMMDB, "query_into", (VALUE (*)(...)) MMDB_query_into, 4);
}
