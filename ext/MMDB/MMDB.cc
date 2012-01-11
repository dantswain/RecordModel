#include <assert.h> // assert
#include <stdio.h> // snprintf 
#include <strings.h> // bzero
#include "../../include/RecordModel.h"
#include "MmapFile.h"
#include "ruby.h"
#include <pthread.h>

/*
 * Declared in ../RecordModel/RecordModel.cc
 */
extern RecordModel* get_RecordModel(VALUE);
extern RecordModelInstance* get_RecordModelInstance(VALUE);
extern RecordModelInstanceArray* get_RecordModelInstanceArray(VALUE);

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
 * Thread safetly:
 *
 * It is safe to use the methods "put_bulk", "commit" and "query_all"
 * concurrently.
 * 
 */
struct MMDB
{

  RecordModel *model;

private:

  MmapFile *db_slices;
  MmapFile *db_data;
  MmapFile **db_keys;
  size_t num_keys;
  bool readonly;
  size_t num_slices;
  size_t num_records;

  pthread_rwlock_t rwlock;
  pthread_mutex_t mutex;

public:

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
    pthread_rwlock_init(&rwlock, NULL);
    pthread_mutex_init(&mutex, NULL);
  }

  ~MMDB()
  {
    close();
    pthread_mutex_destroy(&mutex);
    pthread_rwlock_destroy(&rwlock);
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
    db_slices = new MmapFile(&rwlock);
    ok = db_slices->open(name, sizeof(uint32_t)*num_slices, sizeof(uint32_t)*_hint_slices, readonly);
    if (!ok) goto fail;

    // open data file
    snprintf(name, name_sz, "%sdata_%ld", path_prefix, model->size_values());
    db_data = new MmapFile(&rwlock);
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
      db_keys[i] = new MmapFile(&rwlock);
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

  bool commit(size_t &_num_slices, size_t &_num_records)
  {
    assert(!readonly);
    bool res = false;

    int err = pthread_mutex_lock(&mutex);
    assert(!err);

    err = pthread_rwlock_rdlock(&rwlock);
    assert(!err);
    
    if (!db_slices->sync())
      goto end;

    if (!db_data->sync())
      goto end;

    for (size_t i = 0; i < num_keys; ++i)
    {
      if (!db_keys[i]->sync())
        goto end;
    }

    _num_slices = num_slices;
    _num_records = num_records;
    res = true;
end:
    err = pthread_rwlock_unlock(&rwlock);
    assert(!err);
    err = pthread_mutex_unlock(&mutex);
    assert(!err);

    return res;
  }

  /*
   * XXX: Do not mix size_t and uint32_t
   */
  void put_bulk(RecordModelInstanceArray *arr, bool verify=false)
  {
    assert(!readonly);
    assert(arr);
    assert(model == arr->model);

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
	ia._ptr = arr->ptr_at(i-1);
	ib._ptr = arr->ptr_at(i);
	assert(ia.compare_keys(&ib) <= 0);
      }
    }

    /*
     * There cannot be more than one thread calling put_bulk
     * at the same time. Use a mutex to guarantee that.
     */
    int err = pthread_mutex_lock(&mutex);
    assert(!err);

    /*
     * Because we might munmap or mremap the memory location of a MmapFile 
     * database in case we have to expand it, the code within MmapFile
     * might acquire an exclusive rwlock lock. 
     */

    // store the slice length
    db_slices->append_value<uint32_t>(n);

    for (size_t i = 0; i < n; ++i)
    {
      void *rec_ptr = arr->ptr_at(i);

      // copy data
      for (size_t k = 0; k < model->_num_values; ++k)
      {
	RM_Type *field = model->_values[k];
	field->copy_to_memory(rec_ptr, db_data->ptr_append(field->size())); 
      }

      // copy keys
      for (size_t k = 0; k < model->_num_keys; ++k)
      {
	RM_Type *field = model->_keys[k];
	field->copy_to_memory(rec_ptr, db_keys[k]->ptr_append(field->size())); 
      }
    }

    num_records += n;
    ++num_slices;

    err = pthread_mutex_unlock(&mutex);
    assert(!err);
  }

  // -----------------------------------------------
  // Query support
  // -----------------------------------------------

  static const int ITER_CONTINUE = 0; 
  static const int ITER_NEXT_SLICE = 1;
  static const int ITER_STOP = 2;

private:

  /*
   * Compare the key we are looking for with the element at position 'index'.
   */
  inline int compare(const void *key_ptr, uint64_t index)
  {
    for (size_t i = 0; i < this->num_keys; ++i)
    {
      RM_Type *field = model->_keys[i];
      const void *b_ptr = this->db_keys[i]->ptr_read_element(index, field->size());

      int cmp = field->compare_with_memory(key_ptr, b_ptr);
      if (cmp != 0) return cmp;
    }
    return 0;
  }

  void copy_keys_in(RecordModelInstance *rec, uint64_t index)
  {
    for (size_t i = 0; i < this->num_keys; ++i)
    {
      RM_Type *field = model->_keys[i];
      const void *c = this->db_keys[i]->ptr_read_element(index, field->size());
      field->set_from_memory(rec->ptr(), c);
    }
  }

  void copy_values_in(RecordModelInstance *rec, uint64_t index)
  {
    const void *c = this->db_data->ptr_read_element(index, model->size_values());

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

  int query(uint64_t idx_from, uint64_t idx_to, const RecordModelInstance *range_from, const RecordModelInstance *range_to,
            RecordModelInstance *current,
            int (*iterator)(RecordModelInstance*, void*), void *data)
  {
    assert(idx_from <= idx_to);

    /*
     * What we are looking for is completely out of bound.
     *
     * [range_from, range_to] ... [idx_from, idx_to]
     */
    if (compare(range_to->ptr(), idx_from) < 0)
      return ITER_CONTINUE;

    /*
     * What we are looking for is completely out of bound.
     *
     * [idx_from, idx_to] ... [range_from, range_to]
     */
    if (compare(range_from->ptr(), idx_to) > 0)
      return ITER_CONTINUE;

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

        int iter = iterator(current, data);
        if (iter != ITER_CONTINUE)
        {
          return iter;
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

    return ITER_CONTINUE; // continue with next slice
  }


public:

  size_t get_num_slices_for_read()
  {
    return this->num_slices;
  }

  /*
   * Queries all slices
   * "slices" is equal to snapshots.
   */
  int query_all(size_t slices, const RecordModelInstance *range_from, const RecordModelInstance *range_to,
                 RecordModelInstance *current,
                 int (*iterator)(RecordModelInstance*, void*), void *data)
  {
    int iter = ITER_CONTINUE;
    size_t offs = 0;

    /*
     * We set a read lock here so a _ptr of a MmapFile cannot be ripped out under us.
     * in case the mmap has to be expanded.
     */
    int err = pthread_rwlock_rdlock(&rwlock);
    assert(!err);

    for (size_t s = 0; s < slices; ++s)
    {
      uint32_t length = db_slices->ptr_read_element_at<uint32_t>(s);
      if (length == 0)
        continue;

      iter = query(offs, offs+length-1, range_from, range_to, current, iterator, data);
      if (iter == ITER_STOP)
        break;
    }

    err = pthread_rwlock_unlock(&rwlock);
    assert(!err);

    return iter;
  }
  
  struct min_iter_data
  {
    RecordModelInstance *min;
  };

  static int min_iter(RecordModelInstance *current, void *_data)
  {
    min_iter_data *data = (min_iter_data*)_data;

    if (data->min)
    {
      if (current->compare_keys(data->min) < 0)
      {
        data->min->copy(current);
      }
    }
    else
    {
      data->min = current->dup(); 
    }

    return ITER_NEXT_SLICE;
  }
  
  /*
   * Returns the smallest element in current. If no element was found, returns false. 
   */
  bool query_min(size_t slices, const RecordModelInstance *range_from, const RecordModelInstance *range_to,
             RecordModelInstance *current)
  {
    min_iter_data data;
    data.min = NULL;

    query_all(slices, range_from, range_to, current, min_iter, &data);

    if (data.min)
    {
      current->copy(data.min);
      delete data.min;
      return true;
    }
    else
    {
      // no record found at all
      return false;
    }
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

  RecordModel *model = get_RecordModel(recordmodel);

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
  params->db->put_bulk(params->arr, params->verify);
  return Qnil;
}

static
VALUE MMDB_put_bulk(VALUE self, VALUE arr)
{
  Params p;

  Data_Get_Struct(self, MMDB, p.db);
  p.arr = get_RecordModelInstanceArray(arr);
  p.verify = false;

  return rb_thread_blocking_region(put_bulk, &p, NULL, NULL);
}

struct yield_iter_data
{
  VALUE _current;
};

static
int yield_iter(RecordModelInstance *current, void *data)
{
  rb_yield(((yield_iter_data*)data)->_current);
  return MMDB::ITER_CONTINUE;
}

static
VALUE MMDB_query_each(VALUE self, VALUE _from, VALUE _to, VALUE _current, VALUE _snapshot)
{
  MMDB *db;
  Data_Get_Struct(self, MMDB, db);

  RecordModelInstance *from = get_RecordModelInstance(_from);
  RecordModelInstance *to = get_RecordModelInstance(_to);
  RecordModelInstance *current = get_RecordModelInstance(_current);

  assert(from->model == to->model);
  assert(from->model == current->model);
  assert(from->model == db->model);

  struct yield_iter_data d;
  d._current = _current;

  size_t snapshot = NUM2ULONG(_snapshot);

  db->query_all(snapshot, from, to, current, yield_iter, &d); 

  return Qnil;
}

struct array_fill_iter_data
{
  RecordModelInstanceArray *arr;
};

static
int array_fill_iter(RecordModelInstance *current, void *data)
{
  bool ok = ((array_fill_iter_data*)data)->arr->push((const RecordModelInstance*)current);
  return (ok ? MMDB::ITER_CONTINUE : MMDB::ITER_STOP);
}
 
struct Params_query_into
{
  MMDB *db;
  RecordModelInstance *from;
  RecordModelInstance *to;
  RecordModelInstance *current;
  RecordModelInstanceArray *arr;
  size_t snapshot;
};

static
VALUE query_into(void *a)
{
  Params_query_into *p = (Params_query_into*)a;
  struct array_fill_iter_data d;
  d.arr = p->arr;

  int iter = p->db->query_all(p->snapshot, p->from, p->to, p->current, array_fill_iter, &d); 

  if (iter == MMDB::ITER_STOP)
    return Qfalse;
  else
    return Qtrue;
}

static
VALUE MMDB_query_into(VALUE self, VALUE _from, VALUE _to, VALUE _current, VALUE _arr, VALUE _snapshot)
{
  Params_query_into p;
  Data_Get_Struct(self, MMDB, p.db);

  p.from = get_RecordModelInstance(_from);
  p.to = get_RecordModelInstance(_to);
  p.current = get_RecordModelInstance(_current);
  p.arr = get_RecordModelInstanceArray(_arr);

  assert(p.arr->model == p.from->model);
  assert(p.from->model == p.to->model);
  assert(p.from->model == p.current->model);
  assert(p.from->model == p.db->model);

  p.snapshot = NUM2ULONG(_snapshot);

  return rb_thread_blocking_region(query_into, &p, NULL, NULL);
}

static
VALUE query_min(void *a)
{
  Params_query_into *p = (Params_query_into*)a;

  bool found = p->db->query_min(p->snapshot, p->from, p->to, p->current); 

  return (found ? Qtrue : Qfalse);
}

/*
 * Returns nil if nothing was found, or the _current value, filled in
 * with the smallest (minimum) record (according to the keys) found.
 * If records with equal keys exist, the first found is used.
 */
static
VALUE MMDB_query_min(VALUE self, VALUE _from, VALUE _to, VALUE _current, VALUE _snapshot)
{
  Params_query_into p;
  Data_Get_Struct(self, MMDB, p.db);

  p.from = get_RecordModelInstance(_from);
  p.to = get_RecordModelInstance(_to);
  p.current = get_RecordModelInstance(_current);

  assert(p.from->model == p.to->model);
  assert(p.from->model == p.current->model);
  assert(p.from->model == p.db->model);

  p.snapshot = NUM2ULONG(_snapshot);

  VALUE res = rb_thread_blocking_region(query_min, &p, NULL, NULL);
  if (RTEST(res))
  {
    return _current;
  }
  return Qnil;
}

/*
 * TODO: in background
 */
static
VALUE MMDB_commit(VALUE self)
{
  MMDB *db;  
  Data_Get_Struct(self, MMDB, db);

  size_t num_slices = 0;
  size_t num_records = 0;

  bool ok = db->commit(num_slices, num_records);
  VALUE res = Qnil;

  if (ok)
  {
    res = rb_ary_new();  
    rb_ary_push(res, ULONG2NUM(num_slices));
    rb_ary_push(res, ULONG2NUM(num_records));
  }

  return res;
}

static
VALUE MMDB_get_snapshot_num(VALUE self)
{
  MMDB *db;  
  Data_Get_Struct(self, MMDB, db);
  return ULONG2NUM(db->get_num_slices_for_read());
}

extern "C"
void Init_RecordModelMMDBExt()
{
  VALUE cMMDB = rb_define_class("RecordModelMMDB", rb_cObject);
  rb_define_singleton_method(cMMDB, "open", (VALUE (*)(...)) MMDB__open, 7);
  rb_define_method(cMMDB, "close", (VALUE (*)(...)) MMDB_close, 0);
  rb_define_method(cMMDB, "put_bulk", (VALUE (*)(...)) MMDB_put_bulk, 1);
  rb_define_method(cMMDB, "query_each", (VALUE (*)(...)) MMDB_query_each, 4);
  rb_define_method(cMMDB, "query_into", (VALUE (*)(...)) MMDB_query_into, 5);
  rb_define_method(cMMDB, "query_min", (VALUE (*)(...)) MMDB_query_min, 4);
  rb_define_method(cMMDB, "commit", (VALUE (*)(...)) MMDB_commit, 0);
  rb_define_method(cMMDB, "get_snapshot_num", (VALUE (*)(...)) MMDB_get_snapshot_num, 0);
}
