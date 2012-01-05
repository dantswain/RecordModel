
#include "../../include/RecordModel.h"
#if 0
#include <algorithm> // std::sort
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include "ruby.h"

#include "MmapFile.h"

#include <assert.h> // assert
#include <stdio.h> // snprintf 
#include <strings.h> // bzero

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
  bool open(RecordModel *_model, const char *path_prefix, size_t _num_slices, size_t _num_records, bool _readonly)
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
    snprintf(name, name_sz, "%sslices_4", path_prefix);
    db_slices = new MmapFile();
    ok = db_slices->open(name, sizeof(uint32_t)*num_slices, readonly);
    if (!ok) goto fail;

    // open data file
    snprintf(name, name_sz, "%sdata_%ld", path_prefix, model->size_values());
    db_data = new MmapFile();
    ok = db_data->open(name, model->size_values()*num_records, readonly);
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
      ok = db_keys[i]->open(name, field->size()*num_records, readonly);
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
VALUE MMDB__open(VALUE klass, VALUE recordmodel, VALUE path_prefix, VALUE num_slices, VALUE num_records, VALUE readonly)
{
  Check_Type(path_prefix, T_STRING);

  RecordModel *model;
  Data_Get_Struct(recordmodel, RecordModel, model);

  MMDB *mdb = new MMDB;

  bool ok = mdb->open(model, RSTRING_PTR(path_prefix), NUM2ULONG(num_slices), NUM2ULONG(num_records), RTEST(readonly));
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
  p.verify = true;

  return rb_thread_blocking_region(put_bulk, &p, NULL, NULL);
}


#if 0

static int64_t bin_search(int64_t l, int64_t r, const char *key, RecordDB *mdb, RecordModel *model, uint64_t *slice)
{
  int64_t m;
  int c;
  while (l < r)
  {
    m = l + (r - l) / 2; 

    assert(l < r);
    assert(l >= 0);
    assert(r > 0);
    assert(m >= 0);
    assert(m >= l);
    //assert(r < len);
    //assert(m < len);

    c = model->compare_keys_buf(key, (const char*)mdb->record_n(slice[m]));
    if (c > 0)
    {
      // search in right half
      l = m + 1;
    }
    else if (c < 0)
    {
      // search in left half
      r = m - 1;
    }
    else
    {
      /* We are assuming that there can be multiple equal keys, so we have to scan left to find the first! */
      assert (c==0);

      for (;;)
      {
        if (m == l) break;
	assert(m > l);

        if (model->compare_keys_buf(key, (const char*)mdb->record_n(slice[m-1])) == 0)
	{
	  --m;
        } 
	else
	{
	  break;
	}
      }

      l = m;
      break;
    }
  }

  return l;
}

struct yield_iter_data
{
  VALUE _current;
};

static bool yield_iter(RecordModelInstance *current, void *data)
{
  rb_yield(((yield_iter_data*)data)->_current);
  return true;
}

struct array_fill_iter_data
{
  RecordModelInstanceArray *arr;
};

static bool array_fill_iter(RecordModelInstance *current, void *data)
{
  array_fill_iter_data *d = (array_fill_iter_data*)data;
  RecordModel *m = current->model;

  if (d->arr->full() && !d->arr->expand(m->size))
  {
    return false;
    //rb_raise(rb_eArgError, "Failed to expand array");
  }
  assert(!d->arr->full());

  memcpy(m->elemptr(d->arr, d->arr->_entries), current->ptr, m->size);

  d->arr->_entries += 1;
 
  return true;
}

static VALUE query_internal(RecordDB *mdb, RecordModel *model, const RecordModelInstance *from, const RecordModelInstance *to, RecordModelInstance *current,
  bool (*iterator)(RecordModelInstance*, void*), void *data)
{
  uint64_t *slice = mdb->slices_beg;
  uint64_t *end_slice = mdb->slices_beg + mdb->header->slices_i;

  while (slice < end_slice) 
  {
    uint64_t len = *slice;
    ++slice;

    if (len == 0)
    {
      continue;
    }
    // whole slice completely out of bounds -> skip slice
    if ((model->compare_keys_buf((const char*)model->keyptr(to), (const char*)mdb->record_n(slice[0])) < 0) ||
        (model->compare_keys_buf((const char*)model->keyptr(from), (const char*)mdb->record_n(slice[len-1])) > 0))
    {
      slice += len;
      continue;
    }

    /* binary_search */
    uint64_t i = bin_search(0, len - 1, (const char*)model->keyptr(from), mdb, model, slice);

    // linear scan from current position (i)
    while (i < len)
    {
      const char *rec = (const char*)mdb->record_n(slice[i]);

      memcpy(model->keyptr(current), rec, model->keysize());
      int kp = model->keys_in_range_pos(current, from, to);
      if (kp == 0)
      {
        memcpy(model->dataptr(current), rec+model->keysize(), model->datasize());
        if (iterator)
        {
          if (!iterator(current, data)) return Qfalse;
        }
        ++i;
      }
      else
      {
	assert(kp != 0);
        int keypos = (kp > 0 ? kp : -kp) - 1;

        if (kp < 0)
	{
	  // the key at position keypos is < left key
	  // just move the key forward, but don't advance the previous key 
	  if (keypos == 0)
	  {
	    // this happens when the initial bin search positions to before 'from'
	    ++i;
	    continue;
          }
	  model->copy_keys(current, from, keypos);
	}
	else if (kp > 0)
	{
	  // key at keypos moved beyond the to-bound. reset it and all 
	  // following keys, and increase the previous key.
          if (keypos == 0)
          {
            // if the first key moves beyond 'to' the search is over for this slice
            assert(model->compare_keys_buf((const char*)model->keyptr(to), (const char*)model->keyptr(current)) < 0);
            break;
          }
          model->copy_keys(current, from, keypos);
          model->increase_key(current, keypos-1); // XXX: check key overflows!
	}

        /* binary_search forward */
        i = bin_search(i+1, len - 1, (const char*)model->keyptr(current), mdb, model, slice);
      }
    } /* inner while */

    // jump to next slice
    slice += len;

  } /* while */

  return Qtrue;
}
 
static VALUE RecordDB_query(VALUE self, VALUE _from, VALUE _to, VALUE _current)
{
  RecordDB *mdb;
  Data_Get_Struct(self, RecordDB, mdb);

  RecordModelInstance *from;
  Data_Get_Struct(_from, RecordModelInstance, from);

  RecordModelInstance *to;
  Data_Get_Struct(_to, RecordModelInstance, to);

  RecordModelInstance *current;
  Data_Get_Struct(_current, RecordModelInstance, current);

  assert(from->model == to->model);
  assert(from->model == current->model);

  struct yield_iter_data d;
  d._current = _current;

  return query_internal(mdb, from->model, from, to, current, yield_iter, &d);
}

struct Params_query_into
{
  RecordDB *mdb;
  RecordModelInstance *from;
  RecordModelInstance *to;
  RecordModelInstance *current;
  RecordModelInstanceArray *mia;
};

static VALUE query_into(void *a)
{
  Params_query_into *p = (Params_query_into*)a;
  struct array_fill_iter_data d;
  d.arr = p->mia;
  return query_internal(p->mdb, p->from->model, p->from, p->to, p->current, array_fill_iter, &d);
}

static VALUE RecordDB_query_into(VALUE self, VALUE _from, VALUE _to, VALUE _current, VALUE _mia)
{
  Params_query_into p;
  Data_Get_Struct(self, RecordDB, p.mdb);

  Data_Get_Struct(_from, RecordModelInstance, p.from);

  Data_Get_Struct(_to, RecordModelInstance, p.to);

  Data_Get_Struct(_current, RecordModelInstance, p.current);

  Data_Get_Struct(_mia, RecordModelInstanceArray, p.mia);

  assert(p.mia->model == p.from->model);
  assert(p.from->model == p.to->model);
  assert(p.from->model == p.current->model);

  return rb_thread_blocking_region(query_into, &p, NULL, NULL);
}
#endif

extern "C"
void Init_RecordModelMMDBExt()
{
  VALUE cMMDB = rb_define_class("RecordModelMMDB", rb_cObject);
  rb_define_singleton_method(cMMDB, "open", (VALUE (*)(...)) MMDB__open, 5);
  rb_define_method(cMMDB, "close", (VALUE (*)(...)) MMDB_close, 0);
  rb_define_method(cMMDB, "put_bulk", (VALUE (*)(...)) MMDB_put_bulk, 1);
  //rb_define_method(cMMDB, "query", (VALUE (*)(...)) RecordDB_query, 3);
  //rb_define_method(cMMDB, "query_into", (VALUE (*)(...)) RecordDB_query_into, 4);
}
