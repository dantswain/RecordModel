#include <sys/mman.h>

#include "../../include/RecordModel.h"
#include <assert.h> // assert
#include <algorithm> // std::sort
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include <string.h> // strerror
#include <errno.h>  // errno

#include "ruby.h"

struct ondisk
{
  uint64_t max_num_records;
  uint64_t num_records;
  uint64_t model_size;
  uint64_t slices_i; // how many uint64_t in the slices section
};

struct RecordDB
{
  VALUE modelklass;
  int db_handle;
  char *ptr;
  size_t length;
  bool writable;

  ondisk *header;

  char *record_ptr;
  uint64_t *slices_beg;
  RecordModel *model;

  char* record_n(size_t n)
  {
    assert(n < header->max_num_records);
    return (char*)record_ptr + (n*model->size);
  }

  RecordDB()
  {
    header = NULL;
    db_handle = -1;
    ptr = NULL;
    length = 0;
    modelklass = Qnil;
    model = NULL;
    writable = false;
  }

  ~RecordDB()
  {
    close();
  }

  void close()
  {
    if (ptr)
    {
      munmap(ptr, length);
      ptr = NULL;
    }

    if (db_handle != -1)
    {
      ::close(db_handle);
      db_handle = -1;
    }
  }
};

static void RecordDB__mark(void *ptr)
{
  RecordDB *mdb = (RecordDB*) ptr;
  if (mdb)
  {
    rb_gc_mark(mdb->modelklass);
  }
}

static void RecordDB__free(void *ptr)
{
  RecordDB *mdb = (RecordDB*) ptr;
  if (mdb)
  {
    delete(mdb);
  }
}

static RecordDB& RecordDB__get(VALUE self) {
  RecordDB *ptr;
  Data_Get_Struct(self, RecordDB, ptr);
  return *ptr;
}

static VALUE RecordDB__open(VALUE klass, VALUE path, VALUE modelklass, VALUE max_num_records, VALUE writable)
{
  Check_Type(path, T_STRING);

  VALUE model = rb_cvar_get(modelklass, rb_intern("@@__model")); // XXX: funcall
  RecordModel *m;
  Data_Get_Struct(model, RecordModel, m);
 
  // GC model from ModelDB
  RecordDB *mdb = new RecordDB;
  mdb->modelklass = modelklass;
  mdb->model = m;

  mdb->writable = RTEST(writable);

  if (!writable)
  {
    mdb->db_handle = open(RSTRING_PTR(path), O_RDONLY);
    assert(mdb->db_handle != -1);

    struct stat buf;
    int err = fstat(mdb->db_handle, &buf);
    assert(err != -1);

    mdb->length = buf.st_size;
    assert(mdb->length >= sizeof(ondisk));

    void *mm_ptr = mmap(NULL, mdb->length, PROT_READ, MAP_SHARED /*| MAP_HUGETLB*/, mdb->db_handle, 0);
    if (mm_ptr == MAP_FAILED)
    {
      printf("mmap failed: %s\n", strerror(errno));
      assert(false);
    }
    mdb->ptr = (char*)mm_ptr;
  }
  else
  {
    // create a new database. XXX: open r/w
    ondisk od_copy;
    od_copy.model_size = m->size;
    od_copy.max_num_records = NUM2ULONG(max_num_records);
    od_copy.num_records = 0;
    od_copy.slices_i = 0;

    ondisk *od = &od_copy;

    mdb->db_handle = open(RSTRING_PTR(path), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
    assert(mdb->db_handle != -1);

    mdb->length = sizeof(ondisk) + (od->model_size * od->max_num_records) + 2*sizeof(uint64_t)*od->max_num_records;
    printf("max_num_records: %ld, length: %ld\n", od->max_num_records, mdb->length);

    int err = ftruncate(mdb->db_handle, mdb->length); 
    assert(err == 0);

    void *mm_ptr = mmap(NULL, mdb->length, PROT_READ|PROT_WRITE, MAP_SHARED /*| MAP_HUGETLB*/, mdb->db_handle, 0);
    if (mm_ptr == MAP_FAILED)
    {
      printf("mmap failed: %s\n", strerror(errno));
      assert(false);
    }
    mdb->ptr = (char*)mm_ptr;

    // XXX: align
    
    //
    // copy od_copy into mmaped memory
    //
    mdb->header = (ondisk*)mdb->ptr;
    memcpy(mdb->header, od, sizeof(ondisk));
    od = mdb->header;
  }

  mdb->header = (ondisk*)mdb->ptr;
  mdb->record_ptr = mdb->ptr + sizeof(ondisk);
  mdb->slices_beg = (uint64_t*) (mdb->record_ptr + mdb->header->model_size * mdb->header->max_num_records);

  VALUE obj;
  obj = Data_Wrap_Struct(klass, RecordDB__mark, RecordDB__free, mdb);
  return obj;
}

static VALUE RecordDB_close(VALUE self)
{
  RecordDB &mdb = RecordDB__get(self);
  mdb.close();
  return Qnil;
}

struct Params 
{
  RecordDB *mdb;
  RecordModelInstanceArray *arr;
};

struct sorter
{
  RecordModel *model;
  RecordModelInstanceArray *arr;
 
  bool operator()(uint32_t a, uint32_t b)
  {
    return (model->compare_keys_buf(model->keyptr(arr, a), model->keyptr(arr, b)) < 0);
  }
};

static VALUE put_bulk(void *p)
{
  Params *a = (Params*)p;
  RecordModel *m = a->arr->model;
  RecordDB *mdb = a->mdb;

  assert(mdb->writable);
  
  size_t n = a->arr->entries();
  uint32_t *idxs = (uint32_t*)malloc(sizeof(uint32_t) * n);
  assert(idxs);
  for (size_t i = 0; i < n; ++i)
  {
    idxs[i] = i;
  }
  sorter s;
  s.model = m; 
  s.arr = a->arr; 
  std::sort(idxs, idxs+n, s);

  for (size_t i = 1; i < n; ++i)
  {
    assert(m->compare_keys_buf(m->keyptr(a->arr, idxs[i]), m->keyptr(a->arr, idxs[i-1])) >= 0);
  }

  uint64_t *idx_arr = &mdb->slices_beg[mdb->header->slices_i];
  mdb->header->slices_i += n+1;

  idx_arr[0] = n;
  idx_arr++;

  for (size_t i = 0; i < n; ++i)
  {
    size_t rno = mdb->header->num_records++;
    idx_arr[i] = rno; 
    memcpy(mdb->record_n(rno), m->elemptr(a->arr, idxs[i]), m->size);
  }

  free(idxs);

  return Qtrue;
}

static VALUE RecordDB_put_bulk(VALUE self, VALUE arr)
{
  Params p;

  Data_Get_Struct(self, RecordDB, p.mdb);
  Data_Get_Struct(arr, RecordModelInstanceArray, p.arr);

  return rb_thread_blocking_region(put_bulk, &p, NULL, NULL);
}

int64_t bin_search(int64_t l, int64_t r, const char *key, RecordDB *mdb, RecordModel *model, uint64_t *slice)
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

  RecordModel *model = from->model;

  uint64_t *slice = mdb->slices_beg;
  uint64_t *end_slice = mdb->slices_beg + mdb->header->slices_i;

  int n = 0;
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
      if (model->compare_keys_buf((const char*)model->keyptr(to), rec) < 0)
      {
        break;
      }

      memcpy(model->keyptr(current), rec, model->keysize());
      int kp = model->keys_in_range_pos(current, from, to);
      if (kp == 0)
      {
        memcpy(model->dataptr(current), rec+model->keysize(), model->datasize());
        rb_yield(_current);
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
	  assert(keypos > 0); // should never happen as then the key is > to
          model->copy_keys(current, from, keypos);
          model->increase_key(current, keypos-1); // XXX: check key overflows!
	}

        /* binary_search forward */
        i = bin_search(i+1, len - 1, (const char*)model->keyptr(current), mdb, model, slice);
      }
    }

    // jump to next slice
    slice += len;
    n++;

  } /* while */

  return Qnil;
}


extern "C"
void Init_RecordModelMMDBExt()
{
  VALUE cKCDB = rb_define_class("RecordModelMMDB", rb_cObject);
  rb_define_singleton_method(cKCDB, "open", (VALUE (*)(...)) RecordDB__open, 4);
  rb_define_method(cKCDB, "close", (VALUE (*)(...)) RecordDB_close, 0);
  rb_define_method(cKCDB, "put_bulk", (VALUE (*)(...)) RecordDB_put_bulk, 1);
  rb_define_method(cKCDB, "query", (VALUE (*)(...)) RecordDB_query, 3);
}
