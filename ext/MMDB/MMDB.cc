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

struct RecordDB
{
  VALUE modelklass;
  int db_handle;
  char *ptr;
  size_t length;
  size_t max_num_records;


  char *record_ptr;
  size_t num_records;

  char *array_ptr_beg;
  char *current_array_ptr;

  RecordModel *model;

  char* record_n(size_t n)
  {
    assert(n < max_num_records);
    return (char*)record_ptr + (n*model->size);
  }

  RecordDB()
  {
    db_handle = -1;
    ptr = NULL;
    length = 0;
    max_num_records = 0;
    num_records = 0;
    modelklass = Qnil;
    model = NULL;
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

static VALUE RecordDB__open(VALUE klass, VALUE path, VALUE modelklass, VALUE max_num_records)
{
  Check_Type(path, T_STRING);

  VALUE model = rb_cvar_get(modelklass, rb_intern("@@__model")); // XXX: funcall
  RecordModel *m;
  Data_Get_Struct(model, RecordModel, m);
 
  // GC model from ModelDB
  RecordDB *mdb = new RecordDB;

  mdb->max_num_records = NUM2ULONG(max_num_records);

  mdb->db_handle = open(RSTRING_PTR(path), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
  assert(mdb->db_handle != -1);

  mdb->length = (m->size * mdb->max_num_records) + 2*sizeof(uint64_t)*mdb->max_num_records;
  printf("max_num_records: %ld, length: %ld\n", mdb->max_num_records, mdb->length);

  int err = ftruncate(mdb->db_handle, mdb->length); 
  assert(err == 0);

  void *mm_ptr = mmap(NULL, mdb->length, PROT_READ|PROT_WRITE, MAP_SHARED /*| MAP_HUGETLB*/, mdb->db_handle, 0);
  if (mm_ptr == MAP_FAILED)
  {
    printf("mmap failed: %s\n", strerror(errno));
    assert(false);
  }
  mdb->ptr = (char*)mm_ptr;

  mdb->modelklass = modelklass;
  mdb->model = m;

  mdb->record_ptr = mdb->ptr;
  mdb->array_ptr_beg = mdb->ptr + m->size * mdb->max_num_records;
  mdb->current_array_ptr = mdb->array_ptr_beg;

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
    return (model->compare_keys(model->keyptr(arr, a), model->keyptr(arr, b)) < 0);
  }
};

static VALUE put_bulk(void *p)
{
  Params *a = (Params*)p;
  RecordModel *m = a->arr->model;
  RecordDB *mdb = a->mdb;
  
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

  uint64_t *idx_arr = (uint64_t*)mdb->current_array_ptr;
  mdb->current_array_ptr += sizeof(uint64_t) * (n+1);

  idx_arr[0] = n;
  idx_arr++;

  for (size_t i = 0; i < n; ++i)
  {
    size_t rno = mdb->num_records++;
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

extern "C"
void Init_RecordModelMMDBExt()
{
  VALUE cKCDB = rb_define_class("RecordModelMMDB", rb_cObject);
  rb_define_singleton_method(cKCDB, "open", (VALUE (*)(...)) RecordDB__open, 3);
  rb_define_method(cKCDB, "close", (VALUE (*)(...)) RecordDB_close, 0);
  rb_define_method(cKCDB, "put_bulk", (VALUE (*)(...)) RecordDB_put_bulk, 1);
}
