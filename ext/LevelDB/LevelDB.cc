#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/comparator.h"

#include "../../include/RecordModel.h"
#include <assert.h> // assert
#include <string.h> // memcpy
#include <string>   // std::string

#include "ruby.h"

struct RecordModelComparator : public leveldb::Comparator
{
  RecordModel *model;

  RecordModelComparator(RecordModel *_model) : model(_model) {}

  virtual int Compare(const leveldb::Slice& ka, const leveldb::Slice& kb) const
  {
    return model->compare_keys(ka.data(), ka.size(), kb.data(), kb.size());
  }

  virtual const char* Name() const { return "RecordModelComparator"; }
  virtual void FindShortestSeparator(std::string*, const leveldb::Slice&) const { }
  virtual void FindShortSuccessor(std::string*) const { }
};

struct RecordDB
{
  leveldb::DB *db;
  RecordModelComparator *comparator;

  RecordDB()
  {
    db = NULL;
    comparator = NULL;
  }

  ~RecordDB()
  {
    if (db)
    {
      delete db;
      db = NULL;
    }
    if (comparator)
    {
      delete comparator;
      comparator = NULL;
    }
  }

  void close()
  {
    if (db)
    {
      delete db;
      db = NULL;
    }
  }
};

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

static VALUE RecordDB__open(VALUE klass, VALUE path, VALUE modelklass)
{
  Check_Type(path, T_STRING);

  VALUE model = rb_cvar_get(modelklass, rb_intern("@@__model"));
  RecordModel *m;
  Data_Get_Struct(model, RecordModel, m);
 
  // GC model from ModelDB
  RecordDB *mdb = new RecordDB;
  mdb->comparator = new RecordModelComparator(m);

  leveldb::Options options;
  options.create_if_missing = true;
  options.comparator = mdb->comparator; 
  leveldb::Status status = leveldb::DB::Open(options, RSTRING_PTR(path), &mdb->db); 

  if (!status.ok())
  {
    delete mdb;
    return Qnil;
  }

  VALUE obj;
  obj = Data_Wrap_Struct(klass, NULL, RecordDB__free, mdb);

  return obj;
}

static VALUE RecordDB_close(VALUE self)
{
  RecordDB &mdb = RecordDB__get(self);
  mdb.close();
  return Qnil;
}

static VALUE RecordDB_put(VALUE self, VALUE _mi)
{
  RecordDB &mdb = RecordDB__get(self);
  RecordModelInstance *mi;
  Data_Get_Struct(_mi, RecordModelInstance, mi);
  RecordModel *m = mi->model;

  const leveldb::Slice key((const char*)mi + sizeof(RecordModelInstance), m->keysize);
  const leveldb::Slice value((const char*)mi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);

  leveldb::Status s = mdb.db->Put(leveldb::WriteOptions(), key, value);

  if (s.ok())
    return Qtrue;
  else
    return Qfalse;
}

static VALUE RecordDB_put_or_sum(VALUE self, VALUE _mi)
{
  RecordDB &mdb = RecordDB__get(self);
  RecordModelInstance *mi;
  Data_Get_Struct(_mi, RecordModelInstance, mi);
  RecordModel *m = mi->model;


  const leveldb::Slice key((const char*)mi + sizeof(RecordModelInstance), m->keysize);
  std::string value;

  leveldb::Status s;

  s = mdb.db->Get(leveldb::ReadOptions(), key, &value);  

  if (s.ok())
  {
    // we have an existing key -> accum record 
    assert(value.size() == (m->size - m->keysize));
    RecordModelInstance *newmi = m->create_instance();
    assert(newmi);
    memcpy((char*)newmi + sizeof(RecordModelInstance) + m->keysize, value.data(), value.size() /*m->size - m->keysize*/);
    m->sum_instance(newmi, mi);
    const leveldb::Slice value((const char*)newmi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);
    s = mdb.db->Put(leveldb::WriteOptions(), key, value);
    free(newmi);
  }
  else
  {
    // key does not exist
    const leveldb::Slice value((const char*)mi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);
    s = mdb.db->Put(leveldb::WriteOptions(), key, value);
  }


  if (s.ok())
  {
    return Qtrue;
  }
  else
  {
    return Qfalse;
  }
}

/*
 * Retrieves into _mi
 */
static VALUE RecordDB_get(VALUE self, VALUE _mi)
{
  RecordDB &mdb = RecordDB__get(self);
  RecordModelInstance *mi;
  Data_Get_Struct(_mi, RecordModelInstance, mi);
  RecordModel *m = mi->model;

  const leveldb::Slice key((const char*)mi + sizeof(RecordModelInstance), m->keysize);
  std::string value;

  leveldb::Status s = mdb.db->Get(leveldb::ReadOptions(), key, &value);  

  if (s.ok() && value.size() == (m->size - m->keysize))
  {
    memcpy((char*)mi + sizeof(RecordModelInstance) + m->keysize, value.data(), value.size() /*m->size - m->keysize*/);
    return _mi;
  }
  else
  {
    return Qnil;
  }
}

extern "C"
void Init_RecordModelLevelDBExt()
{
  VALUE cLevelDB = rb_define_class("RecordModelLevelDB", rb_cObject);
  rb_define_singleton_method(cLevelDB, "open", (VALUE (*)(...)) RecordDB__open, 2);
  rb_define_method(cLevelDB, "close", (VALUE (*)(...)) RecordDB_close, 0);
  rb_define_method(cLevelDB, "put", (VALUE (*)(...)) RecordDB_put, 1);
  rb_define_method(cLevelDB, "put_or_sum", (VALUE (*)(...)) RecordDB_put_or_sum, 1);
  rb_define_method(cLevelDB, "get", (VALUE (*)(...)) RecordDB_get, 1);
}
