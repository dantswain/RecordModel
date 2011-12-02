#include <kcdb.h>
#include <kchashdb.h>

#include "../../include/RecordModel.h"
#include <assert.h> // assert

#include "ruby.h"

struct ModelComparator : public kyotocabinet::Comparator
{
  RecordModel *model;

  ModelComparator(RecordModel *_model) : model(_model) {}
  virtual ~ModelComparator() {}

  virtual int32_t compare(const char* akbuf, size_t aksiz, const char* bkbuf, size_t bksiz)
  {
    return model->compare_keys(akbuf, aksiz, bkbuf, bksiz);
  }
};

struct RecordDB
{
  kyotocabinet::TreeDB *db;
  kyotocabinet::Comparator *comparator;

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
    db->close();
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
  using namespace kyotocabinet;
  Check_Type(path, T_STRING);

  VALUE model = rb_cvar_get(modelklass, rb_intern("@@__model"));
  RecordModel *m;
  Data_Get_Struct(model, RecordModel, m);
 
  // GC model from ModelDB
  RecordDB *mdb = new RecordDB;
  mdb->db = new TreeDB;
  mdb->comparator = new ModelComparator(m);
  mdb->db->tune_comparator(mdb->comparator);

  if (!mdb->db->open(RSTRING_PTR(path), TreeDB::OWRITER | TreeDB::OREADER | TreeDB::OCREATE))
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

// Stores mi into the database
static VALUE RecordDB_put(VALUE self, VALUE _mi)
{
  RecordDB &mdb = RecordDB__get(self);
  RecordModelInstance *mi;
  Data_Get_Struct(_mi, RecordModelInstance, mi);
  RecordModel *m = mi->model;

  bool res = mdb.db->set((const char*)mi + sizeof(RecordModelInstance), m->keysize,
                         (const char*)mi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);

  if (res)
    return Qtrue;
  else
    return Qfalse;
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

  int32_t res = mdb.db->get((const char*)mi + sizeof(RecordModelInstance), m->keysize,
                         (char*)mi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);

  if (res == -1 || res != (m->size - m->keysize))
    return Qnil;
  else
    return _mi;
}

extern "C"
void Init_RecordModelKCDBExt()
{
  VALUE cKCDB = rb_define_class("RecordModelKCDB", rb_cObject);
  rb_define_singleton_method(cKCDB, "open", (VALUE (*)(...)) RecordDB__open, 2);
  rb_define_method(cKCDB, "close", (VALUE (*)(...)) RecordDB_close, 0);
  rb_define_method(cKCDB, "put", (VALUE (*)(...)) RecordDB_put, 1);
  rb_define_method(cKCDB, "get", (VALUE (*)(...)) RecordDB_get, 1);
}
