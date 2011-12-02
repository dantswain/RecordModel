#include <tcutil.h>
#include <tcfdb.h>
#include <tcbdb.h>

#include "../../include/RecordModel.h"
//#include <assert.h> // assert
#include <string.h> // memcpy

#include "ruby.h"

static int compare(const char* akbuf, int aksiz, const char* bkbuf, int bksiz, void *ptr)
{
  return ((RecordModel*)ptr)->compare_keys(akbuf, aksiz, bkbuf, bksiz);
}

struct RecordDB
{
  TCBDB *db;

  RecordDB()
  {
    db = NULL;
  }

  ~RecordDB()
  {
    if (db)
    {
      tcbdbclose(db); // XXX
      tcbdbdel(db);
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
  mdb->db = tcbdbnew();
  tcbdbsetcmpfunc(mdb->db, compare, m);
  //tcbdbtune(mdb->db, 256, 512, 2000000, -1, -1, BDBTLARGE | BDBTDEFLATE); // XXX

  if (!tcbdbopen(mdb->db, RSTRING_PTR(path), FDBOWRITER|FDBOCREAT))
  {
    delete mdb;
    return Qnil;
  }

  VALUE obj;
  obj = Data_Wrap_Struct(klass, NULL, RecordDB__free, mdb);

  return obj;
}


// Stores mi into the database
static VALUE RecordDB_put(VALUE self, VALUE _mi)
{
  RecordDB &mdb = RecordDB__get(self);
  RecordModelInstance *mi;
  Data_Get_Struct(_mi, RecordModelInstance, mi);
  RecordModel *m = mi->model;

  bool res = tcbdbput(mdb.db, (const char*)mi + sizeof(RecordModelInstance), m->keysize,
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

  int sz = 0;
  const void *res = tcbdbget3(mdb.db, (const char*)mi + sizeof(RecordModelInstance), m->keysize, &sz);

  if (res == NULL || sz != (m->size - m->keysize))
    return Qnil;

  memcpy((char*)mi + sizeof(RecordModelInstance) + m->keysize, res, sz);
  return _mi;
}

extern "C"
void Init_RecordModelTCDBExt()
{
  VALUE cKCDB = rb_define_class("RecordModelTCDB", rb_cObject);
  rb_define_singleton_method(cKCDB, "open", (VALUE (*)(...)) RecordDB__open, 2);
  rb_define_method(cKCDB, "put", (VALUE (*)(...)) RecordDB_put, 1);
  rb_define_method(cKCDB, "get", (VALUE (*)(...)) RecordDB_get, 1);
}
