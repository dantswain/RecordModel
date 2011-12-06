#include <kcdb.h>
#include <kchashdb.h>

#include "../../include/RecordModel.h"
#include <assert.h> // assert
#include <string> // std::string

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
  VALUE modelklass;

  RecordDB()
  {
    db = NULL;
    comparator = NULL;
    modelklass = Qnil;
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

static VALUE RecordDB__open(VALUE klass, VALUE path, VALUE modelklass, VALUE writeable, VALUE hash)
{
  using namespace kyotocabinet;
  Check_Type(path, T_STRING);
  Check_Type(hash, T_HASH);

  VALUE model = rb_cvar_get(modelklass, rb_intern("@@__model"));
  RecordModel *m;
  Data_Get_Struct(model, RecordModel, m);
 
  // GC model from ModelDB
  RecordDB *mdb = new RecordDB;
  mdb->db = new TreeDB;
  mdb->modelklass = modelklass;
  mdb->comparator = new ModelComparator(m);
  mdb->db->tune_comparator(mdb->comparator);

  int options = 0;

  if (RTEST(rb_hash_aref(hash, ID2SYM(rb_intern("compress")))))
  {
    options |= TreeDB::TCOMPRESS;
  }

  if (RTEST(rb_hash_aref(hash, ID2SYM(rb_intern("linear")))))
  {
    options |= TreeDB::TLINEAR;
  }

  if (RTEST(rb_hash_aref(hash, ID2SYM(rb_intern("small")))))
  {
    options |= TreeDB::TSMALL;
  }

  if (options != 0)
  {
    mdb->db->tune_options(options);
  }

  VALUE tune_buckets = rb_hash_aref(hash, ID2SYM(rb_intern("tune_buckets")));
  if (!NIL_P(tune_buckets))
  {
    mdb->db->tune_buckets(NUM2ULONG(tune_buckets));
  }

  VALUE tune_map = rb_hash_aref(hash, ID2SYM(rb_intern("tune_map")));
  if (!NIL_P(tune_map))
  {
    mdb->db->tune_map(NUM2ULONG(tune_map));
  }

  VALUE tune_page_cache = rb_hash_aref(hash, ID2SYM(rb_intern("tune_page_cache")));
  if (!NIL_P(tune_page_cache))
  {
    mdb->db->tune_page_cache(NUM2ULONG(tune_page_cache));
  }

  int openflags = TreeDB::OREADER;

  if (RTEST(writeable))
  {
    openflags |= TreeDB::OWRITER;
    openflags |= TreeDB::OCREATE;
  }

  if (!mdb->db->open(RSTRING_PTR(path), openflags))
  {
    delete mdb;
    return Qnil;
  }

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

/*
 * Overwrite value if key exists
 */
static VALUE RecordDB_set(VALUE self, VALUE _mi)
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
 * Only add if key does not exist.
 */
static VALUE RecordDB_add(VALUE self, VALUE _mi)
{
  RecordDB &mdb = RecordDB__get(self);
  RecordModelInstance *mi;
  Data_Get_Struct(_mi, RecordModelInstance, mi);
  RecordModel *m = mi->model;

  bool res = mdb.db->add((const char*)mi + sizeof(RecordModelInstance), m->keysize,
                         (const char*)mi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);

  if (res)
    return Qtrue;
  else
    return Qfalse;
}

static VALUE RecordDB_accum_sum(VALUE self, VALUE _mi)
{
  RecordDB &mdb = RecordDB__get(self);
  RecordModelInstance *mi;
  Data_Get_Struct(_mi, RecordModelInstance, mi);
  RecordModel *m = mi->model;

  RecordModelInstance *newmi = m->dup_instance(mi);
  assert(newmi);

  int32_t res = mdb.db->get((const char*)newmi + sizeof(RecordModelInstance), m->keysize,
                         (char*)newmi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);

  bool ret = false;

  if (res == -1 || res != (m->size - m->keysize))
  {
    free(newmi);

    // key does not exist. simply put record.
    ret = mdb.db->add((const char*)mi + sizeof(RecordModelInstance), m->keysize,
                      (const char*)mi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);

  }
  else
  {
    // key exists. accum record.
    m->sum_instance(newmi, mi);
    ret = mdb.db->set((const char*)newmi + sizeof(RecordModelInstance), m->keysize,
                      (const char*)newmi + sizeof(RecordModelInstance) + m->keysize, m->size - m->keysize);
  }

  if (ret)
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

static VALUE RecordDB_query(VALUE self, VALUE _from, VALUE _to, VALUE _current)
{
  using namespace kyotocabinet;
  RecordDB &mdb = RecordDB__get(self);

  RecordModelInstance *from;
  Data_Get_Struct(_from, RecordModelInstance, from);

  RecordModelInstance *to;
  Data_Get_Struct(_to, RecordModelInstance, to);

  RecordModelInstance *current;
  Data_Get_Struct(_current, RecordModelInstance, current);

  assert(from->model == to->model);
  assert(from->model == current->model);

  RecordModel &model = *(from->model);

  model.copy_instance(current, from);

  TreeDB::Cursor *it = mdb.db->cursor();
  assert(it);

  bool valid = false;

  // position
  valid = it->jump((const char*)current + sizeof(RecordModelInstance), model.keysize);

  std::string key_val;

  while (valid)
  {
    if (!it->get_key(&key_val, false))
    {
      delete it; 
      return Qfalse;
    }
    // copy key into current
    assert(key_val.size() == model.keysize);
    memcpy(((char*)current) + sizeof(RecordModelInstance), key_val.data(), model.keysize);

    assert(model.compare_keys(current, from) >= 0);

    if (model.compare_keys(current, to) > 0)
    {
      break;
    }

    if (model.keys_in_range(current, from, to))
    {
      // copy data into current
      if(!it->get_value(&key_val, false))
      {
        delete it;
        return Qfalse;
      }
      assert(key_val.size() == model.size - model.keysize);
      memcpy(((char*)current) + sizeof(RecordModelInstance) + model.keysize, key_val.data(), key_val.size());
      // yield value
      rb_yield(_current); // XXX 
    }
    else
    {
      // TODO: seek to next record in range
    }

    valid = it->step(); 
  }

  delete it;
  return Qnil;
}

extern "C"
void Init_RecordModelKCDBExt()
{
  VALUE cKCDB = rb_define_class("RecordModelKCDB", rb_cObject);
  rb_define_singleton_method(cKCDB, "open", (VALUE (*)(...)) RecordDB__open, 4);
  rb_define_method(cKCDB, "close", (VALUE (*)(...)) RecordDB_close, 0);
  rb_define_method(cKCDB, "set", (VALUE (*)(...)) RecordDB_set, 1);
  rb_define_method(cKCDB, "accum_sum", (VALUE (*)(...)) RecordDB_accum_sum, 1);
  rb_define_method(cKCDB, "add", (VALUE (*)(...)) RecordDB_add, 1);
  rb_define_method(cKCDB, "get", (VALUE (*)(...)) RecordDB_get, 1);
  rb_define_method(cKCDB, "get", (VALUE (*)(...)) RecordDB_get, 1);
  rb_define_method(cKCDB, "query", (VALUE (*)(...)) RecordDB_query, 3);
}
