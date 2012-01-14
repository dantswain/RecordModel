#include "../../include/RecordModel.h"
#include <assert.h> // assert
#include <strings.h> // bzero
#include <ctype.h> // isspace etc.
#include <algorithm> // std::max
#include <unistd.h> // dup()
#include <stdio.h> // feof, fgets, fdopen, fclose
#include "ruby.h"

/*
 * C extension
 */

static VALUE cRecordModel;
static VALUE cRecordModelInstance;
static VALUE cRecordModelInstanceArray;

/*
 * RecordModel
 */

static
RecordModel *new_RecordModel()
{
  return new RecordModel();
}

static
void free_RecordModel(RecordModel *model)
{
  delete model;
}

static
void mark_RecordModel(RecordModel *model)
{
  if (model)
  {
    rb_gc_mark(model->_rm_obj);
  }
}

static
void RecordModel__free(void *ptr)
{
  free_RecordModel((RecordModel*)ptr);
}

static
void RecordModel__mark(void *ptr)
{
  mark_RecordModel((RecordModel*)ptr);
}

static
bool is_RecordModel(VALUE obj)
{
  return (TYPE(obj) == T_DATA && 
      RDATA(obj)->dfree == (RUBY_DATA_FUNC)(RecordModel__free));
}

static
RecordModel* get_RecordModel_nocheck(VALUE obj)
{
  RecordModel *ptr;
  Data_Get_Struct(obj, RecordModel, ptr);
  assert(ptr);
  return ptr;
}

RecordModel* get_RecordModel(VALUE obj)
{
  if (!is_RecordModel(obj))
  {
    rb_raise(rb_eTypeError, "wrong argument type");
  }
  RecordModel *model = get_RecordModel_nocheck(obj);
  assert(model->_rm_obj == obj);
  return model;
}

static
VALUE RecordModel__allocate(VALUE klass)
{
  RecordModel *model;
  VALUE obj;

  model = new_RecordModel(); 
  obj = Data_Wrap_Struct(klass, RecordModel__mark, RecordModel__free, model);
  model->_rm_obj = obj;
  return obj;
}

static
VALUE RecordModel_initialize(VALUE self, VALUE fields)
{
  Check_Type(fields, T_ARRAY);

  RecordModel *model = get_RecordModel(self);

  if (!model->is_virgin())
    rb_raise(rb_eArgError, "RecordModel#initialize called more than once!");

  size_t num_fields = RARRAY_LEN(fields);
  size_t num_keys = 0;
  size_t num_values = 0;

  for (size_t i = 0; i < num_fields; ++i)
  {
    // Each entry has the following form:
    // [:id, :type, is_key, offset, length] 

    VALUE e = RARRAY_PTR(fields)[i];
    assert(TYPE(e) == T_ARRAY);
    assert(RARRAY_LEN(e) == 5);
    assert(SYMBOL_P(RARRAY_PTR(e)[0]));
    assert(SYMBOL_P(RARRAY_PTR(e)[1]));
    assert(FIX2UINT(RARRAY_PTR(e)[3]) <= 0xFFFF);
    assert(FIX2UINT(RARRAY_PTR(e)[4]) <= 0xFF);

    if (RTEST(RARRAY_PTR(e)[2]))
      ++num_keys;
    else
      ++num_values;
  }

  assert(num_keys + num_values == num_fields);

  model->_all_fields = (RM_Type**) malloc(sizeof(RM_Type*) * (num_fields + 1));
  assert(model->_all_fields);

  model->_keys = (RM_Type**) malloc(sizeof(RM_Type*) * (num_keys + 1));
  assert(model->_keys);

  model->_values = (RM_Type**) malloc(sizeof(RM_Type*) * (num_values + 1));
  assert(model->_values);

  // Initialize zero
  for (size_t i = 0; i <= num_fields; ++i) model->_all_fields[i] = NULL;
  for (size_t i = 0; i <= num_keys; ++i) model->_keys[i] = NULL;
  for (size_t i = 0; i <= num_values; ++i) model->_values[i] = NULL;

  size_t max_sz = 0;
  size_t size_keys = 0;
  size_t size_values = 0;

  size_t key_i = 0;
  size_t val_i = 0;

  for (size_t i = 0; i < num_fields; ++i)
  {
    // Each entry has the following form:
    // [:id, :type, is_key, offset, length] 

    VALUE e = RARRAY_PTR(fields)[i];
    assert(TYPE(e) == T_ARRAY);
    assert(RARRAY_LEN(e) == 5);

    VALUE e_type = RARRAY_PTR(e)[1];
    VALUE e_is_key = RARRAY_PTR(e)[2];
    VALUE e_offset = RARRAY_PTR(e)[3];
    VALUE e_length = RARRAY_PTR(e)[4];

    assert(SYMBOL_P(e_type));

    unsigned int offset = FIX2UINT(e_offset);
    assert(offset <= 0xFFFF);

    unsigned int length = FIX2UINT(e_length);
    assert(length <= 0xFF);

    RM_Type *t = NULL;

    if (ID2SYM(rb_intern("uint64")) == e_type)
    {
      t = new RM_UINT64();
    }
    else if (ID2SYM(rb_intern("uint32")) == e_type)
    {
      t = new RM_UINT32();
    }
    else if (ID2SYM(rb_intern("uint16")) == e_type)
    {
      t = new RM_UINT16();
    }
    else if (ID2SYM(rb_intern("uint8")) == e_type)
    {
      t = new RM_UINT8();
    }
    else if (ID2SYM(rb_intern("timestamp")) == e_type)
    {
      t = new RM_TIMESTAMP();
    }
    else if (ID2SYM(rb_intern("timestamp_desc")) == e_type)
    {
      t = new RM_TIMESTAMP_DESC();
    }
    else if (ID2SYM(rb_intern("double")) == e_type)
    {
      t = new RM_DOUBLE();
    }
    else if (ID2SYM(rb_intern("hexstr")) == e_type)
    {
      t = new RM_HEXSTR(length);
    }
    else
    {
      assert(false);
    }
    t->_offset = offset;

    model->_all_fields[i] = t;

    assert(length == t->size());

    if (RTEST(e_is_key))
    {
      model->_keys[key_i++] = t;
      size_keys += t->size();
    }
    else
    {
      model->_values[val_i++] = t;
      size_values += t->size();
    }

    max_sz = std::max(max_sz, (size_t)(t->offset() + t->size()));
  }

  assert(key_i == num_keys && val_i == num_values);

  assert(max_sz >= size_keys + size_values);

  model->_num_fields = num_fields;
  model->_num_keys = num_keys;
  model->_num_values = num_values;
  model->_size = max_sz;
  model->_size_keys = size_keys;
  model->_size_values = size_values;

  return Qnil;
}

static
VALUE RecordModel_size(VALUE self)
{
  return UINT2NUM(get_RecordModel(self)->size());
}

/*
 * RecordModelInstance
 */

static
void RecordModelInstance__free(void *ptr)
{
  RecordModelInstance::deallocate((RecordModelInstance*)ptr);
}

static
void RecordModelInstance__mark(void *ptr)
{
  RecordModelInstance* rec = (RecordModelInstance*)ptr;
  if (rec)
  {
    mark_RecordModel(rec->model);
  }
}

static
bool is_RecordModelInstance(VALUE obj)
{
  return (TYPE(obj) == T_DATA && 
      RDATA(obj)->dfree == (RUBY_DATA_FUNC)(RecordModelInstance__free));
}

static
RecordModelInstance* get_RecordModelInstance_nocheck(VALUE obj)
{
  RecordModelInstance *ptr;
  Data_Get_Struct(obj, RecordModelInstance, ptr);
  assert(ptr);
  return ptr;
}

RecordModelInstance* get_RecordModelInstance(VALUE obj)
{
  if (!is_RecordModelInstance(obj))
  {
    rb_raise(rb_eTypeError, "wrong argument type");
  }
  return get_RecordModelInstance_nocheck(obj);
}

static
VALUE RecordModelInstance__model(VALUE klass)
{
  return rb_cvar_get(klass, rb_intern("@@__model"));
}

static
VALUE RecordModelInstance__allocate2(VALUE klass, bool zero)
{
  VALUE obj;
  RecordModel *model = get_RecordModel(RecordModelInstance__model(klass));
  RecordModelInstance *rec = RecordModelInstance::allocate(model); 
  assert(rec);

  if (zero)
    rec->zero();

  obj = Data_Wrap_Struct(klass, RecordModelInstance__mark, RecordModelInstance__free, rec);
  return obj;
}

static
VALUE RecordModelInstance__allocate(VALUE klass)
{
  return RecordModelInstance__allocate2(klass, true);
}

/*
 * The <=> operator
 */
static
VALUE RecordModelInstance_cmp(VALUE _a, VALUE _b)
{
  RecordModelInstance *a = get_RecordModelInstance(_a);
  RecordModelInstance *b = get_RecordModelInstance(_b);

  return INT2FIX(a->compare_keys(b));
}

static
VALUE RecordModelInstance_to_s(VALUE _self)
{
  RecordModelInstance *self = get_RecordModelInstance(_self);
  return rb_str_new((const char*)self->ptr(), self->size());
}

static
VALUE RecordModelInstance_get(VALUE _self, VALUE field_idx)
{
  const RecordModelInstance *self = (const RecordModelInstance *)get_RecordModelInstance(_self);
  RM_Type *field = self->model->get_field(FIX2UINT(field_idx));

  if (field == NULL)
  {
    rb_raise(rb_eArgError, "Wrong index");
  }

  return field->to_ruby(self->ptr());
}

static
VALUE RecordModelInstance_set(VALUE _self, VALUE field_idx, VALUE val)
{
  RecordModelInstance *self = get_RecordModelInstance(_self);
  RM_Type *field = self->model->get_field(FIX2UINT(field_idx));

  if (field == NULL)
  {
    rb_raise(rb_eArgError, "Wrong index");
  }

  if (is_RecordModelInstance(val))
  {
    // val is another RecordModelInstance
    const RecordModelInstance *other = (const RecordModelInstance *)get_RecordModelInstance(val);
    if (self->model != other->model)
      rb_raise(rb_eArgError, "RecordModelInstance types MUST match");
    field->copy(self->ptr(), other->ptr());
  }
  else
  {
    field->set_from_ruby(self->ptr(), val);
  }

  return Qnil;
}

static
VALUE RecordModelInstance_set_min(VALUE _self, VALUE field_idx)
{
  RecordModelInstance *self = get_RecordModelInstance(_self);
  RM_Type *field = self->model->get_field(FIX2UINT(field_idx));

  if (field == NULL)
  {
    rb_raise(rb_eArgError, "Wrong index");
  }

  field->set_min(self->ptr());

  return Qnil;
}

static
VALUE RecordModelInstance_set_max(VALUE _self, VALUE field_idx)
{
  RecordModelInstance *self = get_RecordModelInstance(_self);
  RM_Type *field = self->model->get_field(FIX2UINT(field_idx));

  if (field == NULL)
  {
    rb_raise(rb_eArgError, "Wrong index");
  }

  field->set_max(self->ptr());

  return Qnil;
}

static
VALUE RecordModelInstance_zero(VALUE _self)
{
  get_RecordModelInstance(_self)->zero();
  return _self;
}

static
VALUE RecordModelInstance_dup(VALUE self)
{
  VALUE obj = RecordModelInstance__allocate2(rb_obj_class(self), false);
  get_RecordModelInstance(obj)->copy((const RecordModelInstance*)get_RecordModelInstance(self));
  return obj;
}

static
VALUE RecordModelInstance_add_values(VALUE self, VALUE other)
{
  get_RecordModelInstance(self)->add_values((const RecordModelInstance*)get_RecordModelInstance(other));
  return self;
}

static
VALUE RecordModelInstance_set_from_string(VALUE _self, VALUE field_idx, VALUE str)
{
  Check_Type(str, T_STRING);
  RecordModelInstance *self = get_RecordModelInstance(_self);
  RM_Type *field = self->model->get_field(FIX2UINT(field_idx));

  if (field == NULL)
  {
    rb_raise(rb_eArgError, "Wrong index");
  }

  int err = field->set_from_string(self->ptr(), RSTRING_PTR(str), RSTRING_END(str));
  if (err)
  {
    rb_raise(rb_eRuntimeError, "set_from_string failed with %d\n", err);
  }
  
  return _self;
}

const char *parse_token(const char **src)
{
  const char *ptr = *src;

  // at first skip whitespaces
  while (isspace(*ptr)) ++ptr;

  const char *beg = ptr;

  // copy the token into the buffer
  while (*ptr != '\0' && !isspace(*ptr))
  {
    ++ptr;
  }

  *src = ptr; // endptr

  return beg;
}

static
void validate_field_arr(RecordModel *model, VALUE _field_arr)
{
  for (int i=0; i < RARRAY_LEN(_field_arr); ++i)
  {
    VALUE e = RARRAY_PTR(_field_arr)[i];

    if (NIL_P(e))
      continue;

    RM_Type *field = model->get_field(FIX2UINT(e));
    if (field == NULL)
    {
      rb_raise(rb_eArgError, "Wrong index");
    }
  }
}

static
int parse_line(RecordModelInstance *self, const char *str, VALUE _field_arr, int &err)
{
  const char *next = str;
  const char *tok = NULL;
  err = RM_ERR_OK;

  for (int i=0; i < RARRAY_LEN(_field_arr); ++i)
  {
    err = RM_ERR_OK;
    VALUE e = RARRAY_PTR(_field_arr)[i];

    tok = parse_token(&next);
    if (tok == next)
      return i; // premature end

    if (NIL_P(e))
      continue;

    RM_Type *field = self->model->get_field(FIX2UINT(e));
    if (field == NULL)
    {
      rb_raise(rb_eArgError, "Wrong index");
    }
    err = field->set_from_string(self->ptr(), tok, next);
    if (err)
    {
      return i;
    }
  }

  tok = parse_token(&next);
  if (tok == next)
    return -2; // means, OK
  else
    return -1; // means, has additional items
}

/*
 * Return Qnil on success, or an Integer.
 * If an Integer is returned, it is the index into _field_arr which could not parsed due to EOL (end of line).
 * Returns -1 if every item could be parsed but there are still some characters left in the string.
 */
static
VALUE RecordModelInstance_parse_line(VALUE _self, VALUE _line, VALUE _field_arr)
{
  RecordModelInstance *self = get_RecordModelInstance(_self);

  Check_Type(_line, T_STRING);
  Check_Type(_field_arr, T_ARRAY);
  validate_field_arr(self->model, _field_arr);

  int err = 0;
  int tokpos = parse_line(self, RSTRING_PTR(_line), _field_arr, err);

  if (err)
  {
    rb_raise(rb_eRuntimeError, "set_from_string failed with %d at token %d\n", err, tokpos);
  }

  if (tokpos == -2) return Qnil;
  return INT2NUM(tokpos);
}



static VALUE RecordModel_to_class(VALUE self)
{
  VALUE klass = rb_class_new(cRecordModelInstance);
  rb_cvar_set(klass, rb_intern("@@__model"), self);
  rb_define_alloc_func(klass, RecordModelInstance__allocate);
  rb_define_singleton_method(klass, "model", (VALUE (*)(...)) RecordModelInstance__model, 0);
  return klass;
}

/*
 * RecordModelInstanceArray
 */

static
void RecordModelInstanceArray__free(void *ptr)
{
  RecordModelInstanceArray *arr = (RecordModelInstanceArray*)ptr;
  if (arr)
  {
    delete arr;
  }
}

static
void RecordModelInstanceArray__mark(void *ptr)
{
  RecordModelInstanceArray *arr = (RecordModelInstanceArray*)ptr;
  if (arr)
  {
    mark_RecordModel(arr->model);
  }
}

static
VALUE RecordModelInstanceArray__allocate(VALUE klass)
{
  VALUE obj;
  obj = Data_Wrap_Struct(klass, RecordModelInstanceArray__mark, RecordModelInstanceArray__free, new RecordModelInstanceArray());
  return obj;
}

static
bool is_RecordModelInstanceArray(VALUE obj)
{
  return (TYPE(obj) == T_DATA && 
      RDATA(obj)->dfree == (RUBY_DATA_FUNC)(RecordModelInstanceArray__free));
}

static
RecordModelInstanceArray* get_RecordModelInstanceArray_nocheck(VALUE obj)
{
  RecordModelInstanceArray *ptr;
  Data_Get_Struct(obj, RecordModelInstanceArray, ptr);
  assert(ptr);
  return ptr;
}

RecordModelInstanceArray* get_RecordModelInstanceArray(VALUE obj)
{
  if (!is_RecordModelInstanceArray(obj))
  {
    rb_raise(rb_eTypeError, "wrong argument type");
  }
  return get_RecordModelInstanceArray_nocheck(obj);
}

static
VALUE RecordModelInstanceArray_initialize(VALUE _self, VALUE modelklass, VALUE _n, VALUE _expandable)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);

  if (!self->is_virgin())
  {
    rb_raise(rb_eArgError, "Already initialized");
  }

  self->model = get_RecordModel(RecordModelInstance__model(modelklass));
  self->expandable = RTEST(_expandable);
  self->_entries = 0;

  if (!self->allocate(NUM2ULONG(_n)))
  {
    rb_raise(rb_eArgError, "Failed to allocate memory");
  }

  assert(self->_capacity > 0);
  assert(self->_ptr);

  return Qnil;
}

static
VALUE RecordModelInstanceArray_is_empty(VALUE _self)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  return self->empty() ? Qtrue : Qfalse;
}

static VALUE RecordModelInstanceArray_is_full(VALUE _self)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);

  if (self->expandable)
    rb_raise(rb_eArgError, "Called #full? for expandable RecordModelInstanceArray"); 

  return self->full() ? Qtrue : Qfalse;
}

static
VALUE RecordModelInstanceArray_bulk_set(VALUE _self, VALUE field_idx, VALUE val)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  RM_Type *field = self->model->get_field(FIX2UINT(field_idx));

  if (field == NULL)
  {
    rb_raise(rb_eArgError, "Wrong index");
  }

  for (size_t i = 0; i < self->entries(); ++i)
  {
    field->set_from_ruby(self->ptr_at(i), val);
  }

  return Qnil;
}

static
VALUE RecordModelInstanceArray_bulk_parse_line(VALUE _self, VALUE _rec, VALUE io_int, VALUE _field_arr, VALUE _bufsz)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  RecordModelInstance *rec = get_RecordModelInstance(_rec);

  Check_Type(_field_arr, T_ARRAY);
  validate_field_arr(self->model, _field_arr);

  size_t bufsz = NUM2INT(_bufsz); 

  FILE *fh = NULL;
  char *buf = NULL;
  const char *errmsg = "unknown error";
  bool res = false;
  int err;
  int tokpos;
  size_t lines_read; 

  fh = fdopen(dup(NUM2INT(io_int)), "r");
  if (!fh)
  {
    errmsg = "failed to open file";
    goto err;
  }

  buf = (char*)malloc(bufsz);
  if (!buf)
  {
    errmsg = "not enough memory";
    goto err;
  }

  for (lines_read=0; true; ++lines_read)
  {
    if (self->full())
    {
      res = true;
      break;
    }
    if (!fgets(buf, bufsz, fh))
    {
      res = false;
      break;
    }

    rec->zero();

    tokpos = parse_line(rec, buf, _field_arr, err);
    if (err || tokpos != -2)
    {
      // an error appeared while parsing. call the block
      if (!RTEST(rb_yield_values(3, INT2NUM(tokpos), INT2NUM(err), _rec)))
      {
        // skip this entry!
        continue;
      }
    }

    if (!self->push(rec))
    {
      errmsg = "Failed to push";
      goto err;
    }
  }

  fclose(fh);
  free(buf);

  return rb_ary_new3(2, res ? Qtrue : Qfalse, ULONG2NUM(lines_read));

err:
  if (fh) fclose(fh);
  if (buf) free(buf);
  rb_raise(rb_eRuntimeError, "%s", errmsg);
}

static
VALUE RecordModelInstanceArray_push(VALUE _self, VALUE _rec)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  const RecordModelInstance *rec = (const RecordModelInstance*)get_RecordModelInstance(_rec);

  if (self->model != rec->model)
  {
    rb_raise(rb_eArgError, "Model mismatch");
  }

  if (!self->push(rec))
  {
    rb_raise(rb_eArgError, "Failed to push");
  }
 
  return _self;
}

static
VALUE RecordModelInstanceArray_reset(VALUE _self)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);

  self->reset();
 
  return _self;
}

static
VALUE RecordModelInstanceArray_size(VALUE _self)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  return ULONG2NUM(self->entries());
}

static
VALUE RecordModelInstanceArray_capacity(VALUE _self)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  return ULONG2NUM(self->capacity());
}

static
VALUE RecordModelInstanceArray_expandable(VALUE _self)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  return self->expandable ? Qtrue : Qfalse;
}

static
VALUE RecordModelInstanceArray_each(VALUE _self, VALUE _rec)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  RecordModelInstance *rec = get_RecordModelInstance(_rec);

  if (self->model != rec->model)
  {
    rb_raise(rb_eArgError, "Model mismatch");
  }

  for (size_t i = 0; i < self->entries(); ++i)
  {
    self->copy(rec, i);
    rb_yield(_rec);
  }

  return Qnil;
}

static
VALUE RecordModelInstanceArray_sort(VALUE _self)
{
  get_RecordModelInstanceArray(_self)->sort();
  return _self;
}


extern "C"
void Init_RecordModelExt()
{
  cRecordModel = rb_define_class("RecordModel", rb_cObject);
  rb_define_alloc_func(cRecordModel, RecordModel__allocate);
  rb_define_method(cRecordModel, "initialize", (VALUE (*)(...)) RecordModel_initialize, 1);
  rb_define_method(cRecordModel, "to_class", (VALUE (*)(...)) RecordModel_to_class, 0);
  rb_define_method(cRecordModel, "size", (VALUE (*)(...)) RecordModel_size, 0);

  cRecordModelInstance = rb_define_class("RecordModelInstance", rb_cObject);
  rb_define_method(cRecordModelInstance, "[]", (VALUE (*)(...)) RecordModelInstance_get, 1);
  rb_define_method(cRecordModelInstance, "[]=", (VALUE (*)(...)) RecordModelInstance_set, 2);
  rb_define_method(cRecordModelInstance, "set_min", (VALUE (*)(...)) RecordModelInstance_set_min, 1);
  rb_define_method(cRecordModelInstance, "set_max", (VALUE (*)(...)) RecordModelInstance_set_max, 1);
  rb_define_method(cRecordModelInstance, "set_from_string", (VALUE (*)(...)) RecordModelInstance_set_from_string, 2);
  rb_define_method(cRecordModelInstance, "zero!", (VALUE (*)(...)) RecordModelInstance_zero, 0);
  rb_define_method(cRecordModelInstance, "dup", (VALUE (*)(...)) RecordModelInstance_dup, 0);
  rb_define_method(cRecordModelInstance, "add_values!", (VALUE (*)(...)) RecordModelInstance_add_values, 1);
  rb_define_method(cRecordModelInstance, "<=>", (VALUE (*)(...)) RecordModelInstance_cmp, 1);
  rb_define_method(cRecordModelInstance, "parse_line", (VALUE (*)(...)) RecordModelInstance_parse_line, 2);
  rb_define_method(cRecordModelInstance, "to_s", (VALUE (*)(...)) RecordModelInstance_to_s, 0);

  cRecordModelInstanceArray = rb_define_class("RecordModelInstanceArray", rb_cObject);
  rb_define_alloc_func(cRecordModelInstanceArray, RecordModelInstanceArray__allocate);
  rb_define_method(cRecordModelInstanceArray, "initialize", (VALUE (*)(...)) RecordModelInstanceArray_initialize, 3);
  rb_define_method(cRecordModelInstanceArray, "empty?", (VALUE (*)(...)) RecordModelInstanceArray_is_empty, 0);
  rb_define_method(cRecordModelInstanceArray, "full?", (VALUE (*)(...)) RecordModelInstanceArray_is_full, 0);
  rb_define_method(cRecordModelInstanceArray, "bulk_set", (VALUE (*)(...)) RecordModelInstanceArray_bulk_set, 2);
  rb_define_method(cRecordModelInstanceArray, "bulk_parse_line", (VALUE (*)(...)) RecordModelInstanceArray_bulk_parse_line, 4);
  rb_define_method(cRecordModelInstanceArray, "<<", (VALUE (*)(...)) RecordModelInstanceArray_push, 1);
  rb_define_method(cRecordModelInstanceArray, "reset", (VALUE (*)(...)) RecordModelInstanceArray_reset, 0);
  rb_define_method(cRecordModelInstanceArray, "size", (VALUE (*)(...)) RecordModelInstanceArray_size, 0);
  rb_define_method(cRecordModelInstanceArray, "capacity", (VALUE (*)(...)) RecordModelInstanceArray_capacity, 0);
  rb_define_method(cRecordModelInstanceArray, "expandable?", (VALUE (*)(...)) RecordModelInstanceArray_expandable, 0);
  rb_define_method(cRecordModelInstanceArray, "_each", (VALUE (*)(...)) RecordModelInstanceArray_each, 1);
  rb_define_method(cRecordModelInstanceArray, "sort", (VALUE (*)(...)) RecordModelInstanceArray_sort, 0);
}
