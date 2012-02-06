#include "../../include/RecordModel.h"
#include "../../include/LineReader.h"
#include <assert.h> // assert
#include <strings.h> // bzero
#include <algorithm> // std::max
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
    // [:id, :type, is_key, offset, length (,optinal_default_value)]

    VALUE e = RARRAY_PTR(fields)[i];
    assert(TYPE(e) == T_ARRAY);
    assert(RARRAY_LEN(e) >= 5 && RARRAY_LEN(e) <= 6);
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
    // [:id, :type, is_key, offset, length (,optinal_default_value)]

    VALUE e = RARRAY_PTR(fields)[i];
    assert(TYPE(e) == T_ARRAY);
    assert(RARRAY_LEN(e) >= 5 && RARRAY_LEN(e) <= 6);

    VALUE e_type = RARRAY_PTR(e)[1];
    VALUE e_is_key = RARRAY_PTR(e)[2];
    VALUE e_offset = RARRAY_PTR(e)[3];
    VALUE e_length = RARRAY_PTR(e)[4];
    VALUE e_default = Qnil;

    uint64_t e_default_num = 0;
    if (RARRAY_LEN(e) == 6)
    {
      e_default = RARRAY_PTR(e)[5];
      e_default_num = NUM2ULONG(e_default);
    }

    assert(SYMBOL_P(e_type));

    unsigned int offset = FIX2UINT(e_offset);
    assert(offset <= 0xFFFF);

    unsigned int length = FIX2UINT(e_length);
    assert(length <= 0xFF);

    RM_Type *t = NULL;

    if (ID2SYM(rb_intern("uint64")) == e_type)
    {
      t = new RM_UINT64(e_default_num);
    }
    else if (ID2SYM(rb_intern("uint32")) == e_type)
    {
      t = new RM_UINT32(e_default_num);
    }
    else if (ID2SYM(rb_intern("uint16")) == e_type)
    {
      t = new RM_UINT16(e_default_num);
    }
    else if (ID2SYM(rb_intern("uint8")) == e_type)
    {
      t = new RM_UINT8(e_default_num);
    }
    else if (ID2SYM(rb_intern("timestamp")) == e_type)
    {
      t = new RM_TIMESTAMP(e_default_num);
    }
    else if (ID2SYM(rb_intern("timestamp_desc")) == e_type)
    {
      t = new RM_TIMESTAMP_DESC(e_default_num);
    }
    else if (ID2SYM(rb_intern("double")) == e_type)
    {
      t = new RM_DOUBLE();
    }
    else if (ID2SYM(rb_intern("hexstr")) == e_type)
    {
      t = new RM_HEXSTR(length);
    }
    else if (ID2SYM(rb_intern("string")) == e_type)
    {
      t = new RM_STR(length);
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
void conv_field_arr(VALUE arr, int *field_arr, int field_arr_sz)
{
  assert(RARRAY_LEN(arr) == field_arr_sz);

  for (int i=0; i < field_arr_sz; ++i)
  {
    VALUE e = RARRAY_PTR(arr)[i];

    if (NIL_P(e))
      field_arr[i] = -1;
    else
      field_arr[i] = (int)FIX2UINT(e);
  }
}

static
int parse_line(RecordModelInstance *self, const char *str, VALUE _field_arr, char sep, int &err)
{
  int sarr[20];
  int arr_sz;
  int *arr = sarr;

  arr_sz = RARRAY_LEN(_field_arr);
  if (arr_sz > 20)
  {
    arr = new int[arr_sz];
  }

  conv_field_arr(_field_arr, arr, arr_sz);
  
  int res = self->parse_line(str, arr, arr_sz, sep, err);

  if (arr_sz > 20)
  {
    delete []arr;
  }

  return res;
}

/*
 * Return Qnil on success, or an Integer.
 * If an Integer is returned, it is the number of tokens that could be parsed successfully
 * (or the index into _field_arr which could not parsed due to end of line).
 * See parse_line2. 
 */
static
VALUE RecordModelInstance_parse_line(VALUE _self, VALUE _line, VALUE _field_arr, VALUE _sep)
{
  RecordModelInstance *self = get_RecordModelInstance(_self);

  Check_Type(_line, T_STRING);
  Check_Type(_field_arr, T_ARRAY);
  Check_Type(_sep, T_STRING);
  validate_field_arr(self->model, _field_arr);

  if (RSTRING_LEN(_sep) != 1)
    rb_raise(rb_eArgError, "Single character string expected");

  char sep = RSTRING_PTR(_sep)[0];

  int err = 0;
  int num_tokens = parse_line(self, RSTRING_PTR(_line), _field_arr, sep, err);

  if (err)
  {
    rb_raise(rb_eRuntimeError, "set_from_string failed with %d at token %d\n", err, num_tokens);
  }

  return INT2NUM(num_tokens);
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

/*
 * If the value of field_idx is equal to val, then yield the record to the block, and
 * write back the modified record.
 */
static
VALUE RecordModelInstanceArray_update_each(VALUE _self, VALUE field_idx, VALUE val, VALUE _rec)
{
  RecordModelInstanceArray *self = get_RecordModelInstanceArray(_self);
  RecordModelInstance *rec = get_RecordModelInstance(_rec);

  if (self->model != rec->model)
  {
    rb_raise(rb_eArgError, "Model mismatch");
  }

  RM_Type *field = self->model->get_field(FIX2UINT(field_idx));

  if (field == NULL)
  {
    rb_raise(rb_eArgError, "Wrong index");
  }

  for (size_t i = 0; i < self->entries(); ++i)
  {
    if (field->equal_ruby(self->ptr_at(i), val))
    {
      self->copy_out(rec, i);
      rb_yield(_rec);
      self->copy_in(rec, i);
    }
  }

  return Qnil;
}

struct Params
{
  RecordModelInstanceArray *self;
  RecordModelInstance *rec;
  int *field_arr;
  int field_arr_sz;
  VALUE _rec;
  size_t lines_read; 

  LineReader *linereader;
  char *buf;
  size_t bufsz;
  int fd;

  int parse_error;
  int num_tokens;

  bool reject_token_parse_error;
  bool reject_invalid_num_tokens;
  int min_num_tokens;
  int max_num_tokens;

  char sep;
};

static
void *bulk_parse_line_yield(void *ptr)
{
  Params *p = (Params*)ptr;

  if (!RTEST(rb_yield_values(3, INT2NUM(p->num_tokens), INT2NUM(p->parse_error), p->_rec)))
    return NULL;
  return ptr;
}

extern "C" void *
rb_thread_call_with_gvl(void *(*func)(void *), void *data1);

static
VALUE bulk_parse_line(void *ptr)
{
  Params *p = (Params*)ptr;
  char *line = NULL;

  while (true)
  {
    if (p->self->full())
    {
      return Qtrue;
    }
    line = p->linereader->readline();
    if (!line)
    {
      return Qfalse;
    }
    ++p->lines_read;

    p->rec->zero();

    p->num_tokens = p->rec->parse_line(line, p->field_arr, p->field_arr_sz, p->sep, p->parse_error);
    if (p->parse_error)
    {
      // We either reject item for which a parse error occured, or we have 
      // to call the block. If the block returns false (or nil), we also
      // reject it.
      if (p->reject_token_parse_error ||
          rb_thread_call_with_gvl(bulk_parse_line_yield, p) == NULL)
      {
        // skip item
        continue;
      }
    }
    else
    {
      // no parse error occured, but we still might have to little or
      // to many tokens.
      if (p->num_tokens < p->min_num_tokens || (p->max_num_tokens > 0 && p->num_tokens > p->max_num_tokens))
      {
        // we either want to generally reject items with wrong number of tokens,
        // otherwise we call the block to determine what to do.
        if (p->reject_invalid_num_tokens || rb_thread_call_with_gvl(bulk_parse_line_yield, p) == NULL)
        {
          continue;
        }
      }
    }

    bool ok = p->self->push(p->rec);
    assert(ok);
  }

  assert(false);
}

static
VALUE RecordModelInstanceArray_bulk_parse_line(VALUE _self, VALUE _rec, VALUE io_int, VALUE _field_arr, VALUE _sep, VALUE _bufsz,
  VALUE _reject_token_parse_error, VALUE _reject_invalid_num_tokens, VALUE _min_num_tokens, VALUE _max_num_tokens)
{
  Params p;

  p._rec = _rec;
  p.self = get_RecordModelInstanceArray(_self);
  p.rec = get_RecordModelInstance(p._rec);
  Check_Type(_field_arr, T_ARRAY);
  Check_Type(_sep, T_STRING);
  validate_field_arr(p.self->model, _field_arr);

  if (RSTRING_LEN(_sep) != 1)
    rb_raise(rb_eArgError, "Single character string expected");

  p.sep = RSTRING_PTR(_sep)[0];

  p.field_arr_sz = RARRAY_LEN(_field_arr); 
  p.field_arr = new int[p.field_arr_sz];

  conv_field_arr(_field_arr, p.field_arr, p.field_arr_sz);

  p.lines_read = 0;
  p.reject_token_parse_error = RTEST(_reject_token_parse_error);
  p.reject_invalid_num_tokens = RTEST(_reject_invalid_num_tokens);
  p.min_num_tokens = NUM2INT(_min_num_tokens);
  p.max_num_tokens = NUM2INT(_max_num_tokens);

  size_t bufsz = NUM2INT(_bufsz);
  char *buf = (char*)malloc(bufsz);
  if (!buf)
  {
    delete [] p.field_arr;
    rb_raise(rb_eRuntimeError, "Not enough memory");
  }

  LineReader lr(NUM2INT(io_int), buf, bufsz);
  p.linereader = &lr;

  VALUE res = rb_thread_blocking_region(bulk_parse_line, &p, NULL, NULL);

  delete [] p.field_arr;
  free(buf);

  return rb_ary_new3(2, res, ULONG2NUM(p.lines_read));
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
    self->copy_out(rec, i);
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
  rb_define_method(cRecordModelInstance, "parse_line", (VALUE (*)(...)) RecordModelInstance_parse_line, 3);
  rb_define_method(cRecordModelInstance, "to_s", (VALUE (*)(...)) RecordModelInstance_to_s, 0);

  cRecordModelInstanceArray = rb_define_class("RecordModelInstanceArray", rb_cObject);
  rb_define_alloc_func(cRecordModelInstanceArray, RecordModelInstanceArray__allocate);
  rb_define_method(cRecordModelInstanceArray, "initialize", (VALUE (*)(...)) RecordModelInstanceArray_initialize, 3);
  rb_define_method(cRecordModelInstanceArray, "empty?", (VALUE (*)(...)) RecordModelInstanceArray_is_empty, 0);
  rb_define_method(cRecordModelInstanceArray, "full?", (VALUE (*)(...)) RecordModelInstanceArray_is_full, 0);
  rb_define_method(cRecordModelInstanceArray, "bulk_set", (VALUE (*)(...)) RecordModelInstanceArray_bulk_set, 2);
  rb_define_method(cRecordModelInstanceArray, "bulk_parse_line", (VALUE (*)(...)) RecordModelInstanceArray_bulk_parse_line, 9);
  rb_define_method(cRecordModelInstanceArray, "<<", (VALUE (*)(...)) RecordModelInstanceArray_push, 1);
  rb_define_method(cRecordModelInstanceArray, "reset", (VALUE (*)(...)) RecordModelInstanceArray_reset, 0);
  rb_define_method(cRecordModelInstanceArray, "size", (VALUE (*)(...)) RecordModelInstanceArray_size, 0);
  rb_define_method(cRecordModelInstanceArray, "capacity", (VALUE (*)(...)) RecordModelInstanceArray_capacity, 0);
  rb_define_method(cRecordModelInstanceArray, "expandable?", (VALUE (*)(...)) RecordModelInstanceArray_expandable, 0);
  rb_define_method(cRecordModelInstanceArray, "_each", (VALUE (*)(...)) RecordModelInstanceArray_each, 1);
  rb_define_method(cRecordModelInstanceArray, "_update_each", (VALUE (*)(...)) RecordModelInstanceArray_update_each, 3);
  rb_define_method(cRecordModelInstanceArray, "sort", (VALUE (*)(...)) RecordModelInstanceArray_sort, 0);
}
