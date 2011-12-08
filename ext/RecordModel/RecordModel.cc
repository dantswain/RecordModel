#include "../../include/RecordModel.h"
#include <assert.h> // assert
#include <strings.h> // bzero
#include <ctype.h> // isspace etc.
#include <stdlib.h> // atof
#include "ruby.h"

/*
 * C extension
 */

static VALUE cRecordModel;
static VALUE cRecordModelInstance;

static void RecordModel__free(void *ptr)
{
  RecordModel *m = (RecordModel*) ptr;
  if (m)
  {
    delete(m);
  }
}

static VALUE RecordModel__allocate(VALUE klass)
{
  VALUE obj;
  obj = Data_Wrap_Struct(klass, NULL, RecordModel__free, new RecordModel());
  return obj;
}

static RecordModel& RecordModel__get(VALUE self) {
  RecordModel *ptr;
  Data_Get_Struct(self, RecordModel, ptr);
  return *ptr;
}

static VALUE RecordModel_initialize(VALUE self, VALUE keys, VALUE values)
{
  Check_Type(keys, T_ARRAY);
  Check_Type(values, T_ARRAY);

  RecordModel &m = RecordModel__get(self);

  assert(m.items == NULL);
  assert(m.keys == NULL);
  assert(m.values == NULL);

  m.items = new uint32_t[RARRAY_LEN(keys) + RARRAY_LEN(values) + 2];
  m.keys = &m.items[0];
  m.values = &m.items[RARRAY_LEN(keys) + 1];

  uint32_t offset = 0;

  int i;

  for (i = 0; i < RARRAY_LEN(keys); ++i)
  {
    uint32_t desc = NUM2UINT(RARRAY_PTR(keys)[i]);
    assert(RecordModelOffset(desc) == 0 || RecordModelOffset(desc) == offset);

    m.keys[i] = (offset << 16) | RecordModelType(desc);
    offset += RecordModelTypeSize(desc);
  }
  m.keys[i] = 0x00;
  m.keysize = offset;

  for (i = 0; i < RARRAY_LEN(values); ++i)
  {
    uint32_t desc = NUM2UINT(RARRAY_PTR(values)[i]);
    assert(RecordModelOffset(desc) == 0 || RecordModelOffset(desc) == offset);

    m.values[i] = (offset << 16) | RecordModelType(desc);
    offset += RecordModelTypeSize(desc);
  }
  m.values[i] = 0x00;

  m.size = offset;

  return Qnil;
}

static VALUE RecordModel_size(VALUE self)
{
  RecordModel &m = RecordModel__get(self);
  return UINT2NUM(m.size);
}

static void RecordModelInstance__free(void *ptr)
{
  free(ptr);
}

static VALUE RecordModelInstance__allocate(VALUE klass)
{
  VALUE model = rb_cvar_get(klass, rb_intern("@@__model"));
  RecordModel &m = RecordModel__get(model);

  VALUE obj;
  // XXX: gc!
  obj = Data_Wrap_Struct(klass, NULL, RecordModelInstance__free, m.create_instance());
  return obj;
}

static RecordModelInstance& RecordModelInstance__get(VALUE self) {
  RecordModelInstance *ptr;
  Data_Get_Struct(self, RecordModelInstance, ptr);
  return *ptr;
}

static VALUE RecordModelInstance__model(VALUE klass)
{
  return rb_cvar_get(klass, rb_intern("@@__model"));
}

static char to_hex_digit(uint8_t v)
{
  if (/*v >= 0 && */v <= 9) return '0' + v;
  if (v >= 10 && v <= 15) return 'A' + v - 10;
  return '#';
}

static int from_hex_digit(char c)
{
  if (c >= '0' && c <= '9') return c-'0';
  if (c >= 'a' && c <= 'f') return c-'a'+10;
  if (c >= 'A' && c <= 'F') return c-'A'+10;
  return -1;
}

static VALUE RecordModelInstance_get(VALUE self, VALUE _desc)
{
  RecordModelInstance *mi;
  Data_Get_Struct(self, RecordModelInstance, mi);
  const RecordModel &model = *mi->model;

  uint32_t desc = NUM2UINT(_desc);

  assert(RecordModelOffset(desc) + RecordModelTypeSize(desc) <= model.size);

  const char *ptr = model.ptr_to_field(mi, desc);

  if (RecordModelType(desc) == RMT_UINT64)
  {
    return ULONG2NUM( *((uint64_t*)ptr) );
  }
  else if (RecordModelType(desc) == RMT_UINT32)
  {
    return UINT2NUM( *((uint32_t*)ptr) );
  }
  else if (RecordModelType(desc) == RMT_UINT16)
  {
    return UINT2NUM( *((uint16_t*)ptr) );
  }
  else if (RecordModelType(desc) == RMT_UINT8)
  {
    return UINT2NUM( *((uint8_t*)ptr) );
  }
  else if (RecordModelType(desc) == RMT_DOUBLE)
  {
    return rb_float_new( *((double*)ptr) );
  }
  else if (RecordModelTypeNoSize(desc) == RMT_HEXSTR)
  {
    const uint8_t *ptr2 = (const uint8_t*)ptr;

    VALUE strbuf = rb_str_buf_new(2*RecordModelTypeSize(desc));
    char cbuf[3];
    cbuf[2] = 0;
    for (int i = 0; i < RecordModelTypeSize(desc); ++i)
    {
      cbuf[0] = to_hex_digit((ptr2[i]) >> 4);
      cbuf[1] = to_hex_digit((ptr2[i]) & 0x0F);
      rb_str_buf_cat_ascii(strbuf, cbuf);
    }
    return strbuf;
  }
  else
  {
    // XXX: raise
    return Qnil;
  }
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

void parse_hexstring(uint8_t *v, uint32_t desc, int strlen, const char *str)
{
  const int max_sz = 2*RecordModelTypeSize(desc);

  if (strlen > max_sz)
  {
    rb_raise(rb_eArgError, "Invalid string size. Was: %d, Max: %d", strlen, max_sz);
  }

  bzero(v, RecordModelTypeSize(desc));

  const int i_off = max_sz - strlen;

  for (int i = 0; i < strlen; ++i)
  {
    int digit = from_hex_digit(str[i]);
    if (digit < 0)
      rb_raise(rb_eArgError, "Invalid hex digit at %s", &str[i]);

    v[(i+i_off)/2] = (v[(i+i_off)/2] << 4) | (uint8_t)digit;
  }
}

double conv_str_to_double(const char *s, const char *e)
{
  char c = *e;
  *((char*)e) = '\0';
  double v = atof(s);
  *((char*)e) = c;
  return v;
#if 0
  char buf[32];
  int sz = (int)(e-s);
  memcpy(buf, s, sz); 
  buf[sz] = '\0';
  return atof(buf);
#endif
}

uint64_t conv_str_to_uint(const char *s, const char *e)
{
  uint64_t v = 0;
  for (; s != e; ++s)
  {
    char c = *s;
    if (c >= '0' && c <= '9')
    {
      v *= 10;
      v += (c-'0');
    }
    else
    {
      return 0; // invalid
    }
  }
  return v;
}


static VALUE RecordModelInstance_parse_line(VALUE self, VALUE _line, VALUE _desc_arr)
{
  RecordModelInstance *mi;
  Data_Get_Struct(self, RecordModelInstance, mi);
  const RecordModel &model = *mi->model;

  Check_Type(_line, T_STRING);
  Check_Type(_desc_arr, T_ARRAY);

  const char *next = RSTRING_PTR(_line); 
  const char *tok = NULL;

  int i;
  for (i=0; i < RARRAY_LEN(_desc_arr); ++i)
  {
    VALUE e = RARRAY_PTR(_desc_arr)[i];

    uint32_t desc;
    bool has_additional;

    if (TYPE(e) == T_ARRAY)
    {
      has_additional = true;
      desc = NUM2UINT(RARRAY_PTR(e)[0]);
    }
    else
    {
      has_additional = false;
      desc = NUM2UINT(e);
    }

    assert(RecordModelOffset(desc) + RecordModelTypeSize(desc) <= model.size);
    const char *ptr = model.ptr_to_field(mi, desc);

    tok = parse_token(&next);
    if (tok == next)
      return UINT2NUM(i);

    if (RecordModelType(desc) == RMT_UINT64)
    {
      uint64_t v;

      if (has_additional)
      {
        assert(RARRAY_LEN(e) == 2);
        double mul = NUM2DBL(RARRAY_PTR(e)[1]);
        double x = conv_str_to_double(tok, next);
        v = (uint64_t)(x * mul);
      }
      else
      {
        v = conv_str_to_uint(tok, next);
      }

      *((uint64_t*)ptr) = v;
    }
    else if (RecordModelType(desc) == RMT_UINT32)
    {
      uint64_t v = conv_str_to_uint(tok, next);
      if (v > 0xFFFFFFFF) rb_raise(rb_eArgError, "Integer out of uint32 range: %ld", v);
      *((uint32_t*)ptr) = (uint32_t)v;
    }
    else if (RecordModelType(desc) == RMT_UINT16)
    {
      uint64_t v = conv_str_to_uint(tok, next);
      if (v > 0xFFFF) rb_raise(rb_eArgError, "Integer out of uint16 range: %ld", v);
      *((uint16_t*)ptr) = (uint16_t)v;
    }
    else if (RecordModelType(desc) == RMT_UINT8)
    {
      uint64_t v = conv_str_to_uint(tok, next);
      if (v > 0xFF) rb_raise(rb_eArgError, "Integer out of uint8 range: %ld", v);
      *((uint8_t*)ptr) = (uint8_t)v;
    }
    else if (RecordModelTypeNoSize(desc) == RMT_HEXSTR)
    {
      parse_hexstring((uint8_t*)ptr, desc, (int)(next-tok), tok); 
    }
    else if (RecordModelType(desc) == RMT_DOUBLE)
    {
      *((double*)ptr) = conv_str_to_double(tok, next);
    }
    else
    {
      rb_raise(rb_eArgError, "Wrong description");
    }
  }

  tok = parse_token(&next);
  if (tok == next)
    return Qnil;
  else
    return UINT2NUM(i); // means, has additional item (as i >= size)
}

static VALUE RecordModelInstance_set(VALUE self, VALUE _desc, VALUE _val)
{
  RecordModelInstance *mi;
  Data_Get_Struct(self, RecordModelInstance, mi);
  const RecordModel &model = *mi->model;

  uint32_t desc = NUM2UINT(_desc);

  assert(RecordModelOffset(desc) + RecordModelTypeSize(desc) <= model.size);

  const char *ptr = model.ptr_to_field(mi, desc);

  if (RecordModelType(desc) == RMT_UINT64)
  {
    *((uint64_t*)ptr) = (uint64_t)NUM2ULONG(_val);
  }
  else if (RecordModelType(desc) == RMT_UINT32)
  {
    uint64_t v = NUM2UINT(_val);
    if (v > 0xFFFFFFFF) rb_raise(rb_eArgError, "Integer out of uint32 range: %ld", v);
    *((uint32_t*)ptr) = (uint32_t)v;
  }
  else if (RecordModelType(desc) == RMT_UINT16)
  {
    uint64_t v = NUM2UINT(_val);
    if (v > 0xFFFF) rb_raise(rb_eArgError, "Integer out of uint16 range: %ld", v);
    *((uint16_t*)ptr) = (uint16_t)v;
  }
  else if (RecordModelType(desc) == RMT_UINT8)
  {
    uint64_t v = NUM2UINT(_val);
    if (v > 0xFF) rb_raise(rb_eArgError, "Integer out of uint8 range: %ld", v);
    *((uint8_t*)ptr) = (uint8_t)v;
  }
  else if (RecordModelTypeNoSize(desc) == RMT_HEXSTR)
  {
    Check_Type(_val, T_STRING);
    parse_hexstring((uint8_t*)ptr, desc, RSTRING_LEN(_val), RSTRING_PTR(_val));
  }
  else if (RecordModelType(desc) == RMT_DOUBLE)
  {
    *((double*)ptr) = (double)NUM2DBL(_val);
  }
  else
  {
    rb_raise(rb_eArgError, "Wrong description");
  }
  return Qnil;
}

static VALUE RecordModelInstance_zero(VALUE self)
{
  RecordModelInstance *mi;
  Data_Get_Struct(self, RecordModelInstance, mi);
  mi->model->zero_instance(mi);
  return self;
}

static VALUE RecordModelInstance_dup(VALUE self)
{
  VALUE obj = RecordModelInstance__allocate(rb_obj_class(self));

  RecordModelInstance *oldi;
  RecordModelInstance *newi;

  Data_Get_Struct(self, RecordModelInstance, oldi);
  Data_Get_Struct(obj, RecordModelInstance, newi);

  oldi->model->copy_instance(newi, oldi);

  return obj;
}

static VALUE RecordModelInstance_sum_values(VALUE self, VALUE other)
{
  RecordModelInstance *a;
  RecordModelInstance *b;
  Data_Get_Struct(self, RecordModelInstance, a);
  Data_Get_Struct(other, RecordModelInstance, b);

  a->model->sum_instance(a, b);
  return self;
}

static VALUE RecordModel_to_class(VALUE self)
{
  VALUE klass = rb_class_new(cRecordModelInstance);
  rb_cvar_set(klass, rb_intern("@@__model"), self);
  rb_define_alloc_func(klass, RecordModelInstance__allocate);
  rb_define_singleton_method(klass, "model", (VALUE (*)(...)) RecordModelInstance__model, 0);
  rb_define_method(klass, "[]", (VALUE (*)(...)) RecordModelInstance_get, 1);
  rb_define_method(klass, "[]=", (VALUE (*)(...)) RecordModelInstance_set, 2);
  rb_define_method(klass, "zero!", (VALUE (*)(...)) RecordModelInstance_zero, 0);
  rb_define_method(klass, "dup", (VALUE (*)(...)) RecordModelInstance_dup, 0);
  rb_define_method(klass, "sum_values!", (VALUE (*)(...)) RecordModelInstance_sum_values, 1);

  rb_define_method(klass, "parse_line", (VALUE (*)(...)) RecordModelInstance_parse_line, 2);

  return klass;
}

extern "C"
void Init_RecordModelExt()
{
  cRecordModel = rb_define_class("RecordModel", rb_cObject);
  rb_define_alloc_func(cRecordModel, RecordModel__allocate);
  rb_define_method(cRecordModel, "initialize", (VALUE (*)(...)) RecordModel_initialize, 2);
  rb_define_method(cRecordModel, "to_class", (VALUE (*)(...)) RecordModel_to_class, 0);
  rb_define_method(cRecordModel, "size", (VALUE (*)(...)) RecordModel_size, 0);

  cRecordModelInstance = rb_define_class("RecordModelInstance", rb_cObject);
}
