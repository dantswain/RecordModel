#ifndef __MMAP_FILE__HEADER__
#define __MMAP_FILE__HEADER__

#include <assert.h>     // assert
#include <sys/types.h>  // open, fstat, ftruncate
#include <sys/stat.h>   // open, fstat
#include <fcntl.h>      // open
#include <unistd.h>     // close, fstat, ftruncate
#include <sys/mman.h>   // mmap, munmap
#include <algorithm>    // std::max

class MmapFile
{
  int _fh;
  size_t _size;
  size_t _capa;
  bool _readonly;
  void *_ptr;

public:

  MmapFile()
  {
    _fh = -1;
    _size = 0;
    _capa = 0;
    _readonly = true;
    _ptr = NULL;
  }

  size_t size() { return _size; }

  bool valid() { return (_fh != -1 && _ptr != NULL); }

  bool open(const char *path, size_t file_length, bool readonly)
  {
    if (valid()) return false;

    int fh;
    if (readonly)
    {
      fh = open(path, O_RDONLY);
    }
    else
    {
      fh = open(path, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
    }
    if (fh == -1)
      return false;

    struct stat buf;
    int err = fstat(fh, &buf);
    if (err == -1)
    {
      ::close(fh);
      return false;
    }

    if (file_length > buf.st_size)
    {
      ::close(fh);
      return false;
    }

    void *ptr = mmap(NULL, file_length, PROT_READ | (readonly ? 0 : PROT_WRITE), MAP_SHARED, fh, 0);
    if (ptr == MAP_FAILED)
    {
      ::close(fh);
      return false;
    }

    _fh = fh;
    _size = file_length;
    _capa = file_length;
    _readonly = readonly;
    _ptr = ptr;

    return true;
  }

  void close()
  {
    if (_ptr)
    {
      munmap(_ptr, _capa);
      _ptr = NULL;
    }
    if (_fh != -1)
    {
      ::close(_fh);
      _fh = -1;
    }
  }

  // Extends the file and the mmaped region 
  // if false is returned, it can be that you cannot
  // access the data anymore (mremap failed)!
  bool expand(size_t new_capa)
  {
    assert(_ptr != NULL && _fh != -1 && !_readonly);
    if (new_capa < 4096) new_capa = 4096;
    if (new_capa < _capa) return false;

    munmap(_ptr, _capa);
    _ptr = NULL;

    int err = ftruncate(_fh, new_capa); 
    if (err != 0)
      return false;

    void *ptr = mmap(NULL, new_capa, PROT_READ | PROT_WRITE, MAP_SHARED, _fh, 0);
    if (ptr == MAP_FAILED)
    {
      return false;
    }

    _capa = new_capa;
    _ptr = ptr;
  }

  /*
   * Potentially expands the file and both changes _capa and _size
   */ 
  void *ptr_write_at(size_t offset, size_t length)
  {
    assert(!_readonly);
    assert(_ptr);
    if (offset + length > _capa)
    {
      size_t new_capa = _capa;
      while (new_capa < offset + length) new_capa *= 2;
      if (!expand(new_capa))
        return NULL;
    }
    assert(offset + length <= _capa);

    _size = std::max(_size, offset + length);
    assert(_size <= _capa);

    return (void*)(((char*)_ptr) + offset);
  }

  void *ptr_append(size_t length)
  {
    return ptr_write_at(_size, length);
  }

  void *ptr_read_at(size_t offset, size_t length)
  {
    assert(_ptr);
    if (offset + length > _size)
      return NULL;

    return (void*)(((char*)_ptr) + offset);
  }

  /*
   * Very expensive operation!
   */
  bool sync()
  {
    // will call msync, fsync and ftruncate.
  }

};

#endif
