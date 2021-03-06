#ifndef __MMAP_FILE__HEADER__
#define __MMAP_FILE__HEADER__

#include <assert.h>     // assert
#include <sys/types.h>  // open, fstat, ftruncate
#include <sys/stat.h>   // open, fstat
#include <fcntl.h>      // open
#include <unistd.h>     // close, fstat, ftruncate
#include <sys/mman.h>   // mmap, munmap
#include <algorithm>    // std::max
#include <pthread.h>    // pthread_rwlock_t
#include <errno.h>	// errno
#include <string.h>	// strerror

#define LOG_ERR(reason) fprintf(stderr, "%s\n", reason);
#ifndef LOG_ERR
#define LOG_ERR(reason)
#endif

class MmapFile
{
  int _fh;
  size_t _size;
  size_t _capa;
  bool _readonly;
  void *_ptr;
  pthread_rwlock_t *_rwlock;

public:

  MmapFile(pthread_rwlock_t *rwlock)
  {
    _fh = -1;
    _size = 0;
    _capa = 0;
    _readonly = true;
    _ptr = NULL;
    _rwlock = rwlock;
  }

  size_t size() { return _size; }

  bool valid() { return (_fh != -1 && _ptr != NULL); }

  bool open(const char *path, size_t size, size_t capacity, bool readonly)
  {
    int err;

    if (valid())
    {
      LOG_ERR("Opening already open MmapFile");
      return false;
    }

    int fh;
    if (readonly)
    {
      fh = ::open(path, O_RDONLY);
    }
    else
    {
      fh = ::open(path, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
    }
    if (fh == -1)
    {
      LOG_ERR("open failed");
      return false;
    }

    struct stat buf;
    err = fstat(fh, &buf);
    if (err == -1)
    {
      LOG_ERR("fstat failed");
      ::close(fh);
      return false;
    }

    if (buf.st_size < 0 || size > (size_t)buf.st_size)
    {
      LOG_ERR("wrong buf.st_size");
      ::close(fh);
      return false;
    }

    if (capacity < size)
    {
      capacity = size;
    }
    else if (!readonly && capacity < 1L<<20)
    {
        capacity = 1L<<20;
    }

    assert(capacity >= size);

    if (!readonly)
    {
      err = ftruncate(fh, capacity); 
      if (err != 0)
      {
        LOG_ERR("ftruncate failed");
        ::close(fh);
        return false;
      }
   }

    void *ptr = mmap(NULL, capacity, PROT_READ | (readonly ? 0 : PROT_WRITE), MAP_SHARED, fh, 0);
    if (ptr == MAP_FAILED)
    {
      LOG_ERR("mmap failed");
      ::close(fh);
      return false;
    }

    _fh = fh;
    _size = size;
    _capa = capacity;
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
      if (!_readonly)
      {
        if (ftruncate(_fh, _size) != 0)
        {
          LOG_ERR("close: ftruncate failed");
        }
      }
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
    if (new_capa < _capa)
    {
      LOG_ERR("expand: new_capa < _capa");
      return false;
    }

    int err = ftruncate(_fh, new_capa); 
    if (err != 0)
    {
      LOG_ERR("expand: ftruncate failed");
      return false;
    }

    /*
     * Try first to remap without holding the write_lock
     */
#ifndef __APPLE__    
    void *ptr = mremap(_ptr, _capa, new_capa, 0);
#else  /* no mremap on OS X, so force munmap/mmap */
    void *ptr = MAP_FAILED;
#endif    
    if (ptr == MAP_FAILED)
    {
      /*
       * Remapping failed. Try to munmap and mmap again, which
       * gives us a new pointer. That's why we must hold the write_lock.
       */
      int err = pthread_rwlock_wrlock(_rwlock);
      assert(!err);

      munmap(_ptr, _capa);
      _ptr = NULL;
      ptr = mmap(NULL, new_capa, PROT_READ | PROT_WRITE, MAP_SHARED, _fh, 0);
      if (ptr == MAP_FAILED)
      {
        LOG_ERR("expand: mmap failed");
        err = pthread_rwlock_unlock(_rwlock); 
        assert(!err);
        return false;
      }
      _ptr = ptr;

      err = pthread_rwlock_unlock(_rwlock); 
      assert(!err);
    }
    else
    {
      assert(_ptr == ptr);
    }

    _capa = new_capa;
    return true;
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
      {
        LOG_ERR("ptr_write_at failed at expand");
        return NULL;
      }
    }
    assert(offset + length <= _capa);

    _size = std::max(_size, offset + length);
    assert(_size <= _capa);

    return (void*)(((char*)_ptr) + offset);
  }

  inline void *ptr_append(size_t length)
  {
    return ptr_write_at(_size, length);
  }

  template <typename T>
  void append_value(const T& value)
  {
    *((T*)ptr_append(sizeof(T))) = value;
  }

  inline const void *ptr_read_at(size_t offset, size_t length)
  {
    assert(_ptr);
    if (offset + length > _size)
      return NULL;

    return (const void*)(((char*)_ptr) + offset);
  }

  template <typename T>
  T ptr_read_element_at(size_t index)
  {
    return *((const T*)ptr_read_element(index, sizeof(T)));
  }

  const void *ptr_read_element(size_t index, size_t length)
  {
    return ptr_read_at(length*index, length);
  }
 
  /*
   * Potential very expensive operation!
   *
   * Flushes all changes back to disk.
   */
  bool sync()
  {
    int err;
    err = msync(_ptr, _size, MS_SYNC);
    if (err != 0)
    {
      LOG_ERR("sync: msync failed");
      LOG_ERR(strerror(errno));
      return false;
    }

    err = fsync(_fh);
    if (err != 0)
    {
      LOG_ERR("sync: fsync failed");
      LOG_ERR(strerror(errno));
      return false;
    }

    return true;
  }

};

#endif
