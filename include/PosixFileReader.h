#ifndef __POSIX_FILE_READER__HEADER__
#define __POSIX_FILE_READER__HEADER__

#include "FileReader.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

class PosixFileReader : public FileReader
{
  int fd;

  public:

    PosixFileReader()
    {
      fd = -1;
    }

    bool open(const char *path)
    {
      assert(fd == -1);
      fd = ::open(path, O_RDONLY); 
      if (fd == -1)
      {
        return false;
      }
      else
      {
        return true;
      }
    }

    virtual void close()
    {
      assert(fd >= 0);
      ::close(fd);
    }

    virtual ssize_t read(void *buf, size_t buflen)
    {
      assert(fd >= 0);
      return ::read(fd, buf, buflen);
    }
};

#endif
