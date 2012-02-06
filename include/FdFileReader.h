#ifndef __FD_FILE_READER__HEADER__
#define __FD_FILE_READER__HEADER__

#include "FileReader.h"
#include <unistd.h>
#include <assert.h>

/*
 * Like PosixFileReader, but just wrap around a file descriptor
 * and does not close it upon close(). Useful when a fd is passed into from e.g. * Ruby.
 */
class FdFileReader : public FileReader
{
  int fd;

  public:

    FdFileReader()
    {
      this->fd = -1;
    }

    bool open(int fd)
    {
      assert(this->fd == -1);
      if (fd < 0)
      {
        return false;
      }
      else
      {
        this->fd = fd;
	return true;
      }
    }

    virtual void close()
    {
      this->fd = -1;
    }

    virtual ssize_t read(void *buf, size_t buflen)
    {
      assert(fd >= 0);
      return ::read(fd, buf, buflen);
    }
};

#endif
