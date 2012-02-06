#ifndef __GZIP_FILE_READER__HEADER__
#define __GZIP_FILE_READER__HEADER__

#include "FileReader.h"
#include <zlib.h>
#include <assert.h>

class GzipFileReader : public FileReader
{
  gzFile file;

  public:

    GzipFileReader()
    {
      file = NULL;
    }

    bool open(const char *path, unsigned bufsize = 1L << 16)
    {
      assert(file == NULL);
      file = gzopen(path, "r"); 
      if (file)
      {
        gzbuffer(file, bufsize);
        return true;
      }
      else
      {
        return false;
      }
    }

    virtual void close()
    {
      assert(file);
      gzclose(file);
    }

    virtual ssize_t read(void *buf, size_t buflen)
    {
      assert(file);
      return gzread(file, buf, buflen);
    }
};

#endif
