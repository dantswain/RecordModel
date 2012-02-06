#ifndef __AUTO_FILE_READER__HEADER__
#define __AUTO_FILE_READER__HEADER__

#include "FileReader.h"
#include "PosixFileReader.h"
#include "GzipFileReader.h"
#include "XzFileReader.h"
#include <assert.h>
#include <string.h> // strlen
#include <strings.h> // strncasecmp

/*
 * Depending on the filename suffix uses a different FileReader.
 */
class AutoFileReader : public FileReader
{
  PosixFileReader p_fr;
  GzipFileReader gz_fr;
  XzFileReader xz_fr;

  FileReader *file;

  public:

    AutoFileReader()
    {
      file = NULL;
    }

    bool open(const char *path, unsigned bufsize = 1L << 16)
    {
      assert(file == NULL);

      int slen = strlen(path);

      if (slen >= 3 && strncasecmp(&path[slen-3], ".xz", 3) == 0)
      {
        if (!xz_fr.open(path, bufsize)) return false; 
	file = &xz_fr;
      }
      else if (slen >= 3 && strncasecmp(&path[slen-3], ".gz", 3) == 0)
      {
        if (!gz_fr.open(path, bufsize)) return false;
	file = &gz_fr;
      }
      else
      {
        if (!p_fr.open(path)) return false;
	file = &p_fr;
      }

      return true;
    }

    virtual void close()
    {
      if (file)
      {
        file->close();
        file = NULL;
      }
    }

    virtual ssize_t read(void *buf, size_t buflen)
    {
      assert(file);
      return file->read(buf, buflen);
    }
};

#endif
