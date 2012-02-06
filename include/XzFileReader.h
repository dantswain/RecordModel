#ifndef __XZ_FILE_READER__HEADER__
#define __XZ_FILE_READER__HEADER__

#include "FileReader.h"
#include "PosixFileReader.h"
#include <lzma.h>
#include <assert.h>
#include <stdlib.h> // malloc

class XzFileReader : public FileReader
{
  PosixFileReader pf;
  FileReader *file;
  lzma_stream stream;
  void *inbuf;
  size_t bufsize;
  bool is_eof;

  public:

    XzFileReader()
    {
      file = NULL;
      inbuf = NULL;
    }

    bool open(const char *path, unsigned bufsize = 1L << 16)
    {
      assert(file == NULL);

      if (!pf.open(path))
      {
        return false;
      }
      file = &pf; 

      this->inbuf = malloc(bufsize);
      if (!inbuf)
      {
        file->close(); file = NULL;
        return false;
      }
      this->bufsize = bufsize;


      lzma_stream s = LZMA_STREAM_INIT;
      stream = s; // XXX
      const uint32_t flags = LZMA_TELL_UNSUPPORTED_CHECK | LZMA_CONCATENATED;
      const uint64_t memory_limit = UINT64_MAX; /* no memory limit */
      lzma_ret ret = lzma_stream_decoder(&stream, memory_limit, flags);
      if (ret != LZMA_OK)
      {
        file->close(); file = NULL;
	free(inbuf);
        return false;
      }

      is_eof = false;

      stream.avail_in = 0;
      stream.avail_out = 0;

      return true;
    }

    // NOTE: the file itself is not closed!
    virtual void close()
    {
      assert(file);
      file->close(); file = NULL;
      free(inbuf);
      lzma_end(&stream);
    }

    virtual ssize_t read(void *buf, size_t buflen)
    {
      assert(file);

      do 
      {
	if (!is_eof && stream.avail_in == 0)
	{
	  // fill inbuf
	  ssize_t n = file->read(this->inbuf, this->bufsize); 
	  if (n < 0)
	  {
	    // error
	    return -1;
	  }
	  if (n == 0)
	  {
	    is_eof = true;
	  }

	  stream.next_in = (const uint8_t*)this->inbuf;
	  stream.avail_in = n; 
	}

        stream.next_out = (uint8_t*)buf;
	stream.avail_out = buflen;

	lzma_ret ret = lzma_code(&stream, is_eof ? LZMA_FINISH : LZMA_RUN);
	if (ret != LZMA_OK && ret != LZMA_STREAM_END)
	{
	  return -1; // error
        }

        ssize_t len = buflen - stream.avail_out;
        if (len > 0 || is_eof)
	  return len;

        assert(len == 0);

	// XXX: Still something in input buffer, but no output -> error (?)
        if (stream.avail_in > 0)
	  return -1;

        // nothing in output buffer. put something back again into
	// the input buffer -> fall through to while-loop

      } while (stream.avail_in == 0);

      assert(false);
    }
};

#endif
