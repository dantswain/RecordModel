#ifndef __LINEREADER__HEADER__
#define __LINEREADER__HEADER__

#include "FileReader.h"

struct LineReader
{
  char *buf;
  size_t bufsz;
  size_t buflen;
  size_t bufoffs;
  FileReader *reader;
  bool fd_is_eof;

  LineReader(FileReader *reader, char *buf, size_t bufsz)
  {
    this->reader = reader;
    this->fd_is_eof = false;
    this->buf = buf;
    this->bufsz = bufsz;
    this->buflen = 0;
    this->bufoffs = 0;
  }

  char *readline()
  {
    if (fd_is_eof && buflen == 0)
      return NULL;
   
    char *beg = &buf[bufoffs];
    for (size_t i = 0; i < buflen; ++i)
    {
      if (beg[i] == '\n')
      {
	// buf = "abc\ndef", buflen=7, bufoffs=0
        beg[i] = '\0';
	bufoffs += i+1;
	buflen -= (i+1);
	// buf[3] = 0, bufoffs = 4, buflen = 3 
	return beg;
      }
    }

    // no NL was found.
    if (fd_is_eof)
    {
      beg[buflen] = 0;
      buflen = 0;
      return beg;
    }

    for (;;)
    {
      // bufoffs = 2, buflen=2
      // xxAA
      // ssssss
      // bufsz = 6
      ssize_t max_read = (bufsz-1) - (buflen + bufoffs);
      if (max_read > 0) 
      {
        // read into buffer
        ssize_t nread = reader->read(&beg[buflen], max_read);
        if (nread < 0) return NULL; //  // error
        if (nread == 0)
        {
          fd_is_eof = true;
          beg[buflen] = '\0';
          buflen = 0;
          return beg;
        }

	// check for NL in newly read bytes  
        for (size_t i=0; i < (size_t)nread; ++i)
	{
	  if (beg[buflen+i] == '\n')
	  {
	    beg[buflen+i] = '\0';
            bufoffs += buflen+i+1;
	    buflen = nread - i - 1;
	    return beg;
	  }
	}
        buflen += nread;
      }
      else
      {
        if (bufoffs == 0)
        {
          // buffer is completely full. moving does not make it any better
          beg[buflen] = '\0';
          buflen = 0;
          return beg;
        }
        else
        {
          // copy buffer to position 0 and then try again
          for (size_t i = 0; i < buflen; ++i)
  	  {
	    buf[i] = beg[i];
	  }
	  bufoffs = 0;
	  beg = buf;
        }
      }
    } /* for */
  }
};

#endif
