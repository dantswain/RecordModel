#ifndef __FILE_READER__HEADER__
#define __FILE_READER__HEADER__

/*
 * Common interface for file reading
 */
class FileReader
{
  public:
    virtual ssize_t read(void *buf, size_t buflen) = 0;
    virtual void close() = 0;
};

#endif
