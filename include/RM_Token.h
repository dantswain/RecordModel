#ifndef __RECORD_MODEL_TOKEN__HEADER__
#define __RECORD_MODEL_TOKEN__HEADER__

#include <ctype.h>   // isspace

/*
 * Used to parse line
 */
struct RM_Token
{
  const char *beg;
  const char *end;

  RM_Token() : beg(NULL), end(NULL) {}

  bool empty()
  {
    if (beg == end) return true;
    return false;
  }

  const char *parse_space_sep(const char *ptr)
  {
    // at first skip whitespaces
    while (isspace(*ptr)) ++ptr;

    this->beg = ptr;

    // copy the token into the buffer
    while (*ptr != '\0' && !isspace(*ptr))
    {
      ++ptr;
    }

    this->end = ptr; // endptr

    return ptr;
  }

  const char *parse_sep(const char *ptr, char sep)
  {
    this->beg = ptr;

    // copy the token into the buffer
    while (*ptr != '\0' && *ptr != sep)
    {
      ++ptr;
    }

    this->end = ptr; // endptr

    if (*ptr == sep) ++ptr;

    return ptr;
  }

  /*
   * Treat a whitespace as separator as all isspace characters, not just
   * the whitespace (ASCII 32) itself.
   */
  const char *parse(const char *ptr, char sep)
  {
    if (sep == 32)
      return parse_space_sep(ptr);
    else
      return parse_sep(ptr, sep);
  }
};

#endif
