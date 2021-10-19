ODBC Driver package forked from alexbrainman/odbc. Thank you to Alex Brainman
for his numerous contributions to the go community.

This package was manually forked from the source as Alex doesn't have much time
these days to maintain the package or pull in community contributions. This repo
was spun off so that community contributions could be pulled in.


## Original README.md:
odbc driver written in go. Implements database driver interface as used by standard database/sql package. It calls into odbc dll on Windows, and uses cgo (unixODBC) everywhere else.

To get started using odbc, have a look at the [wiki](https://github.com/alexbrainman/odbc/wiki) pages.
