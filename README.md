odspdfcrop
==========

**Synopsis**: 

Given a directory of numbered PDF files,
optionally split them into one-page PDFs while retaining the
numeric order, and crop white space on all sides of each PDF.
Changes the files in-place.
          
Can also be used generically, to (optionally burst and) crop any directory of pdf files.

Comments in code.

**Requirements**: Python 2.7, pyPdf package, and access to Ghostscript executable (for finding the bounding box)

**Assumption**: The original PDF files contain information in an ordered stream,
                and named according to the following pattern::

             somefile.pdf somefile1.pdf somefile2.pdf somefile3.pdf and so on

**Arguments**:

          --dir Specify the directory containing the PDF files.
                The default is a directory `pdf` directly under the
                current working directory.

          --nosplit Omit the splitting and re-numbering process.
                    Use this if you want only to crop the PDF files.

          --ghostscript Specify the full path to the Ghostscript executable
