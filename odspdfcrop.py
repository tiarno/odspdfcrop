"""
:platform: Unix, Windows
:synopsis: Given a directory of numbered PDF files,
           optionally split them into one-page PDFs while retaining the
           numeric order, and crop white space on all sides of each PDF.

:requirements: pyPdf package and access to Ghostscript executable (for finding the bounding box)

:assumption: The original PDF files contain information in an ordered stream,
             and named according to the following pattern::

                 somefile.pdf somefile1.pdf somefile2.pdf somefile3.pdf and so on

.. moduleauthor:: Tim Arnold <jtim.arnold@gmail.com>
"""

import argparse
import multiprocessing
import multiprocessing.forking
import os
import re
import shutil
import subprocess
import sys
import time

from pyPdf import PdfFileReader, PdfFileWriter
from pyPdf.generic import NameObject, createStringObject

pat = re.compile(r'([\w_\d]+?)(\d+)\.pdf')

def get_filedigit(fdict):
    ''' Returns the number at the end of a file-stem.

        Used as sort-key function to sort a list of dictionaries

        Args:
          fdict: a dictionary of file information

        Returns:
          number at the end of the file-stem

        Example:
          fdict = {'name': 'one12.pdf', }
          digit = 12
    '''
    matched = re.search(pat, fdict['name'])
    if matched:
        digit = int(matched.group(2))
    else:
        digit = 0
    return digit

def get_stems(files):
    ''' Returns a list of file-stems from a list of file names.

        Used to organize files by stem name.

        Args:
          list of complete file names

        Returns:
          list of file stems computed from complete names

        Example::

          files = ['one.pdf', 'one1.pdf, 'one2.pdf',
                   'two.pdf', 'two1.pdf']
          stems = ['one', 'two']
    '''
    stems = set()
    for name in files:
        filedigit = re.search(pat, name)
        if filedigit:
            stem = filedigit.group(1)
        else:
            stem = os.path.splitext(name)[0]
        stems.add(stem)
    return stems

def write_page(filename, obj, pagenum, crop=False):
    '''Write a PDF object to disk.

        Args:
          :filename: the file to create
          :obj: the PDF object in memory
          :pagenum: the page in the PDF to write
          :crop: flag indicating whether the function is
                 called from the cropping process

        Used for splitting pdfs by page and writing cropped objects
        to disk. If called from the cropping process, add metadata to
        the PDF so we don't try to split or crop it in some subsequent run.

        Returns: None

    '''
    p = PdfFileWriter()
    if crop:
        infoDict = p._info.getObject()
        infoDict.update({NameObject('/Cropped'): createStringObject(u'True')})

    page = obj.getPage(pagenum)
    p.addPage(page)
    p.write(open(filename, 'wb'))

def get_bbox(ghostscript, filename):
    '''Get the Bounding Box of a page from a PDF file, using Ghostscript

        Args:
          :ghostscript: the full path to the Ghostscript executable
          :filename: the name of the PDF file to query for the bounding box.

        Used to crop a PDF object

        Returns:
          :bounds: a 4-element list of floats representing the bounding box.
    '''
    cmd = '%s -sDEVICE=bbox -dBATCH -dNOPAUSE  -dQUIET %s' % (ghostscript, filename)
    s = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    s = s[s.rindex('HiResBoundingBox:') + 17:].strip()
    str_bounds = s.split()

    if len(str_bounds) > 4:
        print '\nERROR for %s:  %s' % (filename, ' '.join(str_bounds[4:]))
        str_bounds = str_bounds[:4]
    try:
        bounds = map(float, str_bounds)
    except ValueError as e:
        print '%s\nSkipping %s: Bad bounding box' % (e, filename)
        bounds = list()

    return bounds

def rename_files(source_dir, extstring):
    '''Rename files in a directory by removing a string from the name.

        Writing files to disk is faster than passing around open PDF object
        streams. The files are written with a _CROP or _SPLIT flag in the name.
        This function removes the flag, resulting in overwriting the original PDF file.

        Args:
          :source_dir: the directory of files
          :extstring: the string to remove from each name

        Returns: None
    '''
    pdffiles = [x for x in os.listdir(source_dir) if x.endswith(extstring)]
    for name in pdffiles:
        src = os.path.join(source_dir, name)
        tgt = os.path.join(source_dir, name.replace(extstring, '.pdf'))
        if os.path.isfile(src):
            shutil.copy(src, tgt)
            os.unlink(src)

class PDFFixer(object):
    '''Class to (optionally split, re-number, and) crop a directory of PDF files.

        There are three stages to the process:

          1. Initialize. Read the files in the directory using multiple processes.
             Record the PDF's filename, the number of pages it contains, and a
             boolean indicating if it has been cropped. Close the files.

          2. Split and renumber the PDFs. In a set of multiple processes,
             split any PDFs that contain multiple pages into separate files.
             Then re-number the PDF files such that the information contained
             in the original stream is kept it the original order.

          3. Crop the whitespace from the margins of each PDF.
        '''
    def __init__(self, args):
        self.source_dir = args.dir
        self.ghostscript = args.ghostscript

        self.file_info = list()
        self.cropped = list()
        self.pdffiles = [x for x in os.listdir(self.source_dir) if x.endswith('.pdf')]
        print 'Reading %d files' % len(self.pdffiles)

        processes = dict()
        q = multiprocessing.Queue()
        for name in self.pdffiles:
            processes[name] = multiprocessing.Process(target=self.read, args=(q, name))
            processes[name].start()

        for _ in processes:
            item = q.get()
            name, cropped, pages = item['name'], item['cropped'], item['pages']
            self.file_info.append({'name': name, 'pages': pages})
            if cropped:
                self.cropped.append(name)
        print

    def read(self, q, name):
        '''Read a PDF file, find the number of pages it contains and whether
           it contains metadata indicating it has been cropped in a previous
           run. Save the information and place it in a queue that is used after
           all processes have completed.
        '''
        print '.',
        obj = PdfFileReader(open(os.path.join(self.source_dir, name), 'rb'))
        docinfo = obj.getDocumentInfo()
        cropped = docinfo and docinfo.has_key('/Cropped')
        pages = obj.getNumPages()
        q.put({'name': name, 'cropped': cropped, 'pages':pages})
        obj.stream.close()

    def split(self):
        '''Create a data structure, `stem_info`, which contains an ordered list
           of the files that match to each file-stem.

           Process each list of files by file-stem. If no pdf files in the list
           have multiple pages, this method does nothing.

           If multiple pages do exist for at least one file in the list, split the
           files from that point on so that each pdf file contains one page.
           Then write the pages to files, renumbering so the information stream
           keeps its original order.

           Rename the files so the original files are overwritten after all processes
           are complete.

        '''
        stem_info = dict()
        for stem in get_stems(self.pdffiles):
            'Create the data structure to match a list of files according to its stem'
            stem_matches = ['%s.pdf' % stem]
            stem_matches.extend([x for x in self.pdffiles if re.match(r'%s\d+\.pdf' % stem, x)])

            stem_info[stem] = [{'name': x['name'], 'pages': x['pages']}
                               for x in self.file_info if x['name'] in stem_matches]

        for stem in stem_info:
            'if no file in the list contains multiple pages, do nothing'
            if sum(x['pages'] for x in stem_info[stem]) == len(stem_info[stem]):
                continue

            start_splitting = False
            filedigit = 0
            files_info = sorted(stem_info[stem], key=get_filedigit)

            for pdfdict in files_info:
                name = pdfdict['name']
                pages = pdfdict['pages']

                if not start_splitting and pages > 1:
                    start_splitting = True
                if not start_splitting:
                    print 'skipping %s' % name
                    filedigit += 1
                    continue

                print '%30s (%d pages)' % (name, pages)
                '''Write a new one-page file for each page in the stream
                   naming the files consecutively.
                '''
                obj = PdfFileReader(open(os.path.join(self.source_dir, name), 'rb'))
                for pagenum in range(0, pages):
                    if filedigit == 0:
                        fname = os.path.join(self.source_dir, '%s_SPLIT.pdf' % stem)
                        rname = '%s.pdf' % stem
                    else:
                        fname = os.path.join(self.source_dir, '%s%d_SPLIT.pdf' % (stem, filedigit))
                        rname = '%s%d.pdf' % (stem, filedigit)
                    write_page(fname, obj, pagenum)

                    if self.cropped.count(rname):
                        self.cropped.remove(rname)
                    filedigit += 1

                obj.stream.close()

        rename_files(self.source_dir, '_SPLIT.pdf')


    def crop(self):
        '''For each file in the directory, start a subprocess (within multiprocess)
           to crop the file. Rename the files to overwrite the original when all
           processes are complete.
        '''
        processes = dict()
        filenames = [x for x in os.listdir(self.source_dir)
                     if x not in self.cropped and x.endswith('.pdf')]
        if filenames:
            print 'Cropping %d files' % len(filenames)

        for name in filenames:
            processes[name] = multiprocessing.Process(target=self.crop_process, args=(name,))
            processes[name].start()

        for name in processes:
            processes[name].join()
        print
        rename_files(self.source_dir, '_CROP.pdf')

    def crop_process(self, name):
        '''Get the bounding box for each file and set the new dimensions
           on the page object. Write the page object to disk.
        '''
        fullname = os.path.join(self.source_dir, name)
        obj = PdfFileReader(open(fullname, 'rb'))

        print '+',
        bounds = get_bbox(self.ghostscript, fullname)
        if bounds and int(sum(bounds)):
            lx, ly, ux, uy = bounds

            page = obj.getPage(0)
            page.mediaBox.lowerLeft = lx, ly
            page.mediaBox.lowerRight = ux, ly
            page.mediaBox.upperLeft = lx, uy
            page.mediaBox.upperRight = ux, uy

            new_name = os.path.join(self.source_dir, '%s_CROP.pdf' % os.path.splitext(name)[0])
            write_page(new_name, obj, 0, crop=True)

def main(args):
    t0 = time.clock()
    f = PDFFixer(args)
    if not args.nosplit:
        f.split()

    f.crop()
    print 'Finished: ', time.clock() - t0, ' processing seconds'

if __name__ == '__main__':
    '''Set up command line arguments

        If you use pyinstaller to create an executable, you must include
        `multiprocessing.freeze_support` on the Windows platform.

        Arguments:
          :--dir: Specify the directory containing the PDF files.
                  The default is a directory `pdf` directly under the
                  current working directory.

          :--nosplit: Omit the splitting and re-numbering process.
                      Use this if you want only to crop the PDF files.

          :--ghostscript: Specify the full path to the Ghostscript executable

    '''
    if sys.platform.startswith('win'):
        multiprocessing.freeze_support()
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', default=os.path.join(os.getcwd(), 'pdf'))
    parser.add_argument('--nosplit', action='store_true')
    parser.add_argument('--ghostscript', default='c:\\bin\\gswin64c ')
    args = parser.parse_args()
    main(args)
