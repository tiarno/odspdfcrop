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
    matched = re.search(pat, fdict['name'])
    if matched:
        digit = int(matched.group(2))
    else:
        digit = 0
    return digit

def get_stems(files):
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
    p = PdfFileWriter()
    if crop:
        infoDict = p._info.getObject()
        infoDict.update({NameObject('/Cropped'): createStringObject(u'True')})

    page = obj.getPage(pagenum)
    p.addPage(page)
    p.write(open(filename, 'wb'))

def get_bbox(ghostscript, filename):
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
    pdffiles = [x for x in os.listdir(source_dir) if x.endswith(extstring)]
    for name in pdffiles:
        src = os.path.join(source_dir, name)
        tgt = os.path.join(source_dir, name.replace(extstring, '.pdf'))
        if os.path.isfile(src):
            shutil.copy(src, tgt)
            os.unlink(src)

class PDFFixer(object):
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
        print '.',
        obj = PdfFileReader(open(os.path.join(self.source_dir, name), 'rb'))
        docinfo = obj.getDocumentInfo()
        cropped = docinfo and docinfo.has_key('/Cropped')
        pages = obj.getNumPages()
        q.put({'name': name, 'cropped': cropped, 'pages':pages})
        obj.stream.close()

    def split(self):
        stem_info = dict()
        for stem in get_stems(self.pdffiles):
            stem_matches = ['%s.pdf' % stem]
            stem_matches.extend([x for x in self.pdffiles if re.match(r'%s\d+\.pdf' % stem, x)])

            stem_info[stem] = [{'name': x['name'], 'pages': x['pages']}
                               for x in self.file_info if x['name'] in stem_matches]

        for stem in stem_info:
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
    if sys.platform.startswith('win'):
        multiprocessing.freeze_support()
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', default=os.path.join(os.getcwd(), 'pdf'))
    parser.add_argument('--nosplit', action='store_true')
    parser.add_argument('--ghostscript', default='c:\\bin\\gswin64c ')
    args = parser.parse_args()
    main(args)
