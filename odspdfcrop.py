from collections import defaultdict
from multiprocessing import Process, Queue
import os
import re
import shutil
import subprocess
import time

from pyPdf import PdfFileReader, PdfFileWriter
from pyPdf.generic import NameObject, createStringObject


GS = 'c:\\path\\bin\\gswin64c '
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
    for fname in files:
        filedigit = re.search(pat, fname)
        if filedigit:
            stem = filedigit.group(1)
        else:
            stem = os.path.splitext(fname)[0]
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

def get_bbox(filename):
    cmd = '%s -sDEVICE=bbox -dBATCH -dNOPAUSE  -dQUIET %s' % (GS, filename)
    s = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    s = s[s.rindex('HiResBoundingBox:') + 17:].strip()
    return s.split()

def rename_files(source_dir, extstring):
    pdffiles = [x for x in os.listdir(source_dir) if x.endswith(extstring)]
    for fname in pdffiles:
        src = os.path.join(source_dir, fname)
        tgt = os.path.join(source_dir, fname.replace(extstring, '.pdf'))
        if os.path.isfile(src):
            shutil.copy(src, tgt)
            os.unlink(src)

class PDFFixer(object):
    def __init__(self, source_dir):
        self.source_dir = source_dir
        self.file_info = list()
        self.cropped = list()
        self.pdffiles = [x for x in os.listdir(self.source_dir) if x.endswith('.pdf')]
        print 'Reading %d files' % len(self.pdffiles)

        processes = dict()
        q = Queue()
        for fname in self.pdffiles:
            processes[fname] = Process(target=self.read, args=(q, fname))
            processes[fname].start()

        for _ in processes:
            item = q.get()
            fname, cropped, pages = item['fname'], item['stem'], item['cropped'], item['pages']
            self.file_info.append({'fname': fname, 'pages': pages})
            if cropped:
                self.cropped.append(fname)
        print

    def read(self, q, fname):
        print '.',
        obj = PdfFileReader(open(os.path.join(self.source_dir, fname), 'rb'))
        docinfo = obj.getDocumentInfo()
        cropped = docinfo and docinfo.has_key('/Cropped')
        pages = obj.getNumPages()
        q.put({'name': fname, 'cropped': cropped, 'pages':pages})
        obj.stream.close()

    def split(self):
        stem_info = dict()
        for stem in get_stems(self.pdffiles):
            stem_matches = ['%s.pdf' % stem]
            stem_matches.extend([x for x in self.pdffiles if re.match(r'%s\d+\.pdf' % stem, x)])

            stem_info[stem] = [{'fname': x['fname'], 'pages': x['pages']}
                               for x in self.file_info if x['fname'] in stem_matches]

        for stem in stem_info:
            if sum(x['pages'] for x in stem_info[stem]) == len(stem_info[stem]):
                continue

            for pdfdict in sorted(stem_info, key=get_filedigit):
                fname = pdfdict['fname']
                pages = pdfdict['pages']
                print '%30s (%d pages)' % (fname, pages)

                obj = PdfFileReader(open(os.path.join(self.source_dir, fname), 'rb'))
                c = 0
                for pagenum in range(0, pages):
                    if c == 0:
                        fname = os.path.join(self.source_dir, '%s_SPLIT.pdf' % stem)
                        rname = '%s.pdf' % stem
                    else:
                        fname = os.path.join(self.source_dir, '%s%d_SPLIT.pdf' % (stem, c))
                        rname = '%s%d.pdf' % (stem, c)
                    write_page(fname, obj, pagenum)

                    if self.cropped.count(rname):
                        self.cropped.remove(rname)
                    c += 1
                obj.stream.close()

        rename_files(self.source_dir, '_SPLIT.pdf')


    def crop(self):
        processes = dict()
        filenames = [x for x in os.listdir(self.source_dir)
                     if x not in self.cropped and x.endswith('.pdf')]
        if filenames:
            print 'Cropping %d files' % len(filenames)

        for name in filenames:
            processes[name] = Process(target=self.crop_process, args=(name,))
            processes[name].start()

        for name in processes:
            processes[name].join()
        print
        rename_files(self.source_dir, '_CROP.pdf')

    def crop_process(self, name):
        fullname = os.path.join(self.source_dir, name)
        basename = os.path.basename(fullname)
        newstem = os.path.splitext(basename)[0]
        obj = PdfFileReader(open(fullname, 'rb'))

        lx, ly, ux, uy = get_bbox(fullname)
        print '+',
        page = obj.getPage(0)
        page.mediaBox.lowerLeft = lx, ly
        page.mediaBox.lowerRight = ux, ly
        page.mediaBox.upperLeft = lx, uy
        page.mediaBox.upperRight = ux, uy
        write_page(os.path.join(self.source_dir, '%s_CROP.pdf' % newstem), obj, 0, crop=True)

if __name__ == '__main__':
    t0 = time.clock()
    source_dir = 'c:/pdf'
    f = PDFFixer(source_dir)
    f.split()
    f.crop()
    print 'Finished: ', time.clock() - t0, ' processing seconds'