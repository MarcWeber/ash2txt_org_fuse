ash2txt.org file walker and fuse (Python)
=========================================
I wanted a nice way to look at all files and lazily mount them while building
up a local copy slowly I can access. That's what the tool is about.

Python vs Go
============
I started with Python.

Can you use both cause the .json file format is the same.
If you run tools multiple times they might overwrite each other's json file
and download files multiple times.

I started GO in a desparate attempt to make it faster. But turned out that
fuse3 for Python is also fine.

License: GPLv2 GPLv3 MIT (choose one or get in touch if you have other needs)

HOW TO RUN PYTHON VERSION
=========================
python example-main.py ~/cache-directory/ 'https://dl.ash2txt.org' prefetch <SUB-PATH>
-> all options open example-main.py

HOW TO RUN GO VERSION
=====================
./go_fs_project -fuse-version fuse3  ~/cache-directory  'https://dl.ash2txt.org' mount  <mount-path>
Indeed I admit grok wrote most of the go code by >>repeating<< please fix.
Please look at Python code and do it the way. Integrate that. It still dosen't
work. Repeat. ... but .. finally it worked.


FEATURES
========
- prefetch files

- mount ash2txt_org with local cache
  Python supports libfuse3 whose readdir allows passing all folder entries
  and their attrs at the same time which then can be cached by the kernel
  (fues3.py). Without this trying to open a file out of > 20.000 .tif files
  eg in Gimp is unbearable

- cache meta data lazily in .directory_contents_cached_v2.json
  This allows much flexibility such as moving directories later
  and keeping metadata where it belongs. So you can mount one scroll,
  later move files and mount more keeping the cache.
  Removing a folder will also remove its cache (requires restart of the
  mounting)

- once the caching is done du -hs like tools work as expected,
  but the Python du_approximate is many times faster because it uses 
  the approximate data from directory listings from the web

PROBLEMS
========
Some archives zarr / .tiff have very many files. So > 20.000 files in one
directory or the many subdirectories in zarr make initial access slow.

See IMPROVEMENTS/TODO


INSTALL
=======
conda env create -f environment.yml
conda activate khartes
python khartes.py 

RUNNING
========
export ASH2TXT_CACHE=~/.ash2txt-cache-direcory

python example-main.py $ASH2TXT_CACHE 'https://dl.ash2txt.org' <COMMAND_AND_ARGS> >| /tmp/sizes-full-scrolls

COMMAND_AND_ARGS see example-main.py

FILES / HACKING
===============
example-main.py
filesystems/types.py  Some basic types
filesystems/ash2txtorg_cached.py  Together with example-main.py implementsn the walking of the HTTP filessytem with caching on disk on disk
filesystems/fuse.py # works
filesystems/fuse_passthrough.py # wanted to test fh passthrough - no idea how to do it with fuse
filesystems/fuse3.py # unfinished requires fake inodes

file sizes
==========
The HTTP reply doesn't return exact file sizes. That's why there are two
methods folder.file_size_bytes_exact and folder.file_size_bytes_approximate
The first uses HEAD request to get exact reply which is much slower obviously.

walking
=======
So while walking the diretory tree using the operating system works
its very slow for folders which have 20K files or more 
which is why it might be bes to look at filesystems/walking.py and implement what you need yourself

mounting
========
mkdir ~/ash2txt-mountpoint
python example-main.py $ASH2TXT_CACHE 'https://dl.ash2txt.org' fuse-mount "" ~/ash2txt-mountpoint

"" could be "full-scrolls" or another subpath.

speed
=====
It works. Maybe Python is the wrong tool - but for now it works

Speeding up initial access (getting some .directory_contents_cached_v2.json fast)

Checkout this directory: https://github.com/MarcWeber/ash2txt_org_fuse-json-files
You can ues GIT_DIR to move .git out of direcotry later.

A small optimization is done when using the fast size estimation.
If a .zarray is found, then the size is estimated based on the zarray size.
See filesystems/zarray_estimation.py
But this strategy doesn't work for many other folders.


Example: find all scrolls and levels and sizes:
===============================================
python example-main.py $ASH2TXT_CACHE 'https://dl.ash2txt.org' list_special_and_approximate_size_fast "full-scrolls"
Because this fetches all directory contenst its slow.

It approximates the sizes by using size from HTTP responses cause fetching
sizes of each file would take much longer.

You'll be able to grep lines like these fast - so having 0 be the lowest resolution (most data) first wasn't the smartest choice I think

0/ 279217.95 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/0
1/ 80479.19 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/1
2/ 10417.90 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/2
3/ 1253.02 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/3
4/ 143.99 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/4
5/ 17.27 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/5

0/ 180029.73 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/0
1/ 49941.05 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/1
2/ 6762.14 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/2
3/ 847.11 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/3
4/ 101.65 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/4
5/ 12.67 MiB full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/5

0/ 452945.09 MiB full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/0
1/ 126944.28 MiB full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/1
2/ 18460.09 MiB full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/2
3/ 2487.09 MiB full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/3
4/ 317.52 MiB full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/4
5/ 40.61 MiB full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/5

0/ 34504.92 MiB full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/0
1/ 9187.34 MiB full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/1
2/ 1218.05 MiB full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/2
3/ 149.03 MiB full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/3
4/ 18.45 MiB full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/4
5/ 2.34 MiB full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/5

0/ 3338496.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/0
1/ 425984.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/1
2/ 53248.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/2
3/ 6656.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/3
4/ 832.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/4
5/ 112.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/5

0/ 256608.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/0
1/ 34496.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/1
2/ 4312.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/2
3/ 704.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/3
4/ 96.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/4
5/ 12.00 MiB full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/5

0/ 2364797.88 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/0
1/ 322704.00 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/1
2/ 42336.00 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/2
3/ 5292.00 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/3
4/ 880.00 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/4
5/ 144.00 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/5

0/ 458406.52 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/0
1/ 117189.70 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/1
2/ 14864.74 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/2
3/ 1686.85 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/3
4/ 191.32 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/4
5/ 23.17 MiB full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/5

So build a script like this to get levels 2 to 5

for dir in  \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/2 \
full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/2 \
....
;
do
python example-main.py ASH2TXT_CACHE 'https://dl.ash2txt.org' prefetch $dir
done


IMPROVEMENTS/TODO
==================
Having ash2txt_org output exact bytes per file would help and avoid the header
requests.

Server supports range requests which allows downloading parts of files

curl --silent --range 10-19 https://dl.ash2txt.org/community-uploads/waldkauz/fasp/v1/autogen8_1217_ensemble.zip | wc -c

Give more guidance about what is worth downloading ?

Maybe generate a fake ~/getting-started folder and put some files to explore there.
Eg only the low resolution versions of zarr and some .tiff files per scroll


ALTERNATIVES
============
httpdirfs --no-range-check -o debug -o big_writes -o attr_timeout=3600 -o ac_attr_timeout=3600 -o entry_timeout=3600  -f --cache 'https://dl.ash2txt.org/' /mnt3
but you're missing out on features such as prefetch or estimating size without
header requests using approximate data without having to run header requests
for each file. Also folders having 20K files (the tiff storages for example)
load much slower beacuse for each file a disk access hase to be done. So
caching does help.

CHANGELOG:
==========
2025-12-16:
    saving disks pace renaming size_approximate to "a" and size to "s" in json files:
    find -name '.directory_contents_cached_v2.json'  -type f -print0 | xargs -0 sed -i -e 's@"size_approximate"@"a"@g' -e 's@"size"@"s"@g'
