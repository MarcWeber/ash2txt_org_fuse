set -x
for dir in  \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/2 \
full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/2 \
full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/2 \
full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/2 \
full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/2 \
full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/2 \
full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/2 \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/2 \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/3 \
full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/3 \
full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/3 \
full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/3 \
full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/3 \
full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/3 \
full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/3 \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/3 \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/4 \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/4 \
full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/4 \
full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/4 \
full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/4 \
full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/4 \
full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/4 \
full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/4 \
full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231107190228.zarr/5 \
full-scrolls/Scroll4/PHerc1667.volpkg/volumes_zarr/20231117161658.zarr/5 \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1B.zarr/5 \
full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr/20241024131838.zarr/5 \
full-scrolls/Scroll1/PHercParis4.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll1A.zarr/5 \
full-scrolls/Scroll3/PHerc332.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll3.zarr/5 \
full-scrolls/Scroll5/PHerc172.volpkg/volumes_zarr_standardized/53keV_7.91um_Scroll5.zarr/5 \
full-scrolls/Scroll2/PHercParis3.volpkg/volumes_zarr_standardized/54keV_7.91um_Scroll2A.zarr/5 \
;
do
  python example-main.py ~/vesuvis/cache-ash/ 'https://dl.ash2txt.org' prefetch $dir
  echo $dir done
done
