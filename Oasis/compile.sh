#!/bin/bash

# sudo ./a.out 2 /data/nvme_raid/yangty/ADS/soc-LJ.origin.32p.grid_2Mblock/ 16 32

g++ new_ads_construction_v0.cpp -lpthread -laio -o a_v0.out
g++ new_ads_construction_v1.cpp -lpthread -laio -o a_v1.out
g++ new_ads_construction_v2.cpp -lpthread -laio -o a_v2.out
g++ new_ads_construction_v3.cpp -lpthread -laio -o a_v3.out
g++ new_ads_construction_v4.cpp -lpthread -laio -o a_v4.out


