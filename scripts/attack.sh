#!/bin/bash
hping3 -i u100000 -S -c 3000 -p 80 192.168.248.2 &&
hping3 -i u100000 -S -c 3000 -p 80 -2 192.168.248.2 &&
hping3 -i u10000 -S -c 30000 -p 80 -1 192.168.248.2 &&
hping3 -i u10000 -S -c 30000 -p 80 192.168.248.2 &&
hping3 -i u100000 -S -c 3000 -p 80 -2 192.168.248.2 &&
hping3 -i u100000 -S -c 3000 -p 80 -1 192.168.248.2 &&
hping3 -i u10000 -S -c 30000 -p 80 192.168.248.2 &&
hping3 -i u5000 -S -c 24000 -p 80 192.168.248.2 &&
hping3 -i u8000 -S -c 22500 -p 80 192.168.248.2 &&
hping3 -i u100000 -S -c 3000 -p 80 -2 192.168.248.2 &&
hping3 -i u10000 -S -c 30000 -p 80 192.168.248.2 &&
hping3 -i u5000 -S -c 60000 -p 80 -1 192.168.248.2 &&
hping3 -i u100000 -S -c 3000 -p 80 192.168.248.2 ;
