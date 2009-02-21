#!/bin/bash
./numbexctl.py p2p-stop
./numbexctl.py updater-stop
./numbex_client.py sendsign delete.csv freeconet.priv.pem 

./numbex_client.py sendsign foo.csv freeconet.priv.pem 

./numbex_client.py sendsign notsigned.csv freeconet.priv.pem 

./numbexctl.py p2p-export
./numbexctl.py updater-start
./numbexctl.py p2p-start

