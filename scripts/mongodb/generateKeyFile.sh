#!/bin/bash

DST=./keyfile/dev.key

openssl rand -base64 756 > $DST
