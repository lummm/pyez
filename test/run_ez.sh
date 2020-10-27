#!/bin/bash

docker run -d --rm --net=host \
       -e ZMQ_REQ_PORT=9999 \
       -e WORKER_PORT=9998 \
       --name ez \
       tengelisconsulting/ez
