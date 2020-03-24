#!/bin/bash
docker rm -f $(docker ps -aq --filter "name=rockthejvm-*")