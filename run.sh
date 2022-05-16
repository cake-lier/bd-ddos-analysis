#!/bin/bash

set -euo pipefail

if [ $# != 2 ]
then
  echo "This script works by passing two arguments to it: the folder containing the private key and the hostname to which deploy"
  exit 1
fi

sbt +assembly
sbt "remoteDeploy aws"
ssh -i "$1"/bigdata.pem -fL 20888:"$2":20888 hadoop@"$2" sleep 60 &
ssh -i "$1"/bigdata.pem -fL 18080:"$2":18080 hadoop@"$2" sleep 60 &
