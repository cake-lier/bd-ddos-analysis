#!/bin/bash

set -euo pipefail

if [ $# != 2 ]
then
  echo "This script works by passing two arguments to it: the folder containing the private key and the hostname to which deploy"
  exit 1
fi

sbt +assembly
sbt +remoteDeploy "aws"
ssh -i "$1"/bigdata.pem hadoop@"$2" -L 20888:"$2":20888
xdg-open localhost:20888
