#!/bin/bash

LAMBDA="fetch_trade_info"

rm -r lambdapack
mkdir lambdapack

cp -r ../models lambdapack/models
cp -r lambda_function.py lambdapack/lambda_function.py

#install
cd lambdapack
pip install -r ../requirements.txt -t .

#zip
zip -r ../${LAMBDA}_pack_$(date +'%Y%m%d%H%M%S').zip .

cd ..
rm -r lambdapack

