#!/bin/bash

# donwload all relevant dependencies
pip install --system --target ./package -r requirements.txt

# zip all dependencies
cd ./package	
zip -r9 ../function.zip . 

# add function code to zip 
cd ../core
echo $(pwd)
zip -g ../function.zip ./lambda_function.py msk_service.py msk_config.py

# Push new function.zip to AWS
cd ..
aws lambda --profile [YOUR-PROFILE] update-function-code --function-name msk-topic-handler --zip-file fileb://function.zip

# remove package directory and zip
rm -r ./package/
rm function.zip


