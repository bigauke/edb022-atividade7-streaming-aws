$ErrorActionPreference = "Stop"

Write-Host "Instalando dependencias no producer_lambda..."
pip install -r requirements.txt -t producer_lambda/

Write-Host "Instalando dependencias no consumer_lambda..."
pip install -r requirements.txt -t consumer_lambda/

$BucketCodeName = "edb022-atv7-sam-bucket-$((Get-Date).Ticks)"
Write-Host "Criando bucket $BucketCodeName para deploy dos codigos..."
aws s3 mb s3://$BucketCodeName

Write-Host "Executando aws cloudformation package..."
aws cloudformation package --template-file infra/template.yaml --s3-bucket $BucketCodeName --output-template-file infra/packaged.yaml

Write-Host "Executando aws cloudformation deploy..."
aws cloudformation deploy --template-file infra/packaged.yaml --stack-name edb022-streaming --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND

Write-Host "Deploy finalizado!"
