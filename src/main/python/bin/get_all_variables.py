import os

##set environment variables
os.environ['envn']='TEST'
os.environ['header']='True'
os.environ['inferSchema']='True'


##get environment varibles
envn=os.environ['envn']
header=os.environ['header']
inferSchema=os.environ['inferSchema']

## Set other variables
appName='USA prescriber research report'
current_path= os.getcwd()
staging_dim_city=current_path + '\..\staging\dimension_city'
staging_fact=current_path + '\..\staging\\fact'

