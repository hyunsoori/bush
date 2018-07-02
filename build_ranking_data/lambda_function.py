import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime

from models.commons import seoul_area_code
from models.db_models import AptTradeInfo

import boto3
import datetime
import json
from dateutil.relativedelta import relativedelta

engine = create_engine(os.environ['MYSQL_URL'])
connection = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()

def get_trade_count(area_code, yyyymm):
	return session.query(AptTradeInfo).filter_by(city_code=area_code, yyyymm=yyyymm).count()	

def s3_upload(data):
	AWS_ACCESS_KEY_ID=os.environ['ACCESS_KEY_ID']
	AWS_SECRET_ACCESS_KEY=os.environ['SECRET_ACCESS_KEY']
	s3 = boto3.resource(
		's3',
		aws_access_key_id=AWS_ACCESS_KEY_ID,
		aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
	)
	bucket = s3.Bucket('bush-resource')
	path = 'test.json'
	bucket.put_object(
		ACL='public-read',
		ContentType='application/json',
		Key=path,
		Body=json.dumps(data, ensure_ascii=False),
	)


def lambda_handler(event, context):

	now = datetime.datetime.today()    
    
	ranking_data = {}

	for i in range(0, 6):
		target = now + relativedelta(months=i * -1)
		yyyymm = target.strftime('%Y%m')

		area_trades = []
		trade_count = 0
		for area_code in seoul_area_code.keys():
			count = get_trade_count(area_code, yyyymm)
			trade_count += count
	    
			area_trades.append({
				'area_code': area_code,
				'area_name': seoul_area_code.get(area_code),
				'trade_count': trade_count,
			})

		ranking_data[yyyymm] = area_trades

	#sorted(ranking_data, key=lambda r: r['trade_count'])

	s3_upload(ranking_data)



if __name__ == "__main__":
    lambda_handler(None, None)

