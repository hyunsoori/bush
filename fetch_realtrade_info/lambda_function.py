import asyncio
import aiohttp
import os
import logging
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import xmltodict
import hashlib
import json
from dateutil.relativedelta import relativedelta

from models.commons import seoul_area_code
from models.db_models import AptTradeInfo

logger = logging.getLogger(__name__)

engine = create_engine(os.environ['MYSQL_URL'])


async def download(area_code, yyyymm):
    url = os.environ['API_URL']
    url = url.format(area_code, yyyymm)
    async with aiohttp.request('GET', url) as res:
        result_xml = await res.text()
        return xmltodict.parse(result_xml)


async def async_download(session, yyyymm):
    fts = [asyncio.ensure_future(download(area_code, yyyymm)) for area_code in seoul_area_code.keys()]
    for f in asyncio.as_completed(fts):
        r = await f
        for idx, item in enumerate(r['response']['body']['items']['item']):
            try:
                key = hashlib.md5(json.dumps(item, sort_keys=True).encode('utf-8')).hexdigest()
                city_code = item['지역코드']
                apt_trade_info = AptTradeInfo(key=key, yyyymm=yyyymm, days=item['일'], city_code=city_code,
                                              city_name=seoul_area_code[int(city_code)], dong_name=item['법정동'],
                                              apt_name=item['아파트'], floor=int(item['층']), space=item['전용면적'],
                                              price=int(item['거래금액'].replace(',', '')), start_year=item['건축년도'])
                session.add(apt_trade_info)
                session.commit()
            except Exception as e:
                print(e)


def lambda_handler(event, context):
    connection = engine.connect()
    Session = sessionmaker(bind=engine)

    futures = []

    now = datetime.datetime.today()
    for i in range(0, 4):
        target = now + relativedelta(months=i * -1)
        yyyymm = target.strftime('%Y%m')
        print(yyyymm)
        futures.append(async_download(Session(), yyyymm))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(futures))

    connection.close()

