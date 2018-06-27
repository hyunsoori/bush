import asyncio
import aiohttp
import os
import logging
import datetime
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime
import xmltodict
import hashlib
import json
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

engine = create_engine('mysql+pymysql://bush_app_user:a1234567@bushdbinstance.csaae2uji9ic.ap-northeast-2.rds.amazonaws.com/bush?charset=utf8mb4')
Base = declarative_base()

seoul_area_code = {
    11110: "종로구",
    11140: "중구",
    11170: "용산구",
    11200: "성동구",
    11215: "광진구",
    11230: "동대문구",
    11260: "중랑구",
    11290: "성북구",
    11305: "강북구",
    11320: "도봉구",
    11350: "노원구",
    11380: "은평구",
    11410: "서대문구",
    11440: "마포구",
    11470: "양천구",
    11500: "강서구",
    11530: "구로구",
    11545: "금천구",
    11560: "영등포구",
    11590: "동작구",
    11620: "관악구",
    11650: "서초구",
    11680: "강남구",
    11710: "송파구",
    11740: "강동구",
}


class AptTradeInfo(Base):
    __tablename__ = 'apt_trade_info'

    id = Column(Integer, primary_key=True)
    key = Column(String, nullable=False)
    yyyymm = Column(String, nullable=False)
    days = Column(String, nullable=False)
    city_code = Column(String, nullable=False)
    city_name = Column(String, nullable=False)
    dong_name = Column(String, nullable=False)
    apt_name = Column(String, nullable=False)
    floor = Column(Integer, nullable=False)
    space = Column(String, nullable=True)
    price = Column(Integer, nullable=False)
    start_year = Column(String, nullable=True)
    created_date = Column(DateTime, default=datetime.datetime.now)
    updated_date = Column(DateTime, nullable=True)


async def download(area_code, yyyymm):
    # url = os.environ['API_URL']
    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?serviceKey=x%2FeoVcS2U3HDDhBsiIr0JiZsHB6ymm8GO%2FxIwCtrpkMpVgeRmD8IUv2sWjSsc%2B8iJEGVcUKMb5FTJ8QorRMdTg%3D%3D&LAWD_CD={}&DEAL_YMD={}'
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
            except:
                pass


def lambda_handler(event, context):
    connection = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    futures = []

    now = datetime.datetime.today()
    for i in range(0, 4):
        target = now + relativedelta(months=i * -1)
        yyyymm = target.strftime('%Y%m')
        futures.append(async_download(session, yyyymm))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(futures))

    connection.close()


if __name__ == "__main__":
    lambda_handler(None, None)
