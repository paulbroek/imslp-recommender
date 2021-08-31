""" use IMSLP API to get a list of works using a page Id 

    other repo, that uses the mediawiki api: https://github.com/jlumbroso/imslp

    run script:

        conda activate py39
        ipy get_works_list.py -i
"""

from typing import Dict, Any, Optional
import random
import logging
# from time import sleep
import asyncio
from urllib3 import PoolManager # , request
import bs4 as bs
import aiohttp
from yapic import json
import tqdm

# easy for filtering out composers
# import imslp # https://github.com/jlumbroso/imslp

from rarc.utils.misc import chainGatherRes
from rarc.utils.log import setup_logger, set_log_level, loggingLevelNames
from rarc.redis_conn import rs

log_fmt = "%(asctime)s - %(module)-16s - %(lineno)-4s - %(funcName)-16s - %(levelname)-7s - %(message)s"  # name
logger = setup_logger(cmdLevel=logging.INFO, saveFile=0, savePandas=0, color=1, fmt=log_fmt)

# create a new db to save al 36_000 categories in
rcon = rs(home=0, db=4, decode=0)

retformat = 'json'
api_imslp = "http://imslp.org/imslpscripts/API.ISCR.php?account=worklist/disclaimer=accepted/sort=id/type={}/start={}/retformat={}"

manager = PoolManager(10)

def get_imslp(api_type=2) -> Dict[str, Dict[str, Any]]:
    """ get 'people' (api_type=1) and/or 'works' (api_type=2) from IMSLP API 

        they define 'people' as composer, performers, editors, etc. 
        https://imslp.org/wiki/IMSLP:API

        will keep polling until moreresultsavailable metadata flag turns False
        takes total time of about 30 seconds for 'people'
                                 200 seconds for 'works'

        you can store this dict to redis using 'save_redis()' below
    """

    start = 0
    data = dict()
    moreresultsavailable = True

    while moreresultsavailable:
        url = api_imslp.format(api_type, start, retformat)
        logging.info(f'{url=}')
        r = manager.request('GET', url)

        try:
            jsond = json.loads(r.data)
        except Exception as e: 
            logger.error(f'cannot parse data to json. {str(e)=}')

        # reqs[i] = d
        # change ids to string ids: Category:Barbosa, Domingos Caldas
        catd = {d['id']: d for k,d in jsond.items() if 'id'  in d} # last metadata dict does not have 'id' key
        data |= catd

        # update start for next fetch
        # last item is metadata
        # second last item is last data item
        # kys = list(d.keys())
        # start = 1 + int(kys[-2]) # gives string
        start += 1 + len(catd)
        moreresultsavailable = list(jsond.values())[-1]['moreresultsavailable']

        logger.info(f'{len(catd)=:,} {len(data)=:,}, new {start=:<10,} {moreresultsavailable=}')

    return data

# people_data = get_imslp(api_type=1)
# works_data = get_imslp(api_type=2)
# save_redis(name='imslp_people_data', data=people_data)
# save_redis(name='imslp_works_data', data=works_data)
def save_redis(data, name=None) -> None:
    assert name is not None
    rcon.r.set(name, json.dumps(data))

# data = get_redis('imslp_works_data')
def get_redis(name=None) -> Dict[str, Dict[str, Any]]:
    assert name is not None

    d = rcon.r.get(name)
    return json.loads(d)

# ldata = list(data.items())
# dcount = extract_download_count(Id=ldata[1000][0], data=data)
def extract_download_count(Id=None, composer=None, data=None) -> Optional[int]:
    """ extract download count from imslp html page

        id          title id from the HashablePageRecord, e.g. 'Polish Songs, Op.74 (Chopin, Frédéric)'
        composer    first do a composer look up, then look up download counts (slow)
        data        pass the complete dict of ~300K HashablePageRecords, stored in redis.

                    looks like:

                    [....,

                    ('"My Old Dutch" Waltz (West, Alfred H.)',
                     {'id': '"My Old Dutch" Waltz (West, Alfred H.)',
                      'type': '2',
                      'parent': 'Category:West, Alfred H.',
                      'intvals': {'composer': 'West, Alfred H.',
                       'worktitle': '"My Old Dutch" Waltz',
                       'icatno': '',
                       'pageid': '1038607'},
                      'permlink': 'https://imslp.org/wiki/"My_Old_Dutch"_Waltz_(West,_Alfred_H.)'})
                    
                    ..., ]
    """
    assert Id is not None
    assert data is not None

    assert isinstance(data, dict)
    # download counts are stored in /wiki/Special:GetFCtrStats/@{ID}
    # try to extract using beautifulsoup, inside the <a> tag

    url = data[Id]['permlink']
    r = manager.request('GET', url)
    soup = bs.BeautifulSoup(r.data)
    a_title = 'Special:GetFCtrStats' # /
    # ahrefs = soup('a', href=True)

    # is there a quicker way than this?
    # ahref_title_matches = [a for a in ahrefs if a.get('title', '').startswith(a_title)]

    # better?
    ahref_title_matches = soup.find_all(lambda tag: tag.name =='a' and tag.get('title', '').startswith(a_title))

    lahref = len(ahref_title_matches)
    # multiple matches: just create a list, and later a dict, look up the version names
    # assert (lahref := len(ahref_title_matches)) <= 1, f"{lahref=:<4} > 1. {Id=}"

    if lahref == 0: 
        logger.warning(f'no download count found for {Id}')

    dcounts = []
    # for every match, try to extract the download count
    for m in ahref_title_matches:
        # m = ahref_title_matches[0]
        dcount = m.text  

        try:
            dcount = int(dcount)

        except Exception as e:
            logger.error(f'cannot parse download count text: {dcount}, {Id=} {str(e)=}')

        dcounts.append(dcount)

    return dcounts

# dcounts = extract_dcounts(data, ids=None, n=100)
def extract_dcounts(data: dict, ids=None, n=100):
    """ extract a batch of download counts, using a random sample """

    # test on a sample of ids
    if ids is None:
        ids = random.sample(list(data.keys()), n)

    dcounts = dict()

    # todo: make async using aiohttp
    for i in tqdm.tqdm(ids):
        #if i%10 == 0:

        dcounts[i] = extract_download_count(Id=i, data=data)

    return dcounts

async def aextract_download_count(id=None, data=None):
    """ extract download count asynchronously from imlsp html page using aiohttp """

    raise NotImplementedError

async def fetch(session, url) -> Dict[str, str]:
    
    # with aiohttp.Timeout(10):
    async with session.get(url) as response:
        return {id: await response.text()}

async def fetch_all(session, urls: Dict[str, str]):
    """ 
        urls   dict of id keys and url values

    """ 
    results = await asyncio.gather(
        *[fetch(session, url) for id, url in urls.items()],
        return_exceptions=True  # default is false, that would raise
    )

    # for testing purposes only
    # gather returns results in the order of coros
    for idx, url in enumerate(urls):
        print('{}: {}'.format(url, 'ERR' if isinstance(results[idx], Exception) else 'OK'))

    # chain dictionaries into one
    results = chainGatherRes(results)

    return results

async def aextract_dcounts(data: dict, ids=None, n=100):

    if ids is None:
        ids = random.sample(list(data.keys()), n)

    urls = {i: data[i]['permlink'] for i in ids}

    loop = asyncio.get_event_loop()

    async with aiohttp.ClientSession(loop=loop) as session:
        # ress = loop.run_until_complete(fetch_all(session, urls, loop))
        ress = await fetch_all(session, urls)
        # async with session.get('http://httpbin.org/get') as resp:
        #     print(resp.status)
        #     print(await resp.text())

    return ress


if __name__ == "__main__":

    try: 
        wdata 
    except NameError:
        wdata = get_redis('imslp_works_data')

        logger.info(f'got redis data, now running ')

    res = asyncio.run(aextract_dcounts(wdata, ids=None, n=5))

    logger.info(f'{len(res)=}')
