""" use IMSLP API to get a list of works using a page Id 

    other repo, that uses the mediawiki api: https://github.com/jlumbroso/imslp

    run script:

        conda activate py39
        ipy get_works_list.py -i
"""

from typing import Dict, Any, Optional, Callable
import random
import logging
# from time import sleep
from collections import defaultdict
import argparse
import asyncio
from urllib3 import PoolManager # , request
import bs4 as bs
import aiohttp
import re
from yapic import json
import tqdm
import pandas as pd
# import pdb; pdb.set_trace()

# easy for filtering out composers
# import imslp # https://github.com/jlumbroso/imslp

from rarc.utils.misc import chainGatherRes
from rarc.utils.log import setup_logger, set_log_level, loggingLevelNames
from rarc.redis_conn import rs

LOG_FMT = "%(asctime)s - %(module)-16s - %(lineno)-4s - %(funcName)-16s - %(levelname)-7s - %(message)s"  # title

# create a new db to save al 36_000 categories in
rcon = rs(home=0, db=4, decode=0)

retformat = 'json'
api_imslp = "http://imslp.org/imslpscripts/API.ISCR.php?account=worklist/disclaimer=accepted/sort=id/type={}/start={}/retformat={}"
redisKey = 'imslp_download_entries'

manager = PoolManager(10)
id_remap = lambda x: x.replace('Category:','')

def parse_row(row: dict, id_remap: Callable=id_remap) -> dict:
    try:
        if id_remap is not None and 'id' in row:
            row['id'] = id_remap(row['id'])

    except Exception as e: 
        logger.error(f'cannot parse row dict. {e=!r}')   

    return row

def get_imslp(api_type=2, max_pages=None, id_remap: Callable=id_remap) -> Dict[str, Dict[str, Any]]:
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

    i = 0
    while moreresultsavailable:
        url = api_imslp.format(api_type, start, retformat)
        logging.info(f'{url=}')
        r = manager.request('GET', url)

        try:
            jsond = json.loads(r.data)
        except Exception as e: 
            logger.error(f'cannot parse json data to dict. {str(e)=}')

        # breakpoint()
        jsond = {k: parse_row(d) for k,d in jsond.items()}      
        # breakpoint()

        # change ids to string ids: Category:Barbosa, Domingos Caldas
        catd = {d['id']: d for k,d in jsond.items() if 'id' in d} # last metadata dict does not have 'id' key
        data |= catd

        # update start for next fetch
        # last item is metadata
        # second last item is last data item
        # kys = list(d.keys())
        # start = 1 + int(kys[-2]) # gives string
        start += 1 + len(catd)
        moreresultsavailable = list(jsond.values())[-1]['moreresultsavailable']

        logger.info(f'{len(catd)=:,} {len(data)=:,}, new {start=:<10,} {moreresultsavailable=}')

        if max_pages and i > max_pages:
            break

        i += 1

    return data

# people_data = get_imslp(api_type=1, max_pages=1)
# people_data = get_imslp(api_type=1, id_remap=id_remap)
# works_data = get_imslp(api_type=2)
# works_data = get_imslp(api_type=2, max_pages=1)
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

re_patterns = { \
    'size' : lambda x: re.findall(r"(?<=-)(.+)MB", x), 
    'npage' : lambda x: re.findall(r",\s+(\d+)", x),
    # 'ndownload' = lambda x: re.findall(r"- (\d+)×", x), 
    'ndownload' : lambda x: re.findall(r"(\d+)×", x), # less strict
}

# patterns to match if the previous option fails
re_fallback = { \
    'npage' : lambda x: re.findall(r"(\d+):(\d+)", x),
}

re_finalize = { \
    'size' : lambda x: float(x), # less strict
}

error_cnt = defaultdict(int)

# todo: is download size always in MB?
def regex_parse_fields(text):
    # d = defaultdict(None)
    d = dict(partial=False)
    for patt, matcher in re_patterns.items():
        matches = matcher(text)
        if len(matches) == 1:
            d[patt] = matches[0]
        elif len(matches) == 0:
            logger.warning(f"cannot parse '{patt}' \nrow.{text=}")
            error_cnt[patt] += 1
        else:
            logger.warning(f"{len(matches)=} {matches=} \nrow.{text=}")

        # fallback try for 'npage'
        if patt not in d and patt in re_fallback:
            matches = re_fallback[patt](text)
            if len(matches) == 2:
                try:
                    d[patt] = int(matches[1]) - (matches[0])
                    error_cnt[patt] -= 1
                    d['partial'] = True
                except Exception as e:
                    logger.warning(f"could also not parse 'npage' in second try. {e=!r}")

        if patt in d and patt in re_finalize:
            d[patt] = re_finalize[patt](d[patt])

    return d

# todo: auto always send data to redis (done)
# todo: check if dix exists in redis, and stop parsing if it exists. 
# ldata = list(works_data.items())
# dcount = extract_download_count(Id=ldata[1000][0], data=works_data)
def extract_download_count(Id=None, composer=None, data=None, redisKey='imslp_download_entries') -> dict:
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

    # breakpoint()
    url = data[Id]['permlink']
    r = manager.request('GET', url)
    soup = bs.BeautifulSoup(r.data)
    # a_title = 'Special:GetFCtrStats' # /
    # ahrefs = soup('a', href=True)

    # is there a quicker way of doing this?
    # ahref_title_matches = [a for a in ahrefs if a.get('title', '').startswith(a_title)]

    # better?
    # breakpoint()
    info_matches = soup.find_all(attrs={'class': 'we_file_info2'})
    url_matches = soup.find_all(attrs={'class': 'external text'})
    urls = [u.attrs['href'] for u in url_matches if 'https://imslp.org/wiki/Speci' in u.attrs['href']]
    d = {i: span_info.text for i, span_info in enumerate(info_matches)}
    d = {i: regex_parse_fields(v) for i,v in d.items()}
    if len(urls) == len(info_matches):
        for i in range(len(info_matches)):
            d[i]['url'] = urls[i]
            findRix= re.findall(r"/(\d+)$", urls[i]) # rangeindex from imslp
            if len(findRix) == 1:
                d[i]['rix'] = rix = int(findRix[0])
                d[i]['title'] = Id 
                d[i]['itemno'] = i 
                d[i]['parent'] = data[Id]['parent'].replace('Category:','')

                # ugly, but for now, save here to redis
                rcon.r.zadd(redisKey, {json.dumps(d): rix})
    else:
        logger.warning(f"{len(info_matches)=} != {len(urls)=}")
    
    # ahref_title_matches = soup.find_all(lambda tag: tag.name =='a' and tag.get('title', '').startswith(a_title))

    # lahref = len(ahref_title_matches)
    # # multiple matches: just create a list, and later a dict, look up the version names
    # # assert (lahref := len(ahref_title_matches)) <= 1, f"{lahref=:<4} > 1. {Id=}"

    # if lahref == 0: 
    #     logger.warning(f'no download count found for {Id}')

    # # dcounts = []
    # # for every match, try to extract the download count
    # for m in ahref_title_matches:
    #     # m = ahref_title_matches[0]
    #     dcount = m.text  

    #     try:
    #         dcount = int(dcount)

    #     except Exception as e:
    #         logger.error(f'cannot parse download count text: {dcount}, {Id=} {str(e)=}')

    #     dcounts.append(dcount)

    return d

# dcounts = extract_dcounts(data, ids=None, n=100)
def extract_dcounts(data: dict, ids=None, n=100, debug_invl=50):
    """ extract a batch of download counts, using a random sample """

    # test on a sample of ids
    if ids is None and n is not None:
        ids = random.sample(list(data.keys()), n)
    elif ids is None and n is None:
        ids = list(data.keys())

    dcounts = dict()

    # todo: make async using aiohttp
    for i, id_ in enumerate(tqdm.tqdm(ids)):
        if i > 0 and i%debug_invl == 0:
            logger.info(f"error_cnt: {dict(error_cnt)}")

        dcounts[id_] = extract_download_count(Id=id_, data=data)

    return dcounts

async def aextract_download_count(id=None, data=None):
    """ extract download count asynchronously from imlsp html page using aiohttp """

    raise NotImplementedError

def dcounts_to_df(dcounts: Dict[str, Dict[int, dict]]):
    tuples, vals = [], []
    for k,v in dcounts.items():
        for l, row in v.items():
            tuples.append((k, l))
            vals.append(row)
            # print(f"{k} {l}")

    df = pd.DataFrame(vals)
    mix = pd.MultiIndex.from_tuples(tuples)
    df.index = mix

    return df

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
    """ extract a batch of download counts, using a random sample """

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

class ArgParser():
    """ create CLI parser """

    @staticmethod
    def create_parser():
        return argparse.ArgumentParser()

    @classmethod
    def populate_parser(cls):

        CLI = cls.create_parser()

        CLI.add_argument(
          "-v", "--verbosity", 
          type=str,         
          default='info',
          help=f"choose debug log level: {', '.join(loggingLevelNames())}"
        )
        CLI.add_argument(
          "-s", "--save", 
          action='store_true',         
          default=True,
          help="save results to redis"
        )
        CLI.add_argument(
          "--scrape", 
          action='store_true',         
          default=False,
          help="scrape imslp downloadable items and save to redis"
        )
        CLI.add_argument(
          "--nrow", 
          type=str,
          default=None,
          help="number of rows to scrape"
        )

        return CLI


if __name__ == "__main__":

    parser      = ArgParser.populate_parser()
    args        = parser.parse_args()

    logger      = setup_logger(cmdLevel=logging.DEBUG, saveFile=0, savePandas=0, color=1, fmt=LOG_FMT)
    log_level   = args.verbosity.upper()

    set_log_level(logger, level=log_level, fmt=LOG_FMT)

    try: 
        data
    except NameError:
        # data = get_redis('imslp_people_data')
        data = get_redis('imslp_works_data')

        logger.info(f'got redis data, now running ')

    # res = asyncio.run(aextract_dcounts(data, ids=None, n=5))

    # logger.info(f'{len(res)=}')

    if args.scrape:
        nrows = args.nrow
        # nrows = 100_000
        # nrows = len(data)
        # nrows = None
        if nrows is not None:
            nrows = int(nrows)
            logger.info(f"will parse {nrows=:,}")
        else:
            logger.info(f"will parse all {len(data)=:,} rows")

        dcounts = extract_dcounts(data, ids=None, n=nrows)
