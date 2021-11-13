""" use IMSLP API to get a list of works using a page Id 

    other repo, that uses the mediawiki api: https://github.com/jlumbroso/imslp

    run script:

        conda activate py39
        ipy get_works_list.py -i
"""

from typing import Dict, Tuple, List, Any, Union, Callable # , Optional
import random
import logging
# from time import sleep
import sys
import re
from itertools import chain
from collections import defaultdict
from datetime import datetime
import argparse
import asyncio
from urllib3 import PoolManager # , request
import bs4 as bs
import aiohttp
from yapic import json
import tqdm
import pandas as pd
import timeago
# import pdb; pdb.set_trace()

# easy for filtering out composers
# import imslp # https://github.com/jlumbroso/imslp

from rarc.utils.misc import chainGatherRes, unnest_dict, unnest_assign_cols
from rarc.utils.log import setup_logger, set_log_level, loggingLevelNames
from rarc.redis_conn import rs

LOG_FMT = "%(asctime)s - %(module)-16s - %(lineno)-4s - %(funcName)-22s - %(levelname)-7s - %(message)s"  # title
logger = logging.getLogger(__name__)

# create a new db to save al 36_000 categories in
REDIS_DB = 4
rcon = rs(home=0, db=REDIS_DB, decode=0)

retformat = 'json'
api_imslp = "http://imslp.org/imslpscripts/API.ISCR.php?account=worklist/disclaimer=accepted/sort=id/type={}/start={}/retformat={}"
redisKey = 'imslp_download_entries'
redisKeyRawSoup = 'imslp_raw_html'
scrapeVersion = 6

manager = PoolManager(10)
id_remap = lambda x: x.replace('Category:','')
error_cnt = defaultdict(int)

def parse_row(row: dict, id_remap_: Callable=id_remap) -> dict:
    try:
        if id_remap is not None and 'id' in row:
            row['id'] = id_remap_(row['id'])

    except Exception as e: 
        logger.error(f'cannot parse row dict. {e=!r}')   

    return row

def get_imslp(api_type=2, max_pages=None, id_remap_: Callable=id_remap) -> Dict[str, Dict[str, Any]]:
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
        jsond = {k: parse_row(d, id_remap_=id_remap_) for k,d in jsond.items()}      
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
# save_redis(people_data, 'imslp_people_data')
# save_redis(works_data, 'imslp_works_data')
def save_redis(data, key=None) -> None:
    assert key is not None
    rcon.r.set(key, json.dumps(data))

# data = get_redis('imslp_people_data')
def get_redis(key: str) -> Dict[str, Dict[str, Any]]:

    d = rcon.r.get(key)
    return json.loads(d)

# rdata = get_multi_zset('imslp_download_entries')
# rdata = get_multi_zset('imslp_raw_html')
def get_multi_zset(key: str) -> List[Dict[str, Any]]:

    res = rcon.r.zrevrangebyscore(key, '+inf', '-inf')

    return [json.loads(d) for d in res]

def remove_older_versions_redis(key: str):
    """ get all rows from redis, but remove entries that have multiple values, only keep the newest row """

    # todo: implement
    raise NotImplementedError

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
    'size' : float # lambda x: float(x), # less strict
}

def regex_parse_fields(text):

    d = dict(partial=False, parts=False)
        
    for patt, matcher in re_patterns.items():
        matches = matcher(text)
        if len(matches) == 1:
            d[patt] = matches[0]
        elif len(matches) == 0 and (patt not in re_fallback):
            logger.warning(f"cannot parse '{patt}' row.{text=}")
            error_cnt[patt] += 1
        elif patt != 'npage':
            logger.warning(f"cannot parse '{patt}', {len(matches)=} {matches=} row.{text=}")
        else:
            # fallback try for 'npage': a list of numbers can be summed
            try:
                d[patt] = sum(map(int, matches))
                logger.warning(f"parsed 'npage' in second try")
                d['parts'] = True
                continue
            except Exception as e:
                logger.warning(f"cannot parse list of 'npage' items, {len(matches)=} {matches=}")   

        # fallback try for 'npage': 2-30, partial section of a piece can be subtracted
        if patt not in d and patt in re_fallback:
            matches = re_fallback[patt](text)
            if len(matches) == 1:
                try:
                    d[patt] = int(matches[0][1]) - int(matches[0][0])
                    error_cnt[patt] -= 1
                    d['partial'] = True
                    logger.warning(f"parsed 'npage' in second try")
                except Exception as e:
                    logger.warning(f"could not parse 'npage' in second try. {e=!r}")
                    error_cnt[patt] += 1

        if patt in d and patt in re_finalize:
            try:
                d[patt] = re_finalize[patt](d[patt])
            except ValueError:
                logger.warning(f"could not parse {patt=} to float. {d[patt]=} {text=}")

    return d

def parse_metadata_table(text):
    """ 
        metadata table contains year of composition, and categories:

        Jaar van Compositie 1997 or before
        Genre Categorieën   Pieces; For piano; Scores featuring the piano; [4 more...]
    """
    d = dict()

    table = text.find('table')
    if table is None:
        err = 'cannot find metadata table'
        logger.warning(err)
        error_cnt[err] += 1
        return d
    # assert len(table) == 1, f"{len(table)=} \n{table=}"
    # table = table[0]
    tableRows = table.find_all('tr')
    
    for row in tableRows:
        rowHeader = row.find('th')
        rowValue = row.find('td')
        try:
            rowHeaderText = rowHeader.text.strip()
        except Exception as e:
            # logger.warning(f"cannot parse rowHeader text. {e=!r}")
            continue
            
        rowValueText = ''
        try:
            rowValueText = rowValue.text.strip()
        except Exception as e:
            logger.warning(f"cannot parse rowValue text {e=!r}")

        if rowHeaderText == 'Genre Categories':
            # parse to a list of categories
            rowValueText = rowValueText.split('; ')

        d[rowHeaderText] = rowValueText

    return d

# todo: auto always send data to redis (done)
# todo: check if dix exists in redis, and stop parsing if it exists.
# todo: extract 'Genre Categorieën' , upper page
# todo: create postgres tables with sqlalchemy
# todo: make async? 
# todo: scrape the 'Arrangements and Transcriptions' page as well. Click on it, or?
# todo: get popular categories, by running groupby on download count with categories
# ldata = list(works_data.items())
# dcount = extract_all_items(Id=ldata[1000][0], data=works_data)
async def extract_all_items(Id=None, data=None) -> dict:
    """ parse all items on a Works page

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

    if rcon.aior[REDIS_DB] is None:            
        rcon._aior_connect(REDIS_DB)

    # download counts are stored in /wiki/Special:GetFCtrStats/@{ID}
    # try to extract using beautifulsoup, inside the <a> tag

    # breakpoint()
    url = data[Id]['permlink']
    r = manager.request('GET', url)
    soup = bs.BeautifulSoup(r.data, "html.parser")
    metadata = parse_metadata_table(soup)
    logger.debug(f"{metadata=}")
    # sheetmusicSection = soup # now you will also scrape recordings, mind this. 
    # audioSection = soup.find(attrs={'id': 'wpaudiosection'}) # or select audio section here
    sheetmusicSection = soup.find(attrs={'id': 'wpscoresection'})
    if sheetmusicSection is None:
        logger.warning(f"this work has no sheet music items: {Id}")
        return

    boxes = sheetmusicSection.find_all(attrs={'class': 'we'})
    info_matches = [box.find(attrs={'class': 'we_file_download plainlinks'}) for box in boxes]
    info_matches = list(filter(lambda x: x is not None, info_matches))

    if len(info_matches) == 0:
        return

    # logger.info(f"{info_matches=}")

    # breakpoint()
    d = {i: span_info.text for i, span_info in enumerate(info_matches)}
    d = {i: regex_parse_fields(v) for i,v in d.items()}

    # for i in range(len(info_matches)):
    for i, box in enumerate(boxes):
        error_cnt['parsecount'] += 1

        # downloadUrl = ''
        downloadDiv = box.find(attrs={'class': 'external text'})
        if downloadDiv is None or 'href' not in downloadDiv.attrs:
            logger.warning(f"downloadDiv None or attr 'href' not in downloadDiv, cannot extract downloadUrl. {Id=} {downloadDiv=}")
            error_cnt["cannot extract downloadUrl"] += 1
            return
        
        downloadUrl = downloadDiv.attrs['href']

        findRix= re.findall(r"/(\d+)$", downloadUrl) # rangeindex from imslp
        if len(findRix) == 1:
            d[i]['url'] = downloadUrl
            d[i]['rix'] = rix = int(findRix[0])
            d[i]['title'] = Id 
            d[i]['itemno'] = i 
            d[i]['parent'] = data[Id]['parent'].replace('Category:','')
            d[i]['parent_meta'] = metadata # todo: should eventually become psql table, so that metadata gets saved only once for the parent
            d[i]['version'] = scrapeVersion
            d[i]['scrapeDate'] = datetime.utcnow()

            # ugly, but for now, save here to redis
            # rcon.r.zadd(redisKey, {json.dumps(d[i]): rix})
            # rcon.r.zadd(redisKeyRawSoup, {json.dumps(str(soup)): rix})
            await rcon.aior[REDIS_DB].zadd(redisKey, {json.dumps(d[i]): rix})
            await rcon.aior[REDIS_DB].zadd(redisKeyRawSoup, {json.dumps(str(soup)): rix}) # rcon.r.zadd

        else:
            logger.warning(f"{len(findRix)=} != 1")

    return d

# dcounts = scrape_all_works(data, ids=None, n=100)
async def scrape_all_works(data: dict, ids=None, n=100, mininterval=9, debug_invl=50):
    """ extract a batch of download counts, using a random sample """

    # test on a sample of ids
    if ids is None and n is not None:
        ids = random.sample(list(data.keys()), n)
    elif ids is None and n is None:
        ids = list(data.keys())

    ret = dict()

    # todo: make async using aiohttp
    for i, id_ in enumerate(tqdm.tqdm(ids, mininterval=mininterval)):
        if i > 0 and i%debug_invl == 0:
            logger.info(f"error_cnt: {dict(error_cnt)}")

        res = await extract_all_items(Id=id_, data=data)
        if res is not None:
            ret[id_] = res

    return ret

async def aextract_all_items(id_=None, data=None):
    """ extract download count asynchronously from imlsp html page using aiohttp """

    raise NotImplementedError

def extract_dict_keys(row) -> List[str]:
    if isinstance(row, dict):
        return list(row.keys())

    return []

# rdata = get_multi_zset('imslp_download_entries')
# metaCols, df = rdata_to_df(dcounts)
# metaCols, df = rdata_to_df(rdata, renameDict={'parent_meta':'meta'}, sortBy='scrapeDate')
# metaCols, df = rdata_to_df(rdata)
# withmeta = df[~df.parent_meta.isnull()]
def rdata_to_df(dcounts: Union[List[dict], Dict[str, Dict[int, dict]]], renameDict=None, sortBy=None) -> Tuple[List[str], pd.DataFrame]:

    tuples, vals = [], []
    if isinstance(dcounts, dict):
        for k,v in dcounts.items():
            for l, row in v.items():
                tuples.append((k, l))
                vals.append(row)
                # print(f"{k} {l}")

    elif isinstance(dcounts, list):
        tuples = [(v['title'], v['itemno']) for v in dcounts]
        vals = dcounts

    df = pd.DataFrame(vals)
    mix = pd.MultiIndex.from_tuples(tuples)
    df.index = mix

    # try to make columns neater, or return df
    df['ndownload'] = df['ndownload'].astype(float)
    df['npage'] = df['npage'].astype(float)
    df['ago'] = df['scrapeDate'].map(lambda x: timeago.format(x, datetime.utcnow()), na_action='ignore')
    # replace parent_meta na vals with empty dicts. --> cannot replace nans with objects. ?
    # df.fillna(dict(parent_meta=dict()), inplace=True)
    df['parent_meta_keys'] = df.parent_meta.map(extract_dict_keys)

    # todo: if duplicate urls exist, drop the one without parent_metadata
    # todo: or use a version number? And prefer the highest version
    # todo: collaps metadata dict to new columns, what value to use as null placeholder?

    if renameDict is not None:
        df = df.rename(columns=renameDict)

    if sortBy is not None:
        assert sortBy in df.columns, f"{sortBy=} not in cols={list(df.columns)}"
        df = df.sort_values(sortBy, ascending=False)

    # unnest meta dict to new columns
    metaKeys = unique_nested_lists(df)
    ul = [(renameDict['parent_meta'], i) for i in metaKeys] if 'parent_meta' in renameDict else [('parent_meta', i) for i in metaKeys]
    addedCols, df = unnest_assign_cols(df, ul)
    logger.info(f"added {len(addedCols)=:,} cols")

    return addedCols, df

# labs = unique_nested_lists(df)
# cats = unique_nested_lists(df, df[[(col:='meta_Genre Categories)]].dropna(), col=col)
# old records miss metadata values, use dropna before passing dataframe to this method
def unique_nested_lists(df, col='parent_meta_keys') -> Tuple[str]:
    """ extract unique items from a column of nested lists
        mostly looks like:
            ['Composition Year', 'Incipit', 'Genre Categories', "Movements/SectionsMov'ts/Sec's", 'First Publication', 'Related Works'] 
    """
    assert isinstance(df, pd.DataFrame)
    assert col in df.columns, f"{col=} not in cols={list(df.columns)}"

    ll = list(df[col].values)
    unq = set(chain.from_iterable(ll))

    return tuple(unq)

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

async def ascrape_all_works(data: dict, ids=None, n=100):
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
          "--dryrun", 
          action='store_true',         
          default=False,
          help="only load this file, do not scrape"
        )
        CLI.add_argument(
          "--skipExistingTitles", 
          action='store_true',         
          default=False,
          help="first check which titles are already scraped, and scrape new titles first"
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

    bs4version = bs.__version__
    logger.info(f"running imslp parser. {args=} \n{bs4version=} ")

    if args.dryrun:
        sys.exit()

    try: 
        wdata
    except NameError:
        # data = get_redis('imslp_people_data')
        wdata = get_redis('imslp_works_data')

        logger.info(f'got redis data, now running ')

    # res = asyncio.run(ascrape_all_works(wdata, ids=None, n=5))
    # logger.info(f'{len(res)=}')

    nrows = args.nrow
    # nrows = 100_000
    # nrows = len(data)
    # nrows = None
    if nrows is not None:
        nrows = int(nrows)
        logger.info(f"will parse {nrows=:,}")
    else:
        logger.info(f"will parse all {len(wdata)=:,} rows")

    # skip existing titles?
    if args.skipExistingTitles:
        # get titles collected in redis
        rdata = get_multi_zset('imslp_download_entries')
        metaCols, bdf = rdata_to_df(rdata)
        titles = set(bdf.title.values)

        # filter out existing titles
        all_titles = set(wdata.keys())
        unscraped_titles = all_titles - titles

        logger.info(f"{len(all_titles)=:,} {len(unscraped_titles)=:,}")

        to_scrape = {k: v for k,v in wdata.items() if k in unscraped_titles}
        logger.info(f"no titles to scrape: {len(to_scrape):,}")

    else:
        to_scrape = wdata

    # dcounts = scrape_all_works(to_scrape, ids=None, n=nrows)
    dcounts = asyncio.run(scrape_all_works(to_scrape, ids=None, n=nrows))
