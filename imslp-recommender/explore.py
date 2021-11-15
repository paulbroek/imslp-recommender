"""
    explore.py
    
    load scraped data from redis and apply some data science to it

    conda activate py39
    ipy explore.py -i

"""
import random
from collections import defaultdict
import pandas as pd

from get_works_list import get_multi_zset, rdata_to_df, unique_nested_lists

rdata = get_multi_zset('imslp_download_entries')
metaCols, df = rdata_to_df(rdata, renameDict={'parent_meta':'meta'}, sortBy='ndownload', unnestCols=True) # scrapeDate

view = df[['partial','parts','size','npage','rix','title','itemno','ndownload','version']]

col = 'meta_Genre Categories'
cats = unique_nested_lists(df[[col]].dropna(), col='meta_Genre Categories')
scats = list(map(str.lower, cats)) # lowercase to make searching easier
pianocats = [c for c in cats if 'piano' in c]

print(f"{len(pianocats)=} \n")
print(f"top downloaded items: \n\n", view.head(20))

# drop all rows for now that do not have categories scraped (will be scraped later)
bdf = df.dropna(subset=[col])
# hot encode all (4K+) categories?
someCats = random.sample(cats, 500)
factors, labs = pd.factorize(cats)
catDict = dict(zip(factors, labs)) # maps int to str label
catDictRev = dict(zip(labs, factors))

def catListToFactors(item):
    return set([catDictRev[x] for x in item])

def factorsToStr(item) -> str: 
    """ create one comma-separated string, so factors can be extracted using regex """
    return ','.join(map(str, item))

def hasCat(item, category: str | int) -> bool:
    return category in item

bdf['catFactors'] = bdf[col].map(catListToFactors)
bdf['factorsStr'] = bdf['catFactors'].map(factorsToStr)

bdf[[col, 'catFactors', 'factorsStr']].head(10)

catCount = defaultdict(int)
# slow
for cat in factors: # cats
    # catCount[cat] = bdf[col].map(lambda x: hasCat(x, cat)).sum()
    catCount[cat] = bdf['catFactors'].map(lambda x: hasCat(x, cat)).sum()

s = pd.Series(catCount).sort_values(ascending=False)
print(f"popular categories: \n\n", s.head(30))
