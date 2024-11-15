#!pip install psycopg2-binary
#!pip3 install sqlalchemy psycopg2 pandas
import pandas as pd
import psycopg2
import spacy
import os, sys
from collections import Counter

sys.path.append(os.path.abspath(os.path.join(os.path.dirname('__file__'), 'ner_models/spacy')))

from do_ner_spacy import *
from lang_detector import *

sys.path.append(os.path.abspath(os.path.join(os.path.dirname('__file__'), 'db')))
from do_query_to_db import * 

df = do_query_to_local_db(limit = 100, offset=0, db="dj")
df.describe(include=['object'])
