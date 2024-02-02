#!/usr/bin/env python3

import pandas as pd
from argparse import ArgumentParser
from ast import literal_eval
import os
from pathlib import Path

import logging

log = logging.getLogger(__name__)

# 240202 SYP Create a dictionary of ISSN and IF scores
def create_dictionary():
    cwts = pd.read_csv("/mnt/c/Users/박서윤/OneDrive/GRK/journal_crawling/CWTS.csv")
    cwts_year = pd.pivot_table(cwts, values = 'SNIP', index = ['Source title', 'Electronic ISSN'], columns = "Year").reset_index()

    years = [str(i) for i in range(1999, 2023)]

    cwts_year.columns = list(["Full Journal", "ISSN", *years])
    cwts_year = cwts_year[["Full Journal", "ISSN", "2022"]]
    dictionary = cwts_year[["2022", "ISSN"]].set_index("ISSN").to_dict()['2022']
    return dictionary

# 240202 SYP Add IF scores to the dataframe
def output_IF(org_name, out_path):
    dictionary = create_dictionary()

    ######20240202 cyw ISSN 번호에 따라 IF 번호 저장
    df = pd.read_csv(f"{out_path}/{org_name}_KOR.csv", encoding="utf-8-sig")
    df['IF'] = df['ISSN'].apply(lambda x: dictionary.get(x, None))

    df[['Department', 'KOR', 'Year', 'Journal', 'DOI', 'IF']].drop_duplicates().to_csv(f"{out_path}/{org_name}_IF_stats.csv", encoding='utf-8-sig', index = False)

def main():
    args = parsing()
    output_IF(args.org_name, args.out_path)

def parsing():
    """
    This part handles the commandline arguments
    """
    parser = ArgumentParser(description="")
    parser.add_argument("org_name",
                        help="Organization to filter PubMed results. Single input.")
    parser.add_argument("--out_path",
                        type=Path, 
                        help="Output folder. Resulting IF scores and department data will be saved here.",
                        default=os.getcwd())
    return parser.parse_args()

if __name__== "__main__":
    main()