#!/usr/bin/env python3

from argparse import ArgumentParser
from ast import literal_eval
from bs4 import BeautifulSoup
import requests
import urllib.request, urllib.parse, urllib.error
import numpy as np
import pandas as pd
import metapub
from metapub import PubMedFetcher, PubMedAuthor
import sys
from Bio import Entrez
from pathlib import Path
import os


import logging
import re

log = logging.getLogger(__name__)

def search_pubmed(org_name, year, month):
    """
    Searches PubMed for papers that meet the org and date criteria.
    """
    Entrez.email = "riley@grkcon.com"
    handle = Entrez.esearch(db="pubmed", 
                            term=f"{org_name}[affil] AND {year}/{month}/01:{year}/{str(int(month) + 1)}/01[dp]", 
                            retmax=9999)
    record = Entrez.read(handle)
    return record['IdList']

def fetch_pubmed_details(ids):
    """
    Returns pubmed details from ids.
    """
    try:
        handle = Entrez.efetch(db="pubmed", id=ids, rettype="medline", retmode="text")
        records = handle.read()
        return records
    except Exception as e:
        print(f"Error fetching PubMed details: {e}")
        return ''

def pubmed_extraction(org_name, data_file, year, month):
    """
    Extracts data from PubMed and saves it to a CSV file.
    """

    idlist = search_pubmed(org_name, year, month)

    fetch = PubMedFetcher()

    citation_list = []

    n = 0
    for id in idlist:
        article = fetch.article_by_pmid(id)
        mesh_terms = article.mesh.values()
        records = fetch_pubmed_details(id)
        affiliation = re.findall(r'AD  - (.+)', records)

        # Extract the affiliation
        full_affil = [''.join(aff) for aff in affiliation]
        corres_affil = [a for a in full_affil if "@" in a]

        # Extract mesh terms if available
        mesh = [v['descriptor_name'] for v in mesh_terms] if mesh_terms else []

        abstract = [article.abstract if article.abstract else ""]

        n += 1
        print(f"-----------{year}/{month} Completed: {n}/{len(idlist)}!-----------")

        try:
            auth_affil = ''.join(affiliation[0])
            # Check if the organization name is in the affiliation
            if org_name in auth_affil:
                department = auth_affil.split(',')[0]
                hospital = [a for a in auth_affil.split(',') if org_name in a]
                citation = [department, *hospital, article.title, article.journal, article.year, article.authors[0], len(article.authors), ", ".join(mesh), article.doi, article.issn, ", ".join(abstract)]
                citation_list.append(citation)
            elif org_name in corres_affil[0]:
                department = corres_affil[0].split(',')[0]
                hospital = [a for a in corres_affil[0].split(',') if org_name in a]
                citation = [department, *hospital, article.title, article.journal, article.year, article.authors[0], len(article.authors), ", ".join(mesh), article.doi, article.issn, ", ".join(abstract)]
                citation_list.append(citation)
        except IndexError:
            continue

    citation_df = pd.DataFrame(citation_list)[1:10]
    citation_df.columns = ["Department", "Hospital", "Title", "Journal", "Year", "Author", "Number of Authors", "MeSH", "DOI", "ISSN", "Abstract"]
    citation_df.to_csv(f"{data_file}/{org_name}_{year}_{month}-{str(int(month) + 1)}.csv", index=False, encoding="utf-8-sig")


def main():
    args = parsing()
    pubmed_extraction(args)

def parsing():
    """
    This part handles the commandline arguments
    """
    parser = ArgumentParser(description="")
    parser.add_argument("org_name",
                        help="Organization to filter PubMed results. Single input.")
    parser.add_argument("start_year",
                        help="Start year to filter PubMed results.",
                        metavar="start_year")
    parser.add_argument("data_path",
                        type=Path, 
                        help="Data file folder. CSV files extracted from PubMed will be saved here.")
    parser.add_argument("--out_path",
                        type=Path, 
                        help="Output folder. Resulting IF scores and department data will be saved here.",
                        default=os.getcwd())
    parser.add_argument("--map_file",
                    dest="map_file",
                    help="File containing matching English departments to Korean names.",
                    default=False)
    return parser.parse_args()


if __name__== "__main__":
    main()