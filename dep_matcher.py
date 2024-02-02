#!/usr/bin/env python3

import pandas as pd
from argparse import ArgumentParser
from ast import literal_eval
import glob
import os
import re
from rapidfuzz import process, fuzz
import logging
from pathlib import Path

log = logging.getLogger(__name__)

def get_files(data_path):
    files = glob.glob('{}/*.{}'.format(data_path, 'csv'))
    df = pd.concat([pd.read_csv(file, usecols=["Department", "Hospital", "Title", "Journal", "Year", "Author", "Number of Authors", "MeSH", "DOI", "ISSN", "Abstract"]) for file in files])
    return df

department_mapping = {
    'Gastroenterology': '소화기내과',
    'Cardiology': '순환기내과',
    'Pulmonology': '호흡기내과',
    'Nephrology': '신장내과',
    'Rheumatology': '류마티스내과',
    'Endocrinology': '내분비내과',
    'Infectious': '감염내과',
    'Hematology': '혈액종양내과',
    'Oncology': '혈액종양내과',
    'Internal': '내과',
    'Upper Gastrointestinal': '위장관외과(상부)',
    'Hepatobiliary': '간담췌외과',
    'Colorectal': '대장항문외과',
    'Endocrine': '유방내분비외과',
    'Vascular': '이식혈관외과',
    'Trauma': '중환자외상외과',
    # 'Surgery': '외과',
    'Pediatric': '소아외과',
    'Cardiothoracic': '심장혈관흉부외과',
    'Orthopedic': '정형외과',
    'Neurosurgery': '신경외과',
    'Plastic': '성형외과',
    'Obstetrics and Gynecology': '산부인과',
    'Pediatrics': '소아과',
    'Psychiatry': '정신건강의학과',
    'Neurology': '신경과',
    'Oral': '치과',
    'Ophthalmology': '안과',
    'Otorhinolaryngology': '이비인후과',
    'Dermatology': '피부과',
    'Urology': '비뇨의학과',
    'Rehabilitation': '재활의학과',
    'Physical': '재활의학과',
    'Family Medicine': '가정의학과',
    'Dentistry': '치과',
    'Laboratory Medicine': '진단검사의학과',
    'Radiation Oncology': '방사선종양학과',
    'Radiation': '방사선종양학과',
    'Anesthesiology': '마취통증의학과',
    'Pathology': '병리과',
    'Radiology': '영상의학과',
    'Emergency Medicine': '응급의학과',
    'Nuclear Medicine': '핵의학과',
    'Clinical Pharmacology': '임상약리학과',
    'Health Screening': '건진센터',
    'Thyroid': '갑상선센터',
    'International': '국제진료센터',
    'Care': '생활치료센터',
    'Screening': '선별진료소과(소)',
    'Transplant': '이식외과',
    'COVID-19': '코로나19'
}

# 240202 SYP Extend the department_mapping dictionary with a new dictionary from a file
def extend_dict(map_file):
    map = pd.read_csv(f'{map_file}')
    mapped = pd.DataFrame.from_dict(department_mapping, orient="index").reset_index()
    mapped.columns = ["ENG", "KOR"]
    department_map = mapped.merge(map)
    department_map['ENG'] = department_map['ENG'].map(lambda x: clean_text(x))
    department_map = department_map.set_index('ENG').to_dict()['KOR']
    return department_map

# 240202 SYP Clean the text by removing unwanted characters and words
def clean_text(text):
    text = text.lower()
    text = re.sub(r'\W', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip(' ')
    return text

# 240202 SYP Extract the department names from the data and create a key for each department
def get_keys(data_path):
    df = get_files(data_path)

    df['Keys'] = df['Department'].astype(str).map(lambda x: clean_text(x))

    # Define list of words to remove
    pop_list = ["department", "the", "of", "division", "medicine", "center", "and", "from", "a", "surgery"]

    # Remove unwanted words and create a tuple key for each department
    df['Keys'] = df['Keys'].apply(lambda x: tuple([word for word in x.split(' ') if word not in pop_list][0:1]))
    # Convert tuple to string for fuzzy matching
    df['Keys'] = df['Keys'].apply(lambda x: x[0] if x else '')
    
    return df

# 240202 SYP Match the department names to Korean names
def match_to_korean(org_name, data_path, out_path, map_file):

    # 240202 SYP If a map file is provided, use it to extend the department_mapping dictionary
    if map_file:
        department_map = extend_dict(map_file)
    else:
        department_map = department_mapping

    df = get_keys(data_path)

    threshold = 60
    dep_mapping = {}

    # 240202 SYP Iterate through the department names and match them to Korean names
    for item in df['Keys']:
        # 240202 SYP Get the best match from the department_mapping keys and the score
        best_match = process.extractOne(item, department_map.keys(), scorer=fuzz.ratio)
        if best_match[1] >= threshold:
            # 240202 SYP Get the matching Korean department name directly from the dictionary using the match
            dep_mapping[item] = department_map[best_match[0]]

    # 240202 SYP Create a new column 'KOR' in 'pub_df' based on the mapping
    df['KOR'] = df['Keys'].apply(lambda x: dep_mapping.get(x, None))
    df.loc[df['Department'].str.contains("Department of Surgery|Division of Surgery", case=False, na=False), 'KOR'] = "외과"

    df[['Department', 'KOR', 'DOI', 'Year', 'Journal', 'ISSN']].drop_duplicates().to_csv(f"{out_path}/{org_name}_KOR.csv", encoding="utf-8-sig", index = False)

    return df

def main():
    args = parsing()
    match_to_korean(args.org_name, args.data_path, args.out_path, args.map_file)

def parsing():
    """
    This part handles the commandline arguments
    """
    parser = ArgumentParser(description="")
    parser.add_argument("org_name",
                        help="Organization to filter PubMed results. Single input.")
    parser.add_argument("data_path",
                        type=Path, 
                        help="Data file folder. CSV files extracted from PubMed will be saved here.")
    parser.add_argument("--out_path",
                        dest="out_path",
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