#!/usr/bin/env python3

from argparse import ArgumentParser
import logging
import sys
from pathlib import Path
import if_ISSN_matcher
import pub_extractor
import dep_matcher
import os

def file_exists_check(filepath):
    """Check if the file exists and return True if it does."""
    return filepath.exists()

def main():

    parser = ArgumentParser(description="Script to process PubMed data for a specific organization.")
    parser.add_argument("org_name",
                        help="Organization to filter PubMed results. Single input.")
    parser.add_argument("start_year",
                        help="Start year to filter PubMed results.",
                        metavar="start_year")
    parser.add_argument("end_year",
                        help="End year to filter PubMed results.",
                        metavar="end_year")
    parser.add_argument("data_path",
                        type=Path, 
                        help="Data file folder. CSV files extracted from PubMed will be saved here.")
    parser.add_argument("--out_path",
                        type=Path,
                        dest="out_path",
                        help="Output folder. Resulting IF scores and department data will be saved here.",
                        default=os.getcwd())
    parser.add_argument("--map_file",
                    dest="map_file",
                    help="File containing matching English departments to Korean names.",
                    default=False)


    args = parser.parse_args()

    # 20240222 SYP Validate paths
    if not args.data_path.exists() or not args.data_path.is_dir():
        logging.error(f"Data path {args.data_path} does not exist or is not a directory.")
        sys.exit(1)
    
    if not args.out_path.exists():
        logging.info(f"Output path {args.out_path} does not exist. Creating directory.")
        args.out_path.mkdir(parents=True)

    #20240222 SYP 아래코드가 메인인듯#
    #총 개의 함수가 동작 됨 
    #1. pub_extractor --> 기관검색하여 저널명 DOI 진료과등의 정보
    #2. dep_matcher --> 영문진료과 한글 매칭하여 변환
    #3. ISSN_matcher --> ISSN 번호를 통해 IF데이터를 불러와주는 데이터 

    try:
        #20240202 cyw 1.기관검색하여 저널명 DOI 진료과등의 정보
        for year in range(int(args.start_year), int(args.end_year)):
            for month in list(range(1, 13)):
                pub_extractor.pubmed_extraction(args.org_name, args.data_path, year, month)
        #20240202 cyw 2.영문진료과 한글 매칭하여 변환        
        dep_output_file = args.out_path / f"{args.org_name}_KOR.csv"
        if not file_exists_check(dep_output_file):
            dep_matcher.match_to_korean(args.data_path, args.org_name, args.out_path, args.map_file)

        #20240202 cyw 3.ISSN 번호를 통해 IF데이터를 불러와주는 데이터
        if_output_file = args.out_path / f"{args.org_name}_IF_stats.csv"
        if not file_exists_check(if_output_file):
            if_ISSN_matcher.output_IF(args.org_name, args.out_path)
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f"{Path(__file__).parent}/pubmed_if.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    main()