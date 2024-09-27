import json, requests, re, datetime, time, jwt
import pandas as pd
import psycopg2
import numpy as np
import logging
import math
from datetime import date
from fastapi.responses import JSONResponse
from cloudwatch import cloudwatch
from thefuzz import fuzz
from config import * 
from screenerutils import screener_sidebar
import re
from cleanco import basename
# from screenerutils import screener_sidebar, fields_to_convert_map, range_convert

logs = logging.basicConfig(filename='terminal2_¸and_search2.log', encoding='utf-8', level=logging.INFO)

logging.basicConfig(level=logging.INFO, filename='terminal2_and_search2.log')
logger = logging.getLogger("my_logger")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler) 

handler = cloudwatch.CloudwatchHandler(
 log_group = LOG_GROUP,
 access_id = ACCESS_ID, 
 access_key = ACCESS_TOKEN,
 region = REGION
)

handler.setFormatter(formatter)
#Set the level
logger.setLevel(logging.DEBUG)
#Add the handler to the logger
logger.addHandler(handler)  


import stripe
stripe.api_key = 'sk_test_51PFybFSDorRA6LtLkAMNNlzpmANBfzHQVWXji0sVy5WshBRXVIwUMBxNamXNiJenJoSd7KZdQKPUuEfgyngFv52B000pM1cjCL'


api_key = "*&^V&XvTx7ypbVC5e*$^VUE*6REXIBvoV^Ox6Pobvg7x^Cgv"
# VESPA_ENDPOINT = "http://52.38.21.237:8080"
PRODUCTION_VESPA_ENDPOINT = "http://34.216.247.54:8080"
FINANCIAL_VESPA_ENDPOINT = "http://34.211.12.161:8080"
MARCH_VESPA_ENDPOINT = "http://localhost:8080"
VESPA_ENDPOINT = "http://54.69.191.240:8080"
central_server_base_url = "https://search2-api.brightquery.com/api/"
maxEditDistance='{maxEditDistance:2}'
central_system_header =  {
    'Content-Type': 'application/json',
    'api-key':api_key
    }

terminal_yql = "select bq_organization_id, bq_organization_address1_rdi, bq_organization_ein, bq_legal_entity_parent_status, bq_organization_name, bq_organization_structure, bq_organization_company_type, bq_organization_ticker, bq_organization_public_indicator, bq_organization_legal_name, bq_legal_entity_id, bq_legal_entity_name, bq_organization_address1_type, bq_organization_address1_line_2, bq_organization_address1_city, bq_organization_address1_state, bq_organization_address1_zip5, bq_legal_entity_jurisdiction_code, bq_organization_date_founded, bq_organization_current_status, bq_organization_active_indicator, bq_organization_address1_line_1, bq_organization_address1_line_2, bq_employment_mr, bq_score, bq_organization_irs_sector_name, bq_revenue_mr, bq_organization_cik, bq_organization_lei, bq_organization_legal_address1_line_1, bq_organization_legal_address1_line_2, bq_organization_legal_address1_city, bq_organization_legal_address1_state, bq_organization_legal_address1_zip5, bq_organization_website, bq_organization_sector_name, bq_organization_sector_code, bq_organization_naics_sector_name, bq_organization_naics_sector_code, bq_organization_naics_name, bq_legal_entity_children_count, bq_organization_naics_code, bq_organization_current_status, bq_legal_entity_current_status, bq_legal_entity_active_indicator, bq_organization_subsector_name, bq_tickers_related from terminal_screener where"
search_yql = "select bq_organization_id, bq_organization_ein, bq_legal_entity_parent_status, bq_organization_name, bq_organization_structure, bq_organization_company_type, bq_organization_ticker, bq_organization_public_indicator, bq_organization_legal_name, bq_legal_entity_id, bq_organization_address1_type, bq_organization_address1_line_2, bq_organization_address1_city, bq_organization_address1_state, bq_organization_address1_zip5, bq_legal_entity_jurisdiction_code, bq_organization_date_founded, bq_organization_current_status, bq_organization_active_indicator, bq_organization_address1_line_1, bq_organization_address1_line_2, bq_employment_mr, bq_score, bq_organization_irs_sector_name, bq_revenue_mr, bq_organization_cik, bq_organization_lei, bq_organization_legal_address1_line_1, bq_organization_legal_address1_line_2, bq_organization_legal_address1_city, bq_organization_legal_address1_state, bq_organization_legal_address1_zip5, bq_organization_website, bq_organization_sector_name, bq_organization_sector_code, bq_organization_naics_sector_name, bq_organization_naics_sector_code, bq_organization_naics_name, bq_legal_entity_children_count, bq_organization_naics_code, bq_legal_entity_current_status, bq_legal_entity_active_indicator, bq_organization_subsector_name, bq_organization_valuation, bq_revenue_growth_yoy_mr, bq_net_income_mr, bq_gross_profit_mr, bq_organization_address1_state_name, bq_organization_address1_location, bq_organization_address1_latitude, bq_organization_address1_longitude, bq_industry_name, bq_cong_district_cd, bq_cong_district_name, bq_cong_district_id, bq_cong_district_representative_name_from_listing, bq_cong_district_representative_party_name, bq_confidence_score, bq_organization_lfo, bq_organization_address1_country, bq_public_indicator, bq_organization_jurisdiction_code, bq_cong_district_officer_phone from terminal_screener where"
# org_terminal_yql = "select bq_organization_id,bq_organization_name,bq_organization_legal_name,bq_organization_active_indicator,bq_organization_website,bq_organization_cik,bq_organization_ticker,bq_ticker_parent,bq_organization_ein,bq_organization_lei,bq_organization_linkedin_url,bq_organization_address1_line_1,bq_organization_address1_city,bq_organization_address1_state,bq_organization_address1_state_name,bq_organization_address1_zip5,bq_organization_address1_county_name,bq_organization_address1_country,bq_organization_website,bq_tickers_related,bq_organization_naics_sector_name,bq_revenue_mr,bq_employment_mr,bq_legal_entity_id, bq_legal_entity_name,bq_legal_entity_address1_line_1, bq_legal_entity_address1_line_2, bq_legal_entity_address1_city, bq_legal_entity_address1_state, bq_legal_entity_address1_zip5, bq_organization_address_summary from terminal_screener where"
# org_terminal_yql= "select bq_organization_id,bq_organization_structure,bq_organization_member_count,bq_organization_name,bq_organization_legal_name,bq_organization_jurisdiction_code,bq_organization_company_number,bq_organization_lfo,bq_organization_date_founded,bq_organization_active_indicator,bq_organization_address1_type,bq_organization_address1_valid_indicator,bq_organization_address1_line_1,bq_organization_address1_city,bq_organization_address1_state,bq_organization_address1_state_name,bq_organization_address1_county_fips,bq_organization_address1_county_name,bq_organization_address1_country,bq_organization_address1_zip5,bq_organization_address1_latitude,bq_organization_address1_longitude,bq_organization_address1_rdi,bq_organization_address1_cbsa_code,bq_organization_address1_location,bq_organization_address_summary,bq_legal_entity_address_summary,bq_organization_website,bq_organization_ein,bq_organization_nonprofit_indicator,bq_organization_public_indicator,bq_organization_de_ultimate_parent_indicator,bq_organization_irs_industry_name,bq_organization_subsector_code,bq_organization_subsector_name,bq_organization_naics_code,bq_organization_naics_name,bq_organization_naics_sector_code,bq_organization_naics_sector_name,bq_organization_company_type,bq_organization_legal_address1_type,bq_organization_legal_address1_valid_indicator,bq_organization_legal_address1_line_1,bq_organization_legal_address1_city,bq_organization_legal_address1_state,bq_organization_legal_address1_zip5,bq_organization_legal_address1_zip_type,bq_organization_legal_address1_summary,bq_organization_legal_address1_rdi,bq_score,bq_sp500_indicator,bq_legal_entity_id,bq_legal_entity_name,bq_legal_entity_jurisdiction_code,bq_legal_entity_parent_status,bq_legal_entity_immediate_children_ids,bq_legal_entity_immediate_children_count,bq_legal_entity_children_ids,bq_legal_entity_children_count,bq_legal_entity_immediate_establishment_ids,bq_legal_entity_immediate_establishment_count,bq_legal_entity_irs_taxlien_indicator,bq_legal_entity_address1_type,bq_legal_entity_address1_valid_indicator,bq_legal_entity_address1_line_1,bq_legal_entity_address1_city,bq_legal_entity_address1_state,bq_legal_entity_address1_state_name,bq_legal_entity_address1_county_fips,bq_legal_entity_address1_county_name,bq_legal_entity_address1_zip5,bq_legal_entity_address1_latitude,bq_legal_entity_address1_longitude,bq_legal_entity_address1_rdi,bq_legal_entity_current_status,bq_legal_entity_active_indicator,bq_organization_linkedin_url,bq_organization_crunchbase_url,bq_organization_irs_industry_code,bq_organization_sector_code,bq_organization_sector_name,bq_organization_year_founded,bq_business_code,bq_public_indicator,bq_industry_name,bq_confidence_score,bq_organization_ofac_indicator,bq_organization_irs_taxlien_indicator,bq_employment_mr,bq_revenue_mr,bq_tickers_related,bq_organization_eins_related from terminal_screener where "
# org_terminal_yql="select * from terminal_screener where "
# org_terminal_yql = "select * from terminal_screener where"


def key_check(source:dict, data_to_check):
    for i in data_to_check:
        if source.get(i,None) in [None,""]:
            return True
    return False

def remove_and_from_end(input_string):
    if input_string.lower().rstrip().endswith('and'):
        return input_string[:-3].rstrip()
    elif input_string.lower().rstrip().endswith('or'):
        return input_string[:-3].rstrip()
    elif input_string.lower().rstrip().endswith('where'):
        return input_string[:-6].rstrip()
    else:
        return input_string

def pagination(data_list, page_number, page_size):
    total_elements = len(data_list)
    total_pages = (total_elements + page_size - 1) // page_size  # Calculate total pages

    if page_number < 1 or page_number > total_pages:
        return "Invalid page number"
    
    start_index = (page_number - 1) * page_size
    end_index = min(start_index + page_size, total_elements)

    return data_list[start_index:end_index]

def get_next_n_elements(lst, start_index, n):
    if start_index < 0 or start_index >= len(lst):
        return []  # Return an empty list if the start index is out of range
    
    return lst[start_index:start_index + n]
# ------------------------------------------------------------------------------------------------------------------------

def officer_details(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field,search_product, user_id, bq_organization_ticker, bq_organization_lei, bq_legal_entity_parent_status, bq_legal_entity_id, bq_organization_id):
    search_endpoint = f'{VESPA_ENDPOINT}/search/'
    
    if bq_legal_entity_id:
        bq_legal_entity_id = bq_legal_entity_id.strip()
        if bq_legal_entity_parent_status == 'Child':
            yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_parent_status contains '{bq_legal_entity_parent_status}' and bq_legal_entity_id contains '{bq_legal_entity_id}'"
        else:
            # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_parent_status contains '{bq_legal_entity_parent_status}' and bq_legal_entity_id contains '{bq_legal_entity_id}' and bq_legal_entity_active_indicator contains true"
            yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}'"
            # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_parent_status contains 'Ultimate Parent' and bq_legal_entity_id contains '{bq_legal_entity_id}'"
        # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_id contains '{bq_legal_entity_id}' order by bq_officer_full_name asc"
        # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' order by bq_officer_full_name asc"
    else:
        if ' ' in query:
            query1 = query.split()
            query1 = ' '.join(reversed(query1))
            yql = f"select * from bq_officers where (bq_officer_full_name contains '{query}' or bq_officer_full_name contains '{query1}')"
        else:
            yql = f"select * from bq_officers where bq_officer_full_name contains '{query}'"
    
    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
                response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
            if 'bq_organization_sector_name' in filter.keys():
                filter['bq_organization_naics_sector_name'] = filter.pop('bq_organization_sector_name')
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        # print(12121212121212121212,key)
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} AND ({remove_and_from_end(final)}) AND"
                        # print(2353453453453456546567,yql)
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f"{key} contains '{v}' OR "
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} AND {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    
    if not bq_legal_entity_id:
        if orderby:
            if orderby == 'bq_officer_full_name':
                orderbyField = 'bq_officer_full_name'
            elif orderby == 'bq_organization_name':
                orderbyField = 'bq_organization_name'
            else:
                orderbyField = 'bq_organization_name'
            order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            yql = f"{yql} order by {orderbyField} {order}"
        
        else:
            orderbyField = 'bq_organization_name'
            # order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            order = 'asc'
            yql = f"{yql} order by {orderbyField} {order}"
    # print("SSSSSSSSS= ", yql)
    logger.info(f'YQL: {yql} \n Limit: {limit}')
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        # "timeout":20,
        "format": "json"
    }
    response = requests.get(search_endpoint, params=params).json()    
    response = {"response":response,"status":200}

    return response

def search_ticker_prefix(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, request_origin, search_product, user_id=None):
    try:
        hits = 20
        limit=50
        search_endpoint = f"{VESPA_ENDPOINT}/search/"
        if request_origin == "terminal":
            yql = terminal_yql
        elif request_origin =="external":
            yql = f"SELECT  {','.join(QUERY_FIELDS[search_product])} FROM terminal_screener where "
        else:
            yql = "select * from terminal_screener where"

        prefix = '{prefix: true}'
        if field == 'bq_organization_ticker':
            yql = f"{yql} ({field} contains ({prefix}'{query}'))  and (bq_legal_entity_parent_status contains 'Ultimate Parent' OR bq_legal_entity_parent_status contains 'Sole' OR bq_organization_structure contains 'Single-entity organization')"  
        
        elif field == 'bq_ticker_parent':
            yql = f"{yql} ({field} contains ({prefix}'{query}')) and (bq_legal_entity_parent_status contains 'Ultimate Parent' OR bq_legal_entity_parent_status contains 'Sole' OR bq_organization_structure contains 'Single-entity organization')"  

        else:
            yql = f"{yql}  bq_tickers_related contains sameElement(bq_ticker_related_individual contains ({prefix}'{query}')) and (bq_legal_entity_parent_status contains 'Ultimate Parent' OR bq_legal_entity_parent_status contains 'Sole' OR bq_organization_structure contains 'Single-entity organization')  "
            # yql = f'select * from terminal_screener where bq_tickers_related matches "{query}" and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")  '

        if yql:
            if filter:
                try:
                    filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                    filter = json.loads(filter.replace("'", "\""))
                except json.JSONDecodeError as e:
                    response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                    return response
                    # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
                for key, val in filter.items():
                    if len(val) >= 1:
                        if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                            final = ''
                            for items in val:
                                itm = ''
                                for i in items:
                                    itm = f"{itm} {key} {i} AND"
                                itm = remove_and_from_end(itm)
                                itm = f'({itm})'
                                final = f"{final} {itm} OR"
                            yql = f"{remove_and_from_end(yql)} AND ({remove_and_from_end(final)}) AND"
                        else:
                            if len(val) > 1:
                                yql_part = ''
                                for v in val:
                                    yql_part = yql_part + f"{key} contains '{v}' OR "
                                yql_part = remove_and_from_end(yql_part)
                                yql_part = f"({yql_part})"
                                yql = f"{yql} {yql_part} and"
                            elif len(val) == 0:
                                pass
                            else:
                                if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                    yql = remove_and_from_end(yql)
                                    yql = f"{yql} AND {key} contains '{val[0]}' AND"
                                elif yql.lower().rstrip().endswith('where'):
                                    yql = f"{yql} {key} contains '{val[0]}' AND"
                                else:
                                    yql = f"{yql} AND {key} contains '{val[0]}' AND"
        yql = remove_and_from_end(yql)
        # if orderby:
        order_by_map={"bq_organization_name":"bq_organization_name",
            "bq_revenue_mr":"bq_revenue_mr",
            "bq_employment_mr":"bq_employment_mr",
            "bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr",
            "bq_organization_isactive":"bq_organization_active_indicator",
            "bq_score":"bq_score",
            "bq_organization_valuation":"bq_organization_valuation", 
            "bq_organization_structure":"bq_organization_structure", 
            "bq_organization_valuation":"bq_organization_valuation", 
            "bq_organization_address1_line_1":"bq_organization_address1_line_1", 
            "bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code"}
            
        orderbyField = order_by_map.get(orderby,"bq_revenue_mr")

        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        if not orderby:
            if orderbyField == 'bq_revenue_mr':
                order = 'desc'
            
        yql = f"{yql} order by {orderbyField} {order}"
        
        # print('search_ticker_prefix',field,"yql =", yql)
        
        # print(1111111,field,"yql =", yql)
        logger.info(f'YQL: {yql}')
        
        params = {
            'yql': yql,
            # 'query': query,
            'filter': filter,
            'offset': offset,
            'ranking': ranking,
            'limit': limit,
            'type': 'all',
            'hits': str(hits),
            "format": "json",
        }

        response = requests.get(search_endpoint, params=params).json()
        response['yql']=params
        # response.raise_for_status()

        response = {"response":response,"status":200}
        return response

    except requests.RequestException as e:
        response = {"response":{"error": "An error occurred while processing the search request.", "details": str(e)},"status":500}
        return response
    
    except Exception as e:
        response = {"response":{"error": "An unexpected error occurred.", "details": str(e)},"status":500}
        return response

def search_ticker_matches(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, request_origin, search_product):
    try:
        search_endpoint = f"{VESPA_ENDPOINT}/search/"
        hits = 20
        limit=50
        org_terminal_yql = f"SELECT  {','.join(QUERY_FIELDS[search_product])} FROM terminal_screener where "        

        prefix = '{prefix: true}'
        if field == 'bq_organization_ticker':
            yql = f'{org_terminal_yql} {field} contains "{query}" and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")'
        
        elif field == 'bq_ticker_parent':
            yql = f"{org_terminal_yql} {field} contains '{query}' and (bq_legal_entity_parent_status contains 'Ultimate Parent' OR bq_legal_entity_parent_status contains 'Sole' OR bq_organization_structure contains 'Single-entity organization')"  
        else:
            # yql = f'select * from  terminal_screener where bq_tickers_related matches "{query}" and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")'
            yql = f'{org_terminal_yql} bq_tickers_related contains sameElement(bq_ticker_related_individual contains "{query}") and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")' 



        # prefix = '{prefix: true}'
        # if field == 'firmo_bq_ticker':
        #     field = 'bq_organization_ticker'
        #     if request_origin =="external":
        #         yql = f' {org_terminal_yql}  {field} contains "{query}" and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")'
        #     else:
        #         yql = f'select * from  terminal_screener where {field} contains "{query}" and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")'   
        # else:
        #     # yql = f'select * from  terminal_screener where bq_tickers_related matches "{query}" and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")'
        #     yql = f'select * from terminal_screener where bq_ticker_related_individual contains sameElement(bq_ticker_related_individual contains "{query}") and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")' 
        
        if yql:
            if filter:
                try:
                    filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                    filter = json.loads(filter.replace("'", "\""))
                except json.JSONDecodeError as e:
                    response ={"response":{"error": "Invalid filter format. Please provide a valid JSON object."}, "status":400}
                    return response
                    # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
                for key, val in filter.items():
                    if len(val) >= 1:
                        if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                            final = ''
                            for items in val:
                                itm = ''
                                for i in items:
                                    itm = f"{itm} {key} {i} AND"
                                itm = remove_and_from_end(itm)
                                itm = f'({itm})'
                                final = f"{final} {itm} OR"
                            yql = f"{yql} ({remove_and_from_end(final)}) AND"
                        else:
                            if len(val) > 1:
                                yql_part = ''
                                for v in val:
                                    yql_part = yql_part + f"{key} contains '{v}' OR "
                                yql_part = remove_and_from_end(yql_part)
                                yql_part = f"({yql_part})"
                                yql = f"{yql} {yql_part} and"
                            elif len(val) == 0:
                                pass
                            else:
                                if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                    yql = remove_and_from_end(yql)
                                    yql = f"{yql} AND {key} contains '{val[0]}' AND"
                                elif yql.lower().rstrip().endswith('where'):
                                    yql = f"{yql} {key} contains '{val[0]}' AND"
                                else:
                                    yql = f"{yql} AND {key} contains '{val[0]}' AND"
        yql = remove_and_from_end(yql)
        if orderby:
            if orderby == 'bq_organization_structure':
                orderbyField = 'bq_organization_structure'
            elif orderby == 'bq_organization_name':
                orderbyField = 'bq_organization_name'
            elif orderby == 'bq_organization_address1_line_1':
                orderbyField = 'bq_organization_address1_line_1'
            elif orderby == 'bq_organization_jurisdiction_code':
                orderbyField = 'bq_organization_jurisdiction_code'
            elif orderby == 'bq_revenue_mr':            
                orderbyField = 'bq_revenue_mr'
            elif orderby == 'bq_employment_mr':            
                orderbyField = 'bq_employment_mr'
            else:
                orderbyField = 'bq_revenue_mr'
        
            order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            yql = f"{yql} order by {orderbyField} {order}"
        else:
            orderbyField = 'bq_revenue_mr'
        
            order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            yql = f"{yql} order by {orderbyField} {order}"
        
        print("yql =", yql)
        
        params = {
            'yql': yql,
            # 'query': query,
            'filter': filter,
            'offset': offset,
            'ranking': ranking,
            'limit': limit,
            'type': 'all',
            'hits': hits,
            "format": "json",
        }

        response = requests.get(search_endpoint, params=params)
        response.raise_for_status()
        response = {"response":response.json(), "status":200}
        return response
        # return JSONResponse(content=response.json(), status_code=200)

    # except requests.RequestException as e:
    #     response = {"response":{"error": "An error occurred while processing the search request.", "details": str(e)},"status":500}
    #     return response
        # return JSONResponse(content={"error": "An error occurred while processing the search request.", "details": str(e)}, status_code=500)
    
    except Exception as e:
        response = {"response":{"error": "An unexpected error occurred.", "details": str(e)}, "status":400}
        return response
        # return JSONResponse(content={"error": "An unexpected error occurred.", "details": str(e)}, status_code=500)

def search_by_address(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, request_origin, search_product, ult_selection):
    hits = 20
    
    search_endpoint = f'{VESPA_ENDPOINT}/search/'
    if request_origin == "terminal":
        yql = terminal_yql
    elif request_origin == "external":
        yql = f"SELECT  {','.join(QUERY_FIELDS[search_product])} FROM terminal_screener where "
    else:
        yql = search_yql
    # yql = "select bq_organization_id, bq_organization_ein, bq_legal_entity_parent_status, bq_organization_name, bq_organization_structure, bq_organization_company_type, bq_organization_ticker, bq_organization_public_indicator, bq_organization_legal_name, bq_legal_entity_id, bq_organization_address1_type, bq_organization_address1_line_2, bq_organization_address1_city, bq_organization_address1_state, bq_organization_address1_zip5, bq_legal_entity_jurisdiction_code, bq_organization_date_founded, bq_organization_current_status, bq_organization_active_indicator, bq_organization_address1_line_1, bq_organization_address1_line_2, bq_employment_mr, bq_score, bq_organization_irs_sector_name, bq_revenue_mr, bq_organization_cik, bq_organization_lei, bq_organization_legal_address1_line_1, bq_organization_legal_address1_line_2, bq_organization_legal_address1_city, bq_organization_legal_address1_state, bq_organization_legal_address1_zip5, bq_organization_website, bq_organization_sector_name, bq_organization_sector_code, bq_organization_naics_sector_name, bq_organization_naics_sector_code, bq_organization_naics_name, bq_legal_entity_children_count, bq_organization_naics_code, bq_organization_current_status, bq_legal_entity_current_status, bq_legal_entity_active_indicator from terminal_screener where"

    if query:
        query = query.replace(',','')
        query = query.split()
        address_search_yql = ''
       
        for query in query:
            if ult_selection == "orgAddress":
                # address_search_yql = address_search_yql + f"( bq_organization_address1_line_1 contains '{query}' OR bq_organization_address1_line_2 contains '{query}' OR  bq_organization_address1_state contains '{query}' OR bq_organization_address1_state_name contains '{query}' OR bq_organization_address1_zip5 contains '{query}' OR bq_organization_address1_city contains '{query}' OR bq_organization_legal_address1_state contains '{query}' OR bq_organization_legal_address1_zip5 contains '{query}' OR bq_organization_legal_address1_line_1 contains '{query}' OR bq_organization_legal_address1_city contains '{query}') AND "
                address_search_yql = address_search_yql + f"(bq_organization_address_summary contains '{query}') AND "
            else:                
                if request_origin=='terminal':
                    yql = yql.replace(" from terminal_screener where",", ")
                    yql = yql + "bq_legal_entity_address1_line_1, bq_legal_entity_address1_line_2, bq_legal_entity_address1_city, bq_legal_entity_address1_state, bq_legal_entity_address1_zip5 from terminal_screener where"
                # code to search in legal entity - yatin
                # address_search_yql = address_search_yql + f"( bq_legal_entity_address1_line_1 contains '{query}' OR bq_legal_entity_address1_line_2 contains '{query}' OR bq_legal_entity_address1_city contains '{query}' OR bq_legal_entity_address1_state contains '{query}' OR bq_legal_entity_address1_zip5 contains '{query}' OR bq_organization_legal_address1_line_1 contains '{query}' OR bq_organization_legal_address1_line_2 contains '{query}') AND "
                address_search_yql = address_search_yql + f"(bq_legal_entity_address_summary contains '{query}') AND "

        address_search_yql= address_search_yql[:-4].rstrip() 
        if yql.lower().rstrip().endswith('and'):
            # yql = f'{yql} and ( bq_organization_address1_line_1 contains "{query}" OR bq_organization_address1_line_2 contains "{query}" OR  bq_organization_address1_state contains "{query}" OR bq_organization_address1_state_name contains "{query}" OR bq_organization_address1_zip5 contains "{query}" OR bq_organization_address1_city contains "{query}" ) and'
            yql = f"{yql} and ( {address_search_yql} ) and"
        elif yql.lower().rstrip().endswith('where'):
            # yql = f'{yql} {field} contains "{query}" and'
            # yql = f'{yql} ( bq_organization_address1_line_1 contains "{query}" OR bq_organization_address1_line_2 contains "{query}" OR  bq_organization_address1_state contains "{query}" OR bq_organization_address1_state_name contains "{query}" OR bq_organization_address1_zip5 contains "{query}" OR bq_organization_address1_city contains "{query}" ) and'
            yql = f"{yql} ( {address_search_yql} ) and"
        else:
            # yql = f'{yql} and {field} contains "{query}" and'
            # yql = f'{yql} and ( bq_organization_address1_line_1 contains "{query}" OR bq_organization_address1_line_2 contains "{query}" OR  bq_organization_address1_state contains "{query}" OR bq_organization_address1_state_name contains "{query}" OR bq_organization_address1_zip5 contains "{query}" OR bq_organization_address1_city contains "{query}" ) and'
            yql = f"{yql} and ( {address_search_yql} ) and"                  

    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                response ={"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
                # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f"{key} contains '{v}' OR "
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    if orderby:
        order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr","bq_organization_isactive":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation"}
        orderbyField = order_by_map.get(orderby,"bq_revenue_mr")        
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        if ult_selection == "orgAddress":
            yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization')) order by {orderbyField} {order}"
        else:
            yql = f"{yql}  order by {orderbyField} {order}"
    # print(f"\n\nssssssss before = {yql}")
    else:
        if (request_origin =="terminal") | (request_origin =="external"):
            if ult_selection == "orgAddress":
                yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
            else:
                pass
        else:
            yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"

    # print(f"\n\n\nssssss after = {yql}")
    # print(f"\n\n\nssssss final = {yql}")
    logger.info(f'YQL: {yql}')
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }
    #logger.info(f'Endpoint Params: {params}')
    response = requests.get(search_endpoint, params=params).json()
    response['yql']=yql
    response = {"response":response, "status":200}
    return response


# def unique_values(query = None, schema="terminal_screener",yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby='bq_revenue_mr', isAsc=True, field=None):
def unique_values(*args):

    try:    
        # result = {}
        # search_endpoint = f"{VESPA_ENDPOINT}/search/"
        # for field in ["bq_organization_address1_state_name", "bq_organization_address1_cbsa_name", "bq_organization_irs_sector_name", "bq_organization_subsector_name"]:
        #     yql = f"select {field} from terminal_screener where true | all(group({field}) max(5000) each(output(count())))"
        #     params = {
        #       'yql': yql,
        #       'limit': 20,
        #       'type': 'all',
        #       'hits': 20,
        #       "format": "json",
        #     }
        #     response = requests.get(search_endpoint, params=params)

        #     data = response.json()
        #     data = data['root']['children'][0]['children'][0]['children']
        #     unique_fields = list()
        #     for val in data:
        #         # unique_fields.append({"value":val['value'], "count":val['fields']['count()']})
        #         unique_fields.append(val['value'])
        #     result.update({field: unique_fields})
        # result = {"bq_legal_entity_jurisdiction_code":["","US_AK","US_AL","US_AR","US_AZ","US_CA","US_CO","US_CT","US_DC","US_DE","US_FL","US_GA","US_HI","US_IA","US_ID","US_IL","US_IN","US_KS","US_KY","US_LA","US_MA","US_MD","US_ME","US_MI","US_MN","US_MO","US_MS","US_MT","US_NC","US_ND","US_NE","US_NH","US_NJ","US_NM","US_NV","US_NY","US_OH","US_OK","US_OR","US_PA","US_RI","US_SC","US_SD","US_TN","US_TX","US_UT","US_VA","US_VT","US_WA","US_WI","US_WV","US_WY"],"bq_organization_address1_state_name":["","Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Ontario","Oregon","Pennsylvania","Puerto Rico","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming","Alberta","America Samoa","American Samoa","British Columbia","District Of Columbia","Foreign Countries","Guam","Honduras","Manitoba","Mexico","New Brunswick","Newfoundland","Northern Mariana Islands","Northwest Territory","Nova Scotia","Panama Canal Zone","Prince Edward Island","Quebec","Saskatchewan","U.S. Virgin Islands","Virgin Islands","Yukon"],"bq_organization_address1_cbsa_name":["","Aberdeen, WA","Adrian, MI","Akron, OH","Alamogordo, NM","Albany-Schenectady-Troy, NY","Albuquerque, NM","Alexandria, MN","Allentown-Bethlehem-Easton, PA-NJ","Amarillo, TX","Americus, GA","Ames, IA","Anchorage, AK","Angola, IN","Ann Arbor, MI","Appleton, WI","Asheville, NC","Astoria, OR","Athens-Clarke County, GA","Atlanta-Sandy Springs-Alpharetta, GA","Auburn, NY","Augusta-Richmond County, GA-SC","Augusta-Waterville, ME","Austin-Round Rock-Georgetown, TX","Bakersfield, CA","Baltimore-Columbia-Towson, MD","Bardstown, KY","Barnstable Town, MA","Barre, VT","Baton Rouge, LA","Beaumont-Port Arthur, TX","Beaver Dam, WI","Beckley, WV","Bellingham, WA","Bemidji, MN","Billings, MT","Binghamton, NY","Birmingham-Hoover, AL","Bismarck, ND","Blacksburg-Christiansburg, VA","Bloomington, IN","Bloomsburg-Berwick, PA","Boise City, ID","Boone, NC","Boston-Cambridge-Newton, MA-NH","Boulder, CO","Bozeman, MT","Bradford, PA","Brainerd, MN","Branson, MO","Breckenridge, CO","Brevard, NC","Bridgeport-Stamford-Norwalk, CT","Brookings, OR","Brunswick, GA","Buffalo-Cheektowaga, NY","Burlington, NC","Burlington-South Burlington, VT","Canton-Massillon, OH","Cape Coral-Fort Myers, FL","Cape Girardeau, MO-IL","Carlsbad-Artesia, NM","Carson City, NV","Casper, WY","Cañon City, CO","Cedar Rapids, IA","Celina, OH","Centralia, WA","Chambersburg-Waynesboro, PA","Champaign-Urbana, IL","Charleston, WV","Charleston-North Charleston, SC","Charlotte-Concord-Gastonia, NC-SC","Charlottesville, VA","Chattanooga, TN-GA","Cheyenne, WY","Chicago-Naperville-Elgin, IL-IN-WI","Chillicothe, OH","Cincinnati, OH-KY-IN","Clarksburg, WV","Clarksville, TN-KY","Cleveland-Elyria, OH","Clewiston, FL","Clinton, IA","Coeur dAlene, ID","Colorado Springs, CO","Columbia, MO","Columbia, SC","Columbus, GA-AL","Columbus, OH","Concord, NH","Corpus Christi, TX","Cortland, NY","Corvallis, OR","Crestview-Fort Walton Beach-Destin, FL","Dallas-Fort Worth-Arlington, TX","Daphne-Fairhope-Foley, AL","Davenport-Moline-Rock Island, IA-IL","Dayton-Kettering, OH","Defiance, OH","Deltona-Daytona Beach-Ormond Beach, FL","Denver-Aurora-Lakewood, CO","Des Moines-West Des Moines, IA","Detroit-Warren-Dearborn, MI","Dover, DE","Duluth, MN-WI","Durango, CO","Durham-Chapel Hill, NC","East Stroudsburg, PA","Easton, MD","Eau Claire, WI","Effingham, IL","El Paso, TX","Elizabethtown-Fort Knox, KY","Elkhart-Goshen, IN","Enterprise, AL","Erie, PA","Eugene-Springfield, OR","Evansville, IN-KY","Fairfield, IA","Fargo, ND-MN","Faribault-Northfield, MN","Fayetteville, NC","Fayetteville-Springdale-Rogers, AR","Fergus Falls, MN","Fitzgerald, GA","Flint, MI","Florence, SC","Fond du Lac, WI","Fort Collins, CO","Fort Smith, AR-OK","Fort Wayne, IN","Fresno, CA","Gaffney, SC","Gainesville, GA","Gainesville, TX","Gardnerville Ranchos, NV","Gettysburg, PA","Glenwood Springs, CO","Grand Forks, ND-MN","Grand Junction, CO","Grand Rapids-Kentwood, MI","Greeley, CO","Green Bay, WI","Greensboro-High Point, NC","Greenville, MS","Greenville, NC","Greenville-Anderson, SC","Greenwood, SC","Gulfport-Biloxi, MS","Hailey, ID","Hammond, LA","Hannibal, MO","Harrisburg-Carlisle, PA","Hartford-East Hartford-Middletown, CT","Hattiesburg, MS","Heber, UT","Henderson, NC","Hickory-Lenoir-Morganton, NC","Hilo, HI","Hilton Head Island-Bluffton, SC","Hobbs, NM","Hood River, OR","Hot Springs, AR","Houghton, MI","Houma-Thibodaux, LA","Houston-The Woodlands-Sugar Land, TX","Huntington-Ashland, WV-KY-OH","Huntsville, AL","Huntsville, TX","Hutchinson, MN","Idaho Falls, ID","Indianapolis-Carmel-Anderson, IN","Iowa City, IA","Ithaca, NY","Jackson, MI","Jackson, MS","Jackson, WY-ID","Jacksonville, FL","Jefferson City, MO","Kalamazoo-Portage, MI","Kalispell, MT","Kankakee, IL","Kansas City, MO-KS","Keene, NH","Kennewick-Richland, WA","Ketchikan, AK","Key West, FL","Kill Devil Hills, NC","Killeen-Temple, TX","Kingston, NY","Kingsville, TX","LaGrange, GA-AL","Laconia, NH","Lafayette, LA","Lafayette-West Lafayette, IN","Lake Charles, LA","Lake Havasu City-Kingman, AZ","Lakeland-Winter Haven, FL","Lancaster, PA","Lansing-East Lansing, MI","Laredo, TX","Las Vegas-Henderson-Paradise, NV","Lebanon, NH-VT","Lebanon, PA","Lewiston-Auburn, ME","Lexington-Fayette, KY","Lincoln, NE","Little Rock-North Little Rock-Conway, AR","Logan, UT-ID","Longview, TX","Los Angeles-Long Beach-Anaheim, CA","Louisville/Jefferson County, KY-IN","Lumberton, NC","Lynchburg, VA","Madison, IN","Madison, WI","Magnolia, AR","Manchester-Nashua, NH","Mansfield, OH","Marinette, WI-MI","Marion, NC","Marion, OH","McAllen-Edinburg-Mission, TX","Medford, OR","Memphis, TN-MS-AR","Meridian, MS","Mexico, MO","Miami-Fort Lauderdale-Pompano Beach, FL","Milwaukee-Waukesha, WI","Minneapolis-St. Paul-Bloomington, MN-WI","Minot, ND","Missoula, MT","Mobile, AL","Monroe, LA","Montrose, CO","Muncie, IN","Muskegon, MI","Myrtle Beach-Conway-North Myrtle Beach, SC-NC","Napa, CA","Naples-Marco Island, FL","Nashville-Davidson--Murfreesboro--Franklin, TN","New Bern, NC","New Haven-Milford, CT","New Orleans-Metairie, LA","New Philadelphia-Dover, OH","New York-Newark-Jersey City, NY-NJ-PA","Newport, OR","Norfolk, NE","North Platte, NE","North Port-Sarasota-Bradenton, FL","North Wilkesboro, NC","Norwich-New London, CT","Ocala, FL","Ogden-Clearfield, UT","Oklahoma City, OK","Olympia-Lacey-Tumwater, WA","Omaha-Council Bluffs, NE-IA","Oneonta, NY","Ontario, OR-ID","Opelousas, LA","Orlando-Kissimmee-Sanford, FL","Oshkosh-Neenah, WI","Owensboro, KY","Oxford, MS","Oxnard-Thousand Oaks-Ventura, CA","Palm Bay-Melbourne-Titusville, FL","Pensacola-Ferry Pass-Brent, FL","Peoria, IL","Philadelphia-Camden-Wilmington, PA-NJ-DE-MD","Phoenix-Mesa-Chandler, AZ","Pierre, SD","Pinehurst-Southern Pines, NC","Pittsburgh, PA","Pittsfield, MA","Plattsburgh, NY","Pontiac, IL","Poplar Bluff, MO","Port St. Lucie, FL","Portland-South Portland, ME","Portland-Vancouver-Hillsboro, OR-WA","Portsmouth, OH","Pottsville, PA","Poughkeepsie-Newburgh-Middletown, NY","Prescott Valley-Prescott, AZ","Providence-Warwick, RI-MA","Provo-Orem, UT","Pueblo, CO","Punta Gorda, FL","Quincy, IL-MO","Racine, WI","Raleigh-Cary, NC","Rapid City, SD","Reading, PA","Redding, CA","Reno, NV","Richmond, IN","Richmond, VA","Riverside-San Bernardino-Ontario, CA","Rochester, MN","Rochester, NY","Rocky Mount, NC","Ruston, LA","Sacramento-Roseville-Folsom, CA","Safford, AZ","Salem, OR","Salinas, CA","Salisbury, MD-DE","Salt Lake City, UT","San Antonio-New Braunfels, TX","San Diego-Chula Vista-Carlsbad, CA","San Francisco-Oakland-Berkeley, CA","San Jose-Sunnyvale-Santa Clara, CA","San Juan-Bayamón-Caguas, PR","Sandpoint, ID","Sandusky, OH","Santa Fe, NM","Santa Maria-Santa Barbara, CA","Santa Rosa-Petaluma, CA","Savannah, GA","Scottsburg, IN","Scranton--Wilkes-Barre, PA","Searcy, AR","Seattle-Tacoma-Bellevue, WA","Sebastian-Vero Beach, FL","Sebring-Avon Park, FL","Selinsgrove, PA","Shelton, WA","Sheridan, WY","Sherman-Denison, TX","Show Low, AZ","Shreveport-Bossier City, LA","Sikeston, MO","Sioux City, IA-NE-SD","Somerset, KY","Somerset, PA","South Bend-Mishawaka, IN-MI","Spartanburg, SC","Spokane-Spokane Valley, WA","Springfield, IL","Springfield, MA","Springfield, MO","Springfield, OH","St. Cloud, MN","St. George, UT","St. Louis, MO-IL","Statesboro, GA","Staunton, VA","Steamboat Springs, CO","Stockton, CA","Sumter, SC","Syracuse, NY","Tallahassee, FL","Tampa-St. Petersburg-Clearwater, FL","Terre Haute, IN","Texarkana, TX-AR","The Villages, FL","Toledo, OH","Torrington, CT","Traverse City, MI","Trenton-Princeton, NJ","Truckee-Grass Valley, CA","Tucson, AZ","Tulsa, OK","Tupelo, MS","Tuscaloosa, AL","Tyler, TX","Urban Honolulu, HI","Urbana, OH","Utica-Rome, NY","Valdosta, GA","Vallejo, CA","Van Wert, OH","Vermillion, SD","Virginia Beach-Norfolk-Newport News, VA-NC","Visalia, CA","Wabash, IN","Waco, TX","Washington-Arlington-Alexandria, DC-VA-MD-WV","Waterloo-Cedar Falls, IA","Watertown-Fort Atkinson, WI","Wausau-Weston, WI","Weirton-Steubenville, WV-OH","Wenatchee, WA","Wichita, KS","Williamsport, PA","Wilmington, NC","Wilson, NC","Winchester, VA-WV","Winston-Salem, NC","Wooster, OH","Worcester, MA-CT","York-Hanover, PA","Youngstown-Warren-Boardman, OH-PA","Yuma, AZ","Zanesville, OH","Aberdeen, SD","Abilene, TX","Ada, OK","Aguadilla-Isabela, PR","Albany, GA","Albany-Lebanon, OR","Albemarle, NC","Albert Lea, MN","Albertville, AL","Alexander City, AL","Alexandria, LA","Alice, TX","Alma, MI","Alpena, MI","Altoona, PA","Altus, OK","Amsterdam, NY","Andrews, TX","Anniston-Oxford, AL","Arcadia, FL","Ardmore, OK","Arecibo, PR","Arkadelphia, AR","Ashland, OH","Ashtabula, OH","Atchison, KS","Athens, OH","Athens, TN","Athens, TX","Atlantic City-Hammonton, NJ","Atmore, AL","Auburn, IN","Auburn-Opelika, AL","Austin, MN","Bainbridge, GA","Bangor, ME","Baraboo, WI","Bartlesville, OK","Batavia, NY","Batesville, AR","Battle Creek, MI","Bay City, MI","Bay City, TX","Beatrice, NE","Bedford, IN","Beeville, TX","Bellefontaine, OH","Bend, OR","Bennettsville, SC","Bennington, VT","Berlin, NH","Big Rapids, MI","Big Spring, TX","Big Stone Gap, VA","Blackfoot, ID","Bloomington, IL","Bluefield, WV-VA","Bluffton, IN","Blytheville, AR","Bogalusa, LA","Bonham, TX","Borger, TX","Bowling Green, KY","Bremerton-Silverdale-Port Orchard, WA","Brenham, TX","Brookhaven, MS","Brookings, SD","Brownsville, TN","Brownsville-Harlingen, TX","Brownwood, TX","Bucyrus-Galion, OH","Burley, ID","Burlington, IA-IL","Butte-Silver Bow, MT","Cadillac, MI","Calhoun, GA","California-Lexington Park, MD","Cambridge, MD","Cambridge, OH","Camden, AR","Campbellsville, KY","Carbondale-Marion, IL","Carroll, IA","Cedar City, UT","Cedartown, GA","Central City, KY","Centralia, IL","Charleston-Mattoon, IL","Chico, CA","Clarksdale, MS","Clearlake, CA","Cleveland, MS","Cleveland, TN","Clovis, NM","Coamo, PR","Coco, PR","Coffeyville, KS","Coldwater, MI","College Station-Bryan, TX","Columbus, IN","Columbus, MS","Columbus, NE","Connersville, IN","Cookeville, TN","Coos Bay, OR","Cordele, GA","Corinth, MS","Cornelia, GA","Corning, NY","Corsicana, TX","Coshocton, OH","Craig, CO","Crawfordsville, IN","Crescent City, CA","Crossville, TN","Cullman, AL","Cullowhee, NC","Cumberland, MD-WV","Dalton, GA","Danville, IL","Danville, KY","Danville, VA","Dayton, TN","DeRidder, LA","Decatur, AL","Decatur, IL","Decatur, IN","Del Rio, TX","Deming, NM","Dickinson, ND","Dixon, IL","Dodge City, KS","Dothan, AL","Douglas, GA","DuBois, PA","Dublin, GA","Dubuque, IA","Dumas, TX","Duncan, OK","Durant, OK","Dyersburg, TN","Eagle Pass, TX","Edwards, CO","El Campo, TX","El Centro, CA","El Dorado, AR","Elizabeth City, NC","Elk City, OK","Elkins, WV","Elko, NV","Ellensburg, WA","Elmira, NY","Emporia, KS","Enid, OK","Escanaba, MI","Española, NM","Eufaula, AL-GA","Eureka-Arcata, CA","Evanston, WY","Fairbanks, AK","Fairmont, MN","Fairmont, WV","Fallon, NV","Farmington, MO","Farmington, NM","Fernley, NV","Findlay, OH","Flagstaff, AZ","Florence-Muscle Shoals, AL","Forest City, NC","Forrest City, AR","Fort Dodge, IA","Fort Leonard Wood, MO","Fort Madison-Keokuk, IA-IL-MO","Fort Morgan, CO","Fort Payne, AL","Fort Polk South, LA","Frankfort, IN","Frankfort, KY","Fredericksburg, TX","Freeport, IL","Fremont, NE","Fremont, OH","Gadsden, AL","Gainesville, FL","Galesburg, IL","Gallup, NM","Garden City, KS","Georgetown, SC","Gillette, WY","Glasgow, KY","Glens Falls, NY","Gloversville, NY","Goldsboro, NC","Granbury, TX","Grand Island, NE","Grand Rapids, MN","Grants Pass, OR","Grants, NM","Great Bend, KS","Great Falls, MT","Greeneville, TN","Greensburg, IN","Greenville, OH","Greenwood, MS","Grenada, MS","Guayama, PR","Guymon, OK","Hagerstown-Martinsburg, MD-WV","Hanford-Corcoran, CA","Harrison, AR","Harrisonburg, VA","Hastings, NE","Hays, KS","Helena, MT","Helena-West Helena, AR","Hereford, TX","Hermiston-Pendleton, OR","Hillsdale, MI","Hinesville, GA","Holland, MI","Homosassa Springs, FL","Hope, AR","Hudson, NY","Huntingdon, PA","Huntington, IN","Huron, SD","Hutchinson, KS","Indiana, PA","Indianola, MS","Iron Mountain, MI-WI","Jackson, OH","Jackson, TN","Jacksonville, IL","Jacksonville, NC","Jacksonville, TX","Jamestown, ND","Jamestown-Dunkirk-Fredonia, NY","Janesville-Beloit, WI","Jasper, AL","Jasper, IN","Jayuya, PR","Jefferson, GA","Jennings, LA","Jesup, GA","Johnson City, TN","Johnstown, PA","Jonesboro, AR","Joplin, MO","Juneau, AK","Kahului-Wailuku-Lahaina, HI","Kapaa, HI","Kearney, NE","Kendallville, IN","Kennett, MO","Kerrville, TX","Kingsport-Bristol, TN-VA","Kinston, NC","Kirksville, MO","Klamath Falls, OR","Knoxville, TN","Kokomo, IN","La Crosse-Onalaska, WI-MN","La Grande, OR","Lake City, FL","Lamesa, TX","Laramie, WY","Las Cruces, NM","Las Vegas, NM","Laurel, MS","Laurinburg, NC","Lawrence, KS","Lawrenceburg, TN","Lawton, OK","Lebanon, MO","Levelland, TX","Lewisburg, PA","Lewisburg, TN","Lewiston, ID-WA","Lewistown, PA","Lexington, NE","Liberal, KS","Lima, OH","Lincoln, IL","Lock Haven, PA","Logansport, IN","London, KY","Longview, WA","Los Alamos, NM","Lubbock, TX","Ludington, MI","Lufkin, TX","Macomb, IL","Macon-Bibb County, GA","Madera, CA","Madisonville, KY","Malone, NY","Malvern, AR","Manhattan, KS","Manitowoc, WI","Mankato, MN","Marietta, OH","Marion, IN","Marquette, MI","Marshall, MN","Marshall, MO","Marshalltown, IA","Martin, TN","Martinsville, VA","Maryville, MO","Mason City, IA","Mayagüez, PR","Mayfield, KY","Maysville, KY","McAlester, OK","McComb, MS","McMinnville, TN","McPherson, KS","Meadville, PA","Menomonie, WI","Merced, CA","Miami, OK","Michigan City-La Porte, IN","Middlesborough, KY","Midland, MI","Midland, TX","Milledgeville, GA","Minden, LA","Mineral Wells, TX","Mitchell, SD","Moberly, MO","Modesto, CA","Monroe, MI","Montgomery, AL","Morehead City, NC","Morgan City, LA","Morgantown, WV","Morristown, TN","Moscow, ID","Moses Lake, WA","Moultrie, GA","Mount Airy, NC","Mount Gay-Shamrock, WV","Mount Pleasant, MI","Mount Pleasant, TX","Mount Sterling, KY","Mount Vernon, IL","Mount Vernon, OH","Mount Vernon-Anacortes, WA","Mountain Home, AR","Mountain Home, ID","Murray, KY","Muscatine, IA","Muskogee, OK","Nacogdoches, TX","Natchez, MS-LA","Natchitoches, LA","New Castle, IN","New Castle, PA","New Ulm, MN","Newberry, SC","Newport, TN","Niles, MI","Nogales, AZ","North Vernon, IN","Norwalk, OH","Oak Harbor, WA","Ocean City, NJ","Odessa, TX","Ogdensburg-Massena, NY","Oil City, PA","Okeechobee, FL","Olean, NY","Orangeburg, SC","Oskaloosa, IA","Othello, WA","Ottawa, IL","Ottawa, KS","Ottumwa, IA","Owatonna, MN","Ozark, AL","Paducah, KY-IL","Pahrump, NV","Palatka, FL","Palestine, TX","Pampa, TX","Panama City, FL","Paragould, AR","Paris, TN","Paris, TX","Parkersburg-Vienna, WV","Parsons, KS","Payson, AZ","Pearsall, TX","Pecos, TX","Pella, IA","Peru, IN","Picayune, MS","Pine Bluff, AR","Pittsburg, KS","Plainview, TX","Platteville, WI","Plymouth, IN","Pocatello, ID","Point Pleasant, WV-OH","Ponca City, OK","Ponce, PR","Port Angeles, WA","Port Lavaca, TX","Portales, NM","Price, UT","Prineville, OR","Pullman, WA","Raymondville, TX","Red Bluff, CA","Red Wing, MN","Rexburg, ID","Richmond-Berea, KY","Rio Grande City-Roma, TX","Riverton, WY","Roanoke Rapids, NC","Roanoke, VA","Rochelle, IL","Rock Springs, WY","Rockford, IL","Rockingham, NC","Rockport, TX","Rolla, MO","Rome, GA","Roseburg, OR","Roswell, NM","Ruidoso, NM","Russellville, AR","Rutland, VT","Saginaw, MI","Salem, OH","Salina, KS","San Angelo, TX","San Germán, PR","San Luis Obispo-Paso Robles, CA","Sanford, NC","Santa Cruz-Watsonville, CA","Santa Isabel, PR","Sault Ste. Marie, MI","Sayre, PA","Scottsbluff, NE","Scottsboro, AL","Sedalia, MO","Selma, AL","Seneca Falls, NY","Seneca, SC","Sevierville, TN","Seymour, IN","Shawano, WI","Shawnee, OK","Sheboygan, WI","Shelby, NC","Shelbyville, TN","Sidney, OH","Sierra Vista-Douglas, AZ","Silver City, NM","Sioux Falls, SD","Snyder, TX","Sonora, CA","Spearfish, SD","Spencer, IA","Spirit Lake, IA","St. Joseph, MO-KS","St. Marys, GA","St. Marys, PA","Starkville, MS","State College, PA","Stephenville, TX","Sterling, CO","Sterling, IL","Stevens Point, WI","Stillwater, OK","Storm Lake, IA","Sturgis, MI","Sulphur Springs, TX","Summerville, GA","Sunbury, PA","Susanville, CA","Sweetwater, TX","Tahlequah, OK","Talladega-Sylacauga, AL","Taos, NM","Taylorville, IL","The Dalles, OR","Thomaston, GA","Thomasville, GA","Tiffin, OH","Tifton, GA","Toccoa, GA","Topeka, KS","Troy, AL","Tullahoma-Manchester, TN","Twin Falls, ID","Ukiah, CA","Union City, TN","Union, SC","Uvalde, TX","Vernal, UT","Vernon, TX","Vicksburg, MS","Victoria, TX","Vidalia, GA","Vincennes, IN","Vineland-Bridgeton, NJ","Vineyard Haven, MA","Wahpeton, ND-MN","Walla Walla, WA","Wapakoneta, OH","Warner Robins, GA","Warren, PA","Warrensburg, MO","Warsaw, IN","Washington Court House, OH","Washington, IN","Washington, NC","Watertown, SD","Watertown-Fort Drum, NY","Wauchula, FL","Waycross, GA","Weatherford, OK","West Plains, MO","West Point, MS","Wheeling, WV-OH","Whitewater, WI","Wichita Falls, TX","Williston, ND","Willmar, MN","Wilmington, OH","Winfield, KS","Winnemucca, NV","Winona, MN","Wisconsin Rapids-Marshfield, WI","Woodward, OK","Worthington, MN","Yakima, WA","Yankton, SD","Yauco, PR","Yuba City, CA","Zapata, TX"],"bq_organization_sector_name":["","Accommodation and Food Services","Administrative and Support and Waste Management and Remediation Services","Agriculture, Forestry, Fishing and Hunting","Arts, Entertainment, and Recreation","Construction","Educational Services","Finance and Insurance","Governmental Instrumentality or Agency","Health Care and Social Assistance","Information","Management of Companies (Holding Companies)","Manufacturing","Mining","No Classification","Other Services","Professional, Scientific, and Technical Services","Real Estate and Rental and Leasing","Retail Trade","Transportation and Warehousing","Utilities","Wholesale Trade"],"bq_organization_subsector_name":["","Accommodation","Accounting, Tax Preparation, Bookkeeping, and Payroll Services","Activities Related to Credit Intermediation","Administrative and Support Services","Air, Rail, and Water Transportation","Amusement, Gambling, and Recreation Industries","Animal Production","Apparel Manufacturing","Architectural, Engineering, and Related Services","Beverage and Tobacco Product Manufacturing","Broadcasting (except Internet)","Building Material and Garden Equipment and Supplies Dealers","Chemical Manufacturing","Clothing and Clothing Accessories Stores","Computer Systems Design and Related Services","Computer and Electronic Product Manufacturing","Construction of Buildings","Couriers and Messengers","Crop Production","Data Processing Services","Depository Credit Intermediation","Educational Services","Electrical Equipment, Appliance, and Component Manufacturing","Electronics and Appliance Stores","Fabricated Metal Product Manufacturing","Fishing, Hunting and Trapping","Food Manufacturing","Food Services and Drinking Places","Food and Beverage Stores","Forestry and Logging","Furniture and Home Furnishings Stores","Gasoline Stations","General Merchandise Stores","Governmental Instrumentality or Agency","Health and Personal Care Stores","Heavy and Civil Engineering Construction","Home Health Care Services","Insurance Carriers and Related Activities","Legal Services","Lessors of Nonfinancial Intangible Assets (except copyrighted works)","Management of Companies (Holding Companies)","Medical and Diagnostic Laboratories","Merchant Wholesalers, Durable Goods","Merchant Wholesalers, Nondurable Goods","Mining","Miscellaneous Manufacturing","Miscellaneous Store Retailers","Motion Picture and Sound Recording Industries","Motor Vehicle and Parts Dealers","No Classification","Nondepository Credit Intermediation","Nonmetallic Mineral Product Manufacturing","Nonstore Retailers","Nursing and Residential Care Facilities","Offices of Other Health Practitioners","Offices of Physicians and Dentists","Other Information Services","Other Professional, Scientific, and Technical Services","Outpatient Care Centers","Performing Arts, Spectator Sports, and Related Industries","Personal and Laundry Services","Petroleum and Coal Products Manufacturing","Primary Metal Manufacturing","Printing and Related Support Activities","Publishing Industries (except Internet)","Real Estate","Religious, Grantmaking, Civic, Professional, and Similar Organizations","Rental and Leasing Services","Repair and Maintenance","Securities, Commodity Contracts, and Other Financial Investments and Related Activities","Social Assistance","Specialized Design Services","Specialty Trade Contractors","Sporting Goods, Hobby, Book, and Music Stores","Support Activities for Agriculture and Forestry","Support Activities for Transportation","Telecommunications","Textile Mills and Textile Product Mills","Transit and Ground Passenger Transportation","Truck Transportation","Utilities","Warehousing and Storage","Waste Management and Remediation Services","Wholesale Electronic Markets and Agents and Brokers","Wood Product Manufacturing","Funds, Trusts, and Other Financial Vehicles"],"state_names_with_codes":[{"state_name":"","state_code":""},{"state_name":"American Samoa","state_code":"AS"},{"state_name":"Guam","state_code":"GU"},{"state_name":"Northern Mariana Islands","state_code":"MP"},{"state_name":"Puerto Rico","state_code":"PR"},{"state_name":"U.S. Virgin Islands","state_code":"VI"},{"state_name":"Alabama","state_code":"AL"},{"state_name":"Alaska","state_code":"AK"},{"state_name":"Arizona","state_code":"AZ"},{"state_name":"Arkansas","state_code":"AR"},{"state_name":"California","state_code":"CA"},{"state_name":"Colorado","state_code":"CO"},{"state_name":"Connecticut","state_code":"CT"},{"state_name":"Delaware","state_code":"DE"},{"state_name":"Florida","state_code":"FL"},{"state_name":"Georgia","state_code":"GA"},{"state_name":"Hawaii","state_code":"HI"},{"state_name":"Idaho","state_code":"ID"},{"state_name":"Illinois","state_code":"IL"},{"state_name":"Indiana","state_code":"IN"},{"state_name":"Iowa","state_code":"IA"},{"state_name":"Kansas","state_code":"KS"},{"state_name":"Kentucky","state_code":"KY"},{"state_name":"Louisiana","state_code":"LA"},{"state_name":"Maine","state_code":"ME"},{"state_name":"Maryland","state_code":"MD"},{"state_name":"Massachusetts","state_code":"MA"},{"state_name":"Michigan","state_code":"MI"},{"state_name":"Minnesota","state_code":"MN"},{"state_name":"Mississippi","state_code":"MS"},{"state_name":"Missouri","state_code":"MO"},{"state_name":"Montana","state_code":"MT"},{"state_name":"Nebraska","state_code":"NE"},{"state_name":"Nevada","state_code":"NV"},{"state_name":"New Hampshire","state_code":"NH"},{"state_name":"New Jersey","state_code":"NJ"},{"state_name":"New Mexico","state_code":"NM"},{"state_name":"New York","state_code":"NY"},{"state_name":"North Carolina","state_code":"NC"},{"state_name":"North Dakota","state_code":"ND"},{"state_name":"Ohio","state_code":"OH"},{"state_name":"Oklahoma","state_code":"OK"},{"state_name":"Oregon","state_code":"OR"},{"state_name":"Pennsylvania","state_code":"PA"},{"state_name":"Rhode Island","state_code":"RI"},{"state_name":"South Carolina","state_code":"SC"},{"state_name":"South Dakota","state_code":"SD"},{"state_name":"Tennessee","state_code":"TN"},{"state_name":"Texas","state_code":"TX"},{"state_name":"Utah","state_code":"UT"},{"state_name":"Vermont","state_code":"VT"},{"state_name":"Virginia","state_code":"VA"},{"state_name":"Washington","state_code":"WA"},{"state_name":"West Virginia","state_code":"WV"},{"state_name":"Wisconsin","state_code":"WI"},{"state_name":"Wyoming","state_code":"WY"},{"state_name":"District of Columbia","state_code":"DC"}]}
        result = {"bq_legal_entity_jurisdiction_code":[{"jurisdiction_code":"","jurisdiction_name":""},{"jurisdiction_code":"US_AK","jurisdiction_name":"Alaska"},{"jurisdiction_code":"US_AL","jurisdiction_name":"Alabama"},{"jurisdiction_code":"US_AR","jurisdiction_name":"Arkansas"},{"jurisdiction_code":"US_AZ","jurisdiction_name":"Arizona"},{"jurisdiction_code":"US_CA","jurisdiction_name":"California"},{"jurisdiction_code":"US_CO","jurisdiction_name":"Colorado"},{"jurisdiction_code":"US_CT","jurisdiction_name":"Connecticut"},{"jurisdiction_code":"US_DC","jurisdiction_name":"District of Columbia"},{"jurisdiction_code":"US_DE","jurisdiction_name":"Delaware"},{"jurisdiction_code":"US_FL","jurisdiction_name":"Florida"},{"jurisdiction_code":"US_GA","jurisdiction_name":"Georgia"},{"jurisdiction_code":"US_HI","jurisdiction_name":"Hawaii"},{"jurisdiction_code":"US_IA","jurisdiction_name":"Iowa"},{"jurisdiction_code":"US_ID","jurisdiction_name":"Idaho"},{"jurisdiction_code":"US_IL","jurisdiction_name":"Illinois"},{"jurisdiction_code":"US_IN","jurisdiction_name":"Indiana"},{"jurisdiction_code":"US_KS","jurisdiction_name":"Kansas"},{"jurisdiction_code":"US_KY","jurisdiction_name":"Kentucky"},{"jurisdiction_code":"US_LA","jurisdiction_name":"Louisiana"},{"jurisdiction_code":"US_MA","jurisdiction_name":"Massachusetts"},{"jurisdiction_code":"US_MD","jurisdiction_name":"Maryland"},{"jurisdiction_code":"US_ME","jurisdiction_name":"Maine"},{"jurisdiction_code":"US_MI","jurisdiction_name":"Michigan"},{"jurisdiction_code":"US_MN","jurisdiction_name":"Minnesota"},{"jurisdiction_code":"US_MO","jurisdiction_name":"Missouri"},{"jurisdiction_code":"US_MS","jurisdiction_name":"Mississippi"},{"jurisdiction_code":"US_MT","jurisdiction_name":"Montana"},{"jurisdiction_code":"US_NC","jurisdiction_name":"North Carolina"},{"jurisdiction_code":"US_ND","jurisdiction_name":"North Dakota"},{"jurisdiction_code":"US_NE","jurisdiction_name":"Nebraska"},{"jurisdiction_code":"US_NH","jurisdiction_name":"New Hampshire"},{"jurisdiction_code":"US_NJ","jurisdiction_name":"New Jersey"},{"jurisdiction_code":"US_NM","jurisdiction_name":"New Mexico"},{"jurisdiction_code":"US_NV","jurisdiction_name":"Nevada"},{"jurisdiction_code":"US_NY","jurisdiction_name":"New York"},{"jurisdiction_code":"US_OH","jurisdiction_name":"Ohio"},{"jurisdiction_code":"US_OK","jurisdiction_name":"Oklahoma"},{"jurisdiction_code":"US_OR","jurisdiction_name":"Oregon"},{"jurisdiction_code":"US_PA","jurisdiction_name":"Pennsylvania"},{"jurisdiction_code":"US_RI","jurisdiction_name":"Rhode Island"},{"jurisdiction_code":"US_SC","jurisdiction_name":"South Carolina"},{"jurisdiction_code":"US_SD","jurisdiction_name":"South Dakota"},{"jurisdiction_code":"US_TN","jurisdiction_name":"Tennessee"},{"jurisdiction_code":"US_TX","jurisdiction_name":"Texas"},{"jurisdiction_code":"US_UT","jurisdiction_name":"Utah"},{"jurisdiction_code":"US_VA","jurisdiction_name":"Virginia"},{"jurisdiction_code":"US_VT","jurisdiction_name":"Vermont"},{"jurisdiction_code":"US_WA","jurisdiction_name":"Washington"},{"jurisdiction_code":"US_WI","jurisdiction_name":"Wisconsin"},{"jurisdiction_code":"US_WV","jurisdiction_name":"West Virginia"},{"jurisdiction_code":"US_WY","jurisdiction_name":"Wyoming"}],"bq_organization_address1_state_name":["","Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Ontario","Oregon","Pennsylvania","Puerto Rico","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming","Alberta","America Samoa","American Samoa","British Columbia","District Of Columbia","Foreign Countries","Guam","Honduras","Manitoba","Mexico","New Brunswick","Newfoundland","Northern Mariana Islands","Northwest Territory","Nova Scotia","Panama Canal Zone","Prince Edward Island","Quebec","Saskatchewan","U.S. Virgin Islands","Virgin Islands","Yukon"],"bq_organization_address1_cbsa_name":["","Aberdeen, WA","Adrian, MI","Akron, OH","Alamogordo, NM","Albany-Schenectady-Troy, NY","Albuquerque, NM","Alexandria, MN","Allentown-Bethlehem-Easton, PA-NJ","Amarillo, TX","Americus, GA","Ames, IA","Anchorage, AK","Angola, IN","Ann Arbor, MI","Appleton, WI","Asheville, NC","Astoria, OR","Athens-Clarke County, GA","Atlanta-Sandy Springs-Alpharetta, GA","Auburn, NY","Augusta-Richmond County, GA-SC","Augusta-Waterville, ME","Austin-Round Rock-Georgetown, TX","Bakersfield, CA","Baltimore-Columbia-Towson, MD","Bardstown, KY","Barnstable Town, MA","Barre, VT","Baton Rouge, LA","Beaumont-Port Arthur, TX","Beaver Dam, WI","Beckley, WV","Bellingham, WA","Bemidji, MN","Billings, MT","Binghamton, NY","Birmingham-Hoover, AL","Bismarck, ND","Blacksburg-Christiansburg, VA","Bloomington, IN","Bloomsburg-Berwick, PA","Boise City, ID","Boone, NC","Boston-Cambridge-Newton, MA-NH","Boulder, CO","Bozeman, MT","Bradford, PA","Brainerd, MN","Branson, MO","Breckenridge, CO","Brevard, NC","Bridgeport-Stamford-Norwalk, CT","Brookings, OR","Brunswick, GA","Buffalo-Cheektowaga, NY","Burlington, NC","Burlington-South Burlington, VT","Canton-Massillon, OH","Cape Coral-Fort Myers, FL","Cape Girardeau, MO-IL","Carlsbad-Artesia, NM","Carson City, NV","Casper, WY","Cañon City, CO","Cedar Rapids, IA","Celina, OH","Centralia, WA","Chambersburg-Waynesboro, PA","Champaign-Urbana, IL","Charleston, WV","Charleston-North Charleston, SC","Charlotte-Concord-Gastonia, NC-SC","Charlottesville, VA","Chattanooga, TN-GA","Cheyenne, WY","Chicago-Naperville-Elgin, IL-IN-WI","Chillicothe, OH","Cincinnati, OH-KY-IN","Clarksburg, WV","Clarksville, TN-KY","Cleveland-Elyria, OH","Clewiston, FL","Clinton, IA","Coeur dAlene, ID","Colorado Springs, CO","Columbia, MO","Columbia, SC","Columbus, GA-AL","Columbus, OH","Concord, NH","Corpus Christi, TX","Cortland, NY","Corvallis, OR","Crestview-Fort Walton Beach-Destin, FL","Dallas-Fort Worth-Arlington, TX","Daphne-Fairhope-Foley, AL","Davenport-Moline-Rock Island, IA-IL","Dayton-Kettering, OH","Defiance, OH","Deltona-Daytona Beach-Ormond Beach, FL","Denver-Aurora-Lakewood, CO","Des Moines-West Des Moines, IA","Detroit-Warren-Dearborn, MI","Dover, DE","Duluth, MN-WI","Durango, CO","Durham-Chapel Hill, NC","East Stroudsburg, PA","Easton, MD","Eau Claire, WI","Effingham, IL","El Paso, TX","Elizabethtown-Fort Knox, KY","Elkhart-Goshen, IN","Enterprise, AL","Erie, PA","Eugene-Springfield, OR","Evansville, IN-KY","Fairfield, IA","Fargo, ND-MN","Faribault-Northfield, MN","Fayetteville, NC","Fayetteville-Springdale-Rogers, AR","Fergus Falls, MN","Fitzgerald, GA","Flint, MI","Florence, SC","Fond du Lac, WI","Fort Collins, CO","Fort Smith, AR-OK","Fort Wayne, IN","Fresno, CA","Gaffney, SC","Gainesville, GA","Gainesville, TX","Gardnerville Ranchos, NV","Gettysburg, PA","Glenwood Springs, CO","Grand Forks, ND-MN","Grand Junction, CO","Grand Rapids-Kentwood, MI","Greeley, CO","Green Bay, WI","Greensboro-High Point, NC","Greenville, MS","Greenville, NC","Greenville-Anderson, SC","Greenwood, SC","Gulfport-Biloxi, MS","Hailey, ID","Hammond, LA","Hannibal, MO","Harrisburg-Carlisle, PA","Hartford-East Hartford-Middletown, CT","Hattiesburg, MS","Heber, UT","Henderson, NC","Hickory-Lenoir-Morganton, NC","Hilo, HI","Hilton Head Island-Bluffton, SC","Hobbs, NM","Hood River, OR","Hot Springs, AR","Houghton, MI","Houma-Thibodaux, LA","Houston-The Woodlands-Sugar Land, TX","Huntington-Ashland, WV-KY-OH","Huntsville, AL","Huntsville, TX","Hutchinson, MN","Idaho Falls, ID","Indianapolis-Carmel-Anderson, IN","Iowa City, IA","Ithaca, NY","Jackson, MI","Jackson, MS","Jackson, WY-ID","Jacksonville, FL","Jefferson City, MO","Kalamazoo-Portage, MI","Kalispell, MT","Kankakee, IL","Kansas City, MO-KS","Keene, NH","Kennewick-Richland, WA","Ketchikan, AK","Key West, FL","Kill Devil Hills, NC","Killeen-Temple, TX","Kingston, NY","Kingsville, TX","LaGrange, GA-AL","Laconia, NH","Lafayette, LA","Lafayette-West Lafayette, IN","Lake Charles, LA","Lake Havasu City-Kingman, AZ","Lakeland-Winter Haven, FL","Lancaster, PA","Lansing-East Lansing, MI","Laredo, TX","Las Vegas-Henderson-Paradise, NV","Lebanon, NH-VT","Lebanon, PA","Lewiston-Auburn, ME","Lexington-Fayette, KY","Lincoln, NE","Little Rock-North Little Rock-Conway, AR","Logan, UT-ID","Longview, TX","Los Angeles-Long Beach-Anaheim, CA","Louisville/Jefferson County, KY-IN","Lumberton, NC","Lynchburg, VA","Madison, IN","Madison, WI","Magnolia, AR","Manchester-Nashua, NH","Mansfield, OH","Marinette, WI-MI","Marion, NC","Marion, OH","McAllen-Edinburg-Mission, TX","Medford, OR","Memphis, TN-MS-AR","Meridian, MS","Mexico, MO","Miami-Fort Lauderdale-Pompano Beach, FL","Milwaukee-Waukesha, WI","Minneapolis-St. Paul-Bloomington, MN-WI","Minot, ND","Missoula, MT","Mobile, AL","Monroe, LA","Montrose, CO","Muncie, IN","Muskegon, MI","Myrtle Beach-Conway-North Myrtle Beach, SC-NC","Napa, CA","Naples-Marco Island, FL","Nashville-Davidson--Murfreesboro--Franklin, TN","New Bern, NC","New Haven-Milford, CT","New Orleans-Metairie, LA","New Philadelphia-Dover, OH","New York-Newark-Jersey City, NY-NJ-PA","Newport, OR","Norfolk, NE","North Platte, NE","North Port-Sarasota-Bradenton, FL","North Wilkesboro, NC","Norwich-New London, CT","Ocala, FL","Ogden-Clearfield, UT","Oklahoma City, OK","Olympia-Lacey-Tumwater, WA","Omaha-Council Bluffs, NE-IA","Oneonta, NY","Ontario, OR-ID","Opelousas, LA","Orlando-Kissimmee-Sanford, FL","Oshkosh-Neenah, WI","Owensboro, KY","Oxford, MS","Oxnard-Thousand Oaks-Ventura, CA","Palm Bay-Melbourne-Titusville, FL","Pensacola-Ferry Pass-Brent, FL","Peoria, IL","Philadelphia-Camden-Wilmington, PA-NJ-DE-MD","Phoenix-Mesa-Chandler, AZ","Pierre, SD","Pinehurst-Southern Pines, NC","Pittsburgh, PA","Pittsfield, MA","Plattsburgh, NY","Pontiac, IL","Poplar Bluff, MO","Port St. Lucie, FL","Portland-South Portland, ME","Portland-Vancouver-Hillsboro, OR-WA","Portsmouth, OH","Pottsville, PA","Poughkeepsie-Newburgh-Middletown, NY","Prescott Valley-Prescott, AZ","Providence-Warwick, RI-MA","Provo-Orem, UT","Pueblo, CO","Punta Gorda, FL","Quincy, IL-MO","Racine, WI","Raleigh-Cary, NC","Rapid City, SD","Reading, PA","Redding, CA","Reno, NV","Richmond, IN","Richmond, VA","Riverside-San Bernardino-Ontario, CA","Rochester, MN","Rochester, NY","Rocky Mount, NC","Ruston, LA","Sacramento-Roseville-Folsom, CA","Safford, AZ","Salem, OR","Salinas, CA","Salisbury, MD-DE","Salt Lake City, UT","San Antonio-New Braunfels, TX","San Diego-Chula Vista-Carlsbad, CA","San Francisco-Oakland-Berkeley, CA","San Jose-Sunnyvale-Santa Clara, CA","San Juan-Bayamón-Caguas, PR","Sandpoint, ID","Sandusky, OH","Santa Fe, NM","Santa Maria-Santa Barbara, CA","Santa Rosa-Petaluma, CA","Savannah, GA","Scottsburg, IN","Scranton--Wilkes-Barre, PA","Searcy, AR","Seattle-Tacoma-Bellevue, WA","Sebastian-Vero Beach, FL","Sebring-Avon Park, FL","Selinsgrove, PA","Shelton, WA","Sheridan, WY","Sherman-Denison, TX","Show Low, AZ","Shreveport-Bossier City, LA","Sikeston, MO","Sioux City, IA-NE-SD","Somerset, KY","Somerset, PA","South Bend-Mishawaka, IN-MI","Spartanburg, SC","Spokane-Spokane Valley, WA","Springfield, IL","Springfield, MA","Springfield, MO","Springfield, OH","St. Cloud, MN","St. George, UT","St. Louis, MO-IL","Statesboro, GA","Staunton, VA","Steamboat Springs, CO","Stockton, CA","Sumter, SC","Syracuse, NY","Tallahassee, FL","Tampa-St. Petersburg-Clearwater, FL","Terre Haute, IN","Texarkana, TX-AR","The Villages, FL","Toledo, OH","Torrington, CT","Traverse City, MI","Trenton-Princeton, NJ","Truckee-Grass Valley, CA","Tucson, AZ","Tulsa, OK","Tupelo, MS","Tuscaloosa, AL","Tyler, TX","Urban Honolulu, HI","Urbana, OH","Utica-Rome, NY","Valdosta, GA","Vallejo, CA","Van Wert, OH","Vermillion, SD","Virginia Beach-Norfolk-Newport News, VA-NC","Visalia, CA","Wabash, IN","Waco, TX","Washington-Arlington-Alexandria, DC-VA-MD-WV","Waterloo-Cedar Falls, IA","Watertown-Fort Atkinson, WI","Wausau-Weston, WI","Weirton-Steubenville, WV-OH","Wenatchee, WA","Wichita, KS","Williamsport, PA","Wilmington, NC","Wilson, NC","Winchester, VA-WV","Winston-Salem, NC","Wooster, OH","Worcester, MA-CT","York-Hanover, PA","Youngstown-Warren-Boardman, OH-PA","Yuma, AZ","Zanesville, OH","Aberdeen, SD","Abilene, TX","Ada, OK","Aguadilla-Isabela, PR","Albany, GA","Albany-Lebanon, OR","Albemarle, NC","Albert Lea, MN","Albertville, AL","Alexander City, AL","Alexandria, LA","Alice, TX","Alma, MI","Alpena, MI","Altoona, PA","Altus, OK","Amsterdam, NY","Andrews, TX","Anniston-Oxford, AL","Arcadia, FL","Ardmore, OK","Arecibo, PR","Arkadelphia, AR","Ashland, OH","Ashtabula, OH","Atchison, KS","Athens, OH","Athens, TN","Athens, TX","Atlantic City-Hammonton, NJ","Atmore, AL","Auburn, IN","Auburn-Opelika, AL","Austin, MN","Bainbridge, GA","Bangor, ME","Baraboo, WI","Bartlesville, OK","Batavia, NY","Batesville, AR","Battle Creek, MI","Bay City, MI","Bay City, TX","Beatrice, NE","Bedford, IN","Beeville, TX","Bellefontaine, OH","Bend, OR","Bennettsville, SC","Bennington, VT","Berlin, NH","Big Rapids, MI","Big Spring, TX","Big Stone Gap, VA","Blackfoot, ID","Bloomington, IL","Bluefield, WV-VA","Bluffton, IN","Blytheville, AR","Bogalusa, LA","Bonham, TX","Borger, TX","Bowling Green, KY","Bremerton-Silverdale-Port Orchard, WA","Brenham, TX","Brookhaven, MS","Brookings, SD","Brownsville, TN","Brownsville-Harlingen, TX","Brownwood, TX","Bucyrus-Galion, OH","Burley, ID","Burlington, IA-IL","Butte-Silver Bow, MT","Cadillac, MI","Calhoun, GA","California-Lexington Park, MD","Cambridge, MD","Cambridge, OH","Camden, AR","Campbellsville, KY","Carbondale-Marion, IL","Carroll, IA","Cedar City, UT","Cedartown, GA","Central City, KY","Centralia, IL","Charleston-Mattoon, IL","Chico, CA","Clarksdale, MS","Clearlake, CA","Cleveland, MS","Cleveland, TN","Clovis, NM","Coamo, PR","Coco, PR","Coffeyville, KS","Coldwater, MI","College Station-Bryan, TX","Columbus, IN","Columbus, MS","Columbus, NE","Connersville, IN","Cookeville, TN","Coos Bay, OR","Cordele, GA","Corinth, MS","Cornelia, GA","Corning, NY","Corsicana, TX","Coshocton, OH","Craig, CO","Crawfordsville, IN","Crescent City, CA","Crossville, TN","Cullman, AL","Cullowhee, NC","Cumberland, MD-WV","Dalton, GA","Danville, IL","Danville, KY","Danville, VA","Dayton, TN","DeRidder, LA","Decatur, AL","Decatur, IL","Decatur, IN","Del Rio, TX","Deming, NM","Dickinson, ND","Dixon, IL","Dodge City, KS","Dothan, AL","Douglas, GA","DuBois, PA","Dublin, GA","Dubuque, IA","Dumas, TX","Duncan, OK","Durant, OK","Dyersburg, TN","Eagle Pass, TX","Edwards, CO","El Campo, TX","El Centro, CA","El Dorado, AR","Elizabeth City, NC","Elk City, OK","Elkins, WV","Elko, NV","Ellensburg, WA","Elmira, NY","Emporia, KS","Enid, OK","Escanaba, MI","Española, NM","Eufaula, AL-GA","Eureka-Arcata, CA","Evanston, WY","Fairbanks, AK","Fairmont, MN","Fairmont, WV","Fallon, NV","Farmington, MO","Farmington, NM","Fernley, NV","Findlay, OH","Flagstaff, AZ","Florence-Muscle Shoals, AL","Forest City, NC","Forrest City, AR","Fort Dodge, IA","Fort Leonard Wood, MO","Fort Madison-Keokuk, IA-IL-MO","Fort Morgan, CO","Fort Payne, AL","Fort Polk South, LA","Frankfort, IN","Frankfort, KY","Fredericksburg, TX","Freeport, IL","Fremont, NE","Fremont, OH","Gadsden, AL","Gainesville, FL","Galesburg, IL","Gallup, NM","Garden City, KS","Georgetown, SC","Gillette, WY","Glasgow, KY","Glens Falls, NY","Gloversville, NY","Goldsboro, NC","Granbury, TX","Grand Island, NE","Grand Rapids, MN","Grants Pass, OR","Grants, NM","Great Bend, KS","Great Falls, MT","Greeneville, TN","Greensburg, IN","Greenville, OH","Greenwood, MS","Grenada, MS","Guayama, PR","Guymon, OK","Hagerstown-Martinsburg, MD-WV","Hanford-Corcoran, CA","Harrison, AR","Harrisonburg, VA","Hastings, NE","Hays, KS","Helena, MT","Helena-West Helena, AR","Hereford, TX","Hermiston-Pendleton, OR","Hillsdale, MI","Hinesville, GA","Holland, MI","Homosassa Springs, FL","Hope, AR","Hudson, NY","Huntingdon, PA","Huntington, IN","Huron, SD","Hutchinson, KS","Indiana, PA","Indianola, MS","Iron Mountain, MI-WI","Jackson, OH","Jackson, TN","Jacksonville, IL","Jacksonville, NC","Jacksonville, TX","Jamestown, ND","Jamestown-Dunkirk-Fredonia, NY","Janesville-Beloit, WI","Jasper, AL","Jasper, IN","Jayuya, PR","Jefferson, GA","Jennings, LA","Jesup, GA","Johnson City, TN","Johnstown, PA","Jonesboro, AR","Joplin, MO","Juneau, AK","Kahului-Wailuku-Lahaina, HI","Kapaa, HI","Kearney, NE","Kendallville, IN","Kennett, MO","Kerrville, TX","Kingsport-Bristol, TN-VA","Kinston, NC","Kirksville, MO","Klamath Falls, OR","Knoxville, TN","Kokomo, IN","La Crosse-Onalaska, WI-MN","La Grande, OR","Lake City, FL","Lamesa, TX","Laramie, WY","Las Cruces, NM","Las Vegas, NM","Laurel, MS","Laurinburg, NC","Lawrence, KS","Lawrenceburg, TN","Lawton, OK","Lebanon, MO","Levelland, TX","Lewisburg, PA","Lewisburg, TN","Lewiston, ID-WA","Lewistown, PA","Lexington, NE","Liberal, KS","Lima, OH","Lincoln, IL","Lock Haven, PA","Logansport, IN","London, KY","Longview, WA","Los Alamos, NM","Lubbock, TX","Ludington, MI","Lufkin, TX","Macomb, IL","Macon-Bibb County, GA","Madera, CA","Madisonville, KY","Malone, NY","Malvern, AR","Manhattan, KS","Manitowoc, WI","Mankato, MN","Marietta, OH","Marion, IN","Marquette, MI","Marshall, MN","Marshall, MO","Marshalltown, IA","Martin, TN","Martinsville, VA","Maryville, MO","Mason City, IA","Mayagüez, PR","Mayfield, KY","Maysville, KY","McAlester, OK","McComb, MS","McMinnville, TN","McPherson, KS","Meadville, PA","Menomonie, WI","Merced, CA","Miami, OK","Michigan City-La Porte, IN","Middlesborough, KY","Midland, MI","Midland, TX","Milledgeville, GA","Minden, LA","Mineral Wells, TX","Mitchell, SD","Moberly, MO","Modesto, CA","Monroe, MI","Montgomery, AL","Morehead City, NC","Morgan City, LA","Morgantown, WV","Morristown, TN","Moscow, ID","Moses Lake, WA","Moultrie, GA","Mount Airy, NC","Mount Gay-Shamrock, WV","Mount Pleasant, MI","Mount Pleasant, TX","Mount Sterling, KY","Mount Vernon, IL","Mount Vernon, OH","Mount Vernon-Anacortes, WA","Mountain Home, AR","Mountain Home, ID","Murray, KY","Muscatine, IA","Muskogee, OK","Nacogdoches, TX","Natchez, MS-LA","Natchitoches, LA","New Castle, IN","New Castle, PA","New Ulm, MN","Newberry, SC","Newport, TN","Niles, MI","Nogales, AZ","North Vernon, IN","Norwalk, OH","Oak Harbor, WA","Ocean City, NJ","Odessa, TX","Ogdensburg-Massena, NY","Oil City, PA","Okeechobee, FL","Olean, NY","Orangeburg, SC","Oskaloosa, IA","Othello, WA","Ottawa, IL","Ottawa, KS","Ottumwa, IA","Owatonna, MN","Ozark, AL","Paducah, KY-IL","Pahrump, NV","Palatka, FL","Palestine, TX","Pampa, TX","Panama City, FL","Paragould, AR","Paris, TN","Paris, TX","Parkersburg-Vienna, WV","Parsons, KS","Payson, AZ","Pearsall, TX","Pecos, TX","Pella, IA","Peru, IN","Picayune, MS","Pine Bluff, AR","Pittsburg, KS","Plainview, TX","Platteville, WI","Plymouth, IN","Pocatello, ID","Point Pleasant, WV-OH","Ponca City, OK","Ponce, PR","Port Angeles, WA","Port Lavaca, TX","Portales, NM","Price, UT","Prineville, OR","Pullman, WA","Raymondville, TX","Red Bluff, CA","Red Wing, MN","Rexburg, ID","Richmond-Berea, KY","Rio Grande City-Roma, TX","Riverton, WY","Roanoke Rapids, NC","Roanoke, VA","Rochelle, IL","Rock Springs, WY","Rockford, IL","Rockingham, NC","Rockport, TX","Rolla, MO","Rome, GA","Roseburg, OR","Roswell, NM","Ruidoso, NM","Russellville, AR","Rutland, VT","Saginaw, MI","Salem, OH","Salina, KS","San Angelo, TX","San Germán, PR","San Luis Obispo-Paso Robles, CA","Sanford, NC","Santa Cruz-Watsonville, CA","Santa Isabel, PR","Sault Ste. Marie, MI","Sayre, PA","Scottsbluff, NE","Scottsboro, AL","Sedalia, MO","Selma, AL","Seneca Falls, NY","Seneca, SC","Sevierville, TN","Seymour, IN","Shawano, WI","Shawnee, OK","Sheboygan, WI","Shelby, NC","Shelbyville, TN","Sidney, OH","Sierra Vista-Douglas, AZ","Silver City, NM","Sioux Falls, SD","Snyder, TX","Sonora, CA","Spearfish, SD","Spencer, IA","Spirit Lake, IA","St. Joseph, MO-KS","St. Marys, GA","St. Marys, PA","Starkville, MS","State College, PA","Stephenville, TX","Sterling, CO","Sterling, IL","Stevens Point, WI","Stillwater, OK","Storm Lake, IA","Sturgis, MI","Sulphur Springs, TX","Summerville, GA","Sunbury, PA","Susanville, CA","Sweetwater, TX","Tahlequah, OK","Talladega-Sylacauga, AL","Taos, NM","Taylorville, IL","The Dalles, OR","Thomaston, GA","Thomasville, GA","Tiffin, OH","Tifton, GA","Toccoa, GA","Topeka, KS","Troy, AL","Tullahoma-Manchester, TN","Twin Falls, ID","Ukiah, CA","Union City, TN","Union, SC","Uvalde, TX","Vernal, UT","Vernon, TX","Vicksburg, MS","Victoria, TX","Vidalia, GA","Vincennes, IN","Vineland-Bridgeton, NJ","Vineyard Haven, MA","Wahpeton, ND-MN","Walla Walla, WA","Wapakoneta, OH","Warner Robins, GA","Warren, PA","Warrensburg, MO","Warsaw, IN","Washington Court House, OH","Washington, IN","Washington, NC","Watertown, SD","Watertown-Fort Drum, NY","Wauchula, FL","Waycross, GA","Weatherford, OK","West Plains, MO","West Point, MS","Wheeling, WV-OH","Whitewater, WI","Wichita Falls, TX","Williston, ND","Willmar, MN","Wilmington, OH","Winfield, KS","Winnemucca, NV","Winona, MN","Wisconsin Rapids-Marshfield, WI","Woodward, OK","Worthington, MN","Yakima, WA","Yankton, SD","Yauco, PR","Yuba City, CA","Zapata, TX"],"bq_organization_sector_name":["","Accommodation and Food Services","Administrative and Support and Waste Management and Remediation Services","Agriculture, Forestry, Fishing and Hunting","Arts, Entertainment, and Recreation","Construction","Educational Services","Finance and Insurance","Governmental Instrumentality or Agency","Health Care and Social Assistance","Information","Management of Companies (Holding Companies)","Manufacturing","Mining","No Classification","Other Services","Professional, Scientific, and Technical Services","Real Estate and Rental and Leasing","Retail Trade","Transportation and Warehousing","Utilities","Wholesale Trade"],"bq_organization_subsector_name":["","Accommodation","Accounting, Tax Preparation, Bookkeeping, and Payroll Services","Activities Related to Credit Intermediation","Administrative and Support Services","Air, Rail, and Water Transportation","Amusement, Gambling, and Recreation Industries","Animal Production","Apparel Manufacturing","Architectural, Engineering, and Related Services","Beverage and Tobacco Product Manufacturing","Broadcasting (except Internet)","Building Material and Garden Equipment and Supplies Dealers","Chemical Manufacturing","Clothing and Clothing Accessories Stores","Computer Systems Design and Related Services","Computer and Electronic Product Manufacturing","Construction of Buildings","Couriers and Messengers","Crop Production","Data Processing Services","Depository Credit Intermediation","Educational Services","Electrical Equipment, Appliance, and Component Manufacturing","Electronics and Appliance Stores","Fabricated Metal Product Manufacturing","Fishing, Hunting and Trapping","Food Manufacturing","Food Services and Drinking Places","Food and Beverage Stores","Forestry and Logging","Furniture and Home Furnishings Stores","Gasoline Stations","General Merchandise Stores","Governmental Instrumentality or Agency","Health and Personal Care Stores","Heavy and Civil Engineering Construction","Home Health Care Services","Insurance Carriers and Related Activities","Legal Services","Lessors of Nonfinancial Intangible Assets (except copyrighted works)","Management of Companies (Holding Companies)","Medical and Diagnostic Laboratories","Merchant Wholesalers, Durable Goods","Merchant Wholesalers, Nondurable Goods","Mining","Miscellaneous Manufacturing","Miscellaneous Store Retailers","Motion Picture and Sound Recording Industries","Motor Vehicle and Parts Dealers","No Classification","Nondepository Credit Intermediation","Nonmetallic Mineral Product Manufacturing","Nonstore Retailers","Nursing and Residential Care Facilities","Offices of Other Health Practitioners","Offices of Physicians and Dentists","Other Information Services","Other Professional, Scientific, and Technical Services","Outpatient Care Centers","Performing Arts, Spectator Sports, and Related Industries","Personal and Laundry Services","Petroleum and Coal Products Manufacturing","Primary Metal Manufacturing","Printing and Related Support Activities","Publishing Industries (except Internet)","Real Estate","Religious, Grantmaking, Civic, Professional, and Similar Organizations","Rental and Leasing Services","Repair and Maintenance","Securities, Commodity Contracts, and Other Financial Investments and Related Activities","Social Assistance","Specialized Design Services","Specialty Trade Contractors","Sporting Goods, Hobby, Book, and Music Stores","Support Activities for Agriculture and Forestry","Support Activities for Transportation","Telecommunications","Textile Mills and Textile Product Mills","Transit and Ground Passenger Transportation","Truck Transportation","Utilities","Warehousing and Storage","Waste Management and Remediation Services","Wholesale Electronic Markets and Agents and Brokers","Wood Product Manufacturing","Funds, Trusts, and Other Financial Vehicles"],"state_names_with_codes":[{"state_name":"","state_code":""},{"state_name":"American Samoa","state_code":"AS"},{"state_name":"Guam","state_code":"GU"},{"state_name":"Northern Mariana Islands","state_code":"MP"},{"state_name":"Puerto Rico","state_code":"PR"},{"state_name":"U.S. Virgin Islands","state_code":"VI"},{"state_name":"Alabama","state_code":"AL"},{"state_name":"Alaska","state_code":"AK"},{"state_name":"Arizona","state_code":"AZ"},{"state_name":"Arkansas","state_code":"AR"},{"state_name":"California","state_code":"CA"},{"state_name":"Colorado","state_code":"CO"},{"state_name":"Connecticut","state_code":"CT"},{"state_name":"Delaware","state_code":"DE"},{"state_name":"Florida","state_code":"FL"},{"state_name":"Georgia","state_code":"GA"},{"state_name":"Hawaii","state_code":"HI"},{"state_name":"Idaho","state_code":"ID"},{"state_name":"Illinois","state_code":"IL"},{"state_name":"Indiana","state_code":"IN"},{"state_name":"Iowa","state_code":"IA"},{"state_name":"Kansas","state_code":"KS"},{"state_name":"Kentucky","state_code":"KY"},{"state_name":"Louisiana","state_code":"LA"},{"state_name":"Maine","state_code":"ME"},{"state_name":"Maryland","state_code":"MD"},{"state_name":"Massachusetts","state_code":"MA"},{"state_name":"Michigan","state_code":"MI"},{"state_name":"Minnesota","state_code":"MN"},{"state_name":"Mississippi","state_code":"MS"},{"state_name":"Missouri","state_code":"MO"},{"state_name":"Montana","state_code":"MT"},{"state_name":"Nebraska","state_code":"NE"},{"state_name":"Nevada","state_code":"NV"},{"state_name":"New Hampshire","state_code":"NH"},{"state_name":"New Jersey","state_code":"NJ"},{"state_name":"New Mexico","state_code":"NM"},{"state_name":"New York","state_code":"NY"},{"state_name":"North Carolina","state_code":"NC"},{"state_name":"North Dakota","state_code":"ND"},{"state_name":"Ohio","state_code":"OH"},{"state_name":"Oklahoma","state_code":"OK"},{"state_name":"Oregon","state_code":"OR"},{"state_name":"Pennsylvania","state_code":"PA"},{"state_name":"Rhode Island","state_code":"RI"},{"state_name":"South Carolina","state_code":"SC"},{"state_name":"South Dakota","state_code":"SD"},{"state_name":"Tennessee","state_code":"TN"},{"state_name":"Texas","state_code":"TX"},{"state_name":"Utah","state_code":"UT"},{"state_name":"Vermont","state_code":"VT"},{"state_name":"Virginia","state_code":"VA"},{"state_name":"Washington","state_code":"WA"},{"state_name":"West Virginia","state_code":"WV"},{"state_name":"Wisconsin","state_code":"WI"},{"state_name":"Wyoming","state_code":"WY"},{"state_name":"District of Columbia","state_code":"DC"}]}
        response = {"response":result, "status":200}
        return response
        # return JSONResponse(content=result, status_code=200)
    # except requests.RequestException as e:
    #     return JSONResponse(content={"error": "An error occurred while processing the search request.", "details": str(e)}, status_code=500)
    
    except Exception as e:
        response = {"response":{"error": "An unexpected error occurred.", "details": str(e)},"status":400}
        return response
        # return JSONResponse(content={"error": "An unexpected error occurred.", "details": str(e)}, status_code=500)

    # return JSONResponse(content=response.json(), status_code=200)
def parent_entity_details(query, yql, _type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, request_origin):
    try:
        # print("\n\nIn Details \n\n")
        # print("query =",query)
        hits = 1000
        search_endpoint = f"{VESPA_ENDPOINT}/search/"
        # yql = f"select bq_organization_name, bq_organization_structure, bq_organization_company_type, bq_organization_active_indicator, bq_organization_id, bq_organization_ein, bq_organization_website, bq_organization_date_founded, bq_organization_ticker, bq_organization_public_indicator, bq_organization_address1_line_1, bq_organization_address1_line_2, bq_organization_address1_city, bq_organization_address1_state, bq_organization_address1_zip5, bq_organization_legal_name, bq_legal_entity_id, bq_organization_legal_address1_line_1, bq_organization_legal_address1_line_2, bq_organization_legal_address1_city, bq_organization_legal_address1_state, bq_organization_legal_address1_zip5, bq_legal_entity_jurisdiction_code, bq_legal_entity_ultimate_parent_id, bq_legal_entity_parent_id, bq_legal_entity_parent_status, bq_organization_current_status, bq_organization_legal_address1_rdi, bq_organization_nonprofit_indicator, bq_organization_company_type, bq_organization_irs_sector_name, bq_organization_irs_sector_code, bq_organization_naics_sector_name, bq_organization_naics_sector_code, bq_organization_naics_name, bq_organization_naics_code, bq_score, bq_legal_entity_children_count, bq_organization_public_indicator, bq_tickers_related, bq_officer_details, bq_organization_linkedin_url, bq_organization_lei, bq_organization_cik, extracted_on, bq_organization_sector_code, bq_organization_sector_name from  terminal_screener where bq_organization_id contains '{query}'"
        yql = f"select * from  terminal_screener where bq_organization_id contains '{query}'"
        logger.info(f'YQL: {yql}')
        params = {
            'yql': yql,
            # 'query': query,
            'filter': filter,
            'offset': offset,
            'ranking': ranking,
            'limit': limit,
            'type': 'all',
            'hits': hits,
            "format": "json",
        }
        #logger.info(f'Endpoint Params: {params}')
        
        # try:
        #     url = "http://54.190.237.221:8000/q/query/"
        #     tab = "parent_entity"
        #     query = yql.lower() + str(offset) + str(filter).replace(" ","") + str(orderby).replace(" ","")

        #     payload = {"tab":tab, "query":query}
        #     files=[]
        #     headers = {}

        #     response = requests.request("POST", url, headers=headers, data=payload, files=files)
        #     if response.status_code == 200:
        #         null, true, false = None, True, False
        #         response = {"response":eval(response.json())['response'],"status":200}
        #         # print(response)
        #         return response
        #         # return JSONResponse(content=eval(response.json()), status_code=200)
        # except:
        #     pass
        
        response = requests.get(search_endpoint, params=params).json()
        
        try:
            x = response['root']['children']
            index_to_move = next((i for i, d in enumerate(x) if "fields" in d and d["fields"].get("bq_legal_entity_parent_status") == "Ultimate Parent"), None)
            if index_to_move is not None:
                x.insert(0, x.pop(index_to_move))
            response['root']['children']=x
            # response['root']['children'] = sorted(response['root']['children'], key=lambda x: x['fields']['bq_legal_entity_parent_status'], reverse=True)
        except Exception as e:
            pass
            
        for i in response['root']['children']:
            if i['fields'].get("bq_company_ofac_indicator",None) != None:
                if i['fields']['bq_company_ofac_indicator']==True:
                    i['fields']['bq_company_ofac_indicator']=1
                else:
                    i['fields']['bq_company_ofac_indicator']=0
            if i['fields'].get("bq_company_irs_taxlien_indicator",None) != None:
                if i['fields']['bq_company_irs_taxlien_indicator']==True:
                    i['fields']['bq_company_irs_taxlien_indicator']=1
                else:
                    i['fields']['bq_company_irs_taxlien_indicator']=0
        
        response = {"response":response,"status":200}
        # try:
        #     url = "http://54.190.237.221:8000/q/saver/"
        #     tab = "parent_entity"
        #     result = str(response)
        #     query = yql.lower() + str(offset) + str(filter).replace(" ","") + str(orderby).replace(" ","")
        #     payload = {"query":query, "tab":tab, "result":result}
        #     files=[]
        #     headers = {}

        #     response1 = requests.request("POST", url, headers=headers, data=payload, files=files)
        # except:
        #     pass
        print(1111111111111111111,yql)
        return response
        
        

    # except requests.RequestException as e:
    #     return JSONResponse(content={"error": "An error occurred while processing the search request.", "details": str(e)}, status_code=500)
    
    except Exception as e:
        response = {"response":{"error": "An unexpected error occurred.", "details": str(e)},"status":400}
        return response
        # return JSONResponse(content={"error": "An unexpected error occurred.", "details": str(e)}, status_code=500)

def side_bar(tab,query,yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, ult_selection):
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    hits = 50000
    if query == "" or query == None:
        data  = {"total_companies":0,
                "bq_employment_mr_total":0,
                "bq_revenue_mr_total":0,
                "bq_employment_mr_avg":0,
                "bq_revenue_mr_avg":0,}
        # status_code=200
        response = {"response":data,"status":200}
        return response
        # return JSONResponse(content=data, status_code=status_code)
    if tab == 'company_name':
        if tab == 'company_name' and field == 'bq_location_name':
            response = search_by_location_address(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "user_id", "sidebar", ult_selection)
            
            try:
                response = {"response":screener_sidebar(response['response'])['response']}
                response = {"response":response['response'],"status":200}
            except Exception as e:
                response = {"response":str(e),"status":400}
            return response
        query = query.lower()
        orderby='bq_revenue_mr'
        yql = "select bq_organization_id, bq_revenue_mr, bq_employment_mr from terminal_screener where"
        if query:
            # print("Inside If query")
            if field:
                if yql.lower().rstrip().endswith('and'):
                    if field == "bq_organization_legal_name":
                        field = 'bq_legal_entity_name'
                        yql = f'{yql} and (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}")) and'
                    if field == "bq_organization_name":
                        yql = f'{yql} and (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

                    yql = f'{yql} and {field} contains "{query}" and'
                elif yql.lower().rstrip().endswith('where'):
                    if field == "bq_organization_legal_name":
                        field = 'bq_legal_entity_name'
                        yql = f'{yql} (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}")) and'
                    if field == "bq_organization_name":
                        yql = f'{yql} (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'


                else:
                    if field == "bq_organization_legal_name":
                        yql = f'{yql} and (({field} contains "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}")) and'
                    if field == "bq_organization_name":
                        yql = f'{yql} and (({field} contains "{query}") or (bq_organization_id contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

            else:
                query = query.lower()
                if 'and' in query:
                    qqq = query.lower().rstrip().split('and')
                    q1 = qqq[0]
                    q3 = qqq[1].strip()
                    q3 = q3.replace(',', ' ')
                    q3 = q3.replace(';', ' ')
                    q3 = re.sub(r'\s+', ' ', q3)
                    q2 = q3.split(' ')
                    s2 = ''
                    for i in q2:
                        s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                        # break
                    yql = f"{yql} ({remove_and_from_end(s2)}) and"
                else:
                    query = query.replace(',', ' ')
                    query = query.replace(';', ' ')
                    query = re.sub(r'\s+', ' ', query)
                    query1 = query.split(' ')
                    query1 = [word for word in query1 if word not in ["of"]]
                    s1 = ''
                    for word in query1:
                        if word == "":
                            continue
                        s1 = f'{s1} default contains "{word}" OR'
                        # break
                    yql = f"{yql} ({remove_and_from_end(s1)}) and"  
    
    elif tab == 'ticker_prefix':
        prefix = '{prefix: true}'
        if field == 'bq_organization_ticker':
            yql = f'select bq_organization_id, bq_revenue_mr, bq_employment_mr from terminal_screener where {field} contains ({prefix}"{query}") and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")'     
        else:
            yql = f'select bq_organization_id, bq_revenue_mr, bq_employment_mr from terminal_screener where bq_ticker_related_individual contains sameElement(bq_ticker_related_individual contains ({prefix}"{query}")) and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")  '
    
    elif tab == 'ticker_matches':
        field='bq_organization_ticker'
        search_endpoint = f"{VESPA_ENDPOINT}/search/"

        prefix = '{prefix: true}'
        
        yql = f'select bq_organization_id, bq_revenue_mr, bq_employment_mr from terminal_screener where {field} contains "{query}" and (bq_legal_entity_parent_status contains "Ultimate Parent" OR bq_legal_entity_parent_status contains "Sole" OR bq_organization_structure contains "Single-entity organization")'     

    elif tab in ['sidebar_ein','sidebar_lei','sidebar_domain','sidebar_cik','sidebar_universal','company_address']:
        yql = "select bq_organization_id, bq_revenue_mr, bq_employment_mr from terminal_screener where"
        bq_organization_lei = query
        if tab == 'sidebar_cik':
            field = 'bq_organization_cik'
        elif tab == 'sidebar_lei':
            field = 'bq_organization_lei'
        elif tab == 'sidebar_domain':
            field = 'bq_organization_website'
        if query and tab != "company_address":
            # print("Inside If query")
            if field:
                if field =='bq_organization_lei':
                    query = query.replace('&','%')
                    query = query.replace('#','%')
                    # print(7777777777777777777777777777777777777777,query)
                    prefix = '{prefix: true}'
                    if yql.lower().rstrip().endswith('and'):
                        yql = f"{yql} and {field} contains ({prefix}'{query}') and"
                    elif yql.lower().rstrip().endswith('where'):
                        yql = f"{yql} {field} contains ({prefix}'{query}') and"
                    else:
                        yql = f"{yql} and {field} contains ({prefix}'{query}') and"
                elif field == 'bq_organization_website':
                    # print(111111111111111111111111111111111111111111111111111111111111111111111111111111111,query)
                    query = query.replace('www.','')
                    query = query.replace('https://','')
                    query = query.replace('http://','')
                    if yql.lower().rstrip().endswith('and'):
                        yql = f"{yql} and (({field} contains '{query}' or bq_organization_linkedin_url contains '{query}') or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}'))"
                        # yql = f"{yql} and {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' "
                    elif yql.lower().rstrip().endswith('where'):
                        yql = f"{yql} {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}')"
                        # yql = f"{yql} {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' "
                    else:
                        yql = f"{yql} and {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}')"
                    # print(111111111111111111111111111111111111111111111111111111111111111111111111111111111,query, yql)
                else:
                    if yql.lower().rstrip().endswith('and'):
                        yql = f"{yql} and {field} contains '{query}' and"
                    elif yql.lower().rstrip().endswith('where'):
                        yql = f"{yql} {field} contains '{query}' and"
                    else:
                        yql = f"{yql} and {field} contains '{query}' and"
                
            else:
                query = query.lower()
                if 'and' in query:
                    qqq = query.lower().rstrip().split('and')
                    query = re.sub(r'\s{2,}', ' ', query)
                    q1 = qqq[0].strip()
                    q3 = qqq[1].strip()
                    q3 = q3.replace(',', ' ')
                    q3 = q3.replace(';', ' ')
                    q3 = re.sub(r'\s+', ' ', q3)
                    q2 = q3.split(' ')
                    s2 = ''
                    for i in q2:
                        s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                        # break
                    yql = f'{yql} ({remove_and_from_end(s2)}) and'
                else:
                    # print(34124234342,query, field)
                    query = query.replace(',', ' ')
                    query = query.replace(';', ' ')
                    query = re.sub(r'\s+', ' ', query)
                    query1 = query.split(' ')
                    query1 = [word for word in query1 if word not in ["of"]]
                    s1 = ''
                    for word in query1:
                        if word == "":
                            continue
                        s1 = f'{s1} default contains "{word}" OR'
                        # break
                    yql = f"{yql} ({remove_and_from_end(s1)}) and"  
                    # yql = f"{yql} default contains '{remove_and_from_end(query)}' and"
        elif tab == "company_address":
            query = query.replace(',','')
            query = query.split()
            address_search_yql = ''
            ult_selection = field.split("+")[1]
            for query in query:
                if ult_selection == "orgAddress":
                    address_search_yql = address_search_yql + f"( bq_organization_address1_line_1 contains '{query}' OR bq_organization_address1_line_2 contains '{query}' OR  bq_organization_address1_state contains '{query}' OR bq_organization_address1_state_name contains '{query}' OR bq_organization_address1_zip5 contains '{query}' OR bq_organization_address1_city contains '{query}' OR bq_organization_legal_address1_state contains '{query}' OR bq_organization_legal_address1_zip5 contains '{query}' OR bq_organization_legal_address1_line_1 contains '{query}' OR bq_organization_legal_address1_city contains '{query}') AND "
                else:
                    # code to search in legal entity - yatin
                    address_search_yql = address_search_yql + f"( bq_legal_entity_address1_line_1 contains '{query}' OR bq_legal_entity_address1_line_2 contains '{query}' OR bq_legal_entity_address1_city contains '{query}' OR bq_legal_entity_address1_state contains '{query}' OR bq_legal_entity_address1_zip5 contains '{query}' OR bq_organization_legal_address1_line_1 contains '{query}' OR bq_organization_legal_address1_line_2 contains '{query}') AND "

            address_search_yql= address_search_yql[:-4].rstrip() 
            if yql.lower().rstrip().endswith('and'):
                yql = f'{yql} and ( {address_search_yql} ) and'
            elif yql.lower().rstrip().endswith('where'):
                yql = f'{yql} ( {address_search_yql} ) and'
            else:
                yql = f'{yql} and ( {address_search_yql} ) and'  
        
        if yql:
            if filter:
                try:
                    filter = json.loads(filter.replace("'", "\""))
                except json.JSONDecodeError as e:
                    response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                    return response
                    # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
                for key, val in filter.items():
                    if len(val) >= 1:
                        if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                            final = ''
                            for items in val:
                                itm = ''
                                for i in items:
                                    itm = f"{itm} {key} {i} AND"
                                itm = remove_and_from_end(itm)
                                itm = f'({itm})'
                                final = f"{final} {itm} OR"
                            yql = f"{yql} ({remove_and_from_end(final)}) AND"
                        else:
                            if len(val) > 1:
                                yql_part = ''
                                for v in val:
                                    yql_part = yql_part + f"{key} contains '{v}' OR "
                                yql_part = remove_and_from_end(yql_part)
                                yql_part = f"({yql_part})"
                                yql = f"{yql} {yql_part} and"
                            elif len(val) == 0:
                                pass
                            else:
                                if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                    yql = remove_and_from_end(yql)
                                    yql = f"{yql} AND {key} contains '{val[0]}' AND"
                                elif yql.lower().rstrip().endswith('where'):
                                    yql = f"{yql} {key} contains '{val[0]}' AND"
                                else:
                                    yql = f"{yql} AND {key} contains '{val[0]}' AND"
        yql = remove_and_from_end(yql)
        if orderby:
            order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_employment_growth_yoy_mr":"bq_employment_growth_yoy_mr","bq_organization_active_indicator":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation"}
            orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
       
            order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            if ult_selection == "orgAddress":
                yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization')) order by {orderbyField} {order}"
            else:
                yql = f"{yql} and order by {orderbyField} {order}"
        else:
            if ult_selection == "orgAddress":
                yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
            else:
                pass
        params = {
                'yql': yql,
                'filter': filter,
                # 'offset': offset,
                # 'ranking': ranking,
                'limit': 50000,
                'type': 'all',
                'hits': 50000,
                "format": "json",
            }
        # try:
        #     url = "http://54.190.237.221:8000/q/query/"
        #     tab = "side_bar"
        #     query = yql.lower() + str(offset) + str(filter).replace(" ","") + str(orderby).replace(" ","")

        #     payload = {"query":query, "tab":tab}
        #     files=[]
        #     headers = {}

        #     response = requests.request("POST", url, headers=headers, data=payload, files=files)
        #     if response.status_code == 200:
        #         null, true, false = None, True, False
        #         response = eval(response.json())
        #         response = {"response":response,"status":200}
        #         return response
        #         # response = {"response":eval(response),"status":200}
        #     else:
        #         response = False
        #         # return response
        #         # return JSONResponse(content=eval(response.json()), status_code=200)
        # except Exception as e:
        #     print("error details================",str(e))
        #     pass
        
        response = requests.get(search_endpoint, params=params).json()
        
        try:
            # response= response
        
            response = list(pd.DataFrame(response['root']['children']).fillna(0)['fields'])
            # response = [response[i] for i in range(len(response)) if response[i] not in response[:i]]

            bq_employment_mr_total = []
            bq_revenue_mr_total = []
            total_companies = []
            for i in response:
                try:
                    total_companies.append(i.get('bq_organization_id',0))
                except:
                    pass
                try:
                    bq_employment_mr_total.append(i.get('bq_employment_mr',0))
                except:
                    pass
                try:
                    bq_revenue_mr_total.append(i.get('bq_revenue_mr',0))
                except:
                    pass
            total_companies = list(set(total_companies))
            if 0 in total_companies:
                total_companies.remove(0)
            total_companies = len(total_companies)

            companies_count_current_employees_plan_mr = len(bq_employment_mr_total)-bq_employment_mr_total.count(0)
            companies_count_bq_revenue_mr = len(bq_revenue_mr_total) - bq_revenue_mr_total.count(0)

            bq_employment_mr_total = sum(bq_employment_mr_total)
            bq_revenue_mr_total = sum(bq_revenue_mr_total)
            try:
                bq_employment_mr_avg = bq_employment_mr_total / companies_count_current_employees_plan_mr
            except:
                bq_employment_mr_avg = 0
            try:
                bq_revenue_mr_avg = bq_revenue_mr_total / companies_count_bq_revenue_mr
            except:
                bq_revenue_mr_avg = 0
            data  = {"total_companies":total_companies,
                    "bq_employment_mr_total":bq_employment_mr_total,
                    "bq_revenue_mr_total":bq_revenue_mr_total,
                    "bq_employment_mr_avg":bq_employment_mr_avg,
                    "bq_revenue_mr_avg":bq_revenue_mr_avg,}
            # status_code=200
            response = {"response":data,"status":200}
            # try:
            #     url = "http://54.190.237.221:8000/q/saver/"
            #     tab = "side_bar"
            #     result = str(response['response'])
            #     query = yql.lower() + str(offset) + str(filter).replace(" ","") + str(orderby).replace(" ","")
            #     payload = {"query":query, "tab":tab, "result":result}
            #     files=[]
            #     headers = {}

            #     response1 = requests.request("POST", url, headers=headers, data=payload, files=files)
            # except Exception as e:
            #     print("error===============",str(e))
            #     pass
        except:
            data={"total_companies":0,
                    "bq_employment_mr_total":0,
                    "bq_revenue_mr_total":0,
                    "bq_employment_mr_avg":0,
                    "bq_revenue_mr_avg":0,}
            # status_code=200
            response = {"response":data,"status":200}
        return response
        # return JSONResponse(content=data, status_code=status_code)
    else:
        if query:
        # print("Inside If query")
            if field:
                if yql.lower().rstrip().endswith('and'):
                    yql = f'{yql} and {field} contains "{query}" and'
                elif yql.lower().rstrip().endswith('where'):
                    yql = f'{yql} {field} contains "{query}" and'
                else:
                    yql = f'{yql} and {field} contains "{query}" and'
            else:
                query = query.lower()
                if 'and' in query:
                    qqq = query.lower().rstrip().split('and')
                    query = re.sub(r'\s{2,}', ' ', query)
                    q1 = qqq[0].strip()
                    q3 = qqq[1].strip()
                    q3 = q3.replace(',', ' ')
                    q3 = q3.replace(';', ' ')
                    q3 = re.sub(r'\s+', ' ', q3)
                    q2 = q3.split(' ')
                    s2 = ''
                    for i in q2:
                        s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                        # break
                    yql = f'{yql} ({remove_and_from_end(s2)}) and'
                else:
                    query = query.replace(',', ' ')
                    query = query.replace(';', ' ')
                    query = re.sub(r'\s+', ' ', query)
                    query1 = query.split(' ')
                    query1 = [word for word in query1 if word not in ["of"]]
                    s1 = ''
                    for word in query1:
                        if word == "":
                            continue
                        s1 = f'{s1} default contains "{word}" OR'
                        # break
                    yql = f"{yql} ({remove_and_from_end(s1)}) and"  
    if yql:
        if filter:
            try:
                filter = json.loads(filter.replace("'", "\""))
            except json.JSONDecodeError as e:
                response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
                # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f'{key} contains "{v}" OR '
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    if orderby:
        if orderby == 'bq_organization_structure':
            orderbyField = 'bq_organization_structure'
        elif orderby == 'bq_organization_name':
            orderbyField = 'bq_organization_name'
        elif orderby == 'bq_organization_address1_line_1':
            orderbyField = 'bq_organization_address1_line_1'
        elif orderby == 'bq_organization_jurisdiction_code':
            orderbyField = 'bq_organization_jurisdiction_code'
        elif orderby == 'bq_revenue_mr':            
            orderbyField = 'bq_revenue_mr'
        elif orderby == 'bq_employment_mr':            
            orderbyField = 'bq_employment_mr'
        else:
            orderbyField = 'bq_revenue_mr'
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        yql = f"{yql} order by {orderbyField} {order}"
        
    params = {
        'yql': yql,
        'filter': filter,
        # 'offset': offset,
        # 'ranking': ranking,
        'limit': 50000,
        'type': 'all',
        'hits': 50000,
        "format": "json",
    }
    # try:
    #     url = "http://54.190.237.221:8000/q/query/"
    #     tab = "side_bar"
    #     query = yql.lower() + str(offset) + str(filter).replace(" ","") + str(orderby).replace(" ","")

    #     payload = {"tab":tab ,"query":query}
    #     files=[]
    #     headers = {}

    #     response = requests.request("POST", url, headers=headers, data=payload, files=files)
    #     if response.status_code == 200:
    #         null, true, false = None, True, False
    #         response = eval(response.json())
    #         response = {"response":response,"status":200}
    #         return response
    #         # response = {"response":eval(response),"status":200}
    #     else:
    #         response = False
    #         # return response
    #         # return JSONResponse(content=eval(response.json()), status_code=200)
    # except Exception as e:
    #     print("error details================",str(e))
    #     pass
    
    response = requests.get(search_endpoint, params=params).json()
    if response["root"]["fields"]["totalCount"] == 0 and tab == 'company_name':
        yql = yql.replace('and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization"))','')
        print("search_by_company_name = ", yql)
        params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()
    

    try:
        # response= response.json()
      
        response = list(pd.DataFrame(response['root']['children']).fillna(0)['fields'])
        # response = [response[i] for i in range(len(response)) if response[i] not in response[:i]]

        bq_employment_mr_total = []
        bq_revenue_mr_total = []
        total_companies = []
        for i in response:
            try:
                total_companies.append(i.get('bq_organization_id',0))
            except:
                pass
            try:
                bq_employment_mr_total.append(i.get('bq_employment_mr',0))
            except:
                pass
            try:
                bq_revenue_mr_total.append(i.get('bq_revenue_mr',0))
            except:
                pass
        total_companies = list(set(total_companies))
        if 0 in total_companies:
            total_companies.remove(0)
        total_companies = len(total_companies)

        companies_count_current_employees_plan_mr = len(bq_employment_mr_total)-bq_employment_mr_total.count(0)
        companies_count_bq_revenue_mr = len(bq_revenue_mr_total) - bq_revenue_mr_total.count(0)

        bq_employment_mr_total = sum(bq_employment_mr_total)
        bq_revenue_mr_total = sum(bq_revenue_mr_total)
        try:
            bq_employment_mr_avg = bq_employment_mr_total / companies_count_current_employees_plan_mr
        except:
            bq_employment_mr_avg = 0
        try:
            bq_revenue_mr_avg = bq_revenue_mr_total / companies_count_bq_revenue_mr
        except:
            bq_revenue_mr_avg = 0
        data  = {"total_companies":total_companies,
                "bq_employment_mr_total":bq_employment_mr_total,
                "bq_revenue_mr_total":bq_revenue_mr_total,
                "bq_employment_mr_avg":bq_employment_mr_avg,
                "bq_revenue_mr_avg":bq_revenue_mr_avg,}
        # status_code=200
        response = {"response":data,"status":200}
        # try:
        #     url = "http://54.190.237.221:8000/q/saver/"
        #     tab = "side_bar"
        #     result = str(response['response'])
        #     query = yql.lower() + str(offset) + str(filter).replace(" ","") + str(orderby).replace(" ","")
        #     payload = {"query":query, "tab":tab, "result":result}
        #     files=[]
        #     headers = {}

        #     response1 = requests.request("POST", url, headers=headers, data=payload, files=files)
        # except Exception as e:
        #     print("error===============",str(e))
        #     pass
        
    except Exception as e:
        data  = {"total_companies":"N/A",
                "bq_employment_mr_total":"N/A",
                "bq_revenue_mr_total":"N/A",
                "bq_employment_mr_avg":"N/A",
                "bq_revenue_mr_avg":"N/A",}
        # data=e
        # status_code=400
        response = {"response":data,"status":200}
    return response
    # return JSONResponse(content=data, status_code=status_code)

def get_organization_history(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field):
    try:
        hits = 20
        isAsc = True
        # print("query =",query)
        VESPA_ENDPOINT = "http://34.211.12.161:8080"
        search_endpoint = f"{VESPA_ENDPOINT}/search/"
        yql = f"select * from  financial_report where bq_organization_id contains '{query}'"
        print('get_financial : ',yql)
        params = {
            'yql': yql,
            # 'query': query,
            'filter': filter,
            'offset': offset,
            'ranking': ranking,
            'limit': limit,
            'type': 'all',
            'hits': hits,
            "format": "json",
        }

        response = requests.get(search_endpoint, params=params)
        response = {"response":response.json(),"status":200}
        return response
        # print(response)
        # return JSONResponse(content=response.json(), status_code=200)

    # except requests.RequestException as e:
    #     return JSONResponse(content={"error": "An error occurred while processing the search request.", "details": str(e)}, status_code=500)
    
    except Exception as e:
        response = {"response":{"error": "An unexpected error occurred.", "details": str(e)},"status":400}
        return response
        # return JSONResponse(content={"error": "An unexpected error occurred.", "details": str(e)}, status_code=500)

def search(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, request_origin, search_product):
    search_endpoint = f'{VESPA_ENDPOINT}/search/'
    hits=20
    if request_origin == "terminal":
        yql = terminal_yql
    elif request_origin =="external":
        yql = f"SELECT  {','.join(QUERY_FIELDS[search_product])} FROM terminal_screener where "
    else:
        yql = search_yql

    print('request_origin:::::::::', request_origin)
    if field=='company_name_address':
        field=''
    
    if query:
        print(12312321234234234111111111111, query, field)
        # print("Inside If query")
        if field:
            if field in ['bq_organization_website','bq_organization_linkedin_url']:
                # print(111111111111111111111111111111111111111111111111111111111111111111111111111111111,query)
                query = query.replace('www.','')
                query = query.replace('https://','')
                query = query.replace('http://','')
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and (({field} contains '{query}' or bq_organization_linkedin_url contains '{query}') or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}'))"
                    # yql = f"{yql} and {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' "
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}')"
                    # yql = f"{yql} {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' "
                else:
                    yql = f"{yql} and {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}')"
                    # yql = f"{yql} and {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' "
                # print(111111111111111111111111111111111111111111111111111111111111111111111111111111111,query, yql)
            elif field =='bq_organization_lei':
                # print(8888888888888888888888888888888888888888,query)
                query = query.replace('&','%')
                query = query.replace('#','%')
                # print(7777777777777777777777777777777777777777,query)
                prefix = '{prefix: true}'
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and {field} contains ({prefix}'{query}') and"
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} {field} contains ({prefix}'{query}') and"
                else:
                    yql = f"{yql} and {field} contains ({prefix}'{query}') and"
            
            elif field =='bq_organization_ein':
                # print(8888888888888888888888888888888888888888,query)
                query = query.replace('&','%')
                query = query.replace('#','%')
                # print(7777777777777777777777777777777777777777,query)
                prefix = '{prefix: true}'
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and ({field} contains '{query}' or bq_organization_eins_related contains '{query}') and"
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} ({field} contains '{query}' or bq_organization_eins_related contains '{query}') and"
                else:
                    yql = f"{yql} and ({field} contains '{query}' or bq_organization_eins_related contains '{query}') and"
                    
            else:
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and {field} contains '{query}' and"
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} {field} contains '{query}' and"
                else:
                    yql = f"{yql} and {field} contains '{query}' and"
            # print(34124234342,query, field)
            
        else:
            query = query.lower()
            if 'and' in query:
                qqq = query.lower().rstrip().split('and')
                query = re.sub(r'\s{2,}', ' ', query)
                q1 = qqq[0].strip()
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f"{s2} (default contains '{q1}' and default contains '{i}') OR"
                    # break
                yql = f'{yql} ({remove_and_from_end(s2)}) and'
                
            elif '&' in query:
                qqq = query.lower().rstrip().split('&')
                query = re.sub(r'\s{2,}', ' ', query)
                q1 = qqq[0].strip()
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f"{s2} (default contains '{q1}' and default contains '{i}') OR"
                    # break
                yql = f'{yql} ({remove_and_from_end(s2)}) and'
            else:
                # print(34124234342,query, field)
                query = query.replace(',', ' ')
                query = query.replace(';', ' ')
                query = re.sub(r'\s+', ' ', query)
                query1 = query.split(' ')
                query1 = [word for word in query1 if word not in ["of"]]
                s1 = ''
                for word in query1:
                    if word == "":
                        continue
                    s1 = f'{s1} default contains "{word}" OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s1)}) and"  

    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
                # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f"{key} contains '{v}' OR "
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    # print('order by:', orderby)
    if orderby:        
        order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr","bq_organization_isactive":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation", "bq_organization_address1_line_1":"bq_organization_address1_line_1", "bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code"}
        orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
        
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        
        if field !='bq_organization_ein' and field !='bq_organization_lei':
            yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization')) order by {orderbyField} {order}"
        else:
            if (request_origin != "terminal") & (request_origin != "external"):
                yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization')) order by {orderbyField} {order}"
            else:
                yql = f"{yql} order by {orderbyField} {order}"
    else:
        if field !='bq_organization_ein' and field !='bq_organization_lei':
            yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
        else:
            if (request_origin != "terminal") | (request_origin != "external"):
                yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
            else:
                yql = yql

    # print(f"\n\n\nssssss after = {yql}")
    logger.info(f'YQL: {yql} \n Limit: {limit}')
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
        'timeout':39
    }
    response = requests.get(search_endpoint, params=params).json()
    response = {"response":response,"status":200}
    return response
    
    # return JSONResponse(content=response.json(), status_code=200)

def get_financial_data(query=None, yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby='bq_revenue_mr', isAsc=True, field=None):
    try:
        # print("query =",query)
        
        VESPA_ENDPOINT = "http://34.211.12.161:8080"
        search_endpoint = f"{VESPA_ENDPOINT}/search/"
        yql = f"select * from  financial_report where bq_organization_id contains '{query}'"
        params = {
            'yql': yql,
            # 'query': query,
            'filter': filter,
            'offset': offset,
            'ranking': ranking,
            'limit': limit,
            'type': 'all',
            'hits': hits,
            "format": "json",
        }

        response = requests.get(search_endpoint, params=params).json()
        response = {"response":response,"status":200}
        return response
        # response.raise_for_status()
        # print(response)
        # return JSONResponse(content=response.json(), status_code=200)

    # except requests.RequestException as e:
    #     return JSONResponse(content={"error": "An error occurred while processing the search request.", "details": str(e)}, status_code=500)
    
    except Exception as e:
        response = {"response":{"error": "An unexpected error occurred.", "details": str(e)},"status":400}
        return response
        # return JSONResponse(content={"error": "An unexpected error occurred.", "details": str(e)}, status_code=500)

def locationsearch(data):
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    
    # data = await request.json()
    bq_id = data.get('bq_id')
    offset = data.get('offset',0)
    limit = data.get('limit',20)
    hits = data.get('hits',20)
    filter = data.get('filter',"All")
    yql = f"SELECT bq_organization_id, bq_location_name, bq_location_type, bq_location_address_line_1, bq_location_address_city, bq_location_address_state_name, bq_location_address_zip5, bq_location_address_country_name, bq_location_address_state_name FROM bq_location WHERE bq_organization_id contains '{bq_id}'"
    if filter == "All":
        yql = yql + ";"
    else:
        yql = yql + "and bq_location_type contains '" + filter + "';"
    params = {
        'yql': yql,
        'ranking': 'bm25',
        'type': 'all',
        'hits': hits,
        'offset': offset,
        'limit': limit,
        "format": "json"
    }
    response = requests.get(search_endpoint, params=params).json()
    response = {"response":response,"status":200}
    return response

def company_name_fuzzy(query=None,yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby=None, isAsc=False, field=None, user_id=None, request_origin=None, search_product=None):
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    query = query.lower()
    ratio_query = query
    if request_origin == "terminal":
        yql = terminal_yql
        yql = yql.replace(" from terminal_screener where",", ")
        yql = yql + "bq_legal_entity_address1_line_1, bq_legal_entity_address1_line_2, bq_legal_entity_address1_city, bq_legal_entity_address1_state, bq_legal_entity_address1_zip5 from terminal_screener where"    
    elif request_origin =="external":
        yql = f"select {','.join(QUERY_FIELDS[search_product])} from terminal_screener where"
    else:
        yql = search_yql
    
    if query:
        if field:
            if yql.lower().rstrip().endswith('and'):
                if field == "bq_legal_entity_name":
                    yql = f'{yql} and (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}") or (bq_legal_entity_company_number contains "{query}")) and'
                if field == "bq_organization_name":
                    yql = f'{yql} and (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

                yql = f'{yql} and {field} contains "{query}" and'
            elif yql.lower().rstrip().endswith('where'):
                if field == "bq_legal_entity_name":
                    yql = f'{yql} (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains ({maxEditDistance}fuzzy("{query}"))) or (bq_legal_entity_company_number contains "{query}")) and'
                if field == "bq_organization_name":
                    # yql = f'{yql} (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'
                    yql = f'{create_yql_advanced(field, query, yql, yql_flag="fuzzy")} ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

            else:
                if field == "bq_legal_entity_name":
                    yql = f'{yql} and (({field} contains "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}") or (bq_legal_entity_company_number contains "{query}")) and'
                if field == "bq_organization_name":
                    yql = f'{yql} and (({field} contains "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'
        else:
            query = query.lower()
            if 'and' in query:
                qqq = query.lower().rstrip().split('and')
                q1 = qqq[0]
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                yql = f"{yql} ({remove_and_from_end(s2)}) and"
            else:
                query = query.replace(',', ' ')
                query = query.replace(';', ' ')
                query = re.sub(r'\s+', ' ', query)
                query1 = query.split(' ')
                query1 = [word for word in query1 if word not in ["of"]]
                s1 = ''
                for word in query1:
                    if word == "":
                        continue
                    s1 = f'{s1} default contains "{word}" OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s1)}) and"

    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f'{key} contains "{v}" OR '
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    
    yql = remove_and_from_end(yql)
    order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr","bq_organization_isactive":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation", "bq_organization_address1_line_1":"bq_organization_address1_line_1", "bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code"}
    orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
    order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
    yql = f"{yql} order by {orderbyField} {order}"
    print("search_by_company_name_test+fuzzy = ", query,11111111111,yql)
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }
    response = requests.get(search_endpoint, params=params).json()
    if response["root"]["fields"]["totalCount"] == 0:
        yql = yql.replace('and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization"))','')
        print("search_by_company_name = ", yql)
        params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()
    if "children" in response["root"].keys():
        x = response["root"]["children"]
        y = [z['fields'] for z in x]
        # print(get_ratio(ratio_query,y, match_method='fuzzy'))
        top_20 = get_ratio(ratio_query,y, match_method='fuzzy')
        # print(zz)
        top_20_list = []
        cnt=0
        for item in top_20:
            cnt+=1
            dct_item = {}
            dct_item["id"] = str(len(top_20)-cnt)
            dct_item["fields"] = item
            top_20_list.append(dct_item)

        response["root"]["children"]=top_20_list
    else:
        response = {"response":"No records found","status":200}

        
    return response

def company_name(query=None,yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby=None, isAsc=False, field=None, user_id=None, request_origin=None, search_product='BQ_BUSINESS_IDENTITY_API'):    
    # ({maxEditDistance:1}fuzzy("{query}"))
    # VESPA_ENDPOINT = "http://localhost:8080"
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    query_fuzzy = query
    filters_fuzzy = filter
    query = query.lower()
    # orderby='bq_revenue_mr'
    if request_origin == "terminal":
        yql = terminal_yql
        yql = yql.replace(" from terminal_screener where",", ")
        yql = yql + "bq_legal_entity_address1_line_1, bq_legal_entity_address1_line_2, bq_legal_entity_address1_city, bq_legal_entity_address1_state, bq_legal_entity_address1_zip5 from terminal_screener where"
    elif request_origin =="external":
        yql = f"select {','.join(QUERY_FIELDS[search_product])} from terminal_screener where"                    
    else:
        yql = search_yql
    
    if query:
        # print("Inside If query")
        if field:
            if yql.lower().rstrip().endswith('and'):
                if field == "bq_legal_entity_name":
                    # field = 'bq_legal_entity_name'
                    yql = f'{yql} and (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}") or (bq_legal_entity_company_number contains "{query}")) and'
                if field == "bq_organization_name":
                    yql = f'{yql} and (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

                yql = f'{yql} and {field} contains "{query}" and'
            elif yql.lower().rstrip().endswith('where'):
                if field == "bq_legal_entity_name":
                    # yql = f'{yql} (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}") or (bq_legal_entity_company_number contains "{query}")) and' #Before fuzzy
                    yql = f'{yql} (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains ({maxEditDistance}fuzzy("{query}"))) or (bq_legal_entity_company_number contains "{query}")) and'
                if field == "bq_organization_name":
                    # yql = f'{yql} (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'#Before fuzzy
                    yql = f'{yql} (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

            else:
                if field == "bq_legal_entity_name":
                    yql = f'{yql} and (({field} contains "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}") or (bq_legal_entity_company_number contains "{query}")) and'
                if field == "bq_organization_name":
                    yql = f'{yql} and (({field} contains "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

            # if field == "bq_organization_legal_name":
            #     pass
            
            # if field == "bq_organization_name":
            #     yql = f'{yql} ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization"))'
        else:
            query = query.lower()
            if 'and' in query:
                qqq = query.lower().rstrip().split('and')
                q1 = qqq[0]
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s2)}) and"
            else:
                query = query.replace(',', ' ')
                query = query.replace(';', ' ')
                query = re.sub(r'\s+', ' ', query)
                query1 = query.split(' ')
                query1 = [word for word in query1 if word not in ["of"]]
                s1 = ''
                for word in query1:
                    if word == "":
                        continue
                    s1 = f'{s1} default contains "{word}" OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s1)}) and"   
    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f'{key} contains "{v}" OR '
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    # if orderby:
    order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr","bq_organization_isactive":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation", "bq_organization_address1_line_1":"bq_organization_address1_line_1", "bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code"}
    orderbyField = order_by_map.get(orderby,"bq_revenue_mr")    
    print(orderby,"/////////////////---------------")
    order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
    yql = f"{yql} order by {orderbyField} {order}"
    print("search_by_company_name_test = ", query,11111111111,yql)
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }    
    response = requests.get(search_endpoint, params=params).json()
    
    if response["root"]["fields"]["totalCount"] == 0:
        yql = yql.replace('and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization"))','')
        print("search_by_company_name = ", yql)
        params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()

    if response["root"]["fields"]["totalCount"] == 0:
        # print("++++++++++++++++++++++++++++++++++")
        response = company_name_fuzzy(query=query_fuzzy,yql=None, type='all', filter=filters_fuzzy, ranking='bm25', hits=20, limit=50, offset=0, orderby=None, isAsc=False, field=field, user_id=None, request_origin=None, search_product=search_product) 
        # print("**********************************")

    response = {"response":response,"status":200}
    return response

def company_name_old(query=None,yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby=None, isAsc=False, field=None, user_id=None, request_origin=None):
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    query = query.lower()
    # orderby='bq_revenue_mr'
    if request_origin == "terminal":
        yql = terminal_yql
        yql = yql.replace(" from terminal_screener where",", ")
        yql = yql + "bq_legal_entity_address1_line_1, bq_legal_entity_address1_line_2, bq_legal_entity_address1_city, bq_legal_entity_address1_state, bq_legal_entity_address1_zip5 from terminal_screener where"
    elif request_origin =="external":
        yql = org_terminal_yql                
    else:
        yql = search_yql
    # yql = "select bq_organization_id, bq_organization_ein, bq_legal_entity_parent_status, bq_organization_name, bq_organization_structure, bq_organization_company_type, bq_organization_ticker, bq_organization_public_indicator, bq_organization_legal_name, bq_legal_entity_id, bq_organization_address1_type, bq_organization_address1_line_2, bq_organization_address1_city, bq_organization_address1_state, bq_organization_address1_zip5, bq_legal_entity_jurisdiction_code, bq_organization_date_founded, bq_organization_current_status, bq_organization_active_indicator, bq_organization_address1_line_1, bq_organization_address1_line_2, bq_employment_mr, bq_score, bq_organization_irs_sector_name, bq_revenue_mr, bq_organization_cik, bq_organization_lei, bq_organization_legal_address1_line_1, bq_organization_legal_address1_line_2, bq_organization_legal_address1_city, bq_organization_legal_address1_state, bq_organization_legal_address1_zip5, bq_organization_website, bq_organization_sector_name, bq_organization_sector_code, bq_organization_naics_sector_name, bq_organization_naics_sector_code, bq_organization_naics_name, bq_legal_entity_children_count, bq_organization_naics_code, bq_organization_current_status, bq_legal_entity_current_status, bq_legal_entity_active_indicator from terminal_screener where"
    # yql = " select * from terminal_screener where"
    # if query == "": yql += " true "
    if query:
        # print("Inside If query")
        if field:
            if yql.lower().rstrip().endswith('and'):
                if field == "bq_organization_legal_name":
                    field = 'bq_legal_entity_name'
                    yql = f'{yql} and (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}")) and'
                if field == "bq_organization_name":
                    yql = f'{yql} and (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

                yql = f'{yql} and {field} contains "{query}" and'
            elif yql.lower().rstrip().endswith('where'):
                if field == "bq_organization_legal_name":
                    field = 'bq_legal_entity_name'
                    yql = f'{yql} (({field} matches "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}")) and'
                if field == "bq_organization_name":
                    yql = f'{yql} (({field} matches "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

            else:
                if field == "bq_organization_legal_name":
                    yql = f'{yql} and (({field} contains "{query}") or (bq_legal_entity_id contains "{query}") or ({field} contains "{query}")) and'
                if field == "bq_organization_name":
                    yql = f'{yql} and (({field} contains "{query}") or (bq_organization_id contains "{query}") or ({field} contains "{query}")) and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'

            # if field == "bq_organization_legal_name":
            #     pass
            
            # if field == "bq_organization_name":
            #     yql = f'{yql} ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization"))'
        else:
            query = query.lower()
            if 'and' in query:
                qqq = query.lower().rstrip().split('and')
                q1 = qqq[0]
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s2)}) and"
            else:
                query = query.replace(',', ' ')
                query = query.replace(';', ' ')
                query = re.sub(r'\s+', ' ', query)
                query1 = query.split(' ')
                query1 = [word for word in query1 if word not in ["of"]]
                s1 = ''
                for word in query1:
                    if word == "":
                        continue
                    s1 = f'{s1} default contains "{word}" OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s1)}) and"  
    if yql:
        if filter:
            try:
                filter = json.loads(filter.replace("'", "\""))
            except json.JSONDecodeError as e:
                return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f'{key} contains "{v}" OR '
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    # if orderby:
    order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_employment_growth_yoy_mr":"bq_employment_growth_yoy_mr","bq_organization_active_indicator":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation", "bq_organization_address1_line_1":"bq_organization_address1_line_1", "bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code"}
    orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
    # print(orderby,"/////////////////---------------")
        # if orderby == 'bq_organization_structure':
        #     orderbyField = 'bq_organization_structure'
        # elif orderby == 'bq_organization_name':
        #     orderbyField = 'bq_organization_name'
        # elif orderby == 'bq_organization_address1_line_1':
        #     orderbyField = 'bq_organization_address1_line_1'
        # elif orderby == 'bq_organization_jurisdiction_code':
        #     orderbyField = 'bq_organization_jurisdiction_code'
        # elif orderby == 'bq_revenue_mr':            
        #     orderbyField = 'bq_revenue_mr'
        # elif orderby == 'bq_employment_mr':            
        #     orderbyField = 'bq_employment_mr'
        # else:
        #     orderbyField = 'bq_revenue_mr'
    order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
    yql = f"{yql} order by {orderbyField} {order}"
    # print("search_by_company_name = ", yql)
    logger.info(f'YQL: {yql} \n Limit: {limit}')
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }
    #logger.info(f'Endpoint Params: {params}')
    # try:
    #     url = "http://54.190.237.221:8000/q/query/"
    #     tab = "company_name"
    #     query = yql.lower() + str(offset) + str(filter).replace(" ","") + str(orderby).replace(" ","")

    #     payload = {"query":query, "tab":tab}
    #     files=[]
    #     headers = {}

    #     response = requests.request("POST", url, headers=headers, data=payload, files=files)
    #     if response.status_code == 200:
    #         null, true, false = None, True, False
    #         response = {"response":eval(response.json()),"status":200}
    #         return response
    #         # return JSONResponse(content=eval(response.json()), status_code=200)
    # except:
    #     pass
    response = requests.get(search_endpoint, params=params).json()
    # try:
    #     url = "http://54.190.237.221:8000/q/saver/"
    #     tab = "company_name"
    #     result = str(response)
    #     query = yql.lower() + str(offset) + str(filter).replace(" ","") + str(orderby).replace(" ","")
    #     payload = {"query":query, "tab":tab, "result":result}
    #     files=[]
    #     headers = {}

    #     response1 = requests.request("POST", url, headers=headers, data=payload, files=files)
    # except:
    #     pass
    if response["root"]["fields"]["totalCount"] == 0:
        yql = yql.replace('and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization"))','')
        print("search_by_company_name 2 = ", yql)
        params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()
    response = {"response":response,"status":200}
    return response
    # return JSONResponse(content=response.json(), status_code=200)


def save_portfolio(data):
    url = central_server_base_url + "portfolio-save/"
    payload = json.dumps({
    "portal": data['portal'],
    "user_email": data['user_email'],
    "portfolio_name": data['portfolio_name'],
    "data": data['data']
    })
    headers = {
    'Content-Type': 'application/json',
    'api-key': api_key
    }
    response = requests.request("POST", url, headers=headers, data=payload).json()
    # print(response)
    response = {"response":response,"status":200}
    
    return response

def fetch_portfolio(data):
    url = central_server_base_url + "portfolio-retrieve/"
    payload = json.dumps({
    "portal": data['portal'],
    "user_email": data['user_email'],
    "portfolio_name": data.get("portfolio_name",None),
    "page_size": data['page_size'],
    "page_number": data['page_number'],
    "names_only":data.get("names_only",None)
    })
    headers = {
    'Content-Type': 'application/json',
    'api-key': api_key
    }

    response = requests.request("POST", url, headers=headers, data=payload).json()
    # print(response)
    # response = {"response":response,"total_companies":len(response)}
    response = {"response":response,"status":200}
    
    return response

def delete_portfolio(data):
    url = central_server_base_url + "portfolio-delete/"
    payload = json.dumps({
    "portal": data['portal'],
    "user_email": data['user_email'],
    "portfolio_name": data['portfolio_name']
    })
    headers = {
    'Content-Type': 'application/json',
    'api-key': api_key
    }
    response = requests.request("POST", url, headers=headers, data=payload).json()
    response = {"response":response,"status":200}
    return response

def update_portfolio(data):
    url = central_server_base_url + "portfolio-update/"
    payload = json.dumps({
    "portal": data['portal'],
    "user_email": data['user_email'],
    "portfolio_name": data['portfolio_name'],
    "data":data['data'],
    "usage":data['usage']
    })
    headers = {
    'Content-Type': 'application/json',
    'api-key': api_key
    }
    response = requests.request("POST", url, headers=headers, data=payload).json()
    response = {"response":response,"status":200}
    return response

def officer_inside_company_details(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, bq_organization_ticker, bq_organization_lei, bq_legal_entity_parent_status, bq_legal_entity_id, bq_organization_id):
    search_endpoint = f'{VESPA_ENDPOINT}/search/'
    
    if bq_legal_entity_id:
        bq_legal_entity_id = bq_legal_entity_id.strip()
        if bq_legal_entity_parent_status == 'Child':
            yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_parent_status contains '{bq_legal_entity_parent_status}' and bq_legal_entity_id contains '{bq_legal_entity_id}'"
        else:
            # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_parent_status contains '{bq_legal_entity_parent_status}' and bq_legal_entity_id contains '{bq_legal_entity_id}' and bq_legal_entity_active_indicator contains true"
            # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}'"
            yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_parent_status contains '{bq_legal_entity_parent_status}' and bq_legal_entity_id contains '{bq_legal_entity_id}'"
            # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_parent_status contains 'Ultimate Parent' and bq_legal_entity_id contains '{bq_legal_entity_id}'"
        # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_id contains '{bq_legal_entity_id}' order by bq_officer_full_name asc"
        # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' order by bq_officer_full_name asc"
    elif bq_legal_entity_parent_status == 'Ultimate Parent':
        # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' and bq_legal_entity_active_indicator contains 'true'"
        yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}'"
        # yql = f"select * from bq_officers where bq_organization_id contains '{bq_organization_id}' | all(group(bq_officer_full_name) max(5000) each(output(count())))"
    else:
        if query:
            if ' ' in query:
                query1 = query.split()
                query1 = ' '.join(reversed(query1))
                yql = f"select * from bq_officers where bq_officer_full_name contains '{query}' or bq_officer_full_name contains '{query1}'"
            else:
                yql = f"select * from bq_officers where bq_officer_full_name contains '{query}'"
    
    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        # print(12121212121212121212,key)
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} AND ({remove_and_from_end(final)}) AND"
                        # print(2353453453453456546567,yql)
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f"{key} contains '{v}' OR "
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} AND {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    
    if not bq_legal_entity_id:
        if orderby:
            if orderby == 'bq_officer_full_name':
                orderbyField = 'bq_officer_full_name'
            elif orderby == 'bq_organization_name':
                orderbyField = 'bq_organization_name'
            else:
                orderbyField = 'bq_organization_name'
            order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            yql = f"{yql} order by {orderbyField} {order}"
        
        else:
            orderbyField = 'bq_organization_name'
            # order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            order = 'asc'
            yql = f"{yql} order by {orderbyField} {order}"
    # print("SSSSSSSSS= ", yql)
    logger.info(f'YQL: {yql}')
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        # "timeout":20,
        "format": "json",
    }

    response = requests.get(search_endpoint, params=params).json()
    response = {"response":response,"status":200}
    return response

def stats(data):
    try:
        url = "https://search2-api.brightquery.com/api/user_tracker/"
        payload = json.dumps({
            "matrix": "user_tracker",
            "email": data['user_email'],
            "portal": "terminal2",
            "package_name": "Level - 1",
            "package_expiry": datetime.datetime.now(datetime.timezone.utc),
            "is_active":True,
            "hits_available":100,
            "additional_data":None,
            "last_paymeny":None
            })
        headers = {
            'Content-Type': 'application/json',
            "api_key": api_key
            }
        response = requests.request("POST", url, headers=headers, data=payload).json()
        response = {"response":response, "status":200}
        return response
    except:
        response = {"response":"API failed","status":400}
        return response 
    # return JSONResponse(content=response.json(), status_code=200)

def search_by_location_address(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, request_origin, ult_selection):
    if request_origin == "sidebar":
        hits = 50000
        limit = 50000
        yql = "select bq_organization_id, bq_revenue_mr, bq_employment_mr from bq_location_new where"
    elif request_origin == "external":
        hits=10
        yql ="select bq_organization_id,bq_organization_name,bq_organization_legal_name,bq_organization_active_indicator,bq_organization_website,bq_organization_cik,bq_organization_ticker,bq_organization_ein,bq_organization_lei,bq_organization_linkedin_url,bq_organization_address1_line_1,bq_organization_address1_city,bq_organization_address1_state,bq_organization_address1_state_name,bq_organization_address1_zip5,bq_organization_address1_county_name,bq_organization_address1_country,bq_organization_website,bq_tickers_related,bq_organization_naics_sector_name,bq_revenue_mr,bq_employment_mr,bq_location_id,bq_location_name,bq_organization_id,bq_location_address_line_1,bq_location_address_city,bq_location_address_state_name,bq_location_address_zip5,bq_location_address_county_name,bq_location_address_country_name from bq_location_new where "
    else:
        hits = 20
        yql = "select * from bq_location_new where"

    search_endpoint = f'{VESPA_ENDPOINT}/search/'
    

    if query:
        query = query.replace(',','')
        query = query.replace('&','')
        query = query.split()
        address_search_yql = ''
       
        for query in query:
            address_search_yql = address_search_yql + f"( bq_location_address_line_1 contains '{query}' OR  bq_location_address_state_name contains '{query}' OR bq_location_address_zip5 contains '{query}' OR bq_location_address_city contains '{query}') AND "
            # if ult_selection == "orgAddress":
            #     address_search_yql = address_search_yql + f"( bq_organization_address1_line_1 contains '{query}' OR bq_organization_address1_line_2 contains '{query}' OR  bq_organization_address1_state contains '{query}' OR bq_organization_address1_state_name contains '{query}' OR bq_organization_address1_zip5 contains '{query}' OR bq_organization_address1_city contains '{query}' OR bq_organization_legal_address1_state contains '{query}' OR bq_organization_legal_address1_zip5 contains '{query}' OR bq_organization_legal_address1_line_1 contains '{query}' OR bq_organization_legal_address1_city contains '{query}') AND "
            # else:
            #     yql = yql.replace(" from terminal_screener where",", ")
            #     yql = yql + "bq_legal_entity_address1_line_1, bq_legal_entity_address1_line_2, bq_legal_entity_address1_city, bq_legal_entity_address1_state, bq_legal_entity_address1_zip5 from terminal_screener where"
            #     # code to search in legal entity - yatin
            #     address_search_yql = address_search_yql + f"( bq_legal_entity_address1_line_1 contains '{query}' OR bq_legal_entity_address1_line_2 contains '{query}' OR bq_legal_entity_address1_city contains '{query}' OR bq_legal_entity_address1_state contains '{query}' OR bq_legal_entity_address1_zip5 contains '{query}' OR bq_organization_legal_address1_line_1 contains '{query}' OR bq_organization_legal_address1_line_2 contains '{query}') AND "

        address_search_yql= address_search_yql[:-4].rstrip() 
        if yql.lower().rstrip().endswith('and'):
            yql = f"{yql} and ( {address_search_yql} ) and"
        elif yql.lower().rstrip().endswith('where'):
            yql = f"{yql} ( {address_search_yql} ) and"
        else:
            yql = f"{yql} and ( {address_search_yql} ) and"                  

    if yql:
        if filter:
            try:
                filter = json.loads(filter.replace("'", "\""))
            except json.JSONDecodeError as e:
                response ={"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f"{key} contains '{v}' OR "
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    if orderby:
        order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_employment_growth_yoy_mr":"bq_employment_growth_yoy_mr","bq_organization_active_indicator":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation"}
        orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
        
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        # if ult_selection == "orgAddress":
        #     yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization')) order by {orderbyField} {order}"
        # else:
            # yql = f"{yql} and order by {orderbyField} {order}"
        yql = f"{yql}  order by {orderbyField} {order}"
    # else:
    #     # if request_origin =="terminal":
    #     #     if ult_selection == "orgAddress":
    #     #         yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
    #     #     else:
    #     #         pass
    #     # else:
    #     #     yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
    #     yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"

    # print(f"\n\n\nssssss after = {yql}")
    # print(f"\n\n\nssssss search_by_location_address = {yql}")
    logger.info(f'YQL: {yql}')
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }
    #logger.info(f'Endpoint Params: {params}')
    response = requests.get(search_endpoint, params=params).json()
    response['yql']=yql
    response = {"response":response, "status":200}
    return response

def submit_feedback(data):
    if key_check(data,['portal','email','user_message']):
        return {"response":{"error":"One of the necessary key is missing!"},"status":400}
    url = central_server_base_url + "feedback_management"

    if len(data['user_message'])<20:
        return {"response": "Length of feedback must be greater than 20 characters.", "status":400}

    payload = json.dumps({
    "matrix": "save_feedback",
    "user_email": data['email'],
    "portal": data['portal'],
    "user_message": data['user_message'],
    "data": data['data']
    })

    headers = {
    'api-key': '*&^V&XvTx7ypbVC5e*$^VUE*6REXIBvoV^Ox6Pobvg7x^Cgv',
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload).json()
    return response

def search_by_bq_location_name(query=None,yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby=None, isAsc=False, field=None, user_id=None, request_origin=None):
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    query = query.lower()
    
    # if request_origin == "terminal":
    #     yql = terminal_yql
    #     yql = yql.replace(" from terminal_screener where",", ")
    #     yql = yql + "bq_legal_entity_address1_line_1, bq_legal_entity_address1_line_2, bq_legal_entity_address1_city, bq_legal_entity_address1_state, bq_legal_entity_address1_zip5 from terminal_screener where"
                
    # else:
    #     yql = search_yql
    if request_origin=='external':
        yql ="select bq_organization_id,bq_organization_name,bq_organization_legal_name,bq_organization_active_indicator,bq_organization_website,bq_organization_cik,bq_organization_ticker,bq_organization_ein,bq_organization_lei,bq_organization_linkedin_url,bq_organization_address1_line_1,bq_organization_address1_city,bq_organization_address1_state,bq_organization_address1_state_name,bq_organization_address1_zip5,bq_organization_address1_county_name,bq_organization_address1_country,bq_organization_website,bq_tickers_related,bq_organization_naics_sector_name,bq_revenue_mr,bq_employment_mr,bq_location_id,bq_location_name,bq_organization_id,bq_location_address_line_1,bq_location_address_city,bq_location_address_state_name,bq_location_address_zip5,bq_location_address_county_name,bq_location_address_country_name from bq_location_new where "
        # yql = "select * from bq_location_new where"
    else:
        yql = "select * from bq_location_new where"
    if query:
        if field:
            if yql.lower().rstrip().endswith('and'):
                if field == "bq_location_name":
                    yql = f'{yql} and (({field} matches "{query}") or (bq_location_id contains "{query}") or ({field} contains "{query}")) and'

            elif yql.lower().rstrip().endswith('where'):
                if field == "bq_location_name":
                    yql = f'{yql} (({field} matches "{query}") or (bq_location_id contains "{query}") or ({field} contains "{query}")) and'

            else:
                if field == "bq_location_name":
                    yql = f'{yql} and (({field} contains "{query}") or (bq_location_id contains "{query}") or ({field} contains "{query}")) and'

            query = query.lower()
            if 'and' in query:
                qqq = query.lower().rstrip().split('and')
                q1 = qqq[0]
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s2)}) and"
            else:
                query = query.replace(',', ' ')
                query = query.replace(';', ' ')
                query = re.sub(r'\s+', ' ', query)
                query1 = query.split(' ')
                query1 = [word for word in query1 if word not in ["of"]]
                s1 = ''
                for word in query1:
                    if word == "":
                        continue
                    s1 = f'{s1} default contains "{word}" OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s1)}) and"  
    if yql:
        if filter:
            try:
                filter = json.loads(filter.replace("'", "\""))
            except json.JSONDecodeError as e:
                return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f'{key} contains "{v}" OR '
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    # if orderby:
    order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_employment_growth_yoy_mr":"bq_employment_growth_yoy_mr","bq_organization_active_indicator":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation", "bq_organization_address1_line_1":"bq_organization_address1_line_1", "bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code"}
    orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
    # print(orderby,"/////////////////---------------")

    order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
    yql = f"{yql} order by {orderbyField} {order}"
    # print("search_by_company_name = ", yql)
    logger.info(f'YQL: {yql}')
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }
    #logger.info(f'Endpoint Params: {params}')
    response = requests.get(search_endpoint, params=params).json()
    if response["root"]["fields"]["totalCount"] == 0:
        yql = yql.replace('and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization"))','')
        # print("search_by_company_name = ", yql)
        params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()
    response = {"response":response,"status":200}
    return response

def multi_bq_query(request):
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    try:
        # yql = terminal_yql + " bq_organization_id in " + request['bq_id'] + " and bq_legal_entity_parent_status in ('Ultimate Parent','Sole','Single-entity organization')"
        if request.get("yql",None) == None:
            request['bq_id'] = str(tuple(str(item) for item in request['bq_id']))
            yql = "select bq_organization_id from screener where " + " bq_organization_id in " + request['bq_id'] 
        else:
            yql = request.get("yql",None)
        params = {
                    'yql': yql,
                    'offset': 0,
                    'ranking': 'bm25',
                    'limit': 100,
                    'type': 'all',
                    'hits': 100,
                    "format": "json",
                }
        response = requests.get(search_endpoint, params=params).json()

        return {"response":response, "status":200}
    except Exception as e:
        return {"response":str(e), "status":400}

def bucket_management(data):
    VESPA_ENDPOINT = "http://localhost:8080"
    try:
        url = central_server_base_url + "bucket_management/"
        portal = data['portal'].lower()
        
        if data['matrix']=="add_to_bucket" and data["query"].get('flag',None) != None:
            # return {"response":"test", "status":200}
            try:
                if data['query']['flag']=="All":
                    text = data['query']['query']
                    filters = data['query']['filters']
                    field = data['query']['field']
                    from screenerutils import screener_search
                    yql = 'select bq_organization_id from terminal_screener where'
                    bq_ids = screener_search(text, yql, "all", json.dumps(filters), "bm25", 100,100,0, "bq_revenue_mr", "desc", field, "user", "terminal", False  )
                    print(bq_ids)
                    bq_id = []
                    for i in bq_ids["response"]['root']['children']:
                        bq_id.append(i["fields"]['bq_organization_id'])
                    if data['query']['deselect']==[]:
                        data['query']['bq_id']=bq_id
                    else:
                        data['query']['bq_id']=bq_id
                        for i in data['query']['deselect']:
                            data['query']['bq_id'].remove(i)
                    data['query'] = {"bq_id":bq_id}

            except Exception as e:
                return {"response":str(e),"status":400}

        try:
            if len(data['query']['bq_id'])>100:
                response = {"response":"Max length allowed is 100","status":400}
        except:
            pass

        payload = json.dumps({
        "portal": portal,
        "user_email": data['user_email'],
        "query": data.get('query',None),
        "matrix":data['matrix']
        })
        headers = {
        'Content-Type': 'application/json',
        'api-key': api_key
        }
        response = requests.request("POST", url, headers=headers, data=payload).json()
        
        if data['matrix']=="get_bucket":
            orginal_id = response['response']['data']['query']['bq_id']

            total_id = len(orginal_id)
            csv_download = data.get("csv_download",False)
            # selected_page = data.get("offset",0)
            # if selected_page == 0:
            #     selected_page = 1
            # else:
            #     selected_page= get_next_n_elements()
            if csv_download == False:
                orginal_id = get_next_n_elements(orginal_id, data.get("offset",0),10)
            
            bq_org_id = str(tuple(str(item) for item in orginal_id))
            
            if data.get("og_data", False) == False:

                if len(orginal_id)>1:
                    yql = "select bq_organization_id, bq_organization_name from terminal_screener where bq_organization_id in " + bq_org_id + " and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
                elif len(orginal_id)==1:
                    yql = "select bq_organization_id, bq_organization_name from terminal_screener where bq_organization_id contains '" + orginal_id[0] + "'" + " and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
                elif len(orginal_id)== 0:
                    return {"response":[], "status":200}
            else:
                
                if len(orginal_id)>1:
                    yql = "select * from terminal_screener where bq_organization_id in " + bq_org_id + " and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
                elif len(orginal_id)==1:
                    yql = "select * from terminal_screener where bq_organization_id contains '" + orginal_id[0] + "'" + " and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization'))"
                elif len(orginal_id)== 0:
                    return {"response":[], "status":200}
                
            # request = {"bq_id":response['response']['data']['query']['bq_id'],"yql":yql}
            params = {
                    'yql': yql,
                    'offset': 0,
                    'ranking': 'bm25',
                    'limit': 100,
                    'type': 'all',
                    'hits': 100,
                    "format": "json",
                }
            search_endpoint = f'{VESPA_ENDPOINT}/search/'

            all_data_condition = data.get("og_data",False)

            data =     requests.get(search_endpoint, params=params).json()
            data["item_in_bucket"] = total_id
            data = {"response":data,"status":200}
            
            
            if all_data_condition== True and csv_download == False:
                return {"response":data['response'],"status":200}
            elif all_data_condition== True and csv_download == True:
                data = [child["fields"] for child in data["response"]["root"]["children"]]
                data = pd.DataFrame(data)
                return data
                # temp_csv_file = "/home/ubuntu/terminal/Backend/Operations/logs/temp_csv_file.csv"
                # data.to_csv(temp_csv_file, index=False)

                # return {"response":"ok","status":200}

            bq_id = [i["fields"]['bq_organization_id'] for i in data["response"]['root']['children']]
            names = [i["fields"]['bq_organization_name'] for i in data["response"]['root']['children']]
            result = list(zip(bq_id, names))
            
            result = [{"id": bq_id[i], "name": names[i]} for i in range(len(bq_id))]
            result = sorted(result, key=lambda x: orginal_id.index(x['id']))
            result.append({"total_items":total_id})
            response = {"response":result, "status":200}
            # else:
            #     response = {"response":data, "status":200}

        else:
            response = {"response":response,"status":200}
        
        return response
    except Exception as e:
        response = {"response":{"message":"Source API Down", "error":str(e),"yql":yql}, 'status':500}
        return response


def response_to_df(response, field, search_universe, search_product):
    main_df = pd.DataFrame([])
    if "children" in response['root']:
        for r in response['root']['children']:
            tmp_df = pd.DataFrame([r['fields']])
            main_df = pd.concat([main_df, tmp_df])
        main_df['bq_match_types'] = FIELD_BQ_MATCH_TYPES_MAPPING[search_product][search_universe][field]
        main_df['bq_match_type_codes'] = str(FIELD_BQ_MATCH_TYPE_CODES_MAPPING[search_product][search_universe][field])
    return main_df

def append_response(response, search_universe, search_product):
    response_df=pd.DataFrame()
    if len(response)>0:
        for c in response:
            response_ = response_to_df(response[c], c, search_universe, search_product)
            print(f'search by {c} {response_.shape}')
            response_df=response_df._append(response_,ignore_index=True)
            if search_universe =='officers':
                response_df.drop(['sddocname','documentid'], axis=1, inplace=True)
    
    print("Response DF::::::",response_df.shape)
    if len(response_df)>0:        
        response_df['sort'] = response_df.reset_index().index    
        # print(response_df)    
        if search_universe=='org':        
            response_df['bq_freq_count'] = response_df.groupby(['bq_organization_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')
        elif search_universe=='le':
            response_df['bq_freq_count'] = response_df.groupby(['bq_legal_entity_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')
        elif search_universe=='location':
            response_df['bq_freq_count'] = response_df.groupby(['bq_location_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')    
        elif search_universe=='officers':
            response_df['bq_freq_count'] = response_df.groupby(['bq_officer_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')
        elif search_universe=='executives':
            response_df['bq_freq_count'] = response_df.groupby(['bq_executive_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')
        
        if (search_product =='BQ_BQ_BUSINESS_IDENTITY_API') | (search_product =='BQ_ID_API'):
            if 'bq_revenue_mr' in response_df:
                response_df['bq_revenue_range']=response_df.apply(lambda x: bq_revenue_range(x['bq_revenue_mr']), axis=1)
            # else:
            #     response_df['bq_revenue_mr']=0
            #     response_df['bq_revenue_range']=response_df.apply(lambda x: bq_revenue_range(x['bq_revenue_mr']), axis=1)

            if 'bq_employment_mr' in response_df:
                response_df['bq_employment_range']=response_df.apply(lambda x: bq_employment_range(x['bq_employment_mr']), axis=1)
            # print(response_df.head())        
    return response_df
# response_df = append_response(response)

def make_sorter(l):
    """
    Create a dict from the list to map to 0..len(l)
    Returns a mapper to map a series to this custom sort order
    """
    sort_order = {k:v for k,v in zip(l, range(len(l)))}
    return lambda s: s.map(lambda x: sort_order[x])

def get_bq_match_types(df, org_id):
    bq_match_types = ', '.join(df[df['bq_organization_id']==org_id]['bq_match_types'].unique())
    # print(bq_match_types)
    return bq_match_types

def get_bq_match_type_codes(df, org_id):
    bq_match_type_codes = ', '.join(df[df['bq_organization_id']==org_id]['bq_match_type_codes'].unique())
    # print(bq_match_types)
    return bq_match_type_codes

def check_best_match(df, org_id):    
    if df[df['bq_organization_id']==org_id]['bq_match_types'].nunique() >1:
        return True
    return False

def get_best_match(df):
    df_tmp = pd.DataFrame()
    if len(df)>0:
        if(len(df[df['bq_freq_count']>1])>0):
            df_tmp = df[df['bq_freq_count']>1].sort_values(['bq_freq_count','bq_match_type_codes'], ascending=[False, True]).drop_duplicates(subset=['bq_organization_id']).drop(['sort'], axis=1).astype(np.int64, errors='ignore')
            df_tmp['bq_match_types'] = df_tmp.apply(lambda x: get_bq_match_types(df[df['bq_freq_count']>1], x['bq_organization_id']), axis=1)
            df_tmp['bq_match_type_codes'] = df_tmp.apply(lambda x: get_bq_match_type_codes(df[df['bq_freq_count']>1], x['bq_organization_id']), axis=1)
            df_tmp['bq_best_match']=df_tmp.apply(lambda x: check_best_match(df[df['bq_freq_count']>1], x['bq_organization_id']), axis=1)
    return df_tmp
        
def get_remaining_matches(df):
    df_tmp = pd.DataFrame()
    if len(df)>0:
        if(len(df[df['bq_freq_count']==1])>0):
            df_tmp = df[df['bq_freq_count']==1].sort_values(['bq_match_type_codes','sort']).drop(['sort'], axis=1).astype(np.int64, errors='ignore')
            df_tmp['bq_best_match']=False
    return df_tmp

def prep_response(df):
    if df is not None:
        df.fillna('', inplace=True)

    final_response = {"root":{
            "id": "toplevel",
            "relevance": 1.0,"fields": {
                "totalCount": len(df)
            },
            "coverage": {
                "coverage": 100,
                "documents": 101316736,
                "full": "true",
                "nodes": 10,
                "results": 1,
                "resultsFull": 1
            },
                "children":df.to_dict('records')
            },
            'status':200}

    return final_response

def merge_responses(response, search_universe, search_product, request, is_test):
    best_match_df = pd.DataFrame()
    remaining_df = pd.DataFrame()
    final_df = pd.DataFrame()
    response_df = append_response(response, search_universe, search_product)
    print('response_df:::', response_df.shape)
    # response_df.to_csv("/users/amitpawar/Downloads/api_result.csv", index=False)
    best_match_df = get_best_match(response_df)
    print('best_match_df:::::',best_match_df.shape)
    remaining_df = get_remaining_matches(response_df)
    print(f"\nRemaining results:{remaining_df.shape}")
    final_df = pd.concat([best_match_df, remaining_df])
    # print("final df:", final_df.shape)
    if 'bq_revenue_range' in final_df:
        print(final_df['bq_revenue_range'])
    else:
        print('No bq_revenue_range')
    if len(final_df)>0:
        final_df.reset_index(inplace=True)
        final_df['bq_match_type_codes']= final_df['bq_match_type_codes'].astype(str)
        final_df = get_match_types_by_comparing_result(request,final_df, search_product, search_universe)
        final_df['bq_freq_count'] = final_df.apply(lambda row: get_freq_count(row['bq_match_types']),axis=1)
        final_df['bq_match_types'] = final_df.apply(lambda row: add_match_type_suffix(row['bq_match_types']),axis=1)
        
        if search_product =='BQ_BUSINESS_IDENTITY_API':
            if ('bq_revenue_mr' in final_df) & ('bq_employment_mr' in final_df):
                if search_universe != 'officers':
                    final_df.sort_values(['bq_freq_count','bq_match_type_codes','bq_revenue_mr','bq_employment_mr'], ascending=[False, True, False, False], inplace=True)
                    if is_test:
                        final_df.drop(['bq_match_type_codes'],axis=1, inplace=True)
                    else:                
                        final_df.drop(['bq_match_type_codes','bq_revenue_mr','bq_employment_mr','bq_freq_count','bq_best_match'],axis=1, inplace=True)
            elif ('bq_revenue_mr' in final_df):
                if search_universe != 'officers':
                    final_df.sort_values(['bq_freq_count','bq_match_type_codes','bq_revenue_mr'], ascending=[False, True, False], inplace=True)
                    if is_test:
                        final_df.drop(['bq_match_type_codes'],axis=1, inplace=True)
                    else:                
                        final_df.drop(['bq_match_type_codes','bq_revenue_mr','bq_freq_count','bq_best_match'],axis=1, inplace=True)
            elif ('bq_employment_mr' in final_df):    
                if search_universe != 'officers':
                    final_df.sort_values(['bq_freq_count','bq_match_type_codes','bq_employment_mr'], ascending=[False, True, False], inplace=True)
                    if is_test:
                        final_df.drop(['bq_match_type_codes'],axis=1, inplace=True)
                    else:                
                        final_df.drop(['bq_match_type_codes','bq_employment_mr','bq_freq_count','bq_best_match'],axis=1, inplace=True)
        else:
            if ('bq_revenue_mr' in final_df) & ('bq_employment_mr' in final_df):
                if search_universe != 'officers':
                    final_df.sort_values(['bq_freq_count','bq_match_type_codes','bq_revenue_mr','bq_employment_mr'], ascending=[False, True, False, False], inplace=True)
                    if is_test:
                        final_df.drop(['bq_match_type_codes'],axis=1, inplace=True)
                    else:                
                        final_df.drop(['bq_match_type_codes','bq_freq_count','bq_best_match'],axis=1, inplace=True)
            elif ('bq_revenue_mr' in final_df):
                if search_universe != 'officers':
                    final_df.sort_values(['bq_freq_count','bq_match_type_codes','bq_revenue_mr'], ascending=[False, True, False], inplace=True)
                    if is_test:
                        final_df.drop(['bq_match_type_codes'],axis=1, inplace=True)
                    else:                
                        final_df.drop(['bq_match_type_codes','bq_freq_count','bq_best_match'],axis=1, inplace=True)
            elif ('bq_employment_mr' in final_df):    
                if search_universe != 'officers':
                    final_df.sort_values(['bq_freq_count','bq_match_type_codes','bq_employment_mr'], ascending=[False, True, False], inplace=True)
                    if is_test:
                        final_df.drop(['bq_match_type_codes'],axis=1, inplace=True)
                    else:                
                        final_df.drop(['bq_match_type_codes','bq_freq_count','bq_best_match'],axis=1, inplace=True)            
        
        if search_universe=='org':                
            final_df = final_df[[(c) for c in RESPONSE_FIELDS[search_product] if c in final_df.columns]].drop_duplicates(subset=['bq_organization_id'])
        elif search_universe=='le':
            final_df = final_df[[(c) for c in RESPONSE_FIELDS[search_product] if c in final_df.columns]].drop_duplicates(subset=['bq_legal_entity_id'])
        elif search_universe=='location':
            final_df = final_df[[(c) for c in RESPONSE_FIELDS[search_product] if c in final_df.columns]].drop_duplicates(subset=['bq_location_id'])
            if 'index' in final_df:
                final_df.drop(['index'],axis=1, inplace=True)
        elif search_universe=='executives':
            final_df = final_df[[(c) for c in RESPONSE_FIELDS[search_product] if c in final_df.columns]].drop_duplicates(subset=['bq_executive_id'])
            # final_df = final_df.drop_duplicates(subset=['bq_executive_id'])
            if 'index' in final_df:
                final_df.drop(['index','sddocname','documentid'],axis=1, inplace=True)  
        elif search_universe=='officers':
            final_df = final_df.drop_duplicates(subset=['bq_organization_id'])
            final_df = final_df[['bq_officer_id','bq_legal_entity_officer_id','bq_officer_full_name','bq_officer_position','bq_officer_person_or_company','bq_legal_entity_id','bq_legal_entity_name', 'bq_legal_entity_address1_line_1','bq_legal_entity_address1_city','bq_legal_entity_address1_state','bq_legal_entity_address1_zip5','bq_organization_id','bq_organization_name','bq_organization_address1_state_name','bq_organization_address1_state','bq_organization_naics_sector_name','bq_organization_active_indicator','bq_revenue_range','bq_employment_range', 'bq_match_types']]

    # print('API LIMIT::::::',LIMIT_MAPPING_DICT[search_product][search_universe])                
    final_df = final_df.head(LIMIT_MAPPING_DICT[search_product][search_universe])
    if 'index' in final_df.columns:
        final_df.drop(['index'],axis=1, inplace=True)
    final_response = prep_response(final_df)
    
    # print('Columns:',final_df.columns)
    return final_response

def initialize_parameters(request):
    yql=field=user_id=tab=''
    orderby='bq_revenue_mr'
    type='all'
    filter=None 
    ranking='bm25'
    hits=100
    limit=50
    offset=0
    isAsc=False    
    user_level = 1
    side_bar=False
    addressflag=companynameflag=0
    query=address=company_name=""
    search_universe="org"
    ult_selection='orgAddress'
    
    for field, values in request.items():        
        print('field:',field, 'values:',values)
        if 'search_universe' in request:
            search_universe= request['search_universe'] if request['search_universe'] !='' else 'org'
            if search_universe!='org':
                ult_selection=''
        if field in ['bq_organization_name','bq_organization_legal_name','bq_location_name']:
            company_name = values
            companynameflag=1
        
        if field in ['bq_organization_address1_line_1','bq_organization_address1_city','bq_organization_address1_state','bq_organization_address1_state_name','bq_organization_address1_zip5',
                     'bq_legal_entity_address1_line_1','bq_legal_entity_address1_line_2','bq_legal_entity_address1_city','bq_legal_entity_address1_state','bq_legal_entity_address1_zip5',
                     'bq_location_address_line_1','bq_location_address_city','bq_location_address_state_name','bq_location_address_zip5']:
            address += values+' '
            addressflag=1        
        # if field in ['bq_organization_website','bq_organization_address1_line_1','bq_legal_entity_address1_line_1']:
        # if field in ['bq_organization_website','bq_organization_address1_line_1','bq_legal_entity_address1_line_1']:
        #     orderby =''
        if field not in ['search_universe']:
            if field in ['bq_legal_entity_address1_line_1','bq_organization_cik']:
                orderby =''
            else:
                orderby='bq_revenue_mr'

    # if (companynameflag==1) & (addressflag==1):
    #     query= company_name + ' and '+ address
    #     request['company_name_address'] = query
    #     orderby=''

    if search_universe =='officers':
        isAsc='True'
    else:
        isAsc='False'

    print('orderby::',orderby,'isAsc',isAsc)
    # exit()
    return yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe

def bq_revenue_range(revenue):
    if math.isnan(revenue):
        revenue =0
    else:
        revenue = int(revenue)
    bq_revenue_range=''
    if (revenue >=0) and (revenue < 100000000):
        bq_revenue_range = '< $100 Million'
    elif (revenue >= 100000000) and (revenue <= 1000000000):
        bq_revenue_range = '$100 Million - $1 Billion'        
    else:
        bq_revenue_range = '> $1 Billion'
    return bq_revenue_range

"""< 500
500 - 10,000
10,000 +"""

def bq_employment_range(emp_count):
    if math.isnan(emp_count):
        emp_count =0
    else:
        emp_count = int(emp_count)

    emp_range=''
    if (emp_count >=0) and (emp_count < 500):
        emp_range = '< 500'
    elif (emp_count >= 500) and (emp_count <= 10000):
        emp_range = '500 - 10,000'
    else:
        emp_range = '10,000 +'
    return emp_range

def validate_search_universe(search_universe, search_product):
    if search_universe != '':
        if search_universe not in FIELD_MAPPING_DICT[search_product].keys():
            return 'Invalid search universe'

def validate_fields(request, search_product, search_universe):
    error_fields=[]
    print('search_universe:::::', request)
    if(validate_search_universe(search_universe, search_product)):
        return f"Invalid search universe '{search_universe}'"
    else:
        for srch_key, srch_values in request.items():
            if srch_key not in FIELD_MAPPING_DICT[search_product][search_universe].keys():
                error_fields.append(srch_key)

    # if search_product=='INSURANCE_API':
    if search_universe != 'executives':
        print('request', request.keys())
        if 'website' not in request.keys():                        
            if 'email' in request:
                if checkEmail(request['email']):
                    domain = getDomainFromEmail(request['email'])
                    if checkEmailDomains(domain):
                        return f"Email address should be professional '{request['email']}'"
                    else:
                        request['website'] = domain
                else:
                    return f"Invalid Email address '{request['email']}'"
        else:
            if 'email' in request:
                del request['email']
            print('request', request.keys())

    if search_universe == 'location':
        if ('city' in request.keys()) | ('state' in request.keys()) | ('county' in request.keys()):
            if 'location_name' not in request.keys():
                return f"City/State/County are searchable with location_name"


    if len(error_fields)>0:
        return f"Invalid search parameters '{','.join(error_fields)}'"
    return None

def field_mapping(request, search_product, search_universe):
    request_new={}
    # search_universe = 'org'
    # if 'search_universe' in request:
    #     search_universe = request['search_universe'] if request['search_universe'] !="" else 'org'
    #     request['search_universe'] = request['search_universe'] if request['search_universe'] !="" else 'org'
    # else:
    #     request['search_universe'] = 'org' 

    if search_universe:
        request['search_universe'] = search_universe
    else:
        request['search_universe'] = 'org' 

    if ("ticker" in request) & ('exact' in request):
        if request['exact']:  
            request['ticker_exact'] = request['ticker']
            del request['exact']
            del request['ticker']

    if ("ticker_parent" in request) & ('exact' in request):
        if request['exact']:  
            request['ticker_parent_exact'] = request['ticker_parent']
            del request['exact']
            del request['ticker_parent']

    if ("ticker_related" in request) & ('exact' in request):
        if request['exact']:  
            request['ticker_related_exact'] = request['ticker_related']
            del request['exact']
            del request['ticker_related']

    try:   
        for k, v in request.items():
            request_new[FIELD_MAPPING_DICT[search_product][search_universe][k]] = v

        if 'bq_location_name' in request_new:
            if 'bq_location_address_state' in request_new:
                request_new['bq_location_name'] = request_new['bq_location_name']+'@#@bq_location_address_summary_array='+request_new['bq_location_address_state']
                del request_new['bq_location_address_state']
            if 'bq_location_address_city' in request_new:
                request_new['bq_location_name'] = request_new['bq_location_name']+'@#@bq_location_address_summary_array='+request_new['bq_location_address_city']
                del request_new['bq_location_address_city']
            if 'bq_location_address_county_name' in request_new:
                request_new['bq_location_name'] = request_new['bq_location_name']+'@#@bq_location_address_county_name='+request_new['bq_location_address_county_name']
                del request_new['bq_location_address_county_name']
    except:
        print("Invalid search parameters.")

    return request_new


###New code changes shared by Abhijeet
def create_yql_advanced(field, query, yql, yql_flag='fuzzy'):
    filter_words = ['inc.', 'corp.', 'llc', 'llp', 'lp', 'l.l.c.', 'p.c.', 'pc', 's.c.', 's.a.', 'co.', 'ltd.', 'incorporated', 'corporation', 'limited liability company', 'limited liability partnership', 'limited partnership', 'professional corporation', 'service corporation', 'societas anonima ', 'company ', 'limited ', 'incorporated ', 'corporation ', 'and', 'assn', 'assoc', 'associates', 'association', 'bv', 'co', 'comp', 'company', 'corp', 'corporation', 'dmd', 'gmbh', 'group', 'inc', 'incorporated', 'intl', 'limited', 'ltd', 'mfg', 'mgmt', 'management', 'pa', 'plc', 'pllc', 'restaurant', 'sa', 'sales', 'service', 'services', 'store', 'svcs', 'travel', 'unlimited', 'refi', 'subchapter v']
    query_list = query.split()
    query_list = [item.lower() for item in query_list if item.lower() not in filter_words]
    if field == 'bq_organization_address1_state_name' or field == 'bq_organization_address1_city' or field == 'bq_organization_address1_state':
        if len(query)> 0:
            yql = f'{yql} {field} contains "{query}" and'
    else:
        for word in query_list:
            if field == 'bq_organization_name':
                if yql_flag == 'contains':
                    yql = f'{yql} {field}_clean_array contains "{word}" and'
                elif yql_flag == 'fuzzy':
                    if len(word)<=2:
                        yql = f'{yql} {field}_clean_array contains "{word}" and'
                    elif 2<len(word)<=4:
                        maxEditDistance = '{maxEditDistance: 1}'
                        yql = f'{yql} {field}_clean_array contains ({maxEditDistance}fuzzy("{word}")) and'
                    elif 4<len(word)<12:
                        maxEditDistance = '{maxEditDistance: 2}'
                        yql = f'{yql} {field}_clean_array contains ({maxEditDistance}fuzzy("{word}")) and'
                    elif len(word)>=12:
                        maxEditDistance = '{maxEditDistance: 2}'
                        yql = f'{yql} {field}_clean_array contains ({maxEditDistance}fuzzy("{word}")) and'
            elif field == 'bq_organization_address1_state_name' or field == 'bq_organization_address1_line_1':
                    yql = f'{yql} {field} contains "{word}" and'
    return yql

def filter_words_from_string(input_str, filter_words):
    words = input_str.split()
    filtered_words = []
    for word in words:
        if word.lower() not in filter_words:
            filtered_words.append(word)
    return ' '.join(filtered_words)

def remove_punctuation(input_str):
    # Get a string containing all punctuation characters
    # punctuation_symbols = string.punctuation
    punctuation_symbols = '!%#&()*+,-./:;<=>?@[\]^_`{|}~'
    
    # Create a translation table to remove punctuation characters
    translator = str.maketrans('', '', punctuation_symbols)
    
    # Remove punctuation symbols from the input string
    result = input_str.translate(translator)
    
    return result

def get_ratio(query, y, match_method:None):
    filter_words = ['inc.', 'corp.', 'llc', 'llp', 'lp', 'l.l.c.', 'p.c.', 'pc', 's.c.', 's.a.', 'co.', 'ltd.', 'incorporated', 'corporation', 'limited liability company', 'limited liability partnership', 'limited partnership', 'professional corporation', 'service corporation', 'societas anonima ', 'company ', 'limited ', 'incorporated ', 'corporation ', 'and', 'assn', 'assoc', 'associates', 'association', 'bv', 'co', 'comp', 'company', 'corp', 'corporation', 'dmd', 'gmbh', 'group', 'inc', 'incorporated', 'intl', 'limited', 'ltd', 'mfg', 'mgmt', 'management', 'pa', 'plc', 'pllc', 'restaurant', 'sa', 'sales', 'service', 'services', 'store', 'svcs', 'travel', 'unlimited', 'refi', 'subchapter v']
    final_result= []
    for name_dict in y:
        ratio_clean = fuzz.token_sort_ratio(filter_words_from_string(query.lower(), filter_words), filter_words_from_string(name_dict['bq_organization_name'].lower(), filter_words))
        ratio = fuzz.token_sort_ratio(query.lower(), name_dict['bq_organization_name'].lower())
        name_dict["match_ratio_overall"]=ratio
        name_dict["match_ratio_clean"]=ratio_clean
        final_result.append(name_dict)
    final_results = sorted(final_result, key=lambda x: x["match_ratio_overall"], reverse=True)

    if match_method == 'revenue' or match_method == 'bq_revenue_mr':
        final_results = [record for record in final_results if "bq_revenue_mr" in record.keys()]
        final_results = sorted(final_results[:20], key=lambda x: x["bq_revenue_mr"], reverse=True)
    elif match_method == 'match_ratio_clean':
        final_results = sorted(final_results[:20], key=lambda x: x["match_ratio_clean"], reverse=True)
    else:
        final_results = sorted(final_results[:20], key=lambda x: x["match_ratio_overall"], reverse=True)

    return final_results

def company_name_updated(query=None,yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby=None, isAsc=False, field=None, user_id=None, request_origin=None,search_product=None):
    if request_origin == "terminal":
        yql = terminal_yql
        yql = yql.replace(" from terminal_screener where",", ")
        yql = yql + "bq_legal_entity_address1_line_1, bq_legal_entity_address1_line_2, bq_legal_entity_address1_city, bq_legal_entity_address1_state, bq_legal_entity_address1_zip5 from terminal_screener where"                
    elif request_origin =="external":
        yql = f"select {','.join(QUERY_FIELDS[search_product])} from terminal_screener where"
    else:
        yql = search_yql
    query = query.lower()
    
    print('++++++++++++++++++++++++++++++++++',filter, query)
    yql_from_func_contains = create_yql_advanced_updated(field, query, yql, filter, orderby, isAsc, yql_flag='contains')
    yql_from_func_fuzzy = create_yql_advanced_updated(field, query, yql, filter, orderby, isAsc, yql_flag='fuzzy')
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    print("==========yql_from_func_contains==========",yql_from_func_contains)
    params = {
        # 'yql': yql,
        'yql': yql_from_func_contains,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }
    print('11111')
    response = requests.get(search_endpoint, params=params).json()
    print('response:', response)

    if response["root"]["fields"]["totalCount"] == 0:
        print("==========yql_from_func_fuzzy==========",yql_from_func_fuzzy)

        params = {
            'yql': yql_from_func_fuzzy,
            'offset': offset,
            'ranking': ranking,
            'limit': limit,
            'type': 'all',
            'hits': hits,
            "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()
        records = response["root"]["children"]
        list_of_records = [item['fields'] for item in records]
        top_20_results = get_ratio(query, list_of_records, match_method='fuzzy')
        top_20_list = []
        cnt=0
        for item in top_20_results:
            cnt+=1
            dct_item = {}
            dct_item["id"] = str(len(top_20_results)-cnt)
            dct_item["fields"] = item
            top_20_list.append(dct_item)

        response["root"]["children"]=top_20_list

    response = {"response":response,"status":200}
    return response

def create_yql_advanced_updated(field, query, yql, filter, orderby, isAsc, yql_flag='contains'):
    filter_words = ['inc.', 'corp.', 'llc', 'llp', 'lp', 'l.l.c.', 'p.c.', 'pc', 's.c.', 's.a.', 'co.', 'ltd.', 'incorporated', 'corporation', 'limited liability company', 'limited liability partnership', 'limited partnership', 'professional corporation', 'service corporation', 'societas anonima ', 'company ', 'limited ', 'incorporated ', 'corporation ', 'and', 'assn', 'assoc', 'associates', 'association', 'bv', 'co', 'comp', 'company', 'corp', 'corporation', 'dmd', 'gmbh', 'group', 'inc', 'incorporated', 'intl', 'limited', 'ltd', 'mfg', 'mgmt', 'management', 'pa', 'plc', 'pllc', 'restaurant', 'sa', 'sales', 'service', 'services', 'store', 'svcs', 'travel', 'unlimited', 'refi', 'subchapter v']

    query_list = query.split()
    query_list = [item.lower() for item in query_list if item.lower() not in filter_words]
    query_combined = " ".join(query_list)

    # Generate YQL based on input given
    if field not in ['bq_organization_name', 'bq_legal_entity_name']:
        if len(query)> 0:
            yql = f'{yql} {field} contains "{query}" and'
    else:
        # if field == "bq_organization_name":
        #     yql = f'{yql} ({field} contains "{query_combined}" or bq_organization_id contains "{query_combined}") or'
        
        # if field == "bq_legal_entity_name":
        #     yql = f'{yql} ({field} contains "{query_combined}" or bq_legal_entity_id contains "{query_combined}") or'
        # yql = f'{yql} ({field} contains "{query_combined}" or bq_organization_id contains "{query_combined}") or'
        # yql = f'{yql} ({field} contains "{query_combined}" or'
        yql_part_1 = f'({field} contains "{query_combined}" or'

        if field == "bq_organization_name":
            # yql = f'{yql} bq_organization_id contains "{query_combined}") or'
            yql_part_1 = f'{yql_part_1} bq_organization_id contains "{query_combined}") or'
        
        if field == "bq_legal_entity_name":
            # yql = f'{yql} bq_legal_entity_id contains "{query_combined}") or'
            yql_part_1 = f'{yql_part_1} bq_legal_entity_id contains "{query_combined}") or'

        yql_part_2= ""
        for word in query_list:
            if yql_flag == 'contains':
                yql_part_2 = f'{yql_part_2} {field}_array contains "{word}" and'
                # yql = f'{yql} {field}_array contains "{word}" and'
                # yql = f'{yql} {field}_clean_array contains "{word}" or bq_organization_id contains "{word}" and'
            
            
            elif yql_flag == 'fuzzy':
                if len(word)<=2:
                    # yql = f'{yql} {field}_array contains "{word}" and'
                    yql_part_2 = f'{yql_part_2} {field}_array contains "{word}" and'
                    # yql = f'{yql} {field}_clean_array contains "{word}" and'
                elif 2<len(word)<=4:
                    maxEditDistance = '{maxEditDistance: 2}'
                    # yql = f'{yql} {field}_array contains ({maxEditDistance}fuzzy("{word}")) and'
                    yql_part_2 = f'{yql_part_2} {field}_array contains ({maxEditDistance}fuzzy("{word}")) and'
                    # yql = f'{yql} {field}_clean_array contains ({maxEditDistance}fuzzy("{word}")) and'
                elif 4<len(word)<12:
                    maxEditDistance = '{maxEditDistance: 2}'
                    # yql = f'{yql} {field}_array contains ({maxEditDistance}fuzzy("{word}")) and'
                    yql_part_2 = f'{yql_part_2} {field}_array contains ({maxEditDistance}fuzzy("{word}")) and'
                    # yql = f'{yql} {field}_clean_array contains ({maxEditDistance}fuzzy("{word}")) and'
                elif len(word)>=12:
                    maxEditDistance = '{maxEditDistance: 2}'
                    # yql = f'{yql} {field}_array contains ({maxEditDistance}fuzzy("{word}")) and'
                    yql_part_2 = f'{yql_part_2} {field}_array contains ({maxEditDistance}fuzzy("{word}")) and'
                    # yql = f'{yql} {field}_clean_array contains ({maxEditDistance}fuzzy("{word}")) and'
        # yql = f'{remove_and_from_end(yql)} or {field} contains {query}'
        yql_part_1 = remove_and_from_end(yql_part_1)
        yql_part_2 = remove_and_from_end(yql_part_2)
        # print(111111111111111111111111111111111111111111111111,f'({yql_part_1} or {yql_part})')
        # yql = f'{yql} ({yql_part}) and'
        yql = f'{yql} ({yql_part_1} or {yql_part_2}) and'
        if field == 'bq_organization_name':
            yql = f'{yql} ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization")) and'
    
    #adding Filters to YQL
    if filter:
        try:
            filter = json.loads(filter.replace("'", "\""))
            
        except json.JSONDecodeError as e:
            return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
        
        for key, val in filter.items():
            if len(val) >= 1:
                if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr', 'bq_organization_valuation'):
                    filter_yql = ''
                    for items in val:
                        itm = ''
                        for i in items:
                            itm = f"{itm} {key} {i} AND"
                        itm = remove_and_from_end(itm)
                        itm = f'({itm})'
                        filter_yql = f"{filter_yql} {itm} OR"
                    yql = f"{yql} ({remove_and_from_end(filter_yql)}) AND"
                else:
                    if len(val) > 1:
                        filter_yql = ''
                        for v in val:
                            filter_yql = filter_yql + f'{key} contains "{v}" OR '
                        filter_yql = remove_and_from_end(filter_yql)
                        filter_yql = f"({filter_yql})"
                        yql = f"{yql} {filter_yql} and"
                    elif len(val) == 0:
                        pass
                    else:
                        if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                            yql = remove_and_from_end(yql)
                            yql = f"{yql} AND {key} contains '{val[0]}' AND"
                        elif yql.lower().rstrip().endswith('where'):
                            yql = f"{yql} {key} contains '{val[0]}' AND"
                        else:
                            yql = f"{yql} AND {key} contains '{val[0]}' AND"

    yql = remove_and_from_end(yql)

    #add sorting to YQL
    order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr","bq_organization_isactive":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation", "bq_organization_address1_line_1":"bq_organization_address1_line_1", "bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code"}

    orderbyField = order_by_map.get(orderby,"bq_revenue_mr")

    order = 'asc' if isAsc in ('True', 'true', True) else 'desc'

    yql = f"{yql} order by {orderbyField} {order}"

    return yql

def checkEmail(email): 
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    if(re.fullmatch(regex, email)):
        return True
    return False
    
        
def checkEmailDomains(domain):
    personal_email_domain=['gmail.com','yahoo.com','hotmail.com','aol.com','hotmail.co.uk','hotmail.fr','msn.com','yahoo.fr','wanadoo.fr','orange.fr','comcast.net','yahoo.co.uk','yahoo.com.br','yahoo.co.in','live.com','rediffmail.com','free.fr','gmx.de','web.de','yandex.ru','ymail.com','libero.it','outlook.com','uol.com.br','bol.com.br','mail.ru','cox.net','hotmail.it','sbcglobal.net','sfr.fr','live.fr','verizon.net','live.co.uk','googlemail.com','yahoo.es','ig.com.br','live.nl','bigpond.com','terra.com.br','yahoo.it','neuf.fr','yahoo.de','alice.it','rocketmail.com','att.net','laposte.net','facebook.com','bellsouth.net','yahoo.in','hotmail.es','charter.net','yahoo.ca','yahoo.com.au','rambler.ru','hotmail.de','tiscali.it','shaw.ca','yahoo.co.jp','sky.com','earthlink.net','optonline.net','freenet.de','t-online.de','aliceadsl.fr','virgilio.it','home.nl','qq.com','telenet.be','me.com','yahoo.com.ar','tiscali.co.uk','yahoo.com.mx','voila.fr','gmx.net','mail.com','planet.nl','tin.it','live.it','ntlworld.com','arcor.de','yahoo.co.id','frontiernet.net','hetnet.nl','live.com.au','yahoo.com.sg','zonnet.nl','club-internet.fr','juno.com','optusnet.com.au','blueyonder.co.uk','bluewin.ch','skynet.be','sympatico.ca','windstream.net','mac.com','centurytel.net','chello.nl','live.ca','aim.com','bigpond.net.au'] 
    if domain:
        if domain in personal_email_domain:
            return True
        return False
    return True

def getDomainFromEmail(email):
    domain=''
    if email:
        try:
            domain = email.split('@')[1]
        except:
            print('Invalid Email ID')
    return domain

def match_results_with_search_params(request, row, search_product ,search_universe):
    # print('request:', request)
    if len(request)>0:        
        # print("Match Types:",row['bq_match_types'])
        for field, value in request.items():
            if field != 'search_universe':
                if field in FIELD_MAPPING_DICT[search_product][search_universe]:
                    # print(search_product, search_universe, field)
                    try:
                        field_value = str(row[FIELD_MAPPING_DICT[search_product][search_universe][field]])
                        if len(field_value)< len(value):
                             if FIELD_MATCHING_FUNCTIONS[FIELD_MAPPING_DICT[search_product][search_universe][field]](field_value, value):                
                                row['bq_match_types'] = update_match_types(row['bq_match_types'], FIELD_BQ_MATCH_TYPES_MAPPING[search_product][search_universe][FIELD_MAPPING_DICT[search_product][search_universe][field]])
                                row['bq_match_type_codes'] = update_match_type_codes(row['bq_match_type_codes'], str(FIELD_BQ_MATCH_TYPE_CODES_MAPPING[search_product][search_universe][FIELD_MAPPING_DICT[search_product][search_universe][field]]))
                        else:
                            if FIELD_MATCHING_FUNCTIONS[FIELD_MAPPING_DICT[search_product][search_universe][field]](value, field_value):                
                                row['bq_match_types'] = update_match_types(row['bq_match_types'], FIELD_BQ_MATCH_TYPES_MAPPING[search_product][search_universe][FIELD_MAPPING_DICT[search_product][search_universe][field]])
                                row['bq_match_type_codes'] = update_match_type_codes(row['bq_match_type_codes'], str(FIELD_BQ_MATCH_TYPE_CODES_MAPPING[search_product][search_universe][FIELD_MAPPING_DICT[search_product][search_universe][field]]))
                    except:
                        print(f'{FIELD_MAPPING_DICT[search_product][search_universe][field]} not found in data')
    return row['bq_match_types'], row['bq_match_type_codes']
    
def update_match_types(match_types, match_type): 
    new_match_types=match_types
    match_types_arr=[]
    # print('new_match_types', new_match_types)
    if match_types:
        match_types_arr = list(set(match_types.split(", ")))

    # print('Array Match type:',match_types_arr,'match type:',match_type)
    if len(match_types_arr)>0:
        if match_type not in match_types_arr:            
            new_match_types += ', '+match_type
    else:
        new_match_types =match_type

    return new_match_types

def update_match_type_codes(match_type_codes : str, match_type_code : str): 
    new_match_type_codes=str(match_type_codes)
    # print('new_match_type_codes', new_match_type_codes)
    match_type_codes_arr=[]
    if match_type_codes:
        match_type_codes_arr = list(set(match_type_codes.split(", ")))
#     print(match_types_arr)
    if len(match_type_codes_arr)>0:
        if match_type_code not in match_type_codes_arr:
            new_match_type_codes += ', '+match_type_code
    else:
        new_match_type_codes = match_type_code
    return new_match_type_codes
            
def company_name_match(str1, str2):
    str1 = basename(str1.lower())
    str2 = basename(str2.lower())
    score_ = fuzz.token_sort_ratio(str1,str2)
    # print('score:', score_)
    if score_>=70:
        return True
    return False

def website_match(str1, str2):
    str1 = str1.lower()
    str2 = str2.lower()
    str1 = str1.replace('www.','')
    str1 = str1.replace('https://','')
    str1 = str1.replace('http://','')

    str2 = str2.replace('www.','')
    str2 = str2.replace('https://','')
    str2 = str2.replace('http://','')
    # print(str1.rstrip('/'), str2.rstrip('/'))
    if str1.rstrip('/') in str2.rstrip('/'):
        return True
    return False

def exact_match(str1, str2):
    if str1.lower() in str2.lower():
        return True
    else:
        return False
    
def get_match_types_by_comparing_result(request, df, search_product, search_universe):
    for i in range(0, len(df)):
        match_types, match_type_codes = match_results_with_search_params(request, df.loc[i], search_product, search_universe)    
        df.loc[i,'bq_match_types'] = match_types
        df.loc[i,'bq_match_type_codes'] = match_type_codes
        # if search_universe=='org':        
        #     df['bq_freq_count'] = df.groupby(['bq_organization_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')
        # elif search_universe=='le':
        #     df['bq_freq_count'] = df.groupby(['bq_legal_entity_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')
        # elif search_universe=='location':
        #     df['bq_freq_count'] = df.groupby(['bq_location_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')    
        # elif search_universe=='officers':
        #     df['bq_freq_count'] = df.groupby(['bq_officer_id'])['bq_match_types'].transform('nunique').astype(np.int64, errors='ignore')
        i =i+1
    return df

def get_freq_count(match_type):
    freq_count =1
    if match_type:
        match_type_arr = match_type.split(",")
        return len(match_type_arr)

def add_match_type_suffix(match_type):
    if match_type:
        match_type_arr = match_type.split(",")
        if len(match_type_arr) ==1:
            return match_type +" Only"
    return match_type
    
def search_by_location_address_updated(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, request_origin, search_product,ult_selection):
    search_endpoint = f"{MARCH_VESPA_ENDPOINT}/search/"
    if request_origin == "sidebar":
        hits = 50000
        limit = 50000
        yql = "select bq_organization_id, bq_revenue_mr, bq_employment_mr from bq_location_new where"
    elif request_origin == "external":
        yql = f"SELECT  {','.join(QUERY_FIELDS[search_product])} FROM bq_location_new where "
    else:
        hits = 20
        yql = "select * from bq_location_new where"
    search_endpoint = f'{VESPA_ENDPOINT}/search/'

    yql_from_func_contains = create_yql_advanced_location_address(field, query, yql, filter, orderby, isAsc, ult_selection, request_origin, yql_flag='contains')

    print("==========yql_from_func_contains_location_address==========",yql_from_func_contains)

    params = {
        'yql': yql_from_func_contains,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }
    response = requests.get(search_endpoint, params=params).json()
    print('..............',response["root"]["fields"]["totalCount"])

    if response["root"]["fields"]["totalCount"] == 0:

        yql_from_func_fuzzy = create_yql_advanced_location_address(field, query, yql, filter, orderby, isAsc, ult_selection, request_origin, yql_flag='fuzzy')
        print("==========yql_from_func_fuzzy_location_address==========",yql_from_func_fuzzy)
        
        params = {
            'yql': yql_from_func_fuzzy,
            'offset': offset,
            'ranking': ranking,
            'limit': limit,
            'type': 'all',
            'hits': hits,
            "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()
    response = {"response":response, "status":200}
    return response

def create_yql_advanced_location_address(field, query, yql, filter, orderby, isAsc, ult_selection, request_origin, yql_flag='contains'):

    if query:
        query = query.replace(',','')
        query = query.split()
        address_search_yql = ''

        for query in query:
            if yql_flag == 'contains':
                address_search_yql = address_search_yql + f" ({field}_array contains '{query}') and "             
                
            elif yql_flag == 'fuzzy':
                maxEditDistance = '{maxEditDistance: 2}'
                if len(query)<=2:
                    address_search_yql = address_search_yql + f" {field}_array contains '{query}' and"
                elif 2<len(query)<=4:
                    maxEditDistance = '{maxEditDistance: 2}'
                    address_search_yql = address_search_yql + f" {field}_array contains ({maxEditDistance}fuzzy('{query}')) and"
                elif 4<len(query)<12:
                    maxEditDistance = '{maxEditDistance: 2}'
                    address_search_yql = address_search_yql + f" {field}_array contains ({maxEditDistance}fuzzy('{query}')) and"
                elif len(query)>=12:
                    maxEditDistance = '{maxEditDistance: 2}'
                    address_search_yql = address_search_yql + f" {field}_array contains ({maxEditDistance}fuzzy('{query}')) and"
        address_search_yql= address_search_yql[:-4].rstrip()
        yql = f'{yql} ({address_search_yql})'

    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                response ={"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f"{key} contains '{v}' OR "
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    if orderby:
        order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr","bq_organization_isactive":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation"}
        orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
        
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        yql = f"{yql}  order by {orderbyField} {order}"
   
    print(f"search_by_location_address = {yql}")
  

    return yql

def add_yql_location_name(query, field):
    if '@#@' in query:
        query_arr = query.split('@#@')
        print(query_arr)
        q = query_arr[0]
        sub_yql =''
        if len(query_arr)>1:
            for qu in query_arr[1:]:
                k_v_arr = qu.split('=')
                if k_v_arr[1]:
                    for w in k_v_arr[1].split(" "):
                        if w:
                            sub_yql += f' {k_v_arr[0]} contains "{w}" and'
        yql = f'(({field}  matches "{q}") or (bq_location_id contains "{q}") or ({field} contains "{q}")) and {sub_yql}'
    else:
        yql = f'(({field} matches "{query}") or (bq_location_id contains "{query}") or ({field} contains "{query}")) and'
    return yql

def search_by_bq_location_name(query=None,yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby=None, isAsc=False, field=None, user_id=None, request_origin=None, search_product=None):
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    query = query.lower()    
    yql = f"SELECT  {','.join(QUERY_FIELDS[search_product])} FROM bq_location_new where "
    if query:
        if field:
            if field == "bq_location_name":
                if yql.lower().rstrip().endswith('and'):                
                    # yql = f'{yql} and (({field} matches "{query}") or (bq_location_id contains "{query}") or ({field} contains "{query}")) and'
                    yql = f'{yql} and {add_yql_location_name(query, field)} and'
                elif yql.lower().rstrip().endswith('where'):                    
                        yql = f'{yql} {add_yql_location_name(query, field)}'
                else:
                    yql = f'{yql} and {add_yql_location_name(query, field)} and'            
            elif field == 'bq_location_website':
                query = query.replace('www.','')
                query = query.replace('https://','')
                query = query.replace('http://','')
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and (({field} contains '{query}') or ( {field} matches '{query}'))"
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} (({field} contains '{query}') or ( {field} matches '{query}'))"
                else:
                    yql = f"{yql} and (({field} contains '{query}') or ( {field} matches '{query}'))"

            query = query.lower()
            if 'and' in query:                
                qqq = query.lower().rstrip().split('and')
                query = re.sub(r'\s{2,}', ' ', query)
                q1 = qqq[0].strip()
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
               
                for i in q2:
                    s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s2)}) and"
            elif '&' in query:
                qqq = query.lower().rstrip().split('&')
                query = re.sub(r'\s{2,}', ' ', query)
                q1 = qqq[0].strip()
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                    # break
                yql = f'{yql} ({remove_and_from_end(s2)}) and'
            # else:
            #     query = query.replace(',', ' ')
            #     query = query.replace(';', ' ')
            #     query = re.sub(r'\s+', ' ', query)
            #     query1 = query.split(' ')
            #     query1 = [word for word in query1 if word not in ["of"]]
            #     s1 = ''
            #     for word in query1:
            #         if word == "":
            #             continue
            #         s1 = f'{s1} default contains "{word}" OR'
            #         # break
            #     yql = f"{yql} ({remove_and_from_end(s1)}) and"    
    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f'{key} contains "{v}" OR '
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    # if orderby:
    order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_employment_mr":"bq_employment_mr","bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr","bq_organization_isactive":"bq_organization_active_indicator","bq_score":"bq_score","bq_organization_valuation":"bq_organization_valuation", "bq_organization_structure":"bq_organization_structure", "bq_organization_valuation":"bq_organization_valuation", "bq_organization_address1_line_1":"bq_organization_address1_line_1", "bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code"}
    orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
    # print(orderby,"/////////////////---------------")

    order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
    yql = f"{yql} order by {orderbyField} {order}"
    print("search_by_company_name = ", yql)
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
    }
    
    response = requests.get(search_endpoint, params=params).json()
    if response["root"]["fields"]["totalCount"] == 0:
        yql = yql.replace('and ((bq_legal_entity_parent_status contains "Ultimate Parent") OR (bq_legal_entity_parent_status contains "Sole") OR (bq_organization_structure contains "Single-entity organization"))','')
        print("search_by_company_name = ", yql)
        params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()
    response = {"response":response,"status":200}
    return response

def create_yql_advanced_executive( query, yql, field, filter, orderby, isAsc, yql_flag='contains'):

    if query:
       query = query.replace(',','')
       query = query.split()
       yql_executive = ''

       for query in query:    
            if yql_flag == 'contains':
                yql_executive = f"{yql_executive} (bq_executive_name_array contains '{query}') and"
            
            elif yql_flag == 'fuzzy':
                maxEditDistance = '{maxEditDistance: 2}'
                if len(query)<=2:
                    yql_executive = f"{yql_executive} (bq_executive_name_array contains '{query}') and"
                    
                elif 2<len(query)<=4:
                    maxEditDistance = '{maxEditDistance: 2}'
                    yql_executive = f"{yql_executive} (bq_executive_name_array contains ({maxEditDistance}fuzzy('{query}'))) and"
                    
                elif 4<len(query)<12:
                    maxEditDistance = '{maxEditDistance: 2}'
                    yql_executive = f"{yql_executive} (bq_executive_name_array contains ({maxEditDistance}fuzzy('{query}'))) and"
                    
                elif len(query)>=12:
                    maxEditDistance = '{maxEditDistance: 2}'
                    yql_executive = f"{yql_executive} (bq_executive_name_array contains ({maxEditDistance}fuzzy('{query}'))) and"
                    
    yql_executive= yql_executive[:-4].rstrip()

    yql = f'{yql} ({yql_executive})'

    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
            if 'bq_organization_sector_name' in filter.keys():
                filter['bq_organization_naics_sector_name'] = filter.pop('bq_organization_sector_name')
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f"{key} contains '{v}' OR "
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} AND {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    if orderby:
        if orderby == 'bq_executive_name':
            orderbyField = 'bq_executive_name'
        elif orderby == 'bq_organization_name':
            orderbyField = 'bq_organization_name'
        elif orderby == 'bq_revenue_mr':
            orderbyField = 'bq_revenue_mr'
        elif orderby == 'bq_employment_mr':
            orderbyField = 'bq_revenue_mr'
        else:
            orderbyField = 'bq_executive_name'
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        yql = f"{yql} order by {orderbyField} {order}"
    
    else:
        orderbyField = 'bq_executive_name'
        order = 'asc'
        yql = f"{yql} order by {orderbyField} {order}"

    # print('...........',yql)
    return yql

def Search_by_executive_updated(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, bq_organization_ticker, bq_organization_lei, bq_legal_entity_parent_status, bq_legal_entity_id, bq_organization_id):
    search_endpoint = f'{VESPA_ENDPOINT}/search/'
    yql = f"select * from bq_executives where"
    
    yql_from_func_contains = create_yql_advanced_executive(query, yql, field, filter, orderby, isAsc, yql_flag='contains')
    
   
    print("yql_from_func_contains....... ", yql_from_func_contains)
    params = {
        'yql': yql_from_func_contains,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': 20,
        # "timeout":20,
        "format": "json",
    }
    print('params::',params)
    response = requests.get(search_endpoint, params=params).json()
    print('response:::', response)
    if response['root']['fields']['totalCount'] == 0:
        
        yql_from_func_fuzzy = create_yql_advanced_executive(query, yql, field, filter, orderby, isAsc, yql_flag='fuzzy')
        print("yql_from_func_fuzzy.....", yql_from_func_fuzzy)


        params = {
            'yql': yql_from_func_fuzzy,
            'offset': offset,
            'ranking': ranking,
            'limit': limit,
            'type': 'all',
            'hits': hits,
            # "timeout":20,
            "format": "json",
        }
        response = requests.get(search_endpoint, params=params).json()

    response = {"response":response,"status":200}

    return response


def search_executive_other(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, request_origin, search_product):
    search_endpoint = f'{VESPA_ENDPOINT}/search/'
    hits=20
    if request_origin == "terminal":
        yql = terminal_yql
    elif request_origin =="external":
        yql = f"SELECT  {','.join(QUERY_FIELDS[search_product])} FROM bq_executives where "
    else:
        yql = search_yql

    print('request_origin:::::::::', request_origin)
    if field=='company_name_address':
        field=''
    
    if query:
        print(12312321234234234111111111111, query, field)
        # print("Inside If query")
        if field:
            if field in ['bq_executive_linkedin_url']:
                query = query.replace('www.','')
                query = query.replace('https://','')
                query = query.replace('http://','')
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and (({field} contains '{query}' or bq_executive_linkedin_url contains '{query}') or ( {field} matches '{query}' and bq_executive_linkedin_url matches '{query}'))"
                    # yql = f"{yql} and {field} contains '{query}' or bq_executive_linkedin_url contains '{query}' "
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} (({field} contains '{query}' or bq_executive_linkedin_url contains '{query}') or ( {field} matches '{query}' and bq_executive_linkedin_url matches '{query}'))"
                    # yql = f"{yql} {field} contains '{query}' or bq_executive_linkedin_url contains '{query}' "
                else:
                    yql = f"{yql} and (({field} contains '{query}' or bq_executive_linkedin_url contains '{query}') or ( {field} matches '{query}' and bq_executive_linkedin_url matches '{query}'))"
                    # yql = f"{yql} and {field} contains '{query}' or bq_executive_linkedin_url contains '{query}' "
                # print(111111111111111111111111111111111111111111111111111111111111111111111111111111111,query, yql)
            elif field =='bq_executive_emails':
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and (({field} contains '{query}') or ( {field} matches '{query}' ))"
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} (({field} contains '{query}') or ( {field} matches '{query}'))"
                else:
                    yql = f"{yql} and (({field} contains '{query}') or ( {field} matches '{query}'))"
            elif field =='bq_executive_landlines_company':
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and (({field} contains '{query}' or bq_executive_landlines_direct contains '{query}' or bq_executive_mobiles_company contains '{query}' or bq_executive_mobiles_direct contains '{query}') or ( {field} matches '{query}' or bq_executive_landlines_direct matches '{query}' or bq_executive_mobiles_company matches '{query}' or bq_executive_mobiles_direct matches '{query}'))"
                    # yql = f"{yql} and {field} contains '{query}' or bq_executive_emails_professional_current contains '{query}' "
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} (({field} contains '{query}' or bq_executive_landlines_direct contains '{query}' or bq_executive_mobiles_company contains '{query}' or bq_executive_mobiles_direct contains '{query}') or ( {field} matches '{query}' or bq_executive_landlines_direct matches '{query}' or bq_executive_mobiles_company matches '{query}' or bq_executive_mobiles_direct matches '{query}'))"
                    # yql = f"{yql} {field} contains '{query}' or bq_executive_emails_professional_current contains '{query}' "
                else:
                    yql = f"{yql} and (({field} contains '{query}' or bq_executive_landlines_direct contains '{query}' or bq_executive_mobiles_company contains '{query}' or bq_executive_mobiles_direct contains '{query}') or ( {field} matches '{query}' or bq_executive_landlines_direct matches '{query}' or bq_executive_mobiles_company matches '{query}' or bq_executive_mobiles_direct matches '{query}'))"
                    # yql = f"{yql} and {field} contains '{query}' or bq_executive_emails_professional_current contains '{query}' "
                # print(111111111111111111111111111111111111111111111111111111111111111111111111111111111,query, yql)
            
            
            
        else:
            query = query.lower()
            if 'and' in query:
                qqq = query.lower().rstrip().split('and')
                query = re.sub(r'\s{2,}', ' ', query)
                q1 = qqq[0].strip()
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f"{s2} (default contains '{q1}' and default contains '{i}') OR"
                    # break
                yql = f'{yql} ({remove_and_from_end(s2)}) and'
                
            elif '&' in query:
                qqq = query.lower().rstrip().split('&')
                query = re.sub(r'\s{2,}', ' ', query)
                q1 = qqq[0].strip()
                q3 = qqq[1].strip()
                q3 = q3.replace(',', ' ')
                q3 = q3.replace(';', ' ')
                q3 = re.sub(r'\s+', ' ', q3)
                q2 = q3.split(' ')
                s2 = ''
                for i in q2:
                    s2 = f"{s2} (default contains '{q1}' and default contains '{i}') OR"
                    # break
                yql = f'{yql} ({remove_and_from_end(s2)}) and'
            else:
                # print(34124234342,query, field)
                query = query.replace(',', ' ')
                query = query.replace(';', ' ')
                query = re.sub(r'\s+', ' ', query)
                query1 = query.split(' ')
                query1 = [word for word in query1 if word not in ["of"]]
                s1 = ''
                for word in query1:
                    if word == "":
                        continue
                    s1 = f'{s1} default contains "{word}" OR'
                    # break
                yql = f"{yql} ({remove_and_from_end(s1)}) and"  

    if yql:
        if filter:
            try:
                filter = filter.replace('bq_organization_isactive', 'bq_organization_active_indicator')
                filter = json.loads(filter.replace("'", "\""))
                
            except json.JSONDecodeError as e:
                response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
            if 'bq_organization_sector_name' in filter.keys():
                filter['bq_organization_naics_sector_name'] = filter.pop('bq_organization_sector_name')
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_employment_mr','bq_organization_valuation'):
                        final = ''
                        for items in val:
                            itm = ''
                            for i in items:
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    else:
                        if len(val) > 1:
                            yql_part = ''
                            for v in val:
                                yql_part = yql_part + f"{key} contains '{v}' OR "
                            yql_part = remove_and_from_end(yql_part)
                            yql_part = f"({yql_part})"
                            yql = f"{yql} AND {yql_part} and"
                        elif len(val) == 0:
                            pass
                        else:
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = remove_and_from_end(yql)
    # print('order by:', orderby)
    if orderby:
        if orderby == 'bq_executive_name':
            orderbyField = 'bq_executive_name'
        elif orderby == 'bq_organization_name':
            orderbyField = 'bq_organization_name'
        elif orderby == 'bq_revenue_mr':
            orderbyField = 'bq_revenue_mr'
        elif orderby == 'bq_employment_mr':
            orderbyField = 'bq_revenue_mr'
        else:
            orderbyField = 'bq_executive_name'
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        yql = f"{yql} order by {orderbyField} {order}"
    
    else:
        orderbyField = 'bq_executive_name'
        order = 'asc'
        yql = f"{yql} order by {orderbyField} {order}"

    print(f"\n\n\nssssss after = {yql}")
    logger.info(f'YQL: {yql} \n Limit: {limit}')
    params = {
        'yql': yql,
        'offset': offset,
        'ranking': ranking,
        'limit': limit,
        'type': 'all',
        'hits': hits,
        "format": "json",
        'timeout':39
    }
    response = requests.get(search_endpoint, params=params).json()
    response = {"response":response,"status":200}
    return response


FIELD_MATCHING_FUNCTIONS ={
	'bq_organization_ticker':exact_match,
    'bq_ticker_parent':exact_match,
    'bq_tickers_related':exact_match,
	'bq_organization_lei':exact_match,
	'bq_organization_cik':exact_match,
	'bq_organization_ein':exact_match,
	'bq_organization_linkedin_url':website_match,
    'bq_organization_website':website_match,
	'bq_organization_name':company_name_match,
    'bq_organization_id':company_name_match,
	'bq_organization_address1_line_1':exact_match,
	'bq_legal_entity_name':company_name_match,
    'bq_legal_entity_id':company_name_match,
	'bq_legal_entity_address1_line_1':exact_match,
    'bq_location_website':website_match,
    'bq_location_name':company_name_match,
    'bq_location_address_summary':exact_match,
    'bq_executive_name':company_name_match,
    'bq_executive_linkedin_url':website_match,
}