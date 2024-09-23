import os, jwt, json, re
import requests
import pandas as pd

def remove_and_from_end(input_string):
    if input_string.lower().rstrip().endswith('and'):
        return input_string[:-3].rstrip()
    elif input_string.lower().rstrip().endswith('or'):
        return input_string[:-3].rstrip()
    else:
        return input_string

def range_convert(range_type,num):
    revenue_ranges = [
        (0, 1000000, "$0 - $1M"),
        (1000000, 5000000, "$1M - $5M"),
        (5000000, 10000000, "$5M - $10M"),
        (10000000, 25000000, "$10M - $25M"),
        (25000000, 50000000, "$25M - $50M"),
        (50000000, 100000000, "$50M - $100M"),
        (100000000, 250000000, "$100M - $250M"),
        (250000000, 500000000, "$250M - $500M"),
        (500000000, 1000000000, "$500M - $1B"),
        (1000000000, float('inf'), "$1B+")
    ]

    amount_related = [
        (0,100000, "$0 - $100K"),
        (100000,500000, "$100K - $500K"),
        (500000, 1000000, "$500K- $1M"),
        (1000000, 5000000, "$1M - $5M"),
        (5000000, 10000000, "$5M - $10M"),
        (10000000, 25000000, "$10M - $25M"),
        (25000000, 50000000, "$25M - $50M"),
        (50000000, 100000000, "$50M - $100M"),
        (100000000, 250000000, "$100M - $250M"),
        (250000000, 500000000, "$250M - $500M"),
        (500000000, 1000000000, "$500M - $1B"),
        (1000000000, float('inf'), "$1B+")
    ]

    percent_range0_1 = [
        (0.0,0.5, "0 - 0.5%"),
        (0.5,0.10, "0.5 - 0.10%"),
        (0.10, 0.25, "0.10% - 0.25%"),
        (0.25, 0.50, "0.25 - 0.50%"),
        (0.50, 0.75, "0.50 - 0.75%"),
        (0.75, 1, "0.75 - 1%"),
        (1, float('inf'), "Above 1%"),
        
    ]

    percent_range0_1 = [
        (0.0,0.05, "0 - 5%"),
        (0.05,0.10, "5 - 10%"),
        (0.10, 0.25, "10% - 25%"),
        (0.25, 0.50, "25 - 50%"),
        (0.50, 0.75, "50 - 75%"),
        (0.75, 1, "75 - 100%"),
        (1, float('inf'), "Above 100%"),
        
    ]

    headcount_ranges = [
        (1,5, "1 - 4"),
        (5,10, "5 - 9"),
        (10,20, "10 - 19"),
        (20,50 , "20 - 49"),
        (50,100, "50 - 99"),
        (100,250, "100 - 249"),
        (250,500, "250 - 499"),
        (500, float('inf'), "500+")
    ]

    if range_type == "revenue_ranges":
        for lower, upper, label in revenue_ranges:
            if lower <= num < upper:
                return label
        return "Invalid Range"
    elif range_type == "amount_related":
        for lower, upper, label in amount_related:
            if lower <= num < upper:
                return label
        return "Invalid Range"
    elif range_type == "percent_range0_1":
        for lower, upper, label in percent_range0_1:
            if lower <= num < upper:
                return label
        return "Invalid Range"
    elif range_type == "headcount_ranges":
        for lower, upper, label in headcount_ranges:
            if lower <= num < upper:
                return label
        return "Invalid Range"

def divide_by_100(element):
    if element.startswith(">"):
        return ">" + str(float(element[1:]) / 100)
    elif element.startswith("<"):
        return "<" + str(float(element[1:]) / 100)
    else:
        return str(float(element) / 100)

VESPA_ENDPOINT = "http://localhost:8080"
VESPA_ENDPOINT = "http://34.216.247.54:8080"
def screener_sidebar(response):
    try:
        response = list(pd.DataFrame(response['root']['children']).fillna(0)['fields'])

        bq_current_employees_plan_mr_total = []
        bq_revenue_mr_total = []
        total_companies = []
        for i in response:
            try:
                total_companies.append(i.get('bq_organization_id',0))
            except:
                pass
            try:
                bq_current_employees_plan_mr_total.append(i.get('bq_current_employees_plan_mr',0))
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

        companies_count_current_employees_plan_mr = len(bq_current_employees_plan_mr_total)-bq_current_employees_plan_mr_total.count(0)
        companies_count_bq_revenue_mr = len(bq_revenue_mr_total) - bq_revenue_mr_total.count(0)

        bq_current_employees_plan_mr_total = sum(bq_current_employees_plan_mr_total)
        bq_revenue_mr_total = sum(bq_revenue_mr_total)
        try:
            bq_current_employees_plan_mr_avg = bq_current_employees_plan_mr_total / companies_count_current_employees_plan_mr
        except:
            bq_current_employees_plan_mr_avg = 0
        try:
            bq_revenue_mr_avg = bq_revenue_mr_total / companies_count_bq_revenue_mr
        except:
            bq_revenue_mr_avg = 0
        data  = {"total_companies":total_companies,
                "bq_current_employees_plan_mr_total":bq_current_employees_plan_mr_total,
                "bq_revenue_mr_total":bq_revenue_mr_total,
                "bq_current_employees_plan_mr_avg":bq_current_employees_plan_mr_avg,
                "bq_revenue_mr_avg":bq_revenue_mr_avg,}
        response = {"response":data,"status":200}
    except Exception as e:
        data={"total_companies":0,
                "bq_current_employees_plan_mr_total":0,
                "bq_revenue_mr_total":0,
                "bq_current_employees_plan_mr_avg":0,
                "bq_revenue_mr_avg":0,
                "error":str(e)}
        response = {"response":data,"status":200}
    return response


def screener_search(query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, request_origin, side_bar):
    VESPA_ENDPOINT = "http://localhost:8080"
    search_endpoint = f'{VESPA_ENDPOINT}/search/'
    if hits == None:
        hits = 2000
    if yql == None:

        if side_bar == True:
            yql = 'select bq_organization_id, bq_current_employees_plan_mr, bq_revenue_mr from terminal_hybrid where'
        else:
            yql = 'select * from terminal_hybrid where'
    else:
        yql = yql
    
    if query:
        if field:
            if field == 'bq_organization_website':
                query = query.replace('www.','')
                query = query.replace('https://','')
                query = query.replace('http://','')
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and (({field} contains '{query}' or bq_organization_linkedin_url contains '{query}') or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}'))"
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}')"
                else:
                    yql = f"{yql} and {field} contains '{query}' or bq_organization_linkedin_url contains '{query}' or ( {field} matches '{query}' or bq_organization_linkedin_url matches '{query}')"

            elif field =='bq_organization_lei':
                query = query.replace('&','%')
                query = query.replace('#','%')
                prefix = '{prefix: true}'
                if yql.lower().rstrip().endswith('and'):
                    yql = f"{yql} and {field} contains ({prefix}'{query}') and"
                elif yql.lower().rstrip().endswith('where'):
                    yql = f"{yql} {field} contains ({prefix}'{query}') and"
                else:
                    yql = f"{yql} and {field} contains ({prefix}'{query}') and"
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
                    s2 = f"{s2} (default contains '{q1}' and default contains '{i}') OR"
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
                yql = f"{yql} ({remove_and_from_end(s1)}) and"
                
                
        if yql:
            if len(filter)>2:
                try:
                    filter = json.loads(filter.replace("'", "\""))
                    if 'bq_ebitda_margin_mr' in filter.keys():
                        bq_ebitda_margin_mr = filter['bq_ebitda_margin_mr']
                        bq_ebitda_margin_mr = [[divide_by_100(element) for element in inner_list] for inner_list in bq_ebitda_margin_mr]
                        filter['bq_ebitda_margin_mr'] = bq_ebitda_margin_mr

                    if 'bq_net_profit_margin_mr' in filter.keys():
                        bq_net_profit_margin_mr = filter['bq_net_profit_margin_mr']
                        bq_net_profit_margin_mr = [[divide_by_100(element) for element in inner_list] for inner_list in bq_net_profit_margin_mr]
                        filter['bq_net_profit_margin_mr'] = bq_net_profit_margin_mr

                except json.JSONDecodeError as e:
                    response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                    return response
                for key, val in filter.items():
                    if len(val) >= 1:
                        if key in ('bq_organization_year_founded','bq_current_employees_plan_mr','bq_revenue_mr','bq_ebitda_mr','bq_cor_mr','bq_net_income_mr','bq_gross_profit_mr','bq_total_assets_mr','bq_payroll_mr','bq_operating_expenses_mr','bq_operating_income_mr','bq_tax_and_interest_mr','bq_gross_profit_margin_mr','bq_ebitda_margin_mr','bq_asset_turnover_mr','bq_net_profit_margin_mr','bq_return_on_assets_mr','bq_return_on_sales_mr','bq_revenue_growth_yoy_mr','bq_current_employees_plan_growth_yoy_mr','bq_revenue_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_monthly_yoy_mr','bq_revenue_growth_qoq_mr','bq_current_employees_plan_growth_qoq_mr','bq_current_employees_plan_growth_mom_mr','bq_current_assets_mr','bq_cash_mr','bq_trade_notes_and_accounts_receivable_mr','bq_less_allowance_for_bad_debts_mr','bq_inventories_mr','bq_us_government_obligations_mr','bq_tax_exempt_securities_mr','bq_other_current_assets_mr','bq_non_current_assets_mr','bq_loans_to_shareholders_mr','bq_mortgage_and_real_estate_loans_mr','bq_other_investments_mr','bq_buildings_and_other_depreciable_assets_mr','bq_less_accumulated_depreciation_mr','bq_depletable_assets_mr','bq_less_accumulated_depletion_mr','bq_land_mr','bq_intangible_assets_amortizable_mr','bq_less_accumulated_amortization_mr','bq_other_non_current_assets_mr','bq_total_liabilities_and_equity_mr','bq_current_liabilities_mr','bq_accounts_payable_mr','bq_mortgages_notes_bonds_payable_less_than_1year_mr','bq_other_current_liabilities_mr','bq_non_current_liabilities_mr','bq_loans_from_shareholders_mr','bq_mortgages_notes_bonds_payable_more_than_1year_mr','bq_other_non_current_liabilities_mr','bq_shareholders_equity_mr'):
                            final = ''
                            for items in val:
                                itm = ''
                                for i in items:
                                    if key in ['bq_gross_profit_margin_mr','bq_ebitda_margin_mr','bq_asset_turnover_mr','bq_net_profit_margin_mr','bq_return_on_assets_mr','bq_return_on_sales_mr','bq_revenue_growth_yoy_mr','bq_current_employees_plan_growth_yoy_mr','bq_revenue_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_monthly_yoy_mr','bq_revenue_growth_qoq_mr','bq_current_employees_plan_growth_qoq_mr','bq_current_employees_plan_growth_mom_mr','bq_current_assets_mr','bq_non_current_assets_mr','bq_other_non_current_assets_mr','bq_current_liabilities_mr','bq_non_current_liabilities_mr','bq_other_non_current_liabilities_mr','bq_shareholders_equity_mr']:
                                        i = int(f'{i[1:]}')/100
                                        i = f'{sign}={i}'
                                    itm = f"{itm} {key} {i} AND"
                                itm = remove_and_from_end(itm)
                                itm = f'({itm})'
                                final = f"{final} {itm} OR"
                            yql = f"{yql} ({remove_and_from_end(final)}) AND"
                        elif key == 'bq_organization_cik':
                            if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                yql = remove_and_from_end(yql)
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
                            elif yql.lower().rstrip().endswith('where'):
                                yql = f"{yql} {key} contains '{val[0]}' AND"
                            else:
                                yql = f"{yql} AND {key} contains '{val[0]}' AND"
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
    else:
        if len(filter)>2:
            try:
                filter = json.loads(filter.replace("'", "\""))
            except json.JSONDecodeError as e:
                response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."},"status":400}
                return response
            for key, val in filter.items():
                if len(val) >= 1:
                    if key in ('bq_organization_year_founded','bq_current_employees_plan_mr','bq_revenue_mr','bq_ebitda_mr','bq_cor_mr','bq_net_income_mr','bq_gross_profit_mr','bq_total_assets_mr','bq_payroll_mr','bq_operating_expenses_mr','bq_operating_income_mr','bq_tax_and_interest_mr','bq_gross_profit_margin_mr','bq_ebitda_margin_mr','bq_asset_turnover_mr','bq_net_profit_margin_mr','bq_return_on_assets_mr','bq_return_on_sales_mr','bq_revenue_growth_yoy_mr','bq_current_employees_plan_growth_yoy_mr','bq_revenue_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_monthly_yoy_mr','bq_revenue_growth_qoq_mr','bq_current_employees_plan_growth_qoq_mr','bq_current_employees_plan_growth_mom_mr','bq_current_assets_mr','bq_cash_mr','bq_trade_notes_and_accounts_receivable_mr','bq_less_allowance_for_bad_debts_mr','bq_inventories_mr','bq_us_government_obligations_mr','bq_tax_exempt_securities_mr','bq_other_current_assets_mr','bq_non_current_assets_mr','bq_loans_to_shareholders_mr','bq_mortgage_and_real_estate_loans_mr','bq_other_investments_mr','bq_buildings_and_other_depreciable_assets_mr','bq_less_accumulated_depreciation_mr','bq_depletable_assets_mr','bq_less_accumulated_depletion_mr','bq_land_mr','bq_intangible_assets_amortizable_mr','bq_less_accumulated_amortization_mr','bq_other_non_current_assets_mr','bq_total_liabilities_and_equity_mr','bq_current_liabilities_mr','bq_accounts_payable_mr','bq_mortgages_notes_bonds_payable_less_than_1year_mr','bq_other_current_liabilities_mr','bq_non_current_liabilities_mr','bq_loans_from_shareholders_mr','bq_mortgages_notes_bonds_payable_more_than_1year_mr','bq_other_non_current_liabilities_mr','bq_shareholders_equity_mr'):
                        final = ''
                        for items in val:
                            print(888888888888888888888888,items)
                            itm = ''
                            for i in items:
                                if key in ['bq_gross_profit_margin_mr','bq_ebitda_margin_mr','bq_asset_turnover_mr','bq_net_profit_margin_mr','bq_return_on_assets_mr','bq_return_on_sales_mr','bq_revenue_growth_yoy_mr','bq_current_employees_plan_growth_yoy_mr','bq_revenue_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_monthly_yoy_mr','bq_revenue_growth_qoq_mr','bq_current_employees_plan_growth_qoq_mr','bq_current_employees_plan_growth_mom_mr','bq_current_assets_mr','bq_non_current_assets_mr','bq_other_non_current_assets_mr','bq_current_liabilities_mr','bq_non_current_liabilities_mr','bq_other_non_current_liabilities_mr','bq_shareholders_equity_mr']:
                                    sign = i[0]
                                    i = int(f'{i[1:]}')/100
                                    i = f'{sign}={i}'
                                itm = f"{itm} {key} {i} AND"
                            itm = remove_and_from_end(itm)
                            itm = f'({itm})'
                            final = f"{final} {itm} OR"
                        yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    
                    elif key == 'bq_organization_cik':
                        if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                            yql = remove_and_from_end(yql)
                            yql = f"{yql} AND {key} contains '{val[0]}' AND"
                        elif yql.lower().rstrip().endswith('where'):
                            yql = f"{yql} {key} contains '{val[0]}' AND"
                        else:
                            yql = f"{yql} AND {key} contains '{val[0]}' AND"
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
        order_by_map={"bq_organization_name":"bq_organization_name","bq_organization_isactive":"bq_organization_isactive","bq_organization_structure":"bq_organization_structure","bq_score":"bq_score","bq_organization_jurisdiction_code":"bq_organization_jurisdiction_code",'bq_organization_year_founded':'bq_organization_year_founded','bq_current_employees_plan_mr':'bq_current_employees_plan_mr','bq_revenue_mr':'bq_revenue_mr','bq_ebitda_mr':'bq_ebitda_mr','bq_cor_mr':'bq_cor_mr','bq_net_income_mr':'bq_net_income_mr','bq_gross_profit_mr':'bq_gross_profit_mr','bq_total_assets_mr':'bq_total_assets_mr','bq_payroll_mr':'bq_payroll_mr','bq_operating_expenses_mr':'bq_operating_expenses_mr','bq_operating_income_mr':'bq_operating_income_mr','bq_tax_and_interest_mr':'bq_tax_and_interest_mr','bq_gross_profit_margin_mr':'bq_gross_profit_margin_mr','bq_ebitda_margin_mr':'bq_ebitda_margin_mr','bq_asset_turnover_mr':'bq_asset_turnover_mr','bq_net_profit_margin_mr':'bq_net_profit_margin_mr','bq_return_on_assets_mr':'bq_return_on_assets_mr','bq_return_on_sales_mr':'bq_return_on_sales_mr','bq_revenue_growth_yoy_mr':'bq_revenue_growth_yoy_mr','bq_current_employees_plan_growth_yoy_mr':'bq_current_employees_plan_growth_yoy_mr','bq_revenue_growth_quarterly_yoy_mr':'bq_revenue_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_quarterly_yoy_mr':'bq_current_employees_plan_growth_quarterly_yoy_mr','bq_current_employees_plan_growth_monthly_yoy_mr':'bq_current_employees_plan_growth_monthly_yoy_mr','bq_revenue_growth_qoq_mr':'bq_revenue_growth_qoq_mr','bq_current_employees_plan_growth_qoq_mr':'bq_current_employees_plan_growth_qoq_mr','bq_current_employees_plan_growth_mom_mr':'bq_current_employees_plan_growth_mom_mr','bq_current_assets_mr':'bq_current_assets_mr','bq_cash_mr':'bq_cash_mr','bq_trade_notes_and_accounts_receivable_mr':'bq_trade_notes_and_accounts_receivable_mr','bq_less_allowance_for_bad_debts_mr':'bq_less_allowance_for_bad_debts_mr','bq_inventories_mr':'bq_inventories_mr','bq_us_government_obligations_mr':'bq_us_government_obligations_mr','bq_tax_exempt_securities_mr':'bq_tax_exempt_securities_mr','bq_other_current_assets_mr':'bq_other_current_assets_mr','bq_non_current_assets_mr':'bq_non_current_assets_mr','bq_loans_to_shareholders_mr':'bq_loans_to_shareholders_mr','bq_mortgage_and_real_estate_loans_mr':'bq_mortgage_and_real_estate_loans_mr','bq_other_investments_mr':'bq_other_investments_mr','bq_buildings_and_other_depreciable_assets_mr':'bq_buildings_and_other_depreciable_assets_mr','bq_less_accumulated_depreciation_mr':'bq_less_accumulated_depreciation_mr','bq_depletable_assets_mr':'bq_depletable_assets_mr','bq_less_accumulated_depletion_mr':'bq_less_accumulated_depletion_mr','bq_land_mr':'bq_land_mr','bq_intangible_assets_amortizable_mr':'bq_intangible_assets_amortizable_mr','bq_less_accumulated_amortization_mr':'bq_less_accumulated_amortization_mr','bq_other_non_current_assets_mr':'bq_other_non_current_assets_mr','bq_total_liabilities_and_equity_mr':'bq_total_liabilities_and_equity_mr','bq_current_liabilities_mr':'bq_current_liabilities_mr','bq_accounts_payable_mr':'bq_accounts_payable_mr','bq_mortgages_notes_bonds_payable_less_than_1year_mr':'bq_mortgages_notes_bonds_payable_less_than_1year_mr','bq_other_current_liabilities_mr':'bq_other_current_liabilities_mr','bq_non_current_liabilities_mr':'bq_non_current_liabilities_mr','bq_loans_from_shareholders_mr':'bq_loans_from_shareholders_mr','bq_mortgages_notes_bonds_payable_more_than_1year_mr':'bq_mortgages_notes_bonds_payable_more_than_1year_mr','bq_other_non_current_liabilities_mr':'bq_other_non_current_liabilities_mr','bq_shareholders_equity_mr':'bq_shareholders_equity_mr',}
        orderbyField = order_by_map.get(orderby,"bq_revenue_mr")
        
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        
        yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization')) order by {orderbyField} {order}"
    
    else:
        order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
        yql = f"{yql} and ((bq_legal_entity_parent_status contains 'Ultimate Parent') OR (bq_legal_entity_parent_status contains 'Sole') OR (bq_organization_structure contains 'Single-entity organization')) order by bq_revenue_mr {order}"
    
    print("Final_YQL ============= ",yql)
            
    if side_bar == False:
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
    else:
        params = {
            'yql': yql,
            # 'offset': offset,
            # 'ranking': ranking,
            'limit': 50000,
            'type': 'all',
            'hits': 50000,
            "format": "json",
            'timeout':29
        }
    response = requests.get(search_endpoint, params=params).json()
    
    if side_bar == False:
        try:
            fields_to_convert = {
                'bq_current_employees_plan_mr': 'headcount_ranges',
                'bq_revenue_mr': 'revenue_ranges',
                'bq_ebitda_mr': 'amount_related',
                'bq_total_assets_mr': 'amount_related',
                'bq_net_income_mr': 'amount_related',
                'bq_ebitda_margin_mr':'percent_range0_1',
                'bq_net_profit_margin_mr':'percent_range0_1',
                'bq_current_assets_mr':'amount_related',
                'bq_cash_mr':'amount_related',
                'bq_trade_notes_and_accounts_receivable_mr':'amount_related',
                'bq_less_allowance_for_bad_debts_mr':'amount_related',
                'bq_inventories_mr':'amount_related',
                'bq_us_government_obligations_mr':'amount_related',
                'bq_tax_exempt_securities_mr':'amount_related',
                'bq_other_current_assets_mr':'amount_related',
                'bq_non_current_assets_mr':'amount_related',
                'bq_loans_to_shareholders_mr':'amount_related',
                'bq_us_government_obligations_mr':'amount_related',
                'bq_mortgage_and_real_estate_loans_mr':'amount_related',
                'bq_other_investments_mr':'amount_related',
                'bq_buildings_and_other_depreciable_assets_mr':'amount_related',
                'bq_less_accumulated_depreciation_mr':'amount_related',
                'bq_depletable_assets_mr':'amount_related',
                'bq_less_accumulated_depletion_mr':'amount_related',
                'bq_land_mr':'amount_related',
                'bq_intangible_assets_amortizable_mr':'amount_related',
                'bq_less_accumulated_amortization_mr':'amount_related',
                'bq_other_non_current_assets_mr':'amount_related',
                'bq_total_liabilities_and_equity_mr':'amount_related',
                'bq_current_liabilities_mr':'amount_related',
                'bq_accounts_payable_mr':'amount_related',
                'bq_mortgages_notes_bonds_payable_less_than_1year_mr':'amount_related',
                'bq_other_current_liabilities_mr':'amount_related',
                'bq_non_current_liabilities_mr':'amount_related',
                'bq_loans_from_shareholders_mr':'amount_related',
                'bq_mortgages_notes_bonds_payable_more_than_1year_mr':'amount_related',
                'bq_other_non_current_liabilities_mr':'amount_related',
                'bq_shareholders_equity_mr':'amount_related',
            }

            for i in response.get('root', {}).get('children', []):
                for field, conversion_type in fields_to_convert.items():
                    try:
                        i['fields'][field] = range_convert(conversion_type, i['fields'][field])
                    except:
                        pass

        except Exception as e:
            response['error'] = str(e)

    no_of_records = response["root"]["fields"]["totalCount"]
    print('no_of_records=====', no_of_records)
    if side_bar == False:
        if no_of_records <= 50000:
            sidebar = screener_sidebar(response)
            response["sidebar"] = sidebar
            response["flag"] = True
            response = {"response":response,"status":200}
            return response
        else:
            response= {}
            response["flag"] = False
            response = {"response":response,"status":200}
            return response
    else:
        response = {"response":screener_sidebar(response),"status":200}
        return response
        

def get_unique_values(user_level):
    try:
        result = {}
        search_endpoint = f"{VESPA_ENDPOINT}/search/"
        for field in ["bq_organization_irs_industry_name", "bq_organization_address1_state_name", "bq_organization_is_public", "bq_organization_isactive", "bq_organization_subsector_name", "bq_organization_sector_name", "bq_organization_structure", "bq_organization_company_type", "bq_organization_address1_cbsa_name", "bq_organization_jurisdiction_code" , "bq_organization_naics_name", "bq_organization_naics_sector_name", "bq_organization_address1_county_name", "bq_organization_address1_city", "bq_organization_address1_zip5"]:
            yql = f"select {field} from terminal_hybrid where true | all(group({field}) max(90000) each(output(count())))"
            print(yql)
            params = {
                'yql': yql,
                'limit': 20,
                'type': 'all',
                'hits': 20000,
                "format": "json",
            }
            response = requests.get(search_endpoint, params=params)

            data = response.json()
            data = data['root']['children'][0]['children'][0]['children']
            unique_fields = list()
            for val in data:
                unique_fields.append(val['value'])
            result.update({field: unique_fields})
        response = {"response":result,"status":200}
        return response
        
    except requests.RequestException as e:
        response = {"response":{"error": "An error occurred while processing the search request.", "details": str(e)}, "status":500}
        return response
    
    except Exception as e:
        repsonse = {"response":{"error": "An unexpected error occurred.", "details": str(e)},"status":400}
        return response
