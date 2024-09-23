import requests, json, psycopg2, re
from utils import remove_and_from_end
from decimal import Decimal

VESPA_ENDPOINT = "http://localhost:8080"
api_key = "*&^V&XvTx7ypbVC5e*$^VUE*6REXIBvoV^Ox6Pobvg7x^Cgv"
central_server_base_url = "https://search2-api.brightquery.com/api/"

def get_range_revenue(value):
    ranges = [
        (0, 1000001, "$0 - $1M"),
        (1000001, 5000001, "$1M - $5M"),
        (5000001, 10000001, "$5M - $10M"),
        (10000001, 25000001, "$10M - $25M"),
        (25000001, 50000001, "$25M - $50M"),
        (50000001, 100000001, "$50M - $100M"),
        (100000001, 250000001, "$100M - $250M"),
        (250000001, 500000001, "$250M - $500M"),
        (500000001, 1000000001, "$500M - $1B"),
        (1000000001, float('inf'), "$1B+")
    ]

    for lower, upper, title in ranges:
        if value > lower and value <= upper:
            rev_range = [lower, upper]
            return rev_range

    return "Unknown"

def get_range_headcount(value):
    ranges = [
        (0, 5, "1-4"),
        (4, 10, "5-9"),
        (9, 20, "10-19"),
        (19, 50, "20-49"),
        (49, 100, "50-99"),
        (99, 250, "100-249"),
        (249, 500, "250-499"),
        (500, float('inf'), "500+")
    ]

    for lower, upper, title in ranges:
        if value > lower and value <= upper:
            headcount_range = [lower, upper]
            return headcount_range

    return "Unknown"


def search_connected_with_terminal(query=None, yql=None, type='all', filter=None, ranking='bm25', hits=20, limit=50, offset=0, orderby=None, isAsc=True, field=None, user_id=None):
    try:
        # set_counter(user_id)
        search_count = 10
        maxedit='{maxEditDistance: 1}'
        # VESPA_ENDPOINT = "http://52.39.53.239:8080" #terminal_poc_vespa_path
        if search_count < 30:
            search_endpoint = f"{VESPA_ENDPOINT}/search/"
            # yql = 'select * from bqorganization where'
            yql = "select documentid, bq_organization_id, bq_organization_name, bq_legal_entity_id, bq_organization_structure, bq_organization_address1_line_1, bq_organization_address1_line_2, bq_organization_address1_city, bq_organization_address1_state,bq_organization_address1_state_name, bq_organization_address1_country, bq_organization_address1_zip5, bq_organization_isactive, bq_organization_company_type, bq_public_indicator, bq_organization_jurisdiction_code, bq_revenue_mr, bq_organization_lfo, bq_current_employees_plan_mr, bq_organization_ein, bq_organization_year_founded, bq_organization_website, bq_score, bq_organization_naics_sector_name, bq_organization_naics_sector_code, bq_organization_naics_name, bq_organization_naics_code, bq_organization_irs_sector_name, bq_organization_subsector_name, bq_organization_ticker,  bq_confidence_score, bq_officer_details, bq_industry_name, bq_cong_district_name, bq_cong_district_id, bq_cong_district_representative_name_from_listing, bq_cong_district_representative_party_name, bq_gross_profit_mr, bq_net_income_mr, bq_revenue_growth_yoy_mr, bq_cong_district_cd, bq_legal_entity_parent_status, bq_legal_entity_address1_latitude, bq_legal_entity_address1_longitude from terminal_hybrid where"
            
            # query = word_correction(query)
            # print(1000000000000000000000000000000000000000000000000000000000,query)
            if query:
                if field:
                    # print('xxxxxxxxxxxxxxxxxxxxxxxxxx',field)
                    if field == 'bq_organization_address1_line_1':
                        query = query.replace(',','')
                        query = query.split()
                        address_search_yql = ''
                        for query in query:
                            address_search_yql = address_search_yql + f"( bq_organization_address1_line_1 contains '{query}' OR  bq_organization_address1_line_2 contains '{query}' OR  bq_organization_address1_state contains '{query}' OR bq_organization_address1_state_name contains '{query}' OR bq_organization_address1_zip5 contains '{query}' OR bq_organization_address1_city contains '{query}') AND "
                        
                        address_search_yql= address_search_yql[:-4].rstrip()
                        # print(address_search_yql)
                        
                        if yql.lower().rstrip().endswith('and'):
                            # yql = f'{yql} and ( bq_organization_address1_line_1 contains "{query}" OR bq_organization_address1_line_2 contains "{query}" OR  bq_organization_address1_state contains "{query}" OR bq_organization_address1_state_name contains "{query}" OR bq_organization_address1_zip5 contains "{query}" OR bq_organization_address1_city contains "{query}" ) and'
                            yql = f'{yql} and ( {address_search_yql} ) and'
                        elif yql.lower().rstrip().endswith('where'):
                            # yql = f'{yql} {field} contains "{query}" and'
                            # yql = f'{yql} ( bq_organization_address1_line_1 contains "{query}" OR bq_organization_address1_line_2 contains "{query}" OR  bq_organization_address1_state contains "{query}" OR bq_organization_address1_state_name contains "{query}" OR bq_organization_address1_zip5 contains "{query}" OR bq_organization_address1_city contains "{query}" ) and'
                            yql = f'{yql} ( {address_search_yql} ) and'
                        else:
                            # yql = f'{yql} and {field} contains "{query}" and'
                            # yql = f'{yql} and ( bq_organization_address1_line_1 contains "{query}" OR bq_organization_address1_line_2 contains "{query}" OR  bq_organization_address1_state contains "{query}" OR bq_organization_address1_state_name contains "{query}" OR bq_organization_address1_zip5 contains "{query}" OR bq_organization_address1_city contains "{query}" ) and'
                            yql = f'{yql} and ( {address_search_yql} ) and'
                    else:
                        if yql.lower().rstrip().endswith('and'):
                            yql = f'{yql} and {field} contains "{query}" and'
                        elif yql.lower().rstrip().endswith('where'):
                            # yql = f"{yql} {field} contains '{query}' and"
                            if field == "bq_organization_website":
                                yql = f"{yql} {field} contains '{query}' or {field} matches '{query}' and"
                            else:
                                yql = f"{yql} {field} contains '{query}' and"
                        else:
                            yql = f'{yql} and {field} contains "{query}" and'
                else:
                    query = word_correction(query)
                    # print(1000000000000000000000000000000000000000000000000000000000,query)
                    query = query.lower()
                    query = re.sub(r'\s{2,}', ' ', query)
                    if 'and' in query:
                        qqq = query.lower().rstrip().split('and')
                        q1 = qqq[0].strip()
                        q3 = qqq[1].strip()
                        q3 = q3.replace(',', ' ')
                        q3 = q3.replace(';', ' ')
                        q3 = re.sub(r'\s+', ' ', q3)
                        q2 = q3.split(' ')
                        s2 = ''
                        for i in q2:
                            s2 = f'{s2} (default contains "{q1}" and default contains "{i}") OR'
                            # s2 = f'{s2} (default contains {maxedit}fuzzy("{query}") and default contains {maxedit}fuzzy("{query}")) OR'
                        yql = f"{yql} ({remove_and_from_end(s2)}) and"
                    else:
                        query = query.replace(',', ' ')
                        query = query.replace(';', ' ')
                        query = re.sub(r'\s+', ' ', query)
                        query1 = query.split(' ')
                        s1 = ''
                        # for word in query1:
                        #     s1 = f'{s1} default contains "{word}" OR'
                        query1 = set(query1)
                        for query in query1:
                            # print(1111111111111111111111111111111111111111111111111111,query)
                            
                            s1 = s1 + f' (default contains "{query}" ) OR'
                            # s1 = s1 + f'(bq_organization_name contains "{query}" OR bq_legal_entity_id contains "{query}" OR bq_organization_structure contains "{query}" OR bq_organization_address1_line_1 contains "{query}" OR bq_organization_address1_line_2 contains "{query}" OR bq_organization_address1_city contains "{query}" OR bq_organization_address1_state contains "{query}" OR bq_organization_address1_state_name contains "{query}" OR bq_organization_address1_country contains "{query}" OR bq_organization_address1_zip5 contains "{query}" OR bq_organization_isactive contains "{query}" OR bq_organization_company_type contains "{query}" OR bq_public_indicator contains "{query}" OR bq_organization_jurisdiction_code contains "{query}" OR bq_revenue_mr contains "{query}" OR bq_organization_lfo contains "{query}" OR bq_current_employees_plan_mr contains "{query}" OR bq_organization_ein contains "{query}" OR bq_organization_year_founded contains "{query}" OR bq_organization_website contains "{query}" OR bq_score contains "{query}" OR bq_organization_naics_sector_name contains "{query}" OR bq_organization_naics_sector_code contains "{query}" OR bq_organization_naics_name contains "{query}" OR bq_organization_naics_code contains "{query}" OR bq_organization_irs_sector_name contains "{query}" OR bq_organization_subsector_name contains "{query}" OR bq_industry_name contains "{query}" OR bq_organization_ticker contains "{query}" OR bq_cong_district_name contains "{query}" OR bq_cong_district_cd contains "{query}" OR bq_cong_district_id contains "{query}" OR bq_cong_district_representative_name_from_listing contains "{query}" OR bq_cong_district_representative_party_name contains "{query}" OR bq_gross_profit_mr contains "{query}" OR bq_net_income_mr contains "{query}" OR bq_revenue_growth_yoy_mr contains "{query}") OR'

                            missing_query_parameter = 'OR bq_organization_valuation contains "{query}" OR bq_organization_cong_district_representative_last_name contains "{query}" OR bq_organization_address1_location contains "{query}" '
                            # s1 = f'{s1} default contains "{word}" OR'
                        yql = f"{yql} ({remove_and_from_end(s1)}) and"  
            if yql:
                if filter:
                    try:
                        filter = json.loads(filter.replace("'", "\""))
                    except json.JSONDecodeError as e:
                        response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."}, "status" : 400}
                        return response
                        # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
                    for key, val in filter.items():
                        if len(val)>=1:
                            if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_current_employees_plan_mr', 'bq_organization_total_erc_tax_credit_modded','bq_organization_ir_refund_amount','bq_organization_valuation'):
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
                                elif len(val)==0:
                                    pass
                                else:
                                    if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                        yql = remove_and_from_end(yql)
                                        yql = f'{yql} AND {key} contains "{val[0]}" AND'
                                    elif yql.lower().rstrip().endswith('where'):
                                        yql = f"{yql} {key} contains '{val[0]}' AND"
                                    else:
                                        yql = f'{yql} AND {key} contains "{val[0]}" AND'
            yql = remove_and_from_end(yql)
            if field in ['bq_organization_website','bq_organization_ticker','bq_organization_ein']:
                yql = yql + " and bq_legal_entity_parent_status contains 'Ultimate Parent'"


            if orderby:
                order_by_map={"bq_organization_name":"bq_organization_name","bq_revenue_mr":"bq_revenue_mr","bq_current_employees_plan_mr":"bq_current_employees_plan_mr","bq_current_employees_plan_growth_yoy_mr":"bq_current_employees_plan_growth_yoy_mr","bq_organization_isactive":"bq_organization_isactive","bq_score":"bq_score"}
                
                orderbyField = order_by_map.get(orderby,"bq_organization_name")
                order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            
                yql = f"{yql} order by {orderbyField} {order}"
                
            # print(11111111111,yql)
            # yql = yql + ' and bq_legal_entity_parent_status contains "Ultimate Parent"'
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
            response = response.json()
            # response['yql']=yql
            response = {"response":response, "status":200}
            return response
            # return JSONResponse(content=response.json(), status_code=200)

        else:
            response = {"response":1, "status":200}
            return response
        
    except requests.RequestException as e:
        # return JSONResponse(content={"error": "An error occurred while processing the search request.", "details": str(e)}, status_code=500)
        response = {"response":{"error": "An error occurred while processing the search request.", "details": str(e)}, "status":200}
        return response
    
    except Exception as e:
        # return JSONResponse(content={"error": "An unexpected error occurred.", "details": str(e)}, status_code=500)
        response = {"response":{"error": "An unexpected error occurred.", "details": str(e), "details": str(e)}, "status":200}
        return response

def side_filters(data):
    response = {"bq_sector_name":["Accommodation and Food Services","Administrative and Support and Waste Management and Remediation Services","Agriculture, Forestry, Fishing and Hunting","Arts, Entertainment, and Recreation","Construction","Educational Services","Finance and Insurance","Governmental Instrumentality or Agency","Health Care and Social Assistance","Information","Management of Companies (Holding Companies)","Manufacturing","Mining","No Classification","Other Services","Professional, Scientific, and Technical Services","Real Estate and Rental and Leasing","Retail Trade","Transportation and Warehousing","Utilities","Wholesale Trade"],"bq_company_address1_county_name":["Abbeville","Acadia","Acadia Parish","Accomack","Ada","Adair","Adams","Addison","Adjuntas","Adjuntas Municipio","Aguada","Aguada Municipio","Aguadilla","Aguadilla Municipio","Aguas Buenas","Aguas Buenas Municipio","Aibonito","Aibonito Municipio","Aiken","Aitkin","Alachua","Alamance","Alameda","Alamosa","Albany","Albemarle","Alcona","Alcorn","Aleutians East","Aleutians East Borough","Aleutians West","Aleutians West Census Area","Alexander","Alexandria City","Alexandria city","Alfalfa","Alger","Allamakee","Allegan","Allegany","Alleghany","Allegheny","Allen","Allen Parish","Allendale","Alpena","Alpine","Amador","Amelia","American Samoa","Amherst","Amite","Anasco","Anasco Municipio","Anchorage","Anchorage Municipality","Anderson","Andrew","Andrews","Androscoggin","Angelina","Anne Arundel","Anoka","Anson","Antelope","Antrim","Apache","Appanoose","Appling","Appomattox","Aransas","Arapahoe","Archer","Archuleta","Arecibo","Arecibo Municipio","Arenac","Arkansas","Arlington","Armstrong","Aroostook","Arroyo","Arroyo Municipio","Arthur","Ascension","Ascension Parish","Ashe","Ashland","Ashley","Ashtabula","Asotin","Assumption","Assumption Parish","Atascosa","Atchison","Athens","Atkinson","Atlantic","Atoka","Attala","Audrain","Audubon","Auglaize","Augusta","Aurora","Austin","Autauga","Avery","Avoyelles","Avoyelles Parish","Baca","Bacon","Bailey","Baker","Baldwin","Ballard","Baltimore","Baltimore City","Baltimore city","Bamberg","Bandera","Banks","Banner","Bannock","Baraga","Barber","Barbour","Barceloneta","Barceloneta Municipio","Barnes","Barnstable","Barnwell","Barranquitas","Barranquitas Municipio","Barren","Barron","Barrow","Barry","Bartholomew","Barton","Bartow","Bastrop","Bates","Bath","Baxter","Bay","Bayamon","Bayamon Municipio","Bayfield","Baylor","Beadle","Bear Lake","Beaufort","Beauregard","Beauregard Parish","Beaver","Beaverhead","Becker","Beckham","Bedford","Bee","Belknap","Bell","Belmont","Beltrami","Ben Hill","Benewah","Bennett","Bennington","Benson","Bent","Benton","Benzie","Bergen","Berkeley","Berks","Berkshire","Bernalillo","Berrien","Bertie","Bethel","Bethel Census Area","Bexar","Bibb","Bienville","Bienville Parish","Big Horn","Big Stone","Billings","Bingham","Black Hawk","Blackford","Bladen","Blaine","Blair","Blanco","Bland","Bleckley","Bledsoe","Blount","Blue Earth","Boise","Bolivar","Bollinger","Bon Homme","Bond","Bonner","Bonneville","Boone","Borden","Bosque","Bossier","Bossier Parish","Botetourt","Bottineau","Boulder","Boundary","Bourbon","Bowie","Bowman","Box Butte","Box Elder","Boyd","Boyle","Bracken","Bradford","Bradley","Branch","Brantley","Braxton","Brazoria","Brazos","Breathitt","Breckinridge","Bremer","Brevard","Brewster","Briscoe","Bristol","Bristol Bay","Bristol Bay Borough","Bristol City","Bristol city","Broadwater","Bronx","Brooke","Brookings","Brooks","Broome","Broomfield","Broward","Brown","Brule","Brunswick","Bryan","Buchanan","Buckingham","Bucks","Buena Vista","Buena Vista City","Buena Vista city","Buffalo","Bullitt","Bulloch","Bullock","Buncombe","Bureau","Burke","Burleigh","Burleson","Burlington","Burnet","Burnett","Burt","Butler","Butte","Butts","Cabarrus","Cabell","Cabo Rojo","Cabo Rojo Municipio","Cache","Caddo","Caddo Parish","Caguas","Caguas Municipio","Calaveras","Calcasieu","Calcasieu Parish","Caldwell","Caldwell Parish","Caledonia","Calhoun","Callahan","Callaway","Calloway","Calumet","Calvert","Camas","Cambria","Camden","Cameron","Cameron Parish","Camp","Campbell","Camuy","Camuy Municipio","Canadian","Candler","Cannon","Canovanas","Canovanas Municipio","Canyon","Cape Girardeau","Cape May","Carbon","Caribou","Carlisle","Carlton","Carolina","Carolina Municipio","Caroline","Carroll","Carson","Carson City","Carter","Carteret","Carver","Cascade","Casey","Cass","Cassia","Castro","Caswell","Catahoula","Catahoula Parish","Catano","Catano Municipio","Catawba","Catoosa","Catron","Cattaraugus","Cavalier","Cayey","Cayey Municipio","Cayuga","Cecil","Cedar","Ceiba","Ceiba Municipio","Centre","Cerro Gordo","Chaffee","Chambers","Champaign","Chariton","Charles","Charles City","Charles Mix","Charleston","Charlevoix","Charlotte","Charlottesville City","Charlottesville city","Charlton","Chase","Chatham","Chattahoochee","Chattooga","Chautauqua","Chaves","Cheatham","Cheboygan","Chelan","Chemung","Chenango","Cherokee","Cherry","Chesapeake City","Chesapeake city","Cheshire","Chester","Chesterfield","Cheyenne","Chickasaw","Chicot","Childress","Chilton","Chippewa","Chisago","Chittenden","Choctaw","Chouteau","Chowan","Christian","Chugach","Churchill","Ciales","Ciales Municipio","Cibola","Cidra","Cidra Municipio","Cimarron","Citrus","Clackamas","Claiborne","Claiborne Parish","Clallam","Clare","Clarendon","Clarion","Clark","Clarke","Clatsop","Clay","Clayton","Clear Creek","Clearfield","Clearwater","Cleburne","Clermont","Cleveland","Clinch","Clinton","Cloud","Coahoma","Coal","Coamo","Coamo Municipio","Cobb","Cochise","Cochran","Cocke","Coconino","Codington","Coffee","Coffey","Coke","Colbert","Cole","Coleman","Coles","Colfax","Colleton","Collier","Collin","Collingsworth","Colonial Heights City","Colonial Heights city","Colorado","Colquitt","Columbia","Columbiana","Columbus","Colusa","Comal","Comanche","Comerio","Comerio Municipio","Concho","Concordia","Concordia Parish","Conecuh","Conejos","Contra Costa","Converse","Conway","Cook","Cooke","Cooper","Coos","Coosa","Copiah","Copper River","Corozal","Corozal Municipio","Corson","Cortland","Coryell","Coshocton","Costilla","Cottle","Cotton","Cottonwood","Covington","Covington City","Covington city","Coweta","Cowley","Cowlitz","Craig","Craighead","Crane","Craven","Crawford","Creek","Crenshaw","Crisp","Crittenden","Crockett","Crook","Crosby","Cross","Crow Wing","Crowley","Culberson","Culebra","Cullman","Culpeper","Cumberland","Cuming","Currituck","Curry","Custer","Cuyahoga","Dade","Daggett","Dakota","Dale","Dallam","Dallas","Dane","Daniels","Danville City","Danville city","Dare","Darke","Darlington","Dauphin","Davidson","Davie","Daviess","Davis","Davison","Dawes","Dawson","Day","De Baca","De Kalb","De Soto","De Soto Parish","De Witt","DeKalb","DeSoto","DeWitt","Deaf Smith","Dearborn","Decatur","Deer Lodge","Defiance","Del Norte","Delaware","Delta","Denali","Denali Borough","Dent","Denton","Denver","Des Moines","Deschutes","Desha","Desoto","Deuel","Dewey","Dewitt","Dickens","Dickenson","Dickey","Dickinson","Dickson","Dillingham","Dillingham Census Area","Dillon","Dimmit","Dinwiddie","District of Columbia","Divide","Dixie","Dixon","Doddridge","Dodge","Dolores","Dona Ana","Doniphan","Donley","Dooly","Door","Dorado","Dorado Municipio","Dorchester","Dougherty","Douglas","Drew","DuPage","Dubois","Dubuque","Duchesne","Dukes","Dundy","Dunklin","Dunn","Dupage","Duplin","Durham","Dutchess","Duval","Dyer","Eagle","Early","East Baton Rouge","East Baton Rouge Parish","East Carroll","East Carroll Parish","East Feliciana","East Feliciana Parish","Eastland","Eaton","Eau Claire","Echols","Ector","Eddy","Edgar","Edgecombe","Edgefield","Edmonson","Edmunds","Edwards","Effingham","El Dorado","El Paso","Elbert","Elk","Elkhart","Elko","Elliott","Ellis","Ellsworth","Elmore","Emanuel","Emery","Emmet","Emmons","Emporia City","Emporia city","Erath","Erie","Escambia","Esmeralda","Essex","Estill","Etowah","Eureka","Evangeline","Evangeline Parish","Evans","Fairbanks North Star","Fairbanks North Star Borough","Fairfax","Fairfax City","Fairfax city","Fairfield","Fajardo","Fajardo Municipio","Fall River","Fallon","Falls","Falls Church City","Falls Church city","Fannin","Faribault","Faulk","Faulkner","Fauquier","Fayette","Federated States of Micro","Fentress","Fergus","Ferry","Fillmore","Finney","Fisher","Flagler","Flathead","Fleming","Florence","Florida","Florida Municipio","Floyd","Fluvanna","Foard","Fond du Lac","Ford","Forest","Forrest","Forsyth","Fort Bend","Foster","Fountain","Franklin","Franklin City","Franklin Parish","Franklin city","Frederick","Fredericksburg City","Fredericksburg city","Freeborn","Freestone","Fremont","Fresno","Frio","Frontier","Fulton","Furnas","Gadsden","Gage","Gaines","Galax City","Galax city","Gallatin","Gallia","Galveston","Garden","Garfield","Garland","Garrard","Garrett","Garvin","Garza","Gasconade","Gaston","Gates","Geary","Geauga","Gem","Genesee","Geneva","Gentry","George","Georgetown","Gibson","Gila","Gilchrist","Giles","Gillespie","Gilliam","Gilmer","Gilpin","Glacier","Glades","Gladwin","Glascock","Glasscock","Glenn","Gloucester","Glynn","Gogebic","Golden Valley","Goliad","Gonzales","Goochland","Goodhue","Gooding","Gordon","Goshen","Gosper","Gove","Grady","Grafton","Graham","Grainger","Grand","Grand Forks","Grand Isle","Grand Traverse","Granite","Grant","Grant Parish","Granville","Gratiot","Graves","Gray","Grays Harbor","Grayson","Greeley","Green","Green Lake","Greenbrier","Greene","Greenlee","Greensville","Greenup","Greenville","Greenwood","Greer","Gregg","Gregory","Grenada","Griggs","Grimes","Grundy","Guadalupe","Guam","Guanica","Guanica Municipio","Guayama","Guayama Municipio","Guayanilla","Guayanilla Municipio","Guaynabo","Guaynabo Municipio","Guernsey","Guilford","Gulf","Gunnison","Gurabo","Gurabo Municipio","Guthrie","Gwinnett","Haakon","Habersham","Haines","Haines Borough","Hale","Halifax","Hall","Hamblen","Hamilton","Hamlin","Hampden","Hampshire","Hampton","Hampton City","Hampton city","Hancock","Hand","Hanover","Hansford","Hanson","Haralson","Hardee","Hardeman","Hardin","Harding","Hardy","Harford","Harlan","Harmon","Harnett","Harney","Harper","Harris","Harrison","Harrisonburg City","Harrisonburg city","Hart","Hartford","Hartley","Harvey","Haskell","Hatillo","Hatillo Municipio","Hawaii","Hawkins","Hayes","Hays","Haywood","Heard","Hemphill","Hempstead","Henderson","Hendricks","Hendry","Hennepin","Henrico","Henry","Herkimer","Hernando","Hertford","Hettinger","Hickman","Hickory","Hidalgo","Highland","Highlands","Hill","Hillsborough","Hillsdale","Hinds","Hinsdale","Hitchcock","Hocking","Hockley","Hodgeman","Hoke","Holmes","Holt","Honolulu","Hood","Hood River","Hooker","Hoonah Angoon","Hoonah-Angoon Census Area","Hopewell City","Hopewell city","Hopkins","Hormigueros","Hormigueros Municipio","Horry","Hot Spring","Hot Springs","Houghton","Houston","Howard","Howell","Hubbard","Hudson","Hudspeth","Huerfano","Hughes","Humacao","Humacao Municipio","Humboldt","Humphreys","Hunt","Hunterdon","Huntingdon","Huntington","Huron","Hutchinson","Hyde","Iberia","Iberia Parish","Iberville","Iberville Parish","Ida","Idaho","Imperial","Independence","Indian River","Indiana","Ingham","Inyo","Ionia","Iosco","Iowa","Iredell","Irion","Iron","Iroquois","Irwin","Isabela","Isabela Municipio","Isabella","Isanti","Island","Isle of Wight","Issaquena","Itasca","Itawamba","Izard","Jack","Jackson","Jackson Parish","James City","Jasper","Jay","Jayuya","Jayuya Municipio","Jeff Davis","Jefferson","Jefferson Davis","Jefferson Davis Parish","Jefferson Parish","Jenkins","Jennings","Jerauld","Jerome","Jersey","Jessamine","Jewell","Jim Hogg","Jim Wells","Jo Daviess","Johnson","Johnston","Jones","Josephine","Juab","Juana Diaz","Juana Diaz Municipio","Judith Basin","Juncos","Juncos Municipio","Juneau","Juneau City and Borough","Juniata","Kalamazoo","Kalawao","Kalkaska","Kanabec","Kanawha","Kandiyohi","Kane","Kankakee","Karnes","Kauai","Kaufman","Kay","Kearney","Kearny","Keith","Kemper","Kenai Peninsula","Kenai Peninsula Borough","Kendall","Kenedy","Kennebec","Kenosha","Kent","Kenton","Keokuk","Kern","Kerr","Kershaw","Ketchikan Gateway","Ketchikan Gateway Borough","Kewaunee","Keweenaw","Keya Paha","Kidder","Kimball","Kimble","King","King George","King William","King and Queen","Kingfisher","Kingman","Kings","Kingsbury","Kinney","Kiowa","Kit Carson","Kitsap","Kittitas","Kittson","Klamath","Kleberg","Klickitat","Knott","Knox","Kodiak Island","Kodiak Island Borough","Koochiching","Kootenai","Kosciusko","Kossuth","Kusilvak","La Crosse","La Paz","La Plata","La Porte","La Salle","La Salle Parish","LaGrange","LaMoure","LaPorte","LaSalle","Labette","Lac Qui Parle","Lac qui Parle","Lackawanna","Laclede","Lafayette","Lafayette Parish","Lafourche","Lafourche Parish","Lagrange","Lajas","Lajas Municipio","Lake","Lake and Peninsula","Lake and Peninsula Borough","Lake of the Woods","Lamar","Lamb","Lamoille","Lamoure","Lampasas","Lancaster","Lander","Lane","Langlade","Lanier","Lapeer","Laramie","Lares","Lares Municipio","Larimer","Larue","Las Animas","Las Marias","Las Piedras","Las Piedras Municipio","Lassen","Latah","Latimer","Lauderdale","Laurel","Laurens","Lavaca","Lawrence","Le Flore","Le Sueur","Lea","Leake","Leavenworth","Lebanon","Lee","Leelanau","Leflore","Lehigh","Lemhi","Lenawee","Lenoir","Leon","Leslie","Letcher","Levy","Lewis","Lewis and Clark","Lexington","Lexington City","Lexington city","Liberty","Licking","Limestone","Lincoln","Lincoln Parish","Linn","Lipscomb","Litchfield","Little River","Live Oak","Livingston","Livingston Parish","Llano","Logan","Loiza","Loiza Municipio","Long","Lonoke","Lorain","Los Alamos","Los Angeles","Loudon","Loudoun","Louisa","Loup","Love","Loving","Lowndes","Lubbock","Lucas","Luce","Lumpkin","Luna","Lunenburg","Luquillo","Luquillo Municipio","Luzerne","Lycoming","Lyman","Lynchburg City","Lynchburg city","Lynn","Lyon","Mackinac","Macomb","Macon","Macoupin","Madera","Madison","Madison Parish","Magoffin","Mahaska","Mahnomen","Mahoning","Major","Malheur","Manassas City","Manassas Park City","Manassas Park city","Manassas city","Manatee","Manati","Manati Municipio","Manistee","Manitowoc","Marathon","Marengo","Maricao","Maricopa","Maries","Marin","Marinette","Marion","Mariposa","Marlboro","Marquette","Marshall","Marshall Islands","Martin","Martinsville City","Martinsville city","Mason","Massac","Matagorda","Matanuska Susitna","Matanuska-Susitna Borough","Mathews","Maui","Maunabo","Maunabo Municipio","Maury","Maverick","Mayaguez","Mayaguez Municipio","Mayes","McClain","McCone","McCook","McCormick","McCracken","McCreary","McCulloch","McCurtain","McDonald","McDonough","McDowell","McDuffie","McHenry","McIntosh","McKean","McKenzie","McKinley","McLean","McLennan","McLeod","McMinn","McMullen","McNairy","McPherson","Meade","Meagher","Mecklenburg","Mecosta","Medina","Meeker","Meigs","Mellette","Menard","Mendocino","Menifee","Menominee","Merced","Mercer","Meriwether","Merrick","Merrimack","Mesa","Metcalfe","Miami","Miami-Dade","Middlesex","Midland","Mifflin","Milam","Millard","Mille Lacs","Miller","Mills","Milwaukee","Miner","Mineral","Mingo","Minidoka","Minnehaha","Missaukee","Mississippi","Missoula","Mitchell","Mobile","Moca","Moca Municipio","Modoc","Moffat","Mohave","Moniteau","Monmouth","Mono","Monona","Monongalia","Monroe","Montague","Montcalm","Monterey","Montezuma","Montgomery","Montmorency","Montour","Montrose","Moody","Moore","Mora","Morehouse","Morehouse Parish","Morgan","Morovis","Morovis Municipio","Morrill","Morris","Morrison","Morrow","Morton","Motley","Moultrie","Mountrail","Mower","Muhlenberg","Multnomah","Murray","Muscatine","Muscogee","Muskegon","Muskingum","Muskogee","Musselshell","Nacogdoches","Naguabo","Naguabo Municipio","Nance","Nantucket","Napa","Naranjito","Naranjito Municipio","Nash","Nassau","Natchitoches","Natchitoches Parish","Natrona","Navajo","Navarro","Nelson","Nemaha","Neosho","Neshoba","Ness","Nevada","New Castle","New Hanover","New Haven","New Kent","New London","New Madrid","New York","Newaygo","Newberry","Newport","Newport News City","Newport News city","Newton","Nez Perce","Niagara","Nicholas","Nicollet","Niobrara","Noble","Nobles","Nodaway","Nolan","Nome","Nome Census Area","Norfolk","Norfolk City","Norfolk city","Norman","North Slope","North Slope Borough","Northampton","Northumberland","Northwest Arctic","Northwest Arctic Borough","Norton","Norton City","Norton city","Nottoway","Nowata","Noxubee","Nuckolls","Nueces","Nye","O'Brien","Oakland","Obion","Obrien","Ocean","Oceana","Ochiltree","Oconee","Oconto","Ogemaw","Oglala Lakota","Ogle","Oglethorpe","Ohio","Okaloosa","Okanogan","Okeechobee","Okfuskee","Oklahoma","Okmulgee","Oktibbeha","Oldham","Oliver","Olmsted","Oneida","Onondaga","Onslow","Ontario","Ontonagon","Orange","Orangeburg","Oregon","Orleans","Orleans Parish","Orocovis","Orocovis Municipio","Osage","Osborne","Osceola","Oscoda","Oswego","Otero","Otoe","Otsego","Ottawa","Otter Tail","Ouachita","Ouachita Parish","Ouray","Outagamie","Overton","Owen","Owsley","Owyhee","Oxford","Ozark","Ozaukee","Pacific","Page","Palau","Palm Beach","Palo Alto","Palo Pinto","Pamlico","Panola","Park","Parke","Parker","Parmer","Pasco","Pasquotank","Passaic","Patillas","Patillas Municipio","Patrick","Paulding","Pawnee","Payette","Payne","Peach","Pearl River","Pecos","Pembina","Pemiscot","Pend Oreille","Pender","Pendleton","Pennington","Penobscot","Penuelas","Penuelas Municipio","Peoria","Pepin","Perkins","Perquimans","Perry","Pershing","Person","Petersburg","Petersburg Census Area","Petersburg City","Petersburg city","Petroleum","Pettis","Phelps","Philadelphia","Phillips","Piatt","Pickaway","Pickens","Pickett","Pierce","Pike","Pima","Pinal","Pine","Pinellas","Pipestone","Piscataquis","Pitkin","Pitt","Pittsburg","Pittsylvania","Piute","Placer","Plaquemines","Plaquemines Parish","Platte","Pleasants","Plumas","Plymouth","Pocahontas","Poinsett","Pointe Coupee","Pointe Coupee Parish","Polk","Ponce","Ponce Municipio","Pondera","Pontotoc","Pope","Poquoson City","Poquoson city","Portage","Porter","Portsmouth City","Portsmouth city","Posey","Pottawatomie","Pottawattamie","Potter","Powder River","Powell","Power","Poweshiek","Powhatan","Prairie","Pratt","Preble","Prentiss","Presidio","Presque Isle","Preston","Price","Prince Edward","Prince George","Prince George's","Prince Georges","Prince William","Prince of Wales Hyder","Prince of Wales-Hyder Census Area","Providence","Prowers","Pueblo","Pulaski","Pushmataha","Putnam","Quay","Quebradillas","Quebradillas Municipio","Queen Anne's","Queen Annes","Queens","Quitman","Rabun","Racine","Radford","Radford city","Rains","Raleigh","Ralls","Ramsey","Randall","Randolph","Rankin","Ransom","Rapides","Rapides Parish","Rappahannock","Ravalli","Rawlins","Ray","Reagan","Real","Red Lake","Red River","Red River Parish","Red Willow","Redwood","Reeves","Refugio","Reno","Rensselaer","Renville","Republic","Reynolds","Rhea","Rice","Rich","Richardson","Richland","Richland Parish","Richmond","Richmond City","Richmond city","Riley","Rincon","Rincon Municipio","Ringgold","Rio Arriba","Rio Blanco","Rio Grande","Rio Grande Municipio","Ripley","Ritchie","Riverside","Roane","Roanoke","Roanoke City","Roanoke city","Roberts","Robertson","Robeson","Rock","Rock Island","Rockbridge","Rockcastle","Rockdale","Rockingham","Rockland","Rockwall","Roger Mills","Rogers","Rolette","Rooks","Roosevelt","Roscommon","Roseau","Rosebud","Ross","Rota","Routt","Rowan","Runnels","Rush","Rusk","Russell","Rutherford","Rutland","Sabana Grande","Sabana Grande Municipio","Sabine","Sabine Parish","Sac","Sacramento","Sagadahoc","Saginaw","Saguache","Saint Bernard","Saint Charles","Saint Clair","Saint Croix","Saint Francis","Saint Francois","Saint Helena","Saint James","Saint John","Saint Johns","Saint Joseph","Saint Landry","Saint Lawrence","Saint Louis","Saint Louis City","Saint Lucie","Saint Martin","Saint Mary","Saint Marys","Saint Tammany","Saint Thomas","Sainte Genevieve","Saipan","Saipan Municipality","Salem","Salem city","Salinas","Salinas Municipio","Saline","Salt Lake","Saluda","Sampson","San Augustine","San Benito","San Bernardino","San Diego","San Francisco","San German","San German Municipio","San Jacinto","San Joaquin","San Juan","San Juan Municipio","San Lorenzo","San Lorenzo Municipio","San Luis Obispo","San Mateo","San Miguel","San Patricio","San Saba","San Sebastian","San Sebastian Municipio","Sanborn","Sanders","Sandoval","Sandusky","Sangamon","Sanilac","Sanpete","Santa Barbara","Santa Clara","Santa Cruz","Santa Fe","Santa Isabel","Santa Isabel Municipio","Santa Rosa","Sarasota","Saratoga","Sargent","Sarpy","Sauk","Saunders","Sawyer","Schenectady","Schleicher","Schley","Schoharie","Schoolcraft","Schuyler","Schuylkill","Scioto","Scotland","Scott","Scotts Bluff","Screven","Scurry","Searcy","Sebastian","Sedgwick","Seminole","Seneca","Sequatchie","Sequoyah","Sevier","Seward","Shackelford","Shannon","Sharkey","Sharp","Shasta","Shawano","Shawnee","Sheboygan","Shelby","Shenandoah","Sherburne","Sheridan","Sherman","Shiawassee","Shoshone","Sibley","Sierra","Silver Bow","Simpson","Sioux","Siskiyou","Sitka","Sitka City and Borough","Skagit","Skagway","Skagway Municipality","Skamania","Slope","Smith","Smyth","Snohomish","Snyder","Socorro","Solano","Somerset","Somervell","Sonoma","Southampton","Southeast Fairbanks","Southeast Fairbanks Census Area","Spalding","Spartanburg","Spencer","Spink","Spokane","Spotsylvania","St John the Baptist","St Joseph","St. Bernard Parish","St. Charles","St. Charles Parish","St. Clair","St. Croix","St. Croix Island","St. Francis","St. Francois","St. Helena Parish","St. James Parish","St. John Island","St. John the Baptist Parish","St. Johns","St. Joseph","St. Landry Parish","St. Lawrence","St. Louis","St. Louis city","St. Lucie","St. Martin Parish","St. Mary Parish","St. Mary's","St. Tammany Parish","St. Thomas Island","Stafford","Stanislaus","Stanley","Stanly","Stanton","Stark","Starke","Starr","Staunton City","Staunton city","Ste. Genevieve","Stearns","Steele","Stephens","Stephenson","Sterling","Steuben","Stevens","Stewart","Stillwater","Stoddard","Stokes","Stone","Stonewall","Storey","Story","Strafford","Stutsman","Sublette","Suffolk","Suffolk City","Suffolk city","Sullivan","Sully","Summers","Summit","Sumner","Sumter","Sunflower","Surry","Susquehanna","Sussex","Sutter","Sutton","Suwannee","Swain","Sweet Grass","Sweetwater","Swift","Swisher","Switzerland","Talbot","Taliaferro","Talladega","Tallahatchie","Tallapoosa","Tama","Taney","Tangipahoa","Tangipahoa Parish","Taos","Tarrant","Tate","Tattnall","Taylor","Tazewell","Tehama","Telfair","Teller","Tensas","Tensas Parish","Terrebonne","Terrebonne Parish","Terrell","Terry","Teton","Texas","Thayer","Thomas","Throckmorton","Thurston","Tift","Tillamook","Tillman","Tinian","Tioga","Tippah","Tippecanoe","Tipton","Tishomingo","Titus","Toa Alta","Toa Alta Municipio","Toa Baja","Toa Baja Municipio","Todd","Tolland","Tom Green","Tompkins","Tooele","Toole","Toombs","Torrance","Towner","Towns","Traill","Transylvania","Traverse","Travis","Treasure","Trego","Trempealeau","Treutlen","Trigg","Trimble","Trinity","Tripp","Troup","Trousdale","Trujillo Alto","Trujillo Alto Municipio","Trumbull","Tucker","Tulare","Tulsa","Tunica","Tuolumne","Turner","Tuscaloosa","Tuscarawas","Tuscola","Twiggs","Twin Falls","Tyler","Tyrrell","Uinta","Uintah","Ulster","Umatilla","Unicoi","Union","Union Parish","Upshur","Upson","Upton","Utah","Utuado","Utuado Municipio","Uvalde","Val Verde","Valencia","Valley","Van Buren","Van Wert","Van Zandt","Vance","Vanderburgh","Vega Alta","Vega Alta Municipio","Vega Baja","Vega Baja Municipio","Venango","Ventura","Vermilion","Vermilion Parish","Vermillion","Vernon","Vernon Parish","Victoria","Vieques","Vieques Municipio","Vigo","Vilas","Villalba","Villalba Municipio","Vinton","Virginia Beach City","Virginia Beach city","Volusia","Wabash","Wabasha","Wabaunsee","Wadena","Wagoner","Wahkiakum","Wake","Wakulla","Waldo","Walker","Walla Walla","Wallace","Waller","Wallowa","Walsh","Walthall","Walton","Walworth","Wapello","Ward","Ware","Warren","Warrick","Wasatch","Wasco","Waseca","Washakie","Washburn","Washington","Washington Parish","Washita","Washoe","Washtenaw","Watauga","Watonwan","Waukesha","Waupaca","Waushara","Wayne","Waynesboro City","Waynesboro city","Weakley","Webb","Weber","Webster","Webster Parish","Weld","Wells","West Baton Rouge","West Baton Rouge Parish","West Carroll","West Carroll Parish","West Feliciana","West Feliciana Parish","Westchester","Western District","Westmoreland","Weston","Wetzel","Wexford","Wharton","Whatcom","Wheatland","Wheeler","White","White Pine","Whiteside","Whitfield","Whitley","Whitman","Wibaux","Wichita","Wicomico","Wilbarger","Wilcox","Wilkes","Wilkin","Wilkinson","Will","Willacy","Williams","Williamsburg","Williamsburg City","Williamsburg city","Williamson","Wilson","Winchester City","Winchester city","Windham","Windsor","Winkler","Winn","Winn Parish","Winnebago","Winneshiek","Winona","Winston","Wirt","Wise","Wolfe","Wood","Woodbury","Woodford","Woodruff","Woods","Woodson","Woodward","Worcester","Worth","Wrangell","Wrangell City and Borough","Wright","Wyandot","Wyandotte","Wyoming","Wythe","Yabucoa","Yabucoa Municipio","Yadkin","Yakima","Yakutat","Yakutat City and Borough","Yalobusha","Yamhill","Yancey","Yankton","Yates","Yauco","Yauco Municipio","Yavapai","Yazoo","Yell","Yellow Medicine","Yellowstone","Yoakum","Yolo","York","Young","Yuba","Yukon Koyukuk","Yukon-Koyukuk Census Area","Yuma","Zapata","Zavala","Ziebach"],"bq_subsector_name":["Accommodation","Accounting, Tax Preparation, Bookkeeping, and Payroll Services","Activities Related to Credit Intermediation","Administrative and Support Services","Air, Rail, and Water Transportation","Amusement, Gambling, and Recreation Industries","Animal Production","Apparel Manufacturing","Architectural, Engineering, and Related Services","Beverage and Tobacco Product Manufacturing","Broadcasting (except Internet)","Building Material and Garden Equipment and Supplies Dealers","Chemical Manufacturing","Clothing and Clothing Accessories Stores","Computer Systems Design and Related Services","Computer and Electronic Product Manufacturing","Construction of Buildings","Couriers and Messengers","Crop Production","Data Processing Services","Depository Credit Intermediation","Educational Services","Electrical Equipment, Appliance, and Component Manufacturing","Electronics and Appliance Stores","Fabricated Metal Product Manufacturing","Fishing, Hunting and Trapping","Food Manufacturing","Food Services and Drinking Places","Food and Beverage Stores","Forestry and Logging","Funds, Trusts, and Other Financial Vehicles","Furniture and Home Furnishings Stores","Furniture and Related Product Manufacturing","Gasoline Stations","General Merchandise Stores","Governmental Instrumentality or Agency","Health and Personal Care Stores","Heavy and Civil Engineering Construction","Home Health Care Services","Hospital","Insurance Carriers and Related Activities","Leather and Allied Product Manufacturing","Legal Services","Lessors of Nonfinancial Intangible Assets (except copyrighted works)","Machinery Manufacturing","Management of Companies (Holding Companies)","Medical and Diagnostic Laboratories","Merchant Wholesalers, Durable Goods","Merchant Wholesalers, Nondurable Goods","Mining","Miscellaneous Manufacturing","Miscellaneous Store Retailers","Motion Picture and Sound Recording Industries","Motor Vehicle and Parts Dealers","Museums, Historical Sites, and Similar Institutions","No Classification","Nondepository Credit Intermediation","Nonmetallic Mineral Product Manufacturing","Nonstore Retailers","Nursing and Residential Care Facilities","Offices of Other Health Practitioners","Offices of Physicians and Dentists","Other Ambulatory Health Care Services","Other Information Services","Other Professional, Scientific, and Technical Services","Outpatient Care Centers","Paper Manufacturing","Performing Arts, Spectator Sports, and Related Industries","Personal and Laundry Services","Petroleum and Coal Products Manufacturing","Pipeline Transportation","Plastics and Rubber Products Manufacturing","Primary Metal Manufacturing","Printing and Related Support Activities","Publishing Industries (except Internet)","Real Estate","Religious, Grantmaking, Civic, Professional, and Similar Organizations","Rental and Leasing Services","Repair and Maintenance","Scenic & Sightseeing Transportation","Securities, Commodity Contracts, and Other Financial Investments and Related Activities","Social Assistance","Specialized Design Services","Specialty Trade Contractors","Sporting Goods, Hobby, Book, and Music Stores","Support Activities for Agriculture and Forestry","Support Activities for Transportation","Telecommunications","Textile Mills and Textile Product Mills","Transit and Ground Passenger Transportation","Transportation Equipment Manufacturing","Truck Transportation","Utilities","Warehousing and Storage","Waste Management and Remediation Services","Wholesale Electronic Markets and Agents and Brokers","Wood Product Manufacturing"],"bq_industry_name":["Activities Related to Credit Intermediation (including loan brokers, check clearing, & money transmitting)","Advertising & Related Services","Aerospace Product & Parts Manufacturing","Agents & Managers for Artists, Athletes, Entertainers, & Other Public Figures","Agriculture, Construction, & Mining Machinery Manufacturing","Air Transportation","All Other Consumer Goods Rental","All Other Home Furnishings Stores","All Other Miscellaneous Store Retailers (including tobacco, candle, & trophy shops)","All Other Nondepository Credit Intermediation","All Other Outpatient Care Centers","All Other Personal Services","All Other Professional, Scientific, & Technical Services","All Other Specialty Food Stores","All Other Traveler Accommodation","Alumina & Aluminum Production & Processing","Amusement Parks & Arcades","Animal Food Manufacturing","Animal Slaughtering and Processing","Apparel Accessories & Other Apparel Manufacturing","Apparel Knitting Mills","Apparel, Piece Goods, & Notions","Aquaculture (including shellfish & finfish farms & hatcheries)","Architectural & Structural Metals Manufacturing","Architectural Services","Art Dealers","Asphalt Paving, Roofing, & Saturated Materials Manufacturing","Audio & Video Equipment Manufacturing","Automotive Body, Paint, Interior, & Glass Repair","Automotive Equipment Rental & Leasing","Automotive Mechanical & Electrical Repair & Maintenance","Automotive Parts, Accessories, & Tire Stores","Baked Goods Stores","Bakeries, Tortilla & Dry Pasta Manufacturing","Barber Shops","Basic Chemical Manufacturing","Beauty Salons","Bed & Breakfast Inns","Beef Cattle Ranching & Farming","Beer, Wine, & Distilled Alcoholic Beverages","Beer, Wine, & Liquor Stores","Boat Dealers","Boiler, Tank, & Shipping Container Manufacturing","Book Publishers","Book Stores","Books, Periodicals, & Newspapers","Breweries","Building Finishing Contractors (including drywall, insulation, painting, wallcovering, flooring, tile, & finish carpentry)","Building Inspection Services","Business Service Centers (including private mail centers & copy shops)","Business to Business Electronic Markets","Cable & Other Subscription Programming","Cafeterias and Buffets","Carpet & Upholstery Cleaning Services","Casino Hotels","Cattle Feedlots","Cement & Concrete Product Manufacturing","Cemeteries & Crematories","Charter Bus Industry","Chemical & Allied Products","Child Day Care Services","Children's & Infants' Clothing Stores","Clay Product & Refractory Manufacturing","Clothing Accessories Stores","Coal Mining","Coating, Engraving, Heat Treating, & Allied Activities","Coin-Operated Laundries & Drycleaners","Collection Agencies","Combination Gas & Electric","Commercial & Industrial Machinery & Equipment (except Automotive & Electronic) Repair & Maintenance","Commercial & Industrial Machinery & Equipment Rental & Leasing","Commercial & Service Industry Machinery Manufacturing","Commercial Banking","Commodity Contracts Brokerage","Commodity Contracts Dealing","Communications Equipment Manufacturing","Community Food & Housing, & Emergency & Other Relief Services","Computer & Peripheral Equipment Manufacturing","Computer Facilities Management Services","Computer Systems Design Services","Confectionery & Nut Stores","Consumer Electronics & Appliances Rental","Consumer Lending","Convenience Stores","Converted Paper Product Manufacturing","Cosmetics, Beauty Supplies, & Perfume Stores","Couriers","Credit Bureaus","Credit Card Issuing","Credit Unions","Crude Petroleum Extraction","Custom Computer Programming Services","Cut & Sew Apparel Contractors","Cutlery & Handtool Manufacturing","Dairy Cattle & Milk Production","Dairy Product Manufacturing","Data Processing, Hosting, & Related Services","Department Stores","Direct Insurance (except Life, Health & Medical) Carriers","Direct Life, Health, & Medical Insurance Carriers","Directory & Mailing List Publishers","Distilleries","Document Preparation Services","Drafting Services","Drinking Places (Alcoholic Beverages)","Drugs & Druggists' Sundries","Drycleaning & Laundry Services (except Coin-Operated)","Educational Services (including schools, colleges, & universities)","Electric Lighting Equipment Manufacturing","Electric Power Generation, Transmission & Distribution","Electrical Contractors","Electrical Equipment Manufacturing","Electronic & Precision Equipment Repair & Maintenance","Electronic Shopping & Mail-Order Houses","Electronics Stores (including Audio, Video, Computer, and Camera Stores)","Employment Services","Engine, Turbine & Power Transmission Equipment Manufacturing","Engineering Services","Exterminating & Pest Control Services","Facilities Support Services","Family Clothing Stores","Family Planning Centers","Farm Product Raw Materials","Farm Supplies","Fish & Seafood Markets","Fishing","Floor Covering Stores","Florists","Flower, Nursery Stock, & Florists' Supplies","Footwear & Leather Goods Repair","Footwear Manufacturing (including rubber & plastics)","Forest Nurseries & Gathering of Forest Products","Forging & Stamping","Formal Wear & Costume Rental","Foundation, Structure, & Building Exterior Contractors (including framing carpentry, masonry, glass, roofing, & siding)","Foundries","Freestanding Ambulatory Surgical & Emergency Centers","Freight Transportation Arrangement","Fruit & Tree Nut Farming","Fruit & Vegetable Markets","Fruit & Vegetable Preserving & Specialty Food Manufacturing","Fuel Dealers (including Heating Oil and Liquefied Petroleum)","Full-Service Restaurants","Funeral Homes & Funeral Services","Furniture & Home Furnishings","Furniture & Related Product Manufacturing","Furniture Stores","Gambling Industries","Gasoline Stations (including convenience stores with gas)","General Freight Trucking, Local","General Freight Trucking, Long-distance","General Merchandise Stores, incl. Warehouse Clubs & Supercenters","General Rental Centers","Geophysical Surveying & Mapping Services","Gift, Novelty, & Souvenir Stores","Glass & Glass Product Manufacturing","Governmental Instrumentality or Agency","Grain & Oilseed Milling","Greenhouse, Nursery, & Floriculture Production","Grocery & Related Products","HMO Medical Centers","Hardware Manufacturing","Hardware Stores","Hardware, Plumbing & Heating Equipment & Supplies","Highway, Street, & Bridge Construction","Hobby, Toy, & Game Stores","Hog & Pig Farming","Home & Garden Equipment & Appliance Repair & Maintenance","Home Centers","Home Health Care Services","Home Health Equipment Rental","Hospitals","Hotels (except Casino Hotels) & Motels","Household Appliance Manufacturing","Household Appliance Stores","Household Appliances and Electrical & Electronic Goods","Hunting & Trapping","Independent Artists, Writers, & Performers","Individual & Family Services","Industrial Machinery Manufacturing","Insurance & Employee Benefit Funds","Insurance Agencies & Brokerages","International Trade Financing","Interurban & Rural Bus Transportation","Investigation & Security Services","Investment Banking & Securities Dealing","Iron & Steel Mills & Ferroalloy Manufacturing","Janitorial Services","Jewelry Stores","Jewelry, Watches, Precious Stones, & Precious Metals","Kidney Dialysis Centers","Labor Unions and Similar Labor Organizations","Land Subdivision","Landscape Architecture Services","Landscaping Services","Lawn & Garden Equipment & Supplies Stores","Leather & Hide Tanning & Finishing","Lessors of Miniwarehouses & Self-Storage Units (including equity REITs)","Lessors of Nonfinancial Intangible Assets (except copyrighted works)","Lessors of Nonresidential Buildings (except Miniwarehouses) (including equity REITs)","Lessors of Other Real Estate Property (including equity REITs)","Lessors of Residential Buildings & Dwellings (including equity REITs)","Lime & Gypsum Product Manufacturing","Limited-Service Restaurants","Limousine Service","Linen & Uniform Supply","Local Messengers & Local Delivery","Logging","Luggage & Leather Goods Stores","Lumber & Other Construction Materials","Machine Shops; Turned Product; & Screw, Nut, & Bolt Manufacturing","Machinery, Equipment, & Supplies","Management, Scientific, & Technical Consulting Services","Manufactured (Mobile) Home Dealers","Manufacturing & Reproducing Magnetic & Optical Media","Marketing Research & Public Opinion Polling","Meat Markets","Medical & Diagnostic Laboratories","Medical Equipment & Supplies Manufacturing","Men's & Boys' Cut & Sew Apparel Manufacturing","Men's Clothing Stores","Metal Ore Mining","Metals & Minerals (except Petroleum)","Metalworking Machinery Manufacturing","Motion Picture & Video Industries (except video rental)","Motor Vehicle & Motor Vehicle Parts & Supplies","Motor Vehicle Body & Trailer Manufacturing","Motor Vehicle Manufacturing","Motor Vehicle Parts Manufacturing","Motor Vehicle Towing","Motorcycle, ATV, and All Other Motor Vehicle Dealers","Museums, Historical Sites, & Similar Institutions","Musical Instrument & Supplies Stores","Nail Salons","Natural Gas Distribution","Natural Gas Extraction","Navigational, Measuring, Electromedical, & Control Instruments Manufacturing","New Car Dealers","News Dealers & Newsstands","Newspaper Publishers","No Classification","Nonferrous Metal (except Aluminum) Production & Processing","Nonresidential Building Construction","Nursing & Residential Care Facilities","Office Administrative Services","Office Supplies & Stationery Stores","Offices of All Other Miscellaneous Health Practitioners","Offices of Bank Holding Companies","Offices of Certified Public Accountants","Offices of Chiropractors","Offices of Dentists","Offices of Lawyers","Offices of Mental Health Practitioners (except Physicians)","Offices of Optometrists","Offices of Other Holding Companies","Offices of Physical, Occupational & Speech Therapists, & Audiologists","Offices of Physicians (except mental health specialists)","Offices of Physicians, Mental Health Specialists","Offices of Podiatrists","Offices of Real Estate Agents & Brokers","Offices of Real Estate Appraisers","Oilseed & Grain Farming","Open-End Investment Funds (Form 1120-RIC)","Optical Goods Stores","Other Accounting Services","Other Activities Related to Real Estate","Other Ambulatory Health Care Services (including ambulance services & blood & organ banks)","Other Amusement & Recreation Industries (including golf courses, skiing facilities, marinas, fitness centers, & bowling centers)","Other Animal Production","Other Automotive Repair & Maintenance (including oil change & lubrication shops & car washes)","Other Building Equipment Contractors","Other Building Material Dealers","Other Business Support Services (including repossession services, court reporting, & stenotype services)","Other Chemical Product & Preparation Manufacturing","Other Clothing Stores","Other Computer Related Services","Other Crop Farming (including tobacco, cotton, sugarcane, hay, peanut, sugar beet, & all other crop farming)","Other Cut & Sew Apparel Manufacturing","Other Depository Credit Intermediation","Other Direct Selling Establishments (including door-to-door retailing, frozen food plan providers, party plan merchandisers, & coffee-break service providers)","Other Electrical Equipment & Component Manufacturing","Other Fabricated Metal Product Manufacturing","Other Financial Investment Activities (including portfolio management & investment advice)","Other Financial Vehicles (including mortgage REITs & closed-end investment funds)","Other Food Manufacturing (including coffee, tea, flavorings & seasonings)","Other General Purpose Machinery Manufacturing","Other Health & Personal Care Stores","Other Heavy & Civil Engineering Construction","Other Information Services (including news syndicates, libraries, internet publishing & broadcasting)","Other Insurance Related Activities (including third-party administration of insurance and pension funds)","Other Leather & Allied Product Manufacturing","Other Legal Services","Other Miscellaneous Durable Goods","Other Miscellaneous Manufacturing","Other Miscellaneous Nondurable Goods","Other Nonmetallic Mineral Mining & Quarrying","Other Nonmetallic Mineral Product Manufacturing","Other Personal & Household Goods Repair & Maintenance","Other Personal Care Services (including diet & weight reducing centers)","Other Petroleum & Coal Products Manufacturing","Other Publishers","Other Services to Buildings & Dwellings","Other Specialty Trade Contractors (including site preparation)","Other Support Activities for Road Transportation","Other Support Activities for Transportation","Other Support Services (including packaging & labeling services, & convention & trade show organizers)","Other Transit & Ground Passenger Transportation","Other Transportation Equipment Manufacturing","Other Wood Product Manufacturing","Outpatient Mental Health & Substance Abuse Centers","Paint & Wallpaper Stores","Paint, Coating, & Adhesive Manufacturing","Paint, Varnish, & Supplies","Paper & Paper Products","Parking Lots & Garages","Payroll Services","Performing Arts Companies","Periodical Publishers","Pesticide, Fertilizer, & Other Agricultural Chemical Manufacturing","Pet & Pet Supplies Stores","Pet Care (except Veterinary) Services","Petroleum & Petroleum Products","Petroleum Refineries (including integrated)","Pharmaceutical & Medicine Manufacturing","Pharmacies & Drug Stores","Photofinishing","Photographic Services","Pipeline Transportation","Plastics Product Manufacturing","Plumbing, Heating, & Air-Conditioning Contractors","Poultry & Egg Production","Printing & Related Support Activities","Professional & Commercial Equipment & Supplies","Promoters of Performing Arts, Sports, & Similar Events","Pulp, Paper, & Paperboard Mills","RV (Recreational Vehicle) Parks & Recreational Camps","Radio & Television Broadcasting","Rail Transportation","Railroad Rolling Stock Manufacturing","Real Estate Credit (including mortgage bankers & originators)","Real Estate Property Managers","Recreational Goods Rental","Recreational Vehicle Dealers","Recyclable Materials","Reinsurance Carriers","Religious, Grantmaking, Civic, Professional, & Similar Organizations (including condominium and homeowners associations)","Residential Building Construction","Resin, Synthetic Rubber, & Artificial & Synthetic Fibers & Filaments Manufacturing","Reupholstery & Furniture Repair","Rooming & Boarding Houses, Dormitories, and Workers' Camps","Rubber Product Manufacturing","Sales Financing","Sand, Gravel, Clay, & Ceramic & Refractory Minerals Mining & Quarrying","Savings Institutions","Sawmills & Wood Preservation","Scenic & Sightseeing Transportation","School & Employee Bus Transportation","Scientific Research & Development Services","Seafood Product Preparation & Packaging","Secondary Market Financing","Securities & Commodity Exchanges","Securities Brokerage","Semiconductor & Other Electronic Component Manufacturing","Sewing, Needlework, & Piece Goods Stores","Sheep & Goat Farming","Ship & Boat Building","Shoe Stores","Snack and Non-alcoholic Beverage Bars","Soap, Cleaning Compound, & Toilet Preparation Manufacturing","Soft Drink & Ice Manufacturing","Software Publishers","Sound Recording Industries","Special Food Services (including food service contractors & caterers)","Specialized Design Services (including interior, industrial, graphic, & fashion design)","Specialized Freight Trucking","Spectator Sports (including sports clubs & racetracks)","Sporting & Recreational Goods & Supplies","Sporting Goods Stores","Spring & Wire Product Manufacturing","Steel Product Manufacturing from Purchased Steel","Stone Mining & Quarrying","Sugar & Confectionery Product Manufacturing","Supermarkets and Other Grocery (except Convenience) Stores","Support Activities For Forestry","Support Activities for Air Transportation","Support Activities for Animal Production","Support Activities for Crop Production (including cotton ginning, soil preparation, planting, & cultivating)","Support Activities for Mining","Support Activities for Rail Transportation","Support Activities for Water Transportation","Surveying & Mapping (except Geophysical) Services","Tax Preparation Services","Taxi Service","Telecommunications (including paging, cellular, satellite, cable & other program distribution, resellers, other telecommunications, & internet service providers)","Telephone Call Centers","Testing Laboratories","Textile Mills","Textile Product Mills","Timber Tract Operations","Tobacco & Tobacco Products","Tobacco Manufacturing","Toy & Hobby Goods & Supplies","Translation & Interpretation Services","Travel Arrangement & Reservation Services","Trusts, Estates, & Agency Accounts","Urban Transit Systems","Used Car Dealers","Used Merchandise Stores","Utility System Construction","Vegetable & Melon Farming (including potatoes & yams)","Vending Machine Operators","Veneer, Plywood, & Engineered Wood Product Manufacturing","Ventilation, Heating, Air-Conditioning, & Commercial Refrigeration Equipment Manufacturing","Veterinary Services","Video Tape & Disc Rental","Vocational Rehabilitation Services","Warehousing & Storage (except lessors of miniwarehouses & self-storage units)","Waste Management & Remediation Services","Water Transportation","Water, Sewage, & Other Systems","Wholesale Trade Agents & Brokers","Window Treatment Stores","Wineries","Women's & Girls' and Infants' Cut & Sew Apparel Manufacturing","Women's Clothing Stores"],"bq_company_address1_cbsa_name":["Aberdeen, SD","Aberdeen, WA","Abilene, TX","Ada, OK","Adrian, MI","Aguadilla-Isabela, PR","Akron, OH","Alamogordo, NM","Albany, GA","Albany-Lebanon, OR","Albany-Schenectady-Troy, NY","Albemarle, NC","Albert Lea, MN","Albertville, AL","Albuquerque, NM","Alexander City, AL","Alexandria, LA","Alexandria, MN","Alice, TX","Allentown-Bethlehem-Easton, PA-NJ","Alma, MI","Alpena, MI","Altoona, PA","Altus, OK","Amarillo, TX","Americus, GA","Ames, IA","Amsterdam, NY","Anchorage, AK","Andrews, TX","Angola, IN","Ann Arbor, MI","Anniston-Oxford, AL","Appleton, WI","Arcadia, FL","Ardmore, OK","Arecibo, PR","Arkadelphia, AR","Asheville, NC","Ashland, OH","Ashtabula, OH","Astoria, OR","Atchison, KS","Athens, OH","Athens, TN","Athens, TX","Athens-Clarke County, GA","Atlanta-Sandy Springs-Alpharetta, GA","Atlantic City-Hammonton, NJ","Atmore, AL","Auburn, IN","Auburn, NY","Auburn-Opelika, AL","Augusta-Richmond County, GA-SC","Augusta-Waterville, ME","Austin, MN","Austin-Round Rock-Georgetown, TX","Bainbridge, GA","Bakersfield, CA","Baltimore-Columbia-Towson, MD","Bangor, ME","Baraboo, WI","Bardstown, KY","Barnstable Town, MA","Barre, VT","Bartlesville, OK","Batavia, NY","Batesville, AR","Baton Rouge, LA","Battle Creek, MI","Bay City, MI","Bay City, TX","Beatrice, NE","Beaumont-Port Arthur, TX","Beaver Dam, WI","Beckley, WV","Bedford, IN","Beeville, TX","Bellefontaine, OH","Bellingham, WA","Bemidji, MN","Bend, OR","Bennettsville, SC","Bennington, VT","Berlin, NH","Big Rapids, MI","Big Spring, TX","Big Stone Gap, VA","Billings, MT","Binghamton, NY","Birmingham-Hoover, AL","Bismarck, ND","Blackfoot, ID","Blacksburg-Christiansburg, VA","Bloomington, IL","Bloomington, IN","Bloomsburg-Berwick, PA","Bluefield, WV-VA","Bluffton, IN","Blytheville, AR","Bogalusa, LA","Boise City, ID","Bonham, TX","Boone, NC","Borger, TX","Boston-Cambridge-Newton, MA-NH","Boulder, CO","Bowling Green, KY","Bozeman, MT","Bradford, PA","Brainerd, MN","Branson, MO","Breckenridge, CO","Bremerton-Silverdale-Port Orchard, WA","Brenham, TX","Brevard, NC","Bridgeport-Stamford-Norwalk, CT","Brookhaven, MS","Brookings, OR","Brookings, SD","Brownsville, TN","Brownsville-Harlingen, TX","Brownwood, TX","Brunswick, GA","Bucyrus-Galion, OH","Buffalo-Cheektowaga, NY","Burley, ID","Burlington, IA-IL","Burlington, NC","Burlington-South Burlington, VT","Butte-Silver Bow, MT","Cadillac, MI","Calhoun, GA","California-Lexington Park, MD","Cambridge, MD","Cambridge, OH","Camden, AR","Campbellsville, KY","Canton-Massillon, OH","Cape Coral-Fort Myers, FL","Cape Girardeau, MO-IL","Carbondale-Marion, IL","Carlsbad-Artesia, NM","Carroll, IA","Carson City, NV","Casper, WY","Ca�on City, CO","Cedar City, UT","Cedar Rapids, IA","Cedartown, GA","Celina, OH","Central City, KY","Centralia, IL","Centralia, WA","Chambersburg-Waynesboro, PA","Champaign-Urbana, IL","Charleston, WV","Charleston-Mattoon, IL","Charleston-North Charleston, SC","Charlotte-Concord-Gastonia, NC-SC","Charlottesville, VA","Chattanooga, TN-GA","Cheyenne, WY","Chicago-Naperville-Elgin, IL-IN-WI","Chico, CA","Chillicothe, OH","Cincinnati, OH-KY-IN","Clarksburg, WV","Clarksdale, MS","Clarksville, TN-KY","Clearlake, CA","Cleveland, MS","Cleveland, TN","Cleveland-Elyria, OH","Clewiston, FL","Clinton, IA","Clovis, NM","Coamo, PR","Coeur dAlene, ID","Coffeyville, KS","Coldwater, MI","College Station-Bryan, TX","Colorado Springs, CO","Columbia, MO","Columbia, SC","Columbus, GA-AL","Columbus, IN","Columbus, MS","Columbus, NE","Columbus, OH","Concord, NH","Connersville, IN","Cookeville, TN","Coos Bay, OR","Cordele, GA","Corinth, MS","Cornelia, GA","Corning, NY","Corpus Christi, TX","Corsicana, TX","Cortland, NY","Corvallis, OR","Coshocton, OH","Craig, CO","Crawfordsville, IN","Crescent City, CA","Crestview-Fort Walton Beach-Destin, FL","Crossville, TN","Cullman, AL","Cullowhee, NC","Cumberland, MD-WV","Dallas-Fort Worth-Arlington, TX","Dalton, GA","Danville, IL","Danville, KY","Danville, VA","Daphne-Fairhope-Foley, AL","Davenport-Moline-Rock Island, IA-IL","Dayton, TN","Dayton-Kettering, OH","DeRidder, LA","Decatur, AL","Decatur, IL","Decatur, IN","Defiance, OH","Del Rio, TX","Deltona-Daytona Beach-Ormond Beach, FL","Deming, NM","Denver-Aurora-Lakewood, CO","Des Moines-West Des Moines, IA","Detroit-Warren-Dearborn, MI","Dickinson, ND","Dixon, IL","Dodge City, KS","Dothan, AL","Douglas, GA","Dover, DE","DuBois, PA","Dublin, GA","Dubuque, IA","Duluth, MN-WI","Dumas, TX","Duncan, OK","Durango, CO","Durant, OK","Durham-Chapel Hill, NC","Dyersburg, TN","Eagle Pass, TX","East Stroudsburg, PA","Easton, MD","Eau Claire, WI","Edwards, CO","Effingham, IL","El Campo, TX","El Centro, CA","El Dorado, AR","El Paso, TX","Elizabeth City, NC","Elizabethtown-Fort Knox, KY","Elk City, OK","Elkhart-Goshen, IN","Elkins, WV","Elko, NV","Ellensburg, WA","Elmira, NY","Emporia, KS","Enid, OK","Enterprise, AL","Erie, PA","Escanaba, MI","Espa�ola, NM","Eufaula, AL-GA","Eugene-Springfield, OR","Eureka-Arcata, CA","Evanston, WY","Evansville, IN-KY","Fairbanks, AK","Fairfield, IA","Fairmont, MN","Fairmont, WV","Fallon, NV","Fargo, ND-MN","Faribault-Northfield, MN","Farmington, MO","Farmington, NM","Fayetteville, NC","Fayetteville-Springdale-Rogers, AR","Fergus Falls, MN","Fernley, NV","Findlay, OH","Fitzgerald, GA","Flagstaff, AZ","Flint, MI","Florence, SC","Florence-Muscle Shoals, AL","Fond du Lac, WI","Forest City, NC","Forrest City, AR","Fort Collins, CO","Fort Dodge, IA","Fort Leonard Wood, MO","Fort Madison-Keokuk, IA-IL-MO","Fort Morgan, CO","Fort Payne, AL","Fort Polk South, LA","Fort Smith, AR-OK","Fort Wayne, IN","Frankfort, IN","Frankfort, KY","Fredericksburg, TX","Freeport, IL","Fremont, NE","Fremont, OH","Fresno, CA","Gadsden, AL","Gaffney, SC","Gainesville, FL","Gainesville, GA","Gainesville, TX","Galesburg, IL","Gallup, NM","Garden City, KS","Gardnerville Ranchos, NV","Georgetown, SC","Gettysburg, PA","Gillette, WY","Glasgow, KY","Glens Falls, NY","Glenwood Springs, CO","Gloversville, NY","Goldsboro, NC","Granbury, TX","Grand Forks, ND-MN","Grand Island, NE","Grand Junction, CO","Grand Rapids, MN","Grand Rapids-Kentwood, MI","Grants Pass, OR","Grants, NM","Great Bend, KS","Great Falls, MT","Greeley, CO","Green Bay, WI","Greeneville, TN","Greensboro-High Point, NC","Greensburg, IN","Greenville, MS","Greenville, NC","Greenville, OH","Greenville-Anderson, SC","Greenwood, MS","Greenwood, SC","Grenada, MS","Guayama, PR","Gulfport-Biloxi, MS","Guymon, OK","Hagerstown-Martinsburg, MD-WV","Hailey, ID","Hammond, LA","Hanford-Corcoran, CA","Hannibal, MO","Harrisburg-Carlisle, PA","Harrison, AR","Harrisonburg, VA","Hartford-East Hartford-Middletown, CT","Hastings, NE","Hattiesburg, MS","Hays, KS","Heber, UT","Helena, MT","Helena-West Helena, AR","Henderson, NC","Hereford, TX","Hermiston-Pendleton, OR","Hickory-Lenoir-Morganton, NC","Hillsdale, MI","Hilo, HI","Hilton Head Island-Bluffton, SC","Hinesville, GA","Hobbs, NM","Holland, MI","Homosassa Springs, FL","Hood River, OR","Hope, AR","Hot Springs, AR","Houghton, MI","Houma-Thibodaux, LA","Houston-The Woodlands-Sugar Land, TX","Hudson, NY","Huntingdon, PA","Huntington, IN","Huntington-Ashland, WV-KY-OH","Huntsville, AL","Huntsville, TX","Huron, SD","Hutchinson, KS","Hutchinson, MN","Idaho Falls, ID","Indiana, PA","Indianapolis-Carmel-Anderson, IN","Indianola, MS","Iowa City, IA","Iron Mountain, MI-WI","Ithaca, NY","Jackson, MI","Jackson, MS","Jackson, OH","Jackson, TN","Jackson, WY-ID","Jacksonville, FL","Jacksonville, IL","Jacksonville, NC","Jacksonville, TX","Jamestown, ND","Jamestown-Dunkirk-Fredonia, NY","Janesville-Beloit, WI","Jasper, AL","Jasper, IN","Jayuya, PR","Jefferson City, MO","Jefferson, GA","Jennings, LA","Jesup, GA","Johnson City, TN","Johnstown, PA","Jonesboro, AR","Joplin, MO","Juneau, AK","Kahului-Wailuku-Lahaina, HI","Kalamazoo-Portage, MI","Kalispell, MT","Kankakee, IL","Kansas City, MO-KS","Kapaa, HI","Kearney, NE","Keene, NH","Kendallville, IN","Kennett, MO","Kennewick-Richland, WA","Kerrville, TX","Ketchikan, AK","Key West, FL","Kill Devil Hills, NC","Killeen-Temple, TX","Kingsport-Bristol, TN-VA","Kingston, NY","Kingsville, TX","Kinston, NC","Kirksville, MO","Klamath Falls, OR","Knoxville, TN","Kokomo, IN","La Crosse-Onalaska, WI-MN","La Grande, OR","LaGrange, GA-AL","Laconia, NH","Lafayette, LA","Lafayette-West Lafayette, IN","Lake Charles, LA","Lake City, FL","Lake Havasu City-Kingman, AZ","Lakeland-Winter Haven, FL","Lamesa, TX","Lancaster, PA","Lansing-East Lansing, MI","Laramie, WY","Laredo, TX","Las Cruces, NM","Las Vegas, NM","Las Vegas-Henderson-Paradise, NV","Laurel, MS","Laurinburg, NC","Lawrence, KS","Lawrenceburg, TN","Lawton, OK","Lebanon, MO","Lebanon, NH-VT","Lebanon, PA","Levelland, TX","Lewisburg, PA","Lewisburg, TN","Lewiston, ID-WA","Lewiston-Auburn, ME","Lewistown, PA","Lexington, NE","Lexington-Fayette, KY","Liberal, KS","Lima, OH","Lincoln, IL","Lincoln, NE","Little Rock-North Little Rock-Conway, AR","Lock Haven, PA","Logan, UT-ID","Logansport, IN","London, KY","Longview, TX","Longview, WA","Los Alamos, NM","Los Angeles-Long Beach-Anaheim, CA","Louisville/Jefferson County, KY-IN","Lubbock, TX","Ludington, MI","Lufkin, TX","Lumberton, NC","Lynchburg, VA","Macomb, IL","Macon-Bibb County, GA","Madera, CA","Madison, IN","Madison, WI","Madisonville, KY","Magnolia, AR","Malone, NY","Malvern, AR","Manchester-Nashua, NH","Manhattan, KS","Manitowoc, WI","Mankato, MN","Mansfield, OH","Marietta, OH","Marinette, WI-MI","Marion, IN","Marion, NC","Marion, OH","Marquette, MI","Marshall, MN","Marshall, MO","Marshalltown, IA","Martin, TN","Martinsville, VA","Maryville, MO","Mason City, IA","Mayag�ez, PR","Mayfield, KY","Maysville, KY","McAlester, OK","McAllen-Edinburg-Mission, TX","McComb, MS","McMinnville, TN","McPherson, KS","Meadville, PA","Medford, OR","Memphis, TN-MS-AR","Menomonie, WI","Merced, CA","Meridian, MS","Mexico, MO","Miami, OK","Miami-Fort Lauderdale-Pompano Beach, FL","Michigan City-La Porte, IN","Middlesborough, KY","Midland, MI","Midland, TX","Milledgeville, GA","Milwaukee-Waukesha, WI","Minden, LA","Mineral Wells, TX","Minneapolis-St. Paul-Bloomington, MN-WI","Minot, ND","Missoula, MT","Mitchell, SD","Moberly, MO","Mobile, AL","Modesto, CA","Monroe, LA","Monroe, MI","Montgomery, AL","Montrose, CO","Morehead City, NC","Morgan City, LA","Morgantown, WV","Morristown, TN","Moscow, ID","Moses Lake, WA","Moultrie, GA","Mount Airy, NC","Mount Gay-Shamrock, WV","Mount Pleasant, MI","Mount Pleasant, TX","Mount Sterling, KY","Mount Vernon, IL","Mount Vernon, OH","Mount Vernon-Anacortes, WA","Mountain Home, AR","Mountain Home, ID","Muncie, IN","Murray, KY","Muscatine, IA","Muskegon, MI","Muskogee, OK","Myrtle Beach-Conway-North Myrtle Beach, SC-NC","Nacogdoches, TX","Napa, CA","Naples-Marco Island, FL","Nashville-Davidson--Murfreesboro--Franklin, TN","Natchez, MS-LA","Natchitoches, LA","New Bern, NC","New Castle, IN","New Castle, PA","New Haven-Milford, CT","New Orleans-Metairie, LA","New Philadelphia-Dover, OH","New Ulm, MN","New York-Newark-Jersey City, NY-NJ-PA","Newberry, SC","Newport, OR","Newport, TN","Niles, MI","Nogales, AZ","Norfolk, NE","North Platte, NE","North Port-Sarasota-Bradenton, FL","North Vernon, IN","North Wilkesboro, NC","Norwalk, OH","Norwich-New London, CT","Oak Harbor, WA","Ocala, FL","Ocean City, NJ","Odessa, TX","Ogden-Clearfield, UT","Ogdensburg-Massena, NY","Oil City, PA","Okeechobee, FL","Oklahoma City, OK","Olean, NY","Olympia-Lacey-Tumwater, WA","Omaha-Council Bluffs, NE-IA","Oneonta, NY","Ontario, OR-ID","Opelousas, LA","Orangeburg, SC","Orlando-Kissimmee-Sanford, FL","Oshkosh-Neenah, WI","Oskaloosa, IA","Othello, WA","Ottawa, IL","Ottawa, KS","Ottumwa, IA","Owatonna, MN","Owensboro, KY","Oxford, MS","Oxnard-Thousand Oaks-Ventura, CA","Ozark, AL","Paducah, KY-IL","Pahrump, NV","Palatka, FL","Palestine, TX","Palm Bay-Melbourne-Titusville, FL","Pampa, TX","Panama City, FL","Paragould, AR","Paris, TN","Paris, TX","Parkersburg-Vienna, WV","Parsons, KS","Payson, AZ","Pearsall, TX","Pecos, TX","Pella, IA","Pensacola-Ferry Pass-Brent, FL","Peoria, IL","Peru, IN","Philadelphia-Camden-Wilmington, PA-NJ-DE-MD","Phoenix-Mesa-Chandler, AZ","Picayune, MS","Pierre, SD","Pine Bluff, AR","Pinehurst-Southern Pines, NC","Pittsburg, KS","Pittsburgh, PA","Pittsfield, MA","Plainview, TX","Platteville, WI","Plattsburgh, NY","Plymouth, IN","Pocatello, ID","Point Pleasant, WV-OH","Ponca City, OK","Ponce, PR","Pontiac, IL","Poplar Bluff, MO","Port Angeles, WA","Port Lavaca, TX","Port St. Lucie, FL","Portales, NM","Portland-South Portland, ME","Portland-Vancouver-Hillsboro, OR-WA","Portsmouth, OH","Pottsville, PA","Poughkeepsie-Newburgh-Middletown, NY","Prescott Valley-Prescott, AZ","Price, UT","Prineville, OR","Providence-Warwick, RI-MA","Provo-Orem, UT","Pueblo, CO","Pullman, WA","Punta Gorda, FL","Quincy, IL-MO","Racine, WI","Raleigh-Cary, NC","Rapid City, SD","Raymondville, TX","Reading, PA","Red Bluff, CA","Red Wing, MN","Redding, CA","Reno, NV","Rexburg, ID","Richmond, IN","Richmond, VA","Richmond-Berea, KY","Rio Grande City-Roma, TX","Riverside-San Bernardino-Ontario, CA","Riverton, WY","Roanoke Rapids, NC","Roanoke, VA","Rochelle, IL","Rochester, MN","Rochester, NY","Rock Springs, WY","Rockford, IL","Rockingham, NC","Rockport, TX","Rocky Mount, NC","Rolla, MO","Rome, GA","Roseburg, OR","Roswell, NM","Ruidoso, NM","Russellville, AR","Ruston, LA","Rutland, VT","Sacramento-Roseville-Folsom, CA","Safford, AZ","Saginaw, MI","Salem, OH","Salem, OR","Salina, KS","Salinas, CA","Salisbury, MD-DE","Salt Lake City, UT","San Angelo, TX","San Antonio-New Braunfels, TX","San Diego-Chula Vista-Carlsbad, CA","San Francisco-Oakland-Berkeley, CA","San Germ�n, PR","San Jose-Sunnyvale-Santa Clara, CA","San Juan-Bayam�n-Caguas, PR","San Luis Obispo-Paso Robles, CA","Sandpoint, ID","Sandusky, OH","Sanford, NC","Santa Cruz-Watsonville, CA","Santa Fe, NM","Santa Isabel, PR","Santa Maria-Santa Barbara, CA","Santa Rosa-Petaluma, CA","Sault Ste. Marie, MI","Savannah, GA","Sayre, PA","Scottsbluff, NE","Scottsboro, AL","Scottsburg, IN","Scranton--Wilkes-Barre, PA","Searcy, AR","Seattle-Tacoma-Bellevue, WA","Sebastian-Vero Beach, FL","Sebring-Avon Park, FL","Sedalia, MO","Selinsgrove, PA","Selma, AL","Seneca Falls, NY","Seneca, SC","Sevierville, TN","Seymour, IN","Shawano, WI","Shawnee, OK","Sheboygan, WI","Shelby, NC","Shelbyville, TN","Shelton, WA","Sheridan, WY","Sherman-Denison, TX","Show Low, AZ","Shreveport-Bossier City, LA","Sidney, OH","Sierra Vista-Douglas, AZ","Sikeston, MO","Silver City, NM","Sioux City, IA-NE-SD","Sioux Falls, SD","Snyder, TX","Somerset, KY","Somerset, PA","Sonora, CA","South Bend-Mishawaka, IN-MI","Spartanburg, SC","Spearfish, SD","Spencer, IA","Spirit Lake, IA","Spokane-Spokane Valley, WA","Springfield, IL","Springfield, MA","Springfield, MO","Springfield, OH","St. Cloud, MN","St. George, UT","St. Joseph, MO-KS","St. Louis, MO-IL","St. Marys, GA","St. Marys, PA","Starkville, MS","State College, PA","Statesboro, GA","Staunton, VA","Steamboat Springs, CO","Stephenville, TX","Sterling, CO","Sterling, IL","Stevens Point, WI","Stillwater, OK","Stockton, CA","Storm Lake, IA","Sturgis, MI","Sulphur Springs, TX","Summerville, GA","Sumter, SC","Sunbury, PA","Susanville, CA","Sweetwater, TX","Syracuse, NY","Tahlequah, OK","Talladega-Sylacauga, AL","Tallahassee, FL","Tampa-St. Petersburg-Clearwater, FL","Taos, NM","Taylorville, IL","Terre Haute, IN","Texarkana, TX-AR","The Dalles, OR","The Villages, FL","Thomaston, GA","Thomasville, GA","Tiffin, OH","Tifton, GA","Toccoa, GA","Toledo, OH","Topeka, KS","Torrington, CT","Traverse City, MI","Trenton-Princeton, NJ","Troy, AL","Truckee-Grass Valley, CA","Tucson, AZ","Tullahoma-Manchester, TN","Tulsa, OK","Tupelo, MS","Tuscaloosa, AL","Twin Falls, ID","Tyler, TX","Ukiah, CA","Union City, TN","Union, SC","Urban Honolulu, HI","Urbana, OH","Utica-Rome, NY","Uvalde, TX","Valdosta, GA","Vallejo, CA","Van Wert, OH","Vermillion, SD","Vernal, UT","Vernon, TX","Vicksburg, MS","Victoria, TX","Vidalia, GA","Vincennes, IN","Vineland-Bridgeton, NJ","Vineyard Haven, MA","Virginia Beach-Norfolk-Newport News, VA-NC","Visalia, CA","Wabash, IN","Waco, TX","Wahpeton, ND-MN","Walla Walla, WA","Wapakoneta, OH","Warner Robins, GA","Warren, PA","Warrensburg, MO","Warsaw, IN","Washington Court House, OH","Washington, IN","Washington, NC","Washington-Arlington-Alexandria, DC-VA-MD-WV","Waterloo-Cedar Falls, IA","Watertown, SD","Watertown-Fort Atkinson, WI","Watertown-Fort Drum, NY","Wauchula, FL","Wausau-Weston, WI","Waycross, GA","Weatherford, OK","Weirton-Steubenville, WV-OH","Wenatchee, WA","West Plains, MO","West Point, MS","Wheeling, WV-OH","Whitewater, WI","Wichita Falls, TX","Wichita, KS","Williamsport, PA","Williston, ND","Willmar, MN","Wilmington, NC","Wilmington, OH","Wilson, NC","Winchester, VA-WV","Winfield, KS","Winnemucca, NV","Winona, MN","Winston-Salem, NC","Wisconsin Rapids-Marshfield, WI","Woodward, OK","Wooster, OH","Worcester, MA-CT","Worthington, MN","Yakima, WA","Yankton, SD","Yauco, PR","York-Hanover, PA","Youngstown-Warren-Boardman, OH-PA","Yuba City, CA","Yuma, AZ","Zanesville, OH","Zapata, TX"],"bq_cong_district_name":["Alabama 1st","Alabama 2nd","Alabama 3rd","Alabama 4th","Alabama 5th","Alabama 6th","Alabama 7th","Alaska At Large","American Samoa Delegate","Arizona 1st","Arizona 2nd","Arizona 3rd","Arizona 4th","Arizona 5th","Arizona 6th","Arizona 7th","Arizona 8th","Arizona 9th","Arkansas 1st","Arkansas 2nd","Arkansas 3rd","Arkansas 4th","California 10th","California 11th","California 12th","California 13th","California 14th","California 15th","California 16th","California 17th","California 18th","California 19th","California 1st","California 20th","California 21st","California 22nd","California 23rd","California 24th","California 25th","California 26th","California 27th","California 28th","California 29th","California 2nd","California 30th","California 31st","California 32nd","California 33rd","California 34th","California 35th","California 36th","California 37th","California 38th","California 39th","California 3rd","California 40th","California 41st","California 42nd","California 43rd","California 44th","California 45th","California 46th","California 47th","California 48th","California 49th","California 4th","California 50th","California 51st","California 52nd","California 5th","California 6th","California 7th","California 8th","California 9th","Colorado 1st","Colorado 2nd","Colorado 3rd","Colorado 4th","Colorado 5th","Colorado 6th","Colorado 7th","Colorado 8th","Connecticut 1st","Connecticut 2nd","Connecticut 3rd","Connecticut 4th","Connecticut 5th","Delaware At Large","District of Columbia Delegate","Florida 10th","Florida 11th","Florida 12th","Florida 13th","Florida 14th","Florida 15th","Florida 16th","Florida 17th","Florida 18th","Florida 19th","Florida 1st","Florida 20th","Florida 21st","Florida 22nd","Florida 23rd","Florida 24th","Florida 25th","Florida 26th","Florida 27th","Florida 28th","Florida 2nd","Florida 3rd","Florida 4th","Florida 5th","Florida 6th","Florida 7th","Florida 8th","Florida 9th","Georgia 10th","Georgia 11th","Georgia 12th","Georgia 13th","Georgia 14th","Georgia 1st","Georgia 2nd","Georgia 3rd","Georgia 4th","Georgia 5th","Georgia 6th","Georgia 7th","Georgia 8th","Georgia 9th","Guam Delegate","Hawaii 1st","Hawaii 2nd","Idaho 1st","Idaho 2nd","Illinois 10th","Illinois 11th","Illinois 12th","Illinois 13th","Illinois 14th","Illinois 15th","Illinois 16th","Illinois 17th","Illinois 1st","Illinois 2nd","Illinois 3rd","Illinois 4th","Illinois 5th","Illinois 6th","Illinois 7th","Illinois 8th","Illinois 9th","Indiana 1st","Indiana 2nd","Indiana 3rd","Indiana 4th","Indiana 5th","Indiana 6th","Indiana 7th","Indiana 8th","Indiana 9th","Iowa 1st","Iowa 2nd","Iowa 3rd","Iowa 4th","Kansas 1st","Kansas 2nd","Kansas 3rd","Kansas 4th","Kentucky 1st","Kentucky 2nd","Kentucky 3rd","Kentucky 4th","Kentucky 5th","Kentucky 6th","Louisiana 1st","Louisiana 2nd","Louisiana 3rd","Louisiana 4th","Louisiana 5th","Louisiana 6th","Maine 1st","Maine 2nd","Maryland 1st","Maryland 2nd","Maryland 3rd","Maryland 4th","Maryland 5th","Maryland 6th","Maryland 7th","Maryland 8th","Massachusetts 1st","Massachusetts 2nd","Massachusetts 3rd","Massachusetts 4th","Massachusetts 5th","Massachusetts 6th","Massachusetts 7th","Massachusetts 8th","Massachusetts 9th","Michigan 10th","Michigan 11th","Michigan 12th","Michigan 13th","Michigan 1st","Michigan 2nd","Michigan 3rd","Michigan 4th","Michigan 5th","Michigan 6th","Michigan 7th","Michigan 8th","Michigan 9th","Minnesota 1st","Minnesota 2nd","Minnesota 3rd","Minnesota 4th","Minnesota 5th","Minnesota 6th","Minnesota 7th","Minnesota 8th","Mississippi 1st","Mississippi 2nd","Mississippi 3rd","Mississippi 4th","Missouri 1st","Missouri 2nd","Missouri 3rd","Missouri 4th","Missouri 5th","Missouri 6th","Missouri 7th","Missouri 8th","Montana 1st","Montana 2nd","Nebraska 1st","Nebraska 2nd","Nebraska 3rd","Nevada 1st","Nevada 2nd","Nevada 3rd","Nevada 4th","New Hampshire 1st","New Hampshire 2nd","New Jersey 10th","New Jersey 11th","New Jersey 12th","New Jersey 1st","New Jersey 2nd","New Jersey 3rd","New Jersey 4th","New Jersey 5th","New Jersey 6th","New Jersey 7th","New Jersey 8th","New Jersey 9th","New Mexico 1st","New Mexico 2nd","New Mexico 3rd","New York 10th","New York 11th","New York 12th","New York 13th","New York 14th","New York 15th","New York 16th","New York 17th","New York 18th","New York 19th","New York 1st","New York 20th","New York 21st","New York 22nd","New York 23rd","New York 24th","New York 25th","New York 26th","New York 2nd","New York 3rd","New York 4th","New York 5th","New York 6th","New York 7th","New York 8th","New York 9th","North Carolina 10th","North Carolina 11th","North Carolina 12th","North Carolina 13th","North Carolina 14th","North Carolina 1st","North Carolina 2nd","North Carolina 3rd","North Carolina 4th","North Carolina 5th","North Carolina 6th","North Carolina 7th","North Carolina 8th","North Carolina 9th","North Dakota At Large","Northern Mariana Islands Delegate","Ohio 10th","Ohio 11th","Ohio 12th","Ohio 13th","Ohio 14th","Ohio 15th","Ohio 1st","Ohio 2nd","Ohio 3rd","Ohio 4th","Ohio 5th","Ohio 6th","Ohio 7th","Ohio 8th","Ohio 9th","Oklahoma 1st","Oklahoma 2nd","Oklahoma 3rd","Oklahoma 4th","Oklahoma 5th","Oregon 1st","Oregon 2nd","Oregon 3rd","Oregon 4th","Oregon 5th","Oregon 6th","Pennsylvania 10th","Pennsylvania 11th","Pennsylvania 12th","Pennsylvania 13th","Pennsylvania 14th","Pennsylvania 15th","Pennsylvania 16th","Pennsylvania 17th","Pennsylvania 1st","Pennsylvania 2nd","Pennsylvania 3rd","Pennsylvania 4th","Pennsylvania 5th","Pennsylvania 6th","Pennsylvania 7th","Pennsylvania 8th","Pennsylvania 9th","Puerto Rico Resident Commissioner","Rhode Island 1st","Rhode Island 2nd","South Carolina 1st","South Carolina 2nd","South Carolina 3rd","South Carolina 4th","South Carolina 5th","South Carolina 6th","South Carolina 7th","South Dakota At Large","Tennessee 1st","Tennessee 2nd","Tennessee 3rd","Tennessee 4th","Tennessee 5th","Tennessee 6th","Tennessee 7th","Tennessee 8th","Tennessee 9th","Texas 10th","Texas 11th","Texas 12th","Texas 13th","Texas 14th","Texas 15th","Texas 16th","Texas 17th","Texas 18th","Texas 19th","Texas 1st","Texas 20th","Texas 21st","Texas 22nd","Texas 23rd","Texas 24th","Texas 25th","Texas 26th","Texas 27th","Texas 28th","Texas 29th","Texas 2nd","Texas 30th","Texas 31st","Texas 32nd","Texas 33rd","Texas 34th","Texas 35th","Texas 36th","Texas 37th","Texas 38th","Texas 3rd","Texas 4th","Texas 5th","Texas 6th","Texas 7th","Texas 8th","Texas 9th","Utah 1st","Utah 2nd","Utah 3rd","Utah 4th","Vermont At Large","Virgin Islands Delegate","Virginia 10th","Virginia 11th","Virginia 1st","Virginia 2nd","Virginia 3rd","Virginia 4th","Virginia 5th","Virginia 6th","Virginia 7th","Virginia 8th","Virginia 9th","Washington 10th","Washington 1st","Washington 2nd","Washington 3rd","Washington 4th","Washington 5th","Washington 6th","Washington 7th","Washington 8th","Washington 9th","West Virginia 1st","West Virginia 2nd","Wisconsin 1st","Wisconsin 2nd","Wisconsin 3rd","Wisconsin 4th","Wisconsin 5th","Wisconsin 6th","Wisconsin 7th","Wisconsin 8th","Wyoming At Large"]}
    response = {"response":response,"status":200}
    return response

def locations_connected_with_terminal(data):
    # VESPA_ENDPOINT = "http://52.39.53.239:8080"
    search_endpoint = f"{VESPA_ENDPOINT}/search/"

    yql = f"SELECT * FROM bq_location WHERE bq_organization_id = {data['bq_id']};"
    params = {
        'yql': yql,
        'ranking': 'bm25',
        'type': 'all',
        'hits': 1000,
        'offset': 0,
        'limit': 100,
        "format": "json"
    }
    response = requests.get(search_endpoint, params=params).json()
    # response['yql']=yql
    response = {"response":response,"status":200}
    return response

def benchmark_national(data):
    # data = await request.json()
    bq_subsector_name = f"= '{data.get('bq_subsector_name', 'is NULL')}'"
    head_count_range = f"= '{data.get('head_count_range', 'is NULL')}'"
    revenue_range = f"= '{data.get('revenue_range', 'is NULL')}'"
    
    conn = psycopg2.connect(
        dbname='dev',
        host='bq-redshift-prod-a741eb4ce6f34bca.elb.us-west-2.amazonaws.com',
        port='5439',
        user='abhijeet_pawar',
        password='2^;Ben(5z&X7;V'
    )
    print(bq_subsector_name,"--------------------------")
    cursor = conn.cursor()
    print("connection done")
    query = f"select * from reporting.bq_counts_by_subsector where bq_subsector_name {bq_subsector_name} and head_count_range {head_count_range} and revenue_range {revenue_range};"
    print(query)
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    
    row = cursor.fetchone()
    row = list(map(lambda x: float(x) if isinstance(x, Decimal) else x, row))
    conn.commit()
    cursor.close()
    conn.close()
    print("conn closed -----------------------------------------")
    if row:
        result = {column_names[i]: row[i] for i in range(len(column_names))}
        
    else:
        
        return []
        response = {"response":[], "status":200}
        return response
    response = {"response":[result], "status":200}
    return response

def officer_details(data):
    # VESPA_ENDPOINT = "http://52.39.53.239:8080"
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    yql = f"SELECT bq_officer_full_name, bq_officer_position FROM bq_officers WHERE bq_organization_id = {data['bq_id']};"
    params = {
        'yql': yql,
        'ranking': 'bm25',
        'type': 'all',
        'hits': 1000,
        'offset': 0,
        'limit': 100,
        "format": "json"
    }
    response = requests.get(search_endpoint, params=params).json()
    # response['yql']=yql
    try:
        if "fields" not in str(response['root']['children'][0]):
            response = []
    except:
        pass
    response = {"response":response,"status":200}
    return response

# def save_search(data):
#     try:
#         url = "http://54.190.237.221:5000/api/save_search/"
#         payload = json.dumps({
#         "portal": data['portal'],
#         "user_email": data['user_email'],
#         "portfolio_name": data['search_text'],
#         "data": data['category']
#         })
#         headers = {
#         'Content-Type': 'application/json',
#         'api-key': api_key
#         }
#         response = requests.request("POST", url, headers=headers, data=payload).json()
#         # print(response)
#         response = {"response":response,"status":200}
        
#         return response
#     except:
#         response = {"response":"Source API Down", 'status':500}
#         return response

import utils

def search(data):
    try:
        data['request_origin']="search"
        if data.get('field') in [None,""]:
            data['field']="universal_search"
        api_map = {
            "bq_organization_name":utils.company_name,
            "bq_organization_ein":utils.search,
            "bq_organization_website":utils.search,
            "universal_search": utils.search,
            "bq_organization_address1_line_1":utils.search_by_address,
            "bq_organization_ticker":utils.search_ticker_matches
        }
        
        if api_map.get(data['field'],None) != None:
            
            response = api_map[data['field']]
            if data['field']=="universal_search":
                data['field']=None
            if data['field']=="bq_organization_address1_line_1":
                # ult_selection
                response = response(data['query'], data['yql'], data['type'], data['filter'], data['ranking'], data['hits'], data['limit'], data['offset'], data['orderby'], data['isAsc'], data['field'], data['user_id'], data['request_origin'], data['ult_selection'])
            elif data['field']=="bq_organization_ticker":
                response = response(data['query'], data['yql'], data['type'], data['filter'], data['ranking'], data['hits'], data['limit'], data['offset'], data['orderby'], True, data['field'], data['user_id'], data['request_origin'])
            else:
                response = response(data['query'], data['yql'], data['type'], data['filter'], data['ranking'], data['hits'], data['limit'], data['offset'], data['orderby'], data['isAsc'], data['field'], data['user_id'], data['request_origin'])
            
            return response
    except Exception as e:
        return {"response":str(e), "status":400}

def geosearch(data):

    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    print(444444444444444444444,data)
    # data = await request.json()
    bq_id = data.get('bq_id', None)
    legal_entity_id = data.get('bq_legal_entity_id', None)
    radius = data.get('radius', '5 miles')
    limit = data.get('limit', 100)
    offset = data.get('offset', 0)
    filter = data.get('filter', None)
   
    # return bq_id
    if bq_id == None :
        response = {"response":'Please provide bq_id', "status":200}
        return response
        
    get_document_endpoint = f"{VESPA_ENDPOINT}/document/v1/bq_cluster/terminal_hybrid/docid/{bq_id}_{legal_entity_id}"
    response = requests.get(get_document_endpoint)
    organization = response.json()
    organization.pop('pathId')
    # print(888888,organization['fields'])
    # lat = organization['fields']['bq_organization_address1_location']['lat']
    # lng = organization['fields']['bq_organization_address1_location']['lng']
    lat = organization['fields']['bq_organization_address1_latitude']
    lng = organization['fields']['bq_organization_address1_longitude']
    try:
        irs = organization['fields']['bq_organization_subsector_name']
    except KeyError:
        irs = None 
    yql = f"select * from terminal_hybrid where geoLocation(bq_organization_address1_location, {lat}, {lng}, '{radius}') and bq_organization_subsector_name contains '{irs}'"
    if irs is None:
        irs = organization['fields']['bq_organization_irs_sector_name']
        yql = f"select * from terminal_hybrid where geoLocation(bq_organization_address1_location, {lat}, {lng}, '{radius}') and bq_organization_irs_sector_name contains '{irs}'"

    if filter:
        yql = f"{yql} AND"
        for key, val in filter.items():
            if len(val)>=1:
                if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_current_employees_plan_mr','bq_organization_valuation'):
                    final = ''
                    for items in val:
                        itm = ''
                        for i in items:
                            itm = f"{itm} {key} {i} AND"
                            # print(444444444444,itm)
                        itm = remove_and_from_end(itm)
                        itm = f'({itm})'
                        final = f"{final} {itm} OR"
                    yql = f"{yql} ({remove_and_from_end(final)}) AND"
                    # print(44444444, yql)
                else:
                    if len(val) > 1:
                        yql_part = ''
                        for v in val:
                            yql_part = yql_part + f"{key} contains '{v}' OR "
                        yql_part = remove_and_from_end(yql_part)
                        yql_part = f"({yql_part})"
                        yql = f"{yql} {yql_part} and"
                    elif len(val)==0:
                        pass
                    else:
                        if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                            yql = remove_and_from_end(yql)
                            yql = f"{yql} AND {key} contains '{val[0]}' AND"
                        elif yql.lower().rstrip().endswith('where'):
                            yql = f"{yql} {key} contains '{val[0]}' AND"
                        else:
                            yql = f"{yql} AND {key} contains '{val[0]}' AND"
    yql = f"{remove_and_from_end(yql)} order by bq_revenue_mr desc"
    # yql = f"{yql} order by bq_revenue_mr desc"
    # print(333333333333,yql)
    # print(offset,limit)
    params = {
        'yql': yql,
        'ranking': 'bm25',
        'type': 'all',
        'hits': 100,
        'offset': offset,
        'limit': limit,
        "format": "json"
    }
    response = requests.get(search_endpoint, params=params)
    response = response.json()
    response = [organization,response]
    response = {"response":response, "status":200}
    return response

def benchmark_local(data):
    # print(44444444444444444444444444444444444444444444444444444444444444444444444)
    print(444444444444444444444,data)
    
    bq_id = data.get('bq_id', None)
    legal_entity_id = data.get('bq_legal_entity_id', '')
    radius = data.get('radius', '5 miles')
    local = bool(data.get('local', False))
    search_endpoint = f"{VESPA_ENDPOINT}/search/"
    # print(444444444444444444444,bq_id)
    if bq_id == None:
        response = {"response":'Please provide bq_id', "status":200}
        return response
    
    get_document_endpoint = f"{VESPA_ENDPOINT}/document/v1/bq_cluster/terminal_hybrid/docid/{bq_id}_{legal_entity_id}"
    response = requests.get(get_document_endpoint)
    organization = response.json()
    organization.pop('pathId')
    lat = organization['fields']['bq_organization_address1_latitude']
    lng = organization['fields']['bq_organization_address1_longitude']
    
    try:
        revenue = organization['fields']['bq_revenue_mr']
        headcount = organization['fields']['bq_current_employees_plan_mr']
    except KeyError:
        # raise HTTPException(status_code=200, detail="Revenue Data not present for this organization")
        response = {"response":{},"status":200}
        return response
        # raise HTTPException(status_code=200, detail=[])
    try:
        revenue_range = get_range_revenue(revenue)
        headcount_range = get_range_headcount(headcount)
        print(headcount_range)
        if revenue_range[1]!=float('inf'):
            rev_yql = f"(bq_revenue_mr > {revenue_range[0]} AND bq_revenue_mr < {revenue_range[1]})"
        else:
            rev_yql = f"bq_revenue_mr > {revenue_range[0]}"
        
        if headcount_range[1]!=float('inf'):
            hc_yql = f"(bq_current_employees_plan_mr > {headcount_range[0]} AND bq_current_employees_plan_mr < {headcount_range[1]})"
        else:
            hc_yql = f"bq_current_employees_plan_mr > {headcount_range[0]}"
            
    except ValueError:
        print("Invalid input. Please enter a valid numeric value in revenue range.")
    try:
        irs = organization['fields']['bq_organization_subsector_name']
    except KeyError:
        irs = None
    print('/n/n/n',5454545454, local)
    if local == True: 
        yql = f"select bq_organization_isactive, bq_revenue_mr, bq_net_income_mr, bq_gross_profit_mr, bq_revenue_mr_per_emp, bq_revenue_growth_yoy_mr, bq_current_employees_plan_growth_yoy_mr, bq_current_employees_plan_mr, bq_organization_valuation from terminal_hybrid where geoLocation(bq_organization_address1_location, {lat}, {lng}, '{radius}') and bq_organization_subsector_name contains '{irs}' AND {rev_yql} order by bq_revenue_mr desc limit 5000"
    else:
        yql = f"select bq_organization_isactive, bq_revenue_mr, bq_net_income_mr, bq_gross_profit_mr, bq_revenue_mr_per_emp, bq_revenue_growth_yoy_mr, bq_current_employees_plan_growth_yoy_mr, bq_current_employees_plan_mr, bq_organization_valuation from terminal_hybrid where bq_organization_subsector_name contains '{irs}' AND {rev_yql} AND {hc_yql} order by bq_revenue_mr desc limit 5000"
    if irs is None:
        irs = organization['fields']['bq_organization_irs_sector_name']
        if local == True:
            yql = f"select bq_organization_isactive, bq_revenue_mr, bq_net_income_mr, bq_gross_profit_mr, bq_revenue_mr_per_emp, bq_revenue_growth_yoy_mr, bq_current_employees_plan_growth_yoy_mr, bq_current_employees_plan_mr, bq_organization_valuation from terminal_hybrid where geoLocation(bq_organization_address1_location, {lat}, {lng}, '{radius}') and bq_organization_irs_sector_name contains '{irs}' AND {rev_yql} order by bq_revenue_mr desc limit 5000"
        else:
            yql = f"select bq_organization_isactive, bq_revenue_mr, bq_net_income_mr, bq_gross_profit_mr, bq_revenue_mr_per_emp, bq_revenue_growth_yoy_mr, bq_current_employees_plan_growth_yoy_mr, bq_current_employees_plan_mr, bq_organization_valuation from terminal_hybrid where bq_organization_irs_sector_name contains '{irs}' AND {rev_yql} AND {hc_yql} order by bq_revenue_mr desc limit 5000"
    print(yql)
    params = {
        'yql': yql,
        'ranking': 'bm25',
        'type': 'all',
        'hits': 5000,
        "format": "json"
    }
    response = requests.get(search_endpoint, params=params)
    response = response.json()
    try:
        mapping = {'bq_net_income_mr':'bq_organization_net_income_mr',
        'bq_gross_profit_mr':'bq_organization_gross_profit_mr',
        'bq_revenue_mr_per_emp':'bq_organization_revenue_mr_per_emp',
        'bq_revenue_growth_yoy_mr':'bq_organization_revenue_growth_yoy_mr',
        'bq_current_employees_plan_growth_yoy_mr':'bq_current_employees_plan_growth_yoy_mr',
        'bq_current_employees_plan_mr':'bq_current_employees_plan_mr',}
        
        for dictionary in response['root']['children']:
            for key in list(dictionary['fields'].keys()):
                if key in mapping:
                    dictionary['fields'][mapping[key]] = dictionary['fields'].pop(key)
    except Exception as e:
        print(str(e))
    response = {"response":response, "status":200}
    return response

def claim_ownership_management(data):
    try:
        api_map={
            "claim_ownership":central_server_base_url + "claim_business/",
            "unclaim_ownership":central_server_base_url + "unclaim_business/",
            "get_claims":central_server_base_url + "get_claimed_business/",
        }
        payload = json.dumps({
        "portal": data['portal'],
        "user_email": data['email'],
        "data": data.get("bq_id",None)
        })
        headers = {
        'Content-Type': 'application/json',
        'api-key': api_key
        }
        response = requests.request("POST", api_map[data['matrix']], headers=headers, data=payload).json()
        response = {"response":response,"status":200}
        
        return response
    except Exception as e:
        response = {"response":{"message":"Source API Down","error":str(e)}, 'status':400}
        # response = {"response":str(e), 'status':500}
        return response

def save_search_management(data):
    try:
        api_map ={
            "save_search":central_server_base_url + "save_search/",
            "retrieve_search":central_server_base_url + "retrieve_search/",
            "delete_search":central_server_base_url + "delete_search/",
        }
        payload = json.dumps({
        "portal": data['portal'],
        "user_email": data['user_email'],
        "search_text": data.get('search_text',None),
        "category":data.get("category",None),
        "num_days":30,
        "page_size":data.get("limit",None),
        "page_number":data.get("offset",None),
        "name":data.get("name",None),
        "overwrite":data.get("overwrite",False)
        })
        headers = {
        'Content-Type': 'application/json',
        'api-key': api_key
        }
        response = requests.request("POST", api_map[data['matrix']], headers=headers, data=payload).json()
        # print(response)
        response = {"response":response,"status":200}
        
        return response
    except:
        response = {"response":"Source API Down", 'status':500}
        return response

def export_save_search(data):
    try:
        query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id = data["query"], data["yql"], data["type"], data["filter"], data["ranking"], data["hits"], data["limit"], data["offset"], data["orderby"], data["isAsc"], data["field"], data["user_id"]
        # search_count = set_counter(user_id)
        search_count = 10
        if search_count < 30:
            search_endpoint = f"{VESPA_ENDPOINT}/search/"
            # yql = "select documentid, bq_organization_id, bq_organization_name, bq_legal_entity_id, bq_organization_structure, bq_organization_address1_line_1, bq_organization_address1_line_2, bq_organization_address1_city, bq_organization_address1_state, bq_organization_address1_country, bq_organization_address1_zip5, bq_organization_isactive, bq_organization_company_type, bq_public_indicator, bq_organization_jurisdiction_code, bq_revenue_mr, bq_organization_lfo, bq_current_employees_plan_mr, bq_organization_ein, bq_organization_valuation, bq_organization_year_founded, bq_organization_website, bq_score, bq_organization_naics_sector_name, bq_organization_naics_sector_code, bq_organization_naics_name, bq_organization_naics_code, bq_organization_irs_sector_name, bq_organization_subsector_name, bq_organization_industry_name, bq_organization_ticker, bq_organization_cong_district_name, bq_organization_cong_district_cd, bq_organization_cong_district_id, bq_organization_cong_district_representative_name_from_listing, bq_organization_cong_district_representative_last_name, bq_organization_cong_district_representative_party_name, bq_organization_district_officer_phone, bq_confidence_score, bq_officer_details, bq_organization_address1_location, bq_organization_gross_profit_mr, bq_organization_net_income_mr, bq_organization_revenue_growth_yoy_mr from bqorganization where"
            yql = "select bq_organization_id, bq_organization_ein, bq_legal_entity_parent_status, bq_organization_name, bq_organization_structure, bq_organization_company_type, bq_organization_ticker, bq_organization_is_public, bq_organization_legal_name, bq_legal_entity_id, bq_organization_address1_type, bq_organization_address1_line_2, bq_organization_address1_city, bq_organization_address1_state, bq_organization_address1_zip5, bq_legal_entity_jurisdiction_code, bq_organization_date_founded, bq_organization_current_status, bq_organization_isactive, bq_organization_address1_line_1, bq_organization_address1_line_2, bq_current_employees_plan_mr, bq_score, bq_organization_irs_sector_name, bq_revenue_mr, bq_organization_cik, bq_organization_lei, bq_organization_legal_address1_line_1, bq_organization_legal_address1_line_2, bq_organization_legal_address1_city, bq_organization_legal_address1_state, bq_organization_legal_address1_zip5, bq_organization_website, bq_organization_sector_name, bq_organization_sector_code, bq_organization_naics_sector_name, bq_organization_naics_sector_code, bq_organization_naics_name, bq_legal_entity_children_count, bq_organization_naics_code, bq_organization_current_status, bq_legal_entity_current_status, bq_legal_entity_isactive, bq_organization_subsector_name, bq_organization_valuation, bq_revenue_growth_yoy_mr, bq_net_income_mr, bq_gross_profit_mr, bq_organization_address1_state_name, bq_organization_address1_location, bq_organization_address1_latitude, bq_organization_address1_longitude, bq_industry_name, bq_cong_district_cd, bq_cong_district_name, bq_cong_district_id, bq_cong_district_representative_name_from_listing, bq_cong_district_representative_party_name, bq_confidence_score, bq_organization_lfo from terminal_hybrid where"
            if query:
                if field:
                    if yql.lower().rstrip().endswith('and'):
                        yql = f'{yql} and {field} contains "{query}" and'
                    elif yql.lower().rstrip().endswith('where'):
                        yql = f'{yql} {field} contains "{query}" and'
                    else:
                        yql = f'{yql} and {field} contains "{query}" and'
                else:
                    query = query.lower()
                    query = re.sub(r'\s{2,}', ' ', query)
                    if 'and' in query:
                        qqq = query.lower().rstrip().split('and')
                        q1 = qqq[0].strip()
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
                        s1 = ''
                        for word in query1:
                            s1 = f'{s1} default contains "{word}" OR'
                        yql = f"{yql} ({remove_and_from_end(s1)}) and"  
            if yql:
                if filter:
                    try:
                        filter = json.loads(filter.replace("'", "\""))
                    except json.JSONDecodeError as e:
                        response = {"response":{"error": "Invalid filter format. Please provide a valid JSON object."}, "status":400}
                        return response
                        # return JSONResponse(content={"error": "Invalid filter format. Please provide a valid JSON object."}, status_code=400)
                    for key, val in filter.items():
                        if len(val)>=1:
                            if key in ('bq_organization_year_founded', 'bq_revenue_mr', 'bq_current_employees_plan_mr', 'bq_organization_total_erc_tax_credit_modded','bq_organization_ir_refund_amount','bq_organization_valuation'):
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
                                elif len(val)==0:
                                    pass
                                else:
                                    if yql.lower().rstrip().endswith('or') or yql.lower().rstrip().endswith('and'):
                                        yql = remove_and_from_end(yql)
                                        yql = f'{yql} AND {key} contains "{val[0]}" AND'
                                    elif yql.lower().rstrip().endswith('where'):
                                        yql = f"{yql} {key} contains '{val[0]}' AND"
                                    else:
                                        yql = f'{yql} AND {key} contains "{val[0]}" AND'
            yql = remove_and_from_end(yql)
            if orderby:
                if orderby == 'bq_organization_name':
                    orderbyField = 'bq_organization_name'
                elif orderby == 'bq_revenue_mr':
                    orderbyField = 'bq_revenue_mr'
                elif orderby == 'bq_current_employees_plan_mr':
                    orderbyField = 'bq_current_employees_plan_mr'
                elif orderby == 'bq_current_employees_plan_growth_yoy_mr':
                    orderbyField = 'bq_current_employees_plan_growth_yoy_mr'
                elif orderby == 'bq_organization_isactive':
                    orderbyField = 'bq_organization_isactive'
                elif orderby == 'bq_score':
                    orderbyField = 'bq_score'
                else:
                    orderbyField = 'bq_organization_name'
                
                order = 'asc' if isAsc in ('True', 'true', True) else 'desc'
            
                yql = f"{yql} order by {orderbyField} {order}"
                
            print(11111111111,yql)
            
            params = {
                'yql': yql,
                # 'query': query,
                'filter': filter,
                'offset': offset,
                'ranking': ranking,
                'limit': limit,
                'type': 'all',
                'hits': 100,
                "format": "json",
            }
            response = requests.get(search_endpoint, params=params)
            # response.raise_for_status()
            response = {"response":response.json(), "status":200}
            return response

        else:
            response = {"response":1, "status":200}
            return response
        
    # except requests.RequestException as e:
    #     response = {"response":{"error": "An error occurred while processing the search request.", "details": str(e)}, status:400}
    #     return response
    
    except Exception as e:
        response = {"response":{"error": "An unexpected error occurred.", "details": str(e)}, "status":400}
        return response