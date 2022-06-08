import re
import sqlite3
import time
import requests
import lxml.html
import pandas as pd

def get_pages():
    starting_urls = (
        'https://www.iherb.com/sitemaps/products-0-www-0.xml',
        'https://www.iherb.com/sitemaps/products-0-www-1.xml'
        )
    all_links = []
    for url in starting_urls:
        html = requests.get(url)
        tree = lxml.html.fromstring(html.content)
        links = tree.cssselect('loc')
        links = [link.text for link in links]
        all_links.extend(links)

    return all_links[:2001]


def get_items_json(links):
    items_json = []
    for link in links:
        item_id = link.split('/')[-1]

        url = f"https://catalog.app.iherb.com/product/{item_id}"

        payload = ""
        headers = {
            'cookie': 'ih-experiment=eyJzcGVjaWFsc0xhbmRpbmdQYWdlIjp7IkNob3NlblZhcmlhbnQiOjAsIkVuZERhdGUiOiIyMDIzLTAxLTAxVDAwOjAwOjAwIn19; ih-preference=store=0&country=US&language=en-US&currency=USD; ih-cc-pref=eyJmdW5jdGlvbmFsIjowLCJhbmFseXRpY3MiOjB9; ForceTrafficSplitType=B; dscid=001782f2-4630-4857-95e6-c753da1d4b7e; ih-hp-vp=01; user-related={"HasNavigatedSite":{"timestamp":1654427953286}}; ihr-temse=cc=US&expires=05%20Jun%202022%2012:19:13Z; FPID=FPID2.2.d89x3%2F%2Bpeu%2FQqa323HlyJ9ydK1NHzc33ClAQAeEiahU%3D.1654427954; FPLC=l4uxkJD4k%2F%2FEe0bNWR7wgUQgd6eCsYxH%2FxVl9X163PfyMINJ3uHXmBnOnBqX2wKWlOCUu9%2BIffJvocuPxmEQthyC5HlKEGW5GBOAxHMmxeuPJVheNPPnTI%2F7u1E3YA%3D%3D; _gid=GA1.2.1909090377.1654427956; sd_client_id=d19c820e-9b66-47ea-b82f-ecf001710eac; sd_identity={"userId":"undefined","traits":{"email":"null"}}; __cfruid=80f278aefe28f70a09a8f44f666cc161358d067b-1654427961; _gcl_au=1.1.314854648.1654427962; ih-exp-recs=2; _scid=ed430cc4-a478-49f9-aadc-eae485e1cefd; _pin_unauth=dWlkPVpXWTBNemhqTnpVdFpUVm1ZeTAwTmpOakxUa3hOV0V0TUdKak1qaGxZelJqT0RobA; _tt_enable_cookie=1; _ttp=8a135176-dd6a-4f92-8da1-997a9d194a88; _sctr=1|1654358400000; notice_gdpr_prefs=0,1,2,3:; notice_preferences=3:; cmapi_cookie_privacy=permit 1,2,3,4; ConstructorioID_client_id=4dcdbc6d-2ff0-474f-9ca7-f1cd5d04297d; ih-hp-view=1; iher-pref1=storeid=0&sccode=US&lan=en-US&scurcode=USD&pc=ODQwNDc&whr=2&wp=1&lchg=1&ifv=1; pxcts=3afd371e-e4c2-11ec-990a-755879426c52; _pxvid=3838cf72-e4c2-11ec-93a4-7a4877586c69; cmapi_gtm_bl=; _ga=GA1.2.1596962323.1654427954; cto_bundle=E0c_Ql9wWVFHMEtZWWZrdEQlMkI3VnklMkJaeG9VQzN4RTBjYnMzMFRwbnB3OG8lMkJCQU56Um1zRDJoaE1ZY3NQRjM3d3FkSzhnUHM2WW5iRE1uQzZmaSUyRlFVQmpPJTJCNG9Oa2dzcVRkbFM0UWtTbWYlMkJZJTJCOXpiTmw3NnF2VyUyRlJud1RSJTJCeWZtWVBNMiUyRlFtckIyam1BZlpHWEglMkJlbnFLbDNBJTNEJTNE; _uetsid=5a3b6a10e4c111ec936a39b2c9cb04b6; _uetvid=5a3bc3b0e4c111ec9cc96fe22f550e86; __CG=u%3A2255964212680272000%2Cs%3A1062249204%2Ct%3A1654428541245%2Cc%3A5%2Ck%3Awww.iherb.com%2F76%2F76%2F1551%2Cf%3A1%2Ci%3A1; iher-vps=4.43211.261.100439; __cf_bm=WobpPEWMybOPosCagB84OlQIF53DdN8apSa7AnTGqkQ-1654430117-0-AY2XapSUtFZSvUYQyVBMqw/Eo123Pz3Ix2yAPW8eNFflcTxss6pwdw9BCghXJ91eARPwyVc4Thqs5boHCF7goAOGJYsif2NpPBOFPBwbshWa; _ga_SW3NJP516F=GS1.1.1654427954.1.1.1654430118.60; _ga_06BXHBZ0RK=GS1.1.1654427954.1.1.1654430118.60; notice_behavior=implied,eu; _px3=e5e54ddf3152a210ae242219614fd171b507d013039bd187740fcca3b685e270:ovZKurjyqJEVk6EuCHglqzYbNg6r8kgG/NwiHmQPyBV+I873Aj/ccQLmk7zpltW0Q2zr8hFYnSEGkhFXB4YgXg==:1000:j/lhWQV5qYH1vzLVjhhRAd7mgDggkCaGqYsCl8+lezq+AwSmYqe0REp4jGAAz1DpqeKf9ASdM3Q8dy4IeIx4MR9yTY0hWF/BqiOpEbuFukTSBn2mdPUbwDTGI7ZKCpbeU2NHlL7w/9viVEBOfxuOAEN7lTFgr1TqHlKsPH023U0n7SCXTnSyZVOwvjphq5dsF2h55z5G2XLVgIFONR/B7A==',
            'authority': 'catalog.app.iherb.com',
            'accept': "application/json, text/plain, */*",
            'accept-language': "en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7,de;q=0.6",
            'cache-control': "no-cache",
            'origin': "https://www.iherb.com",
            'pragma': "no-cache",
            'referer': "https://www.iherb.com/",
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
            'sec-ch-ua-mobile': "?0",
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': "empty",
            'sec-fetch-mode': "cors",
            'sec-fetch-site': "same-site",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36"
            }

        response = requests.request("GET", url, data=payload, headers=headers)
        items_json.append(response.json())

    return items_json

def get_data(items_json):
    items_data = []
    for item in items_json:
        if item.get('id'):
            title = item.get('displayName')
            brand = item.get('brandName')
            link = item.get('url')
            image_index = item.get('primaryImageIndex')
            brand_code = item.get('brandCode').lower()
            category = item.get('rootCategoryName')
            category_id = item.get('rootCategoryId')
            part_num = item.get('partNumber').lower().replace('-', '')
            image_link = f'https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/{brand_code}/{part_num}/v/{image_index}.jpg'
            item_id = item.get('id')
            package = item.get('packageQuantity')
            price_str = str(item.get('listPrice'))
            price = price_str[1:]
            currency = 'USD' if price_str.startswith('$') else 'EURO' if price_str.startswith('€') else 'RUB' 
            availability = 'Available' if bool(item.get('isAvailableToPurchase')) else 'Unavailable'
            dimensions = item.get('dimensions')
            weight = item.get('actualWeight')
            expiration_date = item.get('formattedExpirationDate')
            rating = item.get('averageRating')
            clean = re.compile('<.*?>')
            description = item.get('description').replace('</li>', '\n').replace('</p>', '\n').replace('&nbsp;', '')
            description = re.sub(clean, '', description)

            item_card = {
                'Title': title, 'Brand': brand, 'ID': item_id, 'Category': category,
                 'Category ID': category_id, 'Package': package, 'Price': price, 
                 'Currency': currency, 'Available': availability,'Dimensions': dimensions, 
                 'Weight': weight, 'Expiration': expiration_date, 'Rating': rating, 
                 'Description': description, 'Link': link, 'Image': image_link
            }
            items_data.append(item_card)
            print(item_card)

    return items_data


def save_to_db(items_data):
    df = pd.DataFrame(items_data)
    # conn = sqlite3.connect('test_iherb_data.db')
    # df.to_sql("iherb_data_info", conn, index=False, if_exists='append')
    # conn.close()
    df.to_csv("iherb_data.csv")


if __name__ == '__main__':
    start = time.perf_counter()
    
    links = get_pages()
    items_json = get_items_json(links)
    items_data = get_data(items_json)
    save_to_db(items_data)
    fin = time.perf_counter() - start
    print(fin)
