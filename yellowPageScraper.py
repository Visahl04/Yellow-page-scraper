from bs4 import BeautifulSoup
from lxml import etree
import requests,json
import concurrent.futures
import pandas as pd

i = 0
all_data = []


def get_details(search,location,i):
    url = f"https://www.yellowpages.com/search?search_terms={search}&geo_location_terms={location}%2C%20TX&page={i}"
    payload = {}
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en-IN;q=0.9,en;q=0.8,hi;q=0.7',
        'Connection': 'keep-alive',
        'Cookie': 'vrid=7664e684-cb4b-4043-b644-bfc5493a4f1d; bucketsrc=front; _ga=GA1.1.640253107.1701249879; s_ecid=MCMID%7C05208298249420194250959729526249647195; s_prop70=November; s_prop71=48; location=geo_term%3ADallas%2C%20TX%7Clat%3A32.783333%7Clng%3A-96.8%7Ccity%3ADallas%7Cstate%3ATX%7Cdisplay_geo%3ADallas%2C%20TX; __gsas=ID=3c93e7081c4590fc:T=1701249908:RT=1701249908:S=ALNI_MaEbcazd9swdSFudNuTQFKMQyeuUA; s_nr=1701250063931; bucket=ypu%3Aypu%3Aserp-filter; s_otb=false; AMCVS_A57E776A5245AEA80A490D44%40AdobeOrg=1; zone=330; s_cc=true; sorted=false; express:sess=eyJka3MiOiIwNGI4MzZlMy04MTY2LTQ2MjgtOTY0Yi02ZWU2MDgxNzcwMTciLCJmbGFzaCI6e30sInByZXZpb3VzUGFnZSI6InNycCJ9; express:sess.sig=x6vy_CGYHQHvykDzxlmMT_wKJ0c; s_prop49=search_results; AMCV_A57E776A5245AEA80A490D44%40AdobeOrg=-1303530583%7CMCIDTS%7C19708%7CMCMID%7C05208298249420194250959729526249647195%7CMCAAMLH-1703426153%7C12%7CMCAAMB-1703426153%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1702828553s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C3.3.0; search_terms=Restaurant%20Equipment%20Repair%20%26%20Service; _ga_0EQTJQH34W=GS1.1.1702821269.15.1.1702821698.59.0.0; s_tp=6697; __gads=ID=bf4f6711768adb46:T=1701249910:RT=1702821698:S=ALNI_Ma9VghSPE_V-_qgc_qIpfNDwMS64A; __gpi=UID=00000c9d047d85db:T=1701249910:RT=1702821698:S=ALNI_MbB2BFslLBPRXASrH4iIYoGvJcSQw; s_ppv=search%2C100%2C5%2C6696; s_sq=%5B%5BB%5D%5D; bucket=ypu%3Aypu%3Aserp-filter; bucketsrc=front; express:sess=eyJka3MiOiIwNGI4MzZlMy04MTY2LTQ2MjgtOTY0Yi02ZWU2MDgxNzcwMTciLCJmbGFzaCI6e30sInByZXZpb3VzUGFnZSI6InNycCJ9; express:sess.sig=x6vy_CGYHQHvykDzxlmMT_wKJ0c; location=geo_term%3ADallas%2C%20TX%7Clat%3A32.783333%7Clng%3A-96.8%7Ccity%3ADallas%7Cstate%3ATX%7Cdisplay_geo%3ADallas%2C%20TX; search_terms=Restaurant%20Equipment%20Repair%20%26%20Service',
        'Referer': 'https://www.yellowpages.com/search?search_terms=Restaurant+Equipment-Repair+%26+Service&geo_location_terms=Dallas%2C+TX',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    return response


def card_links(res):
    soup = BeautifulSoup(res.content, 'html.parser')
    res = etree.HTML(str(soup))
    return res.xpath('//div[@class="info-section info-primary"]/h2/a/@href')


def card_details(list):

    if list:
        url = f'https://www.yellowpages.com{list}'
        payload = {}
        headers = {
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
          'Accept-Language': 'en-US,en-IN;q=0.9,en;q=0.8,hi;q=0.7',
          'Connection': 'keep-alive',
          'Cookie': 'vrid=7664e684-cb4b-4043-b644-bfc5493a4f1d; bucketsrc=front; _ga=GA1.1.640253107.1701249879; s_ecid=MCMID%7C05208298249420194250959729526249647195; s_prop70=November; s_prop71=48; location=geo_term%3ADallas%2C%20TX%7Clat%3A32.783333%7Clng%3A-96.8%7Ccity%3ADallas%7Cstate%3ATX%7Cdisplay_geo%3ADallas%2C%20TX; __gsas=ID=3c93e7081c4590fc:T=1701249908:RT=1701249908:S=ALNI_MaEbcazd9swdSFudNuTQFKMQyeuUA; s_nr=1701250063931; bucket=ypu%3Aypu%3Aserp-filter; s_otb=false; AMCVS_A57E776A5245AEA80A490D44%40AdobeOrg=1; AMCV_A57E776A5245AEA80A490D44%40AdobeOrg=-1303530583%7CMCIDTS%7C19708%7CMCMID%7C05208298249420194250959729526249647195%7CMCAAMLH-1703331639%7C12%7CMCAAMB-1703331639%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1702734039s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C3.3.0; zone=330; s_cc=true; search_terms=restaurant; express:sess=eyJka3MiOiIwNGI4MzZlMy04MTY2LTQ2MjgtOTY0Yi02ZWU2MDgxNzcwMTciLCJmbGFzaCI6e30sInByZXZpb3VzUGFnZSI6InNycCJ9; express:sess.sig=x6vy_CGYHQHvykDzxlmMT_wKJ0c; sorted=false; _ga_0EQTJQH34W=GS1.1.1702726839.9.1.1702728313.59.0.0; __gads=ID=bf4f6711768adb46:T=1701249910:RT=1702728314:S=ALNI_Ma9VghSPE_V-_qgc_qIpfNDwMS64A; __gpi=UID=00000c9d047d85db:T=1701249910:RT=1702728314:S=ALNI_MbB2BFslLBPRXASrH4iIYoGvJcSQw; s_tp=9565; s_ppv=search%2C7%2C7%2C692.7999877929688; srp_return_to_pageYOffset=320.79998779296875; s_sq=%5B%5BB%5D%5D; bucket=ypu%3Aypu%3Aserp-filter; bucketsrc=front; express:sess=eyJka3MiOiIwNGI4MzZlMy04MTY2LTQ2MjgtOTY0Yi02ZWU2MDgxNzcwMTciLCJmbGFzaCI6e30sInByZXZpb3VzUGFnZSI6InNycCJ9; express:sess.sig=x6vy_CGYHQHvykDzxlmMT_wKJ0c; location=geo_term%3ADallas%2C%20TX%7Clat%3A32.783333%7Clng%3A-96.8%7Ccity%3ADallas%7Cstate%3ATX%7Cdisplay_geo%3ADallas%2C%20TX; search_terms=restaurant',
          'Referer': 'https://www.yellowpages.com/search?search_terms=restaurant&geo_location_terms=Dallas%2C+TX',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'same-origin',
          'Sec-Fetch-User': '?1',
          'Upgrade-Insecure-Requests': '1',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        soup = BeautifulSoup(response.content, 'html.parser')
        data = etree.HTML(str(soup))
        name = data.xpath('//h1[@class="dockable business-name"]/text()')
        no = data.xpath('//a[@class="phone dockable"]/strong/text()')
        cat = data.xpath('(//div[@class="categories"])[1]/a/text()')
        link = data.xpath('//a[@class="website-link dockable"]/@href')
        new = {
            'name': name,
            'no': no,
            'cat': cat,
            'link': link,
        }
        all_data.append(new)

def main():
    i = 1
    while True:
        respones = get_details('restaurant','Dallas', i)
        tt = respones.text.find('No results found')
        if tt < 0:
            links = card_links(respones)
            with concurrent.futures.ThreadPoolExecutor() as executer:
                executer.map(card_details, links)
            i += 1
        else:
            break
    df = pd.DataFrame(all_data)
    print(df.head())
    df.to_csv('yellopage.csv')



if __name__ == '__main__':
    main()



