from urllib.parse import urlencode
import os
import yaml
import json
import requests
import xmltodict
import pprint as ppr
from bs4 import BeautifulSoup

from ElasticInstance.EsClient import EsClient
from elasticsearch import helpers

#
#
#

class Weather(EsClient):


    def __init__(self):
        EsClient.__init__(self)
        self.index_ = "indx"
        self.url_params = Weather.get_url_params()
        self.url = Weather.get_url_information()

    def url_request(self):
        """
        :return:
        """
        u_ = self.url["url"] + "?{params_}".format(params_ = self.url_params)
        print(u_)

        sess = requests.Session()

        try:

            html = sess.get(url= u_)
        except requests.exceptions.ConnectionError as err:
            print(err)

        else:

            if html.status_code == 200 and html.ok:
                bs_object = BeautifulSoup(html.text, "html.parser")

                try:
                    dfs_rss = bs_object.select_one("a#dfs_rss")
                except:
                    pass
                else:
                    href_url = dfs_rss.attrs["href"]
                    print(href_url)
                    self.detail_information(sub_url=href_url)
        finally:
            sess.close()

    def detail_information(self, sub_url):
        """
        :param sub_url:
        :return:
        """
        sess = requests.Session()

        try:

            html = sess.get(url= sub_url)
        except requests.exceptions.ConnectionError as err:
            print(err)
        else:
            xpars = xmltodict.parse(html.text)
            json_data = json.loads(json.dumps(xpars, ensure_ascii=False))
            item = json_data["rss"]["channel"]
            data = item["item"]["description"]["body"]["data"]
            print(data)
            """
            data = {
                "category_": item["item"]["category"],
                "datas_"   : item["item"]["description"]["body"]["data"],
                "pubDate_" : item["pubDate"],
                "title_"   : item["title"]
            }"""

            actions = [
                {
                    "_index"  : self.index_,
                    "_source" : Weather.ret_dict_mapping(i)
                }
                for i in data
            ]

            """
                =====================================================
                Elasticsearch bulk insert
                =====================================================
            """

            try:

                helpers.bulk(client= self.es_client, actions= actions)
            except helpers.errors.BulkIndexError as err:
                print("bulk insert fail !! : {}".format(err))
                pass
            else:
                print("bulk insert success !!")

        finally:
            sess.close()

    @classmethod
    def get_url_information(cls):
        URL_PATH = "../Config/info.yml"
        result = os.path.isfile(path=URL_PATH)
        if result:
            with open(file=URL_PATH, mode="r", encoding="utf-8") as fr:
                info_ = yaml.safe_load(fr)
                fr.close()

            return info_
        else:
            exit(1)

    @classmethod
    def get_url_params(cls):
        url_params = urlencode({
            "sido" : 4100000000,
            "gugun": 4119000000,
            "dong" : 4119061000,
            "x"    : 24,
            "y"    : 4
        })

        return url_params

    @classmethod
    def ret_dict_mapping(cls, *arg_data):
        print(arg_data)
        dict_mapping = {
            "hour" : arg_data[0]["hour"] ,
            "day"  : arg_data[0]["day"],
            "temp" : float(arg_data[0]["temp"]),
            "tmx"  : float(arg_data[0]["tmx"]),
            "tmn"  : float(arg_data[0]["tmn"]),
            "sky"  : arg_data[0]["day"],
            "pty"  : arg_data[0]["pty"],
            "wfKor": arg_data[0]["wfKor"],
            "wfEn" : arg_data[0]["wfEn"],
            "pop"  : arg_data[0]["pop"],
            "r12"  : float(arg_data[0]["r12"]),
            "s12"  : float(arg_data[0]["s12"]),
            "ws"   : float(arg_data[0]["ws"]),
            "wd"   : arg_data[0]["wd"],
            "wdKor": arg_data[0]["wdKor"],
            "wdEn" : arg_data[0]["wdEn"],
            "reh"  : int(arg_data[0]["day"]),
            "r06"  : float(arg_data[0]["r06"]),
            "s06"  : float(arg_data[0]["s06"])
        }
        return dict_mapping

if __name__ == "__main__":
    obj = Weather()
    obj.url_request()