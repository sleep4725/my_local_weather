from elasticsearch import Elasticsearch

class EsClient():

    def __init__(self):
        self.es_client = EsClient.ret_es_client()

    @classmethod
    def ret_es_client(cls):
        es_ = Elasticsearch(hosts=["http://127.0.0.1:9200"])
        result = es_.cluster.health()

        try:

            status = result["status"]
        except KeyError as err:
            print(err)
            exit(1)
        else:
            if status == "yellow" or status == "green":
                return es_
            else:
                print("elastic connection error !!!")
                exit(1)
