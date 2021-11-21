from elasticsearch import Elasticsearch
import editdistance

es = Elasticsearch(hosts=['http://geo-qa.cs.upb.de:9200/'])
entity_index_name = "geoqa-entity"
relation_index_name = "geoqa-relation"


def entitySearch(query):
    results = []
    ###################################################
    elasticResults = es.search(index=entity_index_name, body={
        "query": {
            "match": {"label": query}
        }
        , "size": 100
    }
                               )
    for result in elasticResults['hits']['hits']:
        if result["_source"]["label"].lower().replace('.', '').strip() == query.lower().strip():
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40])
        else:
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 40, 0])

    ###################################################
    elasticResults = es.search(index=entity_index_name, body={
        "query": {
            "match": {
                "label": {
                    "query": query,
                    "fuzziness": "AUTO"

                }
            }
        }, "size": 15
        # reduced from 100
    }
                               )
    for result in elasticResults['hits']['hits']:
        edit_distance = editdistance.eval(result["_source"]["label"].lower().replace('.', '').strip(),
                                          query.lower().strip())
        if edit_distance <= 1:
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 30])
        elif edit_distance <= 5:
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 25, 0])
        # else:
        #     results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 25, 0])

    results = sorted(results, key=lambda x: (x[1][x[1].rfind("/")+1:], -x[3], -x[2]))
    seen = set()
    results = [x for x in results if x[1] not in seen and not seen.add(x[1])]
    # results = results[:20]
    results = sorted(results, key=lambda x: (-x[3], -x[2], x[1][x[1].rfind("/")+1:]))

    return results[:15]
    # for result in results['hits']['hits']:
    # print (result["_score"])
    # print (result["_source"])
    # print("-----------")


def propertySearch(query):
    results = []
    ###################################################
    elasticResults = es.search(index=relation_index_name, body={
        "query": {
            "match": {"label": query}
        }
        , "size": 100
    }
                               )
    for result in elasticResults['hits']['hits']:
        if result["_source"]["label"].lower().replace('.', '').strip() == query.lower().strip():
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40])
        else:
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 40, 0])

    ###################################################
    elasticResults = es.search(index=relation_index_name, body={
        "query": {
            "match": {
                "label": {
                    "query": query,
                    "fuzziness": "AUTO"

                }
            }
        }, "size": 10
    }
                               )
    for result in elasticResults['hits']['hits']:
        edit_distance = editdistance.eval(result["_source"]["label"].lower().replace('.', '').strip(),
                                          query.lower().strip())
        if edit_distance <= 1:
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40])
        elif edit_distance <= 5:
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 25, 0])
        else:
            results.append([result["_source"]["label"], result["_source"]["uri"], result["_score"] * 25, 0])

    results = sorted(results, key=lambda x: (x[1][x[1].rfind("/") + 1:], -x[3], -x[2]))
    seen = set()
    results = [x for x in results if x[1] not in seen and not seen.add(x[1])]
    # results = results[:20]
    results = sorted(results, key=lambda x: (-x[3], -x[2], x[1][x[1].rfind("/")+1:]))

    return results[:15]


def propertySearchExactmatch(query):
    ###################################################
    elasticResults = es.search(index=relation_index_name, body={
        "query": {
            "match": {"label": query}
        }
    }
                               )
    for result in elasticResults['hits']['hits']:
        if result["_source"]["label"].lower().replace('.', '').strip() == query.lower().strip():
            return True

    return False
