from elasticsearch import Elasticsearch
from rdflib import Graph

from constants import CLASS_INDEX, RELATION_INDEX
from constants import ENTITY_INDEX
from constants import LABEL_PRED_LOWER

es = Elasticsearch(['http://geo-qa.cs.upb.de:9200/'])


def indexClasses(filepath):
    g = Graph()
    g.parse(filepath)

    for stmt in g:
        if str(stmt[1]).lower() == LABEL_PRED_LOWER and stmt[2]._language.lower() == "en-gb":  # Only english labels
            addToIndexAlt(CLASS_INDEX, str(stmt[0]), stmt[2]._value)


def indexEntities(filepath):
    g = Graph()
    g.parse(filepath)

    for stmt in g:
        if str(stmt[1]).lower() == LABEL_PRED_LOWER:  # Only labels
            addToIndexAlt(ENTITY_INDEX, str(stmt[0]), stmt[2]._value)

def indexProperties(filepath):
    g = Graph()
    g.parse(filepath)

    for stmt in g:
        if str(stmt[1]).lower() == LABEL_PRED_LOWER:  # Only english labels
            addToIndexAlt(RELATION_INDEX, str(stmt[0]), stmt[2]._value)


def addToIndexAlt(index_name, uri, label):
    try:
        es.index(index=index_name, body={"uri": uri, "label": label})
        print(label)
        return True
    except:
        return 'error'


if __name__ == "__main__":
    # indexEntities("./cutomizations/bremen-entitiy-all-labels.nt")
    # print("Entities done")
    # print()
    # indexClasses("./cutomizations/lgdo_2014-07-26.n3")
    # print("Classes done")
    indexProperties("/home/hardik/Projects/falcon2.0/customizations/props.nt")
    print("Properties done")
