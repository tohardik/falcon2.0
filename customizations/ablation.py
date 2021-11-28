from elasticsearch import Elasticsearch

from customizations.constants import RELATION_INDEX, ENTITY_INDEX, CLASS_INDEX
from customizations.extensions import mark_start_end_index
from customizations.model.core import LinkedCandidate, LinkingResponse
from customizations.sparql.query_helper import get_rdf_types

es = Elasticsearch(hosts=['http://geo-qa.cs.upb.de:9200/'])

label_override = {
    'Nelson-Mandela-Park': 'Nelson Mandela Park',
    'Lesum': "Losum",
    'Stadtwald Ahnthannsmoor': 'Stadtwald Ahnthammsmoor',
    'Golfclub Oberneuland': 'Golf-Club Oberneuland',
    'Cycle Path': "cycling path",
    'Ditch': "canals",
}
uri_override = {
    'http://linkedgeodata.org/triplify/relation62718': "Bremen"
}


def read_benchmark():
    with open("./QA44.json") as benchmark_file:
        import json
        benchmark_json = json.load(benchmark_file)
        return benchmark_json


def read_benchmark_questions():
    benchmark_json = read_benchmark()
    questions_json = benchmark_json["questions"]
    questions_list = []
    for question in questions_json:
        questions_list.append(question["question"][0]["string"].strip())
    return questions_list


def prepare_ablation():
    import re
    benchmark = read_benchmark()
    questions_json = benchmark["questions"]
    ablation_data = {}
    for q in questions_json:
        question_text = q["question"][0]["string"].strip()
        sparql = q['query']['sparql']

        entities = [e for e in re.findall(r"(http://linkedgeodata.org/triplify/)([\w\d]*)", sparql)]
        entities = ["".join(e) for e in entities]

        classes = [e for e in re.findall(r"(http://linkedgeodata.org/ontology/)([A-Z]{1})([A-Za-z]+)", sparql)]
        classes = ["".join(e) for e in classes]

        relations = [e for e in re.findall(r"(http://linkedgeodata.org/)(ontology/)?([a-z]+)", sparql)]
        relations = ["".join(e) for e in relations]
        relations = [e for e in relations if 'triplify' not in e and e != "http://linkedgeodata.org/ontology"]

        linked_classes = []
        linked_relations = []
        linked_entities = []

        for e in entities:
            result = index_search_by_uri(e, ENTITY_INDEX)
            result.append(result[4])
            linked_entities.append(result)
        for e in classes:
            result = index_search_by_uri(e, CLASS_INDEX)
            result.append(result[4])
            linked_classes.append(result)
        for e in relations:
            result = index_search_by_uri(e, RELATION_INDEX)
            result.append(result[4])
            linked_relations.append(result)

        # enrich
        linked_classes = [LinkedCandidate.from_value_array(x) for x in linked_classes]
        linked_relations = [LinkedCandidate.from_value_array(x) for x in linked_relations]
        linked_entities = [LinkedCandidate.from_value_array(x) for x in linked_entities]

        # override
        for cl in linked_classes:
            cl.searchTerm = str(cl.searchTerm).lower()[:-1]
            cl.originalTerm = cl.searchTerm

            if cl.label in label_override:
                cl.searchTerm = label_override[cl.label]
                cl.originalTerm = label_override[cl.label]

        for cl in linked_entities:
            if cl.label in label_override:
                cl.searchTerm = label_override[cl.label]
                cl.originalTerm = label_override[cl.label]
            if cl.uri in uri_override:
                cl.label = uri_override[cl.uri]
                cl.searchTerm = uri_override[cl.uri]
                cl.originalTerm = uri_override[cl.uri]

        # mark start and end of search term in the question
        linked_classes = [mark_start_end_index(question_text, x) for x in linked_classes if x.levensteinDistance < 4]
        linked_relations = [mark_start_end_index(question_text, x) for x in linked_relations if
                            x.levensteinDistance < 4]
        linked_entities = [mark_start_end_index(question_text, x) for x in linked_entities if x.levensteinDistance < 4]

        linked_classes = [c for c in linked_classes if len(c.startIndex) > 0]
        linked_relations = [c for c in linked_relations if len(c.startIndex) > 0]
        linked_entities = [c for c in linked_entities if len(c.startIndex) > 0]

        # add types for entities
        entity_uris = [x.uri for x in linked_entities]
        entity_rdf_types = get_rdf_types(entity_uris)
        for le in linked_entities:
            if le.uri in entity_rdf_types:
                le.types = list(entity_rdf_types[le.uri])

        ablation_data[question_text] = LinkingResponse(question_text, linked_classes, linked_relations, linked_entities)
    return ablation_data


def index_search_by_uri(uri, index):
    results = []
    ###################################################
    elasticResults = es.search(index=index, body={
        "query": {
            "match": {"uri": uri}
        }
        , "size": 10
    }
                               )
    for result in elasticResults['hits']['hits']:
        # if result["_source"]["label"].lower().replace('.', '').strip() == .lower().strip():
        #     results.append(
        #         [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40, search_term])
        # else:
        #     results.append(
        #         [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 40, 0, search_term])
        if result["_source"]["uri"] == uri:
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40,
                 result["_source"]["label"]])

    results = sorted(results, key=lambda x: (x[1][x[1].rfind("/") + 1:], -x[3], -x[2]))
    seen = set()
    results = [x for x in results if x[1] not in seen and not seen.add(x[1])]
    results = sorted(results, key=lambda x: (-x[3], -x[2], x[1][x[1].rfind("/") + 1:]))

    return results[0] if len(results) > 0 else None


if __name__ == '__main__':
    print(len(prepare_ablation()))
