import re
from builtins import set

import editdistance
import spacy
from elasticsearch import Elasticsearch

from customizations.constants import CLASS_INDEX
from main import search_props_and_entities
from customizations.model.core import LinkedCandidate
from customizations.model.core import LinkingResponse
from customizations.sparql.query_helper import get_rdf_types

nlp = spacy.load('en_core_web_sm')
es = Elasticsearch(hosts=['http://geo-qa.cs.upb.de:9200/'])


def process_input(question):
    classes = process_text_C(question)
    entities, relations = search_props_and_entities(question)

    # enrich
    linked_classes = [LinkedCandidate.from_value_array(x) for x in classes]
    linked_relations = [LinkedCandidate.from_value_array(x) for x in relations]
    linked_entities = [LinkedCandidate.from_value_array(x) for x in entities]

    # mark start and end of search term in the question
    linked_classes = [mark_start_end_index(question, x) for x in linked_classes if x.levensteinDistance < 4]
    linked_relations = [mark_start_end_index(question, x) for x in linked_relations if x.levensteinDistance < 4]
    linked_entities = [mark_start_end_index(question, x) for x in linked_entities if x.levensteinDistance < 4]

    # add types for entities
    entity_uris = [x.uri for x in linked_entities]
    entity_rdf_types = get_rdf_types(entity_uris)
    for le in linked_entities:
        if le.uri in entity_rdf_types:
            le.types = list(entity_rdf_types[le.uri])

    return LinkingResponse(question, linked_classes, linked_relations, linked_entities)


def mark_start_end_index(question: str, linked_candidate: LinkedCandidate):
    search_term_index = [m.start() for m in re.finditer(linked_candidate.searchTerm, question)]
    if len(search_term_index) == 0:
        search_term_index = [m.start() for m in re.finditer(linked_candidate.searchTerm, question, re.IGNORECASE)]

    linked_candidate.startIndex = search_term_index
    return linked_candidate


def process_text_C(question):
    # print(question)
    doc = nlp(question)
    results = []

    search_candidates = []
    for term in doc:
        if term.pos_ == "NOUN":
            search_candidates.append(term.lemma_)
            search_candidates.append(doc[term.i - 1].text + " " + term.lemma_)

    for search_term in search_candidates:
        search_results = classSearch(search_term)
        if len(search_results) > 0:
            results.extend(search_results)

    results = sorted(results, key=lambda x: (x[1], -x[3], -x[2]))
    seen = set()
    results = [x for x in results if x[1] not in seen and not seen.add(x[1])]
    results = sorted(results, key=lambda x: (-x[3], -x[2], x[1]))  # NOTE Enhancement

    return results


def classSearch(search_term):
    results = []
    ###################################################
    elasticResults = es.search(index=CLASS_INDEX, body={
        "query": {
            "match": {"label": search_term}
        }
        , "size": 100
    }
                               )
    for result in elasticResults['hits']['hits']:
        if result["_source"]["label"].lower().replace('.', '').strip() == search_term.lower().strip():
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40, search_term])
        else:
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 40, 0, search_term])

    ###################################################
    elasticResults = es.search(index=CLASS_INDEX, body={
        "query": {
            "match": {
                "label": {
                    "query": search_term,
                    "fuzziness": "AUTO"

                }
            }
        }, "size": 100
    }
                               )
    for result in elasticResults['hits']['hits']:
        edit_distance = editdistance.eval(result["_source"]["label"].lower().replace('.', '').strip(),
                                          search_term.lower().strip())
        if edit_distance <= 1:
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40, search_term])
        else:
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 25, 0, search_term])

    results = sorted(results, key=lambda x: (x[1][x[1].rfind("/") + 1:], -x[3], -x[2]))
    seen = set()
    results = [x for x in results if x[1] not in seen and not seen.add(x[1])]
    # results = results[:20]
    results = sorted(results, key=lambda x: (-x[3], -x[2], x[1][x[1].rfind("/") + 1:]))

    # TODO take all top ranked results instead of just one
    return results[:1]


def read_benchmark_questions():
    with open("/home/hardik/Projects/falcon2.0/customizations/geo-qa-benchmark.json", "r") as benchmark_file:
        import json
        benchmark_json = json.load(benchmark_file)
        questions_json = benchmark_json["questions"]
        questions_list = []
        for question in questions_json:
            questions_list.append(question["question"][0]["string"])
        return questions_list


if __name__ == "__main__":
    # "/home/hardik/Projects/falcon2.0/customizations/lgdo_2014-07-26.n3"
    questions_list = read_benchmark_questions()

    for q in questions_list:
        # print(q)

        # Find classes
        # classes = process_text_C(q)
        # for result in classes:
        #     print(result)

        # Find entities
        # entities = search_props_and_entities(q)
        # for e in entities[0]:
        #     print(e)

        linking_response = process_input(q)
        print(linking_response.input_str)
        print([f"{x.searchTerm}{x.startIndex}={x.label}" for x in linking_response.linkedClasses])
        print([f"{x.searchTerm}{x.startIndex}={x.label}" for x in linking_response.linkedRelations])
        print([f"{x.searchTerm}{x.startIndex}={x.label}" for x in linking_response.linkedEntities])

        print()
        # break
