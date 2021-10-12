from SPARQLWrapper import SPARQLWrapper, JSON

from customizations.constants import SPARQL_ENDPOINT


def query(q):
    sparql = SPARQLWrapper(SPARQL_ENDPOINT)
    sparql.setQuery(q)
    sparql.setReturnFormat(JSON)
    results = sparql.queryAndConvert()
    return results


def get_rdf_types(subjects: list) -> dict:
    cleaned = []
    for sub in subjects:
        if not sub.startswith("<"):
            sub = "<" + sub
        if not sub.endswith(">"):
            sub = sub + ">"

        cleaned.append(sub)

    query_template = f"""SELECT DISTINCT ?s ?o WHERE {{
    VALUES ?s {{ {" ".join(cleaned)} }} 
    ?s a ?o. }}"""

    response = query(query_template)

    result = {}
    for x in response["results"]["bindings"]:
        uri = x["s"]["value"]
        rdf_type = x["o"]["value"]

        if uri in result:
            result[uri].add(rdf_type)
        else:
            result[uri] = {rdf_type}

    return result
