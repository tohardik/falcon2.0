import editdistance


class LinkedCandidate:
    def __init__(self, uri, label, searchTerm, esScore, multiplier, levensteinDistance, types=[]):
        self.uri = uri
        self.label = label
        self.searchTerm = searchTerm
        self.esScore = esScore
        self.multiplier = multiplier
        self.levensteinDistance = levensteinDistance
        self.types = types
        self.startIndex = []
        self.endIndex = []

    @classmethod
    def from_value_array(self, x):
        return LinkedCandidate(x[1], x[0], x[4], x[2], x[3],
                               editdistance.eval(x[4], x[0]))

    def __str__(self) -> str:
        return f"(uri={self.uri} , label={self.label})"

    def __repr__(self) -> str:
        return f"(uri={self.uri} , label={self.label})"

    def to_dict(self) -> dict:
        return {
            "uri": self.uri,
            "label": self.label,
            "searchTerm": self.searchTerm,
            "esScore": self.esScore,
            "multiplier": self.multiplier,
            "levensteinDistance": self.levensteinDistance,
            "types": self.types,
            "startIndex": self.startIndex,
            "endIndex": [x + len(self.searchTerm) - 1 for x in self.startIndex]
        }


class LinkingResponse:
    def __init__(self, inputText, linkedClasses, linkedRelations, linkedEntities):
        self.inputText = inputText
        self.linkedClasses = linkedClasses
        self.linkedRelations = linkedRelations
        self.linkedEntities = linkedEntities

    def to_dict(self):
        return {
            "inputText": self.inputText,
            "linkedClasses": [x.to_dict() for x in self.linkedClasses],
            "linkedRelations": [x.to_dict() for x in self.linkedRelations],
            "linkedEntities": [x.to_dict() for x in self.linkedEntities]
        }
