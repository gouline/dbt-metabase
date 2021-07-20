from pyparsing import (
    Forward,
    Word,
    Group,
    Optional,
    Literal,
    Combine,
    CaselessLiteral,
    oneOf,
    delimitedList,
    infixNotation,
    opAssoc,
    Suppress,
    alphas,
    alphanums,
    dblQuotedString,
    pyparsing_common,
    ParserElement,
)

ParserElement.enablePackrat()

METABASE_EXPRESSIONS = {
    # aggregations
    "count": {"displayName": "Count", "type": "aggregation", "args": []},
    "cum-count": {
        "displayName": "CumulativeCount",
        "type": "aggregation",
        "args": [],
    },
    "sum": {"displayName": "Sum", "type": "aggregation", "args": ["number"]},
    "cum-sum": {
        "displayName": "CumulativeSum",
        "type": "aggregation",
        "args": ["number"],
    },
    "distinct": {"displayName": "Distinct", "type": "aggregation", "args": ["number"]},
    "stddev": {
        "displayName": "StandardDeviation",
        "type": "aggregation",
        "args": ["number"],
        "requiresFeature": "standard-deviation-aggregations",
    },
    "avg": {"displayName": "Average", "type": "aggregation", "args": ["number"]},
    "min": {"displayName": "Min", "type": "aggregation", "args": ["number"]},
    "max": {"displayName": "Max", "type": "aggregation", "args": ["number"]},
    "share": {"displayName": "Share", "type": "aggregation", "args": ["boolean"]},
    "count-where": {
        "displayName": "CountIf",
        "type": "aggregation",
        "args": ["boolean"],
    },
    "sum-where": {
        "displayName": "SumIf",
        "type": "aggregation",
        "args": ["number", "boolean"],
    },
    "var": {
        "displayName": "Variance",
        "type": "aggregation",
        "args": ["number"],
        "requiresFeature": "standard-deviation-aggregations",
    },
    "median": {
        "displayName": "Median",
        "type": "aggregation",
        "args": ["number"],
        "requiresFeature": "percentile-aggregations",
    },
    "percentile": {
        "displayName": "Percentile",
        "type": "aggregation",
        "args": ["number", "number"],
        "requiresFeature": "percentile-aggregations",
    },
    # string functions
    "lower": {"displayName": "lower", "type": "string", "args": ["string"]},
    "upper": {"displayName": "upper", "type": "string", "args": ["string"]},
    "substring": {
        "displayName": "substring",
        "type": "string",
        "args": ["string", "number", "number"],
    },
    "regex-match-first": {
        "displayName": "regexextract",
        "type": "string",
        "args": ["string", "string"],
        "requiresFeature": "regex",
    },
    "concat": {
        "displayName": "concat",
        "type": "string",
        "args": ["expression"],
        "multiple": True,
    },
    "replace": {
        "displayName": "replace",
        "type": "string",
        "args": ["string", "string", "string"],
    },
    "length": {"displayName": "length", "type": "number", "args": ["string"]},
    "trim": {"displayName": "trim", "type": "string", "args": ["string"]},
    "rtrim": {"displayName": "rtrim", "type": "string", "args": ["string"]},
    "ltrim": {"displayName": "ltrim", "type": "string", "args": ["string"]},
    # numeric functions
    "abs": {
        "displayName": "abs",
        "type": "number",
        "args": ["number"],
        "requiresFeature": "expressions",
    },
    "floor": {
        "displayName": "floor",
        "type": "number",
        "args": ["number"],
        "requiresFeature": "expressions",
    },
    "ceil": {
        "displayName": "ceil",
        "type": "number",
        "args": ["number"],
        "requiresFeature": "expressions",
    },
    "round": {
        "displayName": "round",
        "type": "number",
        "args": ["number"],
        "requiresFeature": "expressions",
    },
    "sqrt": {
        "displayName": "sqrt",
        "type": "number",
        "args": ["number"],
        "requiresFeature": "advanced-math-expressions",
    },
    "power": {
        "displayName": "power",
        "type": "number",
        "args": ["number", "number"],
        "requiresFeature": "advanced-math-expressions",
    },
    "log": {
        "displayName": "log",
        "type": "number",
        "args": ["number"],
        "requiresFeature": "advanced-math-expressions",
    },
    "exp": {
        "displayName": "exp",
        "type": "number",
        "args": ["number"],
        "requiresFeature": "advanced-math-expressions",
    },
    # boolean functions
    "contains": {
        "displayName": "contains",
        "type": "boolean",
        "args": ["string", "string"],
    },
    "starts-with": {
        "displayName": "startsWith",
        "type": "boolean",
        "args": ["string", "string"],
    },
    "ends-with": {
        "displayName": "endsWith",
        "type": "boolean",
        "args": ["string", "string"],
    },
    "between": {
        "displayName": "between",
        "type": "boolean",
        "args": ["expression", "expression", "expression"],
    },
    "time-interval": {
        "displayName": "interval",
        "type": "boolean",
        "args": ["expression", "number", "string"],
    },
    "is-null": {
        "displayName": "isnull",
        "type": "boolean",
        "args": ["expression"],
    },
    "is-empty": {
        "displayName": "isempty",
        "type": "boolean",
        "args": ["expression"],
    },
    # other expression functions
    "coalesce": {
        "displayName": "coalesce",
        "type": "expression",
        "args": ["expression", "expression"],
        "multiple": True,
    },
    "case": {
        "displayName": "case",
        "type": "expression",
        "args": [
            "expression",
            "expression",
        ],  # ideally we'd alternate boolean/expression
        "multiple": True,
    },
    # boolean operators
    "and": {"displayName": "AND", "type": "boolean", "args": ["boolean", "boolean"]},
    "or": {"displayName": "OR", "type": "boolean", "args": ["boolean", "boolean"]},
    "not": {"displayName": "NOT", "type": "boolean", "args": ["boolean"]},
    # numeric operators
    "*": {
        "displayName": "*",
        "tokenName": "Multi",
        "type": "number",
        "args": ["number", "number"],
    },
    "/": {
        "displayName": "/",
        "tokenName": "Div",
        "type": "number",
        "args": ["number", "number"],
    },
    "-": {
        "displayName": "-",
        "tokenName": "Minus",
        "type": "number",
        "args": ["number", "number"],
    },
    "+": {
        "displayName": "+",
        "tokenName": "Plus",
        "type": "number",
        "args": ["number", "number"],
    },
    # comparison operators
    "!=": {
        "displayName": "!=",
        "tokenName": "NotEqual",
        "type": "boolean",
        "args": ["expression", "expression"],
    },
    "<=": {
        "displayName": "<=",
        "tokenName": "LessThanEqual",
        "type": "boolean",
        "args": ["expression", "expression"],
    },
    ">=": {
        "displayName": ">=",
        "tokenName": "GreaterThanEqual",
        "type": "boolean",
        "args": ["expression", "expression"],
    },
    "<": {
        "displayName": "<",
        "tokenName": "LessThan",
        "type": "boolean",
        "args": ["expression", "expression"],
    },
    ">": {
        "displayName": ">",
        "tokenName": "GreaterThan",
        "type": "boolean",
        "args": ["expression", "expression"],
    },
    "=": {
        "displayName": "=",
        "tokenName": "Equal",
        "type": "boolean",
        "args": ["expression", "expression"],
    },
}


class MetabaseMetricCompiler:
    def __init__(self, field_lookup):
        # EXPRESSION NAME [LITERAL]
        self.field_lookup = field_lookup
        self.current_target = None
        self.mb_expr_map_to_api = {
            term["displayName"].lower(): api_name
            for api_name, term in METABASE_EXPRESSIONS.items()
        }
        self.parser = self.build_parser()

    # SORT AND COUNT SAME PRECEDENCE OPERATORS WITHIN GROUPS
    @staticmethod
    def sort_count_ops(y):
        ops = 0
        for j, i in enumerate(y):
            if isinstance(i, str) and i in METABASE_EXPRESSIONS:
                if j > 0:
                    y.insert(ops, y.pop(j))
                ops += 1
        return ops

    # MANIPULATES OBJECT IN PLACE
    def infix_to_prefix(self, y, no_collapse=False):
        """Unnest the pyparsing parsed Metabase expression in infix notation and convert to prefix
        ordering operators left to right since operator precedence is already checked prior to composing groups."""
        if isinstance(y, list) and y[0] != "field":
            # Prefix notation
            self.sort_count_ops(y)
        for ix in range(len(y)):
            # Unnest any over-grouped but grammatically correct expressions
            while (
                isinstance(y[ix], list)
                and len(y[ix]) == 1
                and isinstance(y[ix][0], list)
                and not str(y[0]).lower() == "case"
                and not no_collapse
            ):
                y[ix] = y[ix][0]
            # Recursion
            if isinstance(y[ix], list):
                self.infix_to_prefix(y[ix])
            else:
                # Translate base values to api values where applicable
                y[ix] = self.mb_expr_map_to_api.get(
                    y[ix].lower() if isinstance(y[ix], str) else y[ix], y[ix]
                )

    # CLEANS UP THE INFIX -> PREFIX NOTATION FOR API CONSUMPTION
    def polish_notation(self, y):

        if isinstance(y, list) and y[0] != "field" and "=" not in y:

            total_ops = self.sort_count_ops(y)
            """Starting index of Operands"""

            const = []

            # Lets iterate through all ops
            while total_ops > 1:

                if len(const) == 0:
                    const.append(y.pop(total_ops))

                # Same as prev op, remove op
                if len(y) > 1 and y[0] == y[1]:
                    const.append(y.pop(total_ops))
                    y.pop(0)
                    total_ops -= 1
                else:
                    # Basic Arithmetic parsing
                    this_const = y.pop(total_ops)
                    const.append(this_const)
                    this_op = y.pop(0)
                    total_ops -= 1
                    y.insert(total_ops, [this_op, *const.copy()])
                    const = []

        # Recursion
        for _, term in enumerate(y):
            if isinstance(term, list) and len(term) > 1 and term[0] != "field":
                self.polish_notation(term)

    # PACKS FIELDS -> TO INCLUDE METABASE FIELD ID DETEMRINATION LOGIC
    def to_field(self, y):
        # y[0]
        print(str(y[0]).upper())
        id = (
            self.field_lookup.get(self.current_target, {})
            .get(str(y[0]).upper(), {})
            .get("id", None)
        )
        if id is None:
            exit("No column found")
        return [[["field", id, None]]]

    def build_parser(self) -> Forward:
        # BUILD EXPRESSION
        mb_expr = Forward()

        # CONSTANTS & SUPPRESSIONS
        number = pyparsing_common.number()("constant")
        LPAR, RPAR = map(Suppress, "()")
        LBRACK, RBRACK = map(Suppress, "[]")

        # COLUMN GRAMMAR
        column = (
            LBRACK
            + Combine(
                Optional(Word(alphas + "_", alphanums + "_") + ".")("table")
                + Word(alphas + "_", alphanums + "_")("field")
            )
            + RBRACK
        ).setParseAction(self.to_field)

        # GENERIC EXPRESSION GRAMMAR
        func_name = Word(alphas + "_", alphanums + "_")
        expression = Group(
            func_name("expression")
            + LPAR
            + (Group(Optional(delimitedList(mb_expr))))("args")
            + RPAR
        )

        # SPECIFIC GRAMMAR FOR CASE
        case_expression = Group(
            CaselessLiteral("case")
            + LPAR
            + Group(
                Group(
                    Group(mb_expr)("conditional")
                    + Suppress(",")
                    + Group(mb_expr)("value_if")
                )
            )
            + RPAR
        )

        # SPECIFIC GRAMMAR FOR SUM-WHERE COUNT-WHERE
        where_expression = Group(
            oneOf("count-where sum-where SumIf CountIf", caseless=True)
            + LPAR
            + Group(mb_expr)("value_if")
            + Suppress(",")
            + Group(mb_expr)("conditional")
            + RPAR
        )

        # STRINGS
        string = dblQuotedString.setParseAction(lambda x: x[0].strip('"'))("string")

        # OPERATORS
        eq_op = oneOf("= > < >= <= !=")("comparison")
        and_op = Literal("and")("comparison")
        or_op = Literal("or")("comparison")
        not_op = Literal("not")("comparison")
        sign_op = oneOf("+ -")("operator")
        mult_op = oneOf("* /")("operator")
        plus_op = oneOf("+ -")("operator")

        (
            mb_expr
            << infixNotation(
                number("constant")
                | string("string")
                | column("field")
                | case_expression("case")
                | where_expression("where")
                | expression("expression")
                | Group(LPAR + mb_expr + RPAR)("args"),
                [
                    (sign_op("operator"), 1, opAssoc.RIGHT),
                    (mult_op("operator"), 2, opAssoc.LEFT),
                    (plus_op("operator"), 2, opAssoc.LEFT),
                    (eq_op("comparison"), 2, opAssoc.LEFT),
                    (and_op("comparison"), 2, opAssoc.LEFT),
                    (or_op("comparison"), 2, opAssoc.LEFT),
                    (not_op("comparison"), 1, opAssoc.RIGHT),
                ],
            ).setParseAction(lambda x: x[0])
        )

        return mb_expr

    # TRANSLATE EXPRESSION
    def transpile_expression(self, metabase_expression):
        in_memory_result = self.parser.parseString(metabase_expression).asList()
        self.infix_to_prefix(in_memory_result)
        self.polish_notation(in_memory_result)
        return in_memory_result
