import sqlglot
import pandas as pd
import os
from sqlglot import exp

path = "../queries/1a.sql"


def readFile(path):
    with open(path, 'r') as f:
        return f.read()


# query: query to be parsed
# returns: <table, {attributes}>
def parseQueryTablesAttributes(query):
    # Parse the SQL query
    parsed_query = sqlglot.parse_one(query)

    # Dictionary to hold table names and their corresponding attributes
    table_columns = {}

    # Loop through all columns in the SELECT clause
    for column in parsed_query.find_all(sqlglot.expressions.Column):
        # Get the table and column names
        table_name = column.table
        column_name = column.name

        # Add the column name under the corresponding table name in the dictionary
        if table_name:
            if table_name not in table_columns:
                table_columns[table_name] = []
            table_columns[table_name].append(column_name)

    return table_columns


# query: query to be parsed
# returns: <att, {tables}>
def parseQueryAttributeTables(query):
    # Parse the SQL query
    parsed_query = sqlglot.parse_one(query)

    # Dictionary to hold table names and their corresponding attributes
    columns_tables = {}

    # Loop through all columns in the SELECT clause
    for column in parsed_query.find_all(sqlglot.expressions.Column):
        # Get the table and column names
        table_name = column.table
        column_name = column.name

        # Add the column name under the corresponding table name in the dictionary
        if column_name:
            if column_name not in columns_tables:
                columns_tables[column_name] = []
            columns_tables[column_name].append(table_name)

    return columns_tables


# query: query to be parsed
# returns: list of attributes that appear in aggregate functions of the query
def parseQueryAggregates(query):
    # Parse the SQL query
    parsed_query = sqlglot.parse_one(query)

    # List to hold the aggregation columns
    aggregated_columns = []

    # Loop through all aggregation functions in the query
    for agg_func in parsed_query.find_all(sqlglot.expressions.AggFunc):
        # Get the column being aggregated
        for column in agg_func.find_all(sqlglot.expressions.Column):
            table_name = column.table
            column_name = column.name
            aggregated_columns.append((table_name, column_name))

    return aggregated_columns


# query: query to be parsed
# returns: list of attributes that appear in group by statements of the query
def parseQueryGroupBy(query):
    # Parse the SQL query
    parsed_query = sqlglot.parse_one(query)

    # List to hold the aggregation columns
    grouped_columns = []

    # Loop through all grouped attributes in the query
    for group_by in parsed_query.find_all(sqlglot.expressions.Group):
        # Get the column being grouped
        for column in group_by.find_all(sqlglot.expressions.Column):
            table_name = column.table
            column_name = column.name
            grouped_columns.append((table_name, column_name))

    return grouped_columns


# query: query to be parsed
# returns: number of attributes that appear in group by statement of the query
def getNumberGroupAttributes(query):
    parsed_query = sqlglot.parse_one(query)
    count_group = 0

    for group_by in parsed_query.find_all(sqlglot.expressions.Group):
        # Get the column being grouped
        for column in group_by.find_all(sqlglot.expressions.Column):
            count_group += 1

    return count_group


# query: query to be parsed
# returns: number of attributes that appear in aggregate functions of the query
def getNumberAggAttributes(query):
    parsed_query = sqlglot.parse_one(query)
    count_agg = 0

    for agg_func in parsed_query.find_all(sqlglot.expressions.AggFunc):
        # Get the column being grouped
        for column in agg_func.find_all(sqlglot.expressions.Column):
            count_agg += 1

    return count_agg


# query: query to be parsed
# returns: number of tables that are part of the query
def getNumberTables(query):
    parsed_query = sqlglot.parse_one(query)
    table_names = [table.name for table in parsed_query.find_all(sqlglot.expressions.Table)]
    return len(table_names)


# query: query to be parsed
# returns: list of all aggregated attributes in the query and the corresponding relation they appear in
def getAggregatesRelations(query):
    # Parse the query using sqlglot
    parsed_query = sqlglot.parse_one(query)
    att_tables = parseQueryAttributeTables(query)

    # Dictionary to hold aggregate functions and their corresponding relations
    aggregates_relations = {}

    # Traverse the parsed query to extract aggregate functions
    for agg in parsed_query.find_all(exp.AggFunc):
        # Find the column(s) inside the aggregate function
        columns = agg.find_all(exp.Column)

        for column in columns:
            # Get the column name and table/relation name if available
            column_name = column.name
            table_name = att_tables[column_name][0]  #todo: assumption that two relations to not have attributes with the same name

            if table_name:
                if column_name not in aggregates_relations:
                    aggregates_relations[column_name] = set()
                # Add the table/relation name to the set for the current aggregate function
                aggregates_relations[column_name].add(table_name)

    return aggregates_relations


# query: query to be parsed
# returns: True if all aggregated attributes appear in the same relation (query has guarded aggregates)
def isAggregateGuarded(query):
    aggregates_relations = getAggregatesRelations(query)

    # if there are 0-1 aggregates in the query, the query is trivially guarded
    if len(aggregates_relations) <= 1:
        return True

    # Convert the lists of relations to sets for easier comparison
    relations_sets = {agg: set(relations) for agg, relations in aggregates_relations.items()}

    # Get the intersection of all relation sets
    common_relations = None
    for relations in relations_sets.values():
        if common_relations is None:
            common_relations = relations
        else:
            common_relations &= relations  # Find the intersection of sets

    # If there is at least one common relation in all sets, return True
    if common_relations:
        return True

    # If no common relation exists, return False
    return False


# query: query to be parsed
# returns: list of all grouped attributes in the query and the corresponding relation they appear in
def getGroupByRelations(query):
    # Parse the query using sqlglot
    parsed_query = sqlglot.parse_one(query)
    att_tables = parseQueryAttributeTables(query)

    # Dictionary to hold aggregate functions and their corresponding relations
    grouped_attributes_relations = {}

    # Traverse the parsed query to extract aggregate functions
    for group_att in parsed_query.find_all(exp.Group):
        # Find the column(s) inside the aggregate function
        columns = group_att.find_all(exp.Column)

        for column in columns:
            # Get the column name and table/relation name if available
            column_name = column.name
            table_name = att_tables[column_name][0]  #todo: assumption that two relations do not have attributes with the same name

            if table_name:
                if column_name not in grouped_attributes_relations:
                    grouped_attributes_relations[column_name] = set()
                # Add the table/relation name to the set for the current grouped attribute
                grouped_attributes_relations[column_name].add(table_name)

    return grouped_attributes_relations


# query: query to be parsed
# returns: True if all grouped attributes appear in the same relation (query has guarded aggregates)
def isGroupGuarded(query):
    grouped_att_relations = getGroupByRelations(query)

    # if there are 0-1 aggregates in the query, the query is trivially guarded
    if len(grouped_att_relations) <= 1:
        return True

        # Convert the lists of relations to sets for easier comparison
    relations_sets = {attr: set(relations) for attr, relations in grouped_att_relations.items()}

    # Get the intersection of all relation sets
    common_relations = None
    for relations in relations_sets.values():
        if common_relations is None:
            common_relations = relations
        else:
            common_relations &= relations  # Find the intersection of sets

    # If there is at least one common relation in all sets, return True
    if common_relations:
        return True

    # If no common relation exists, return False
    return False


# path: path to a folder where job, tpc-h, tpc-ds and lsqb benchmark queries can be found
# returns: in /output, the files Benchmark_analysis.txt will be created as well as *_query_data.csv for each benchmark
def analyse_queries(path):
    benchmark_name = ""

    if path.endswith("job/"):
        benchmark_name = "JOB"
    elif path.endswith("tpch-queries/"):
        benchmark_name = "TPC_H"
    elif path.endswith("tpcds-queries/"):
        benchmark_name = "TPC_DS"
    elif path.endswith("lsqb/sql/"):
        benchmark_name = "LSQB"
    elif path.endswith("tpcds-syn/"):
        benchmark_name = "TPC_DS_SYN"
    else:
        print("Unknown benchmark type.")
        exit(0)

    query_info = {}  # qID, #tables, #groupingAttributes, #aggAttributes, #aggGuarded, #groupGuarded
    idx = 0
    skipped = 0
    c_partly_guarded = 0

    # Open one of the files,
    for data_file in sorted(os.listdir(path)):

        if data_file.endswith(".sql") and not data_file.endswith("schema.sql"):
            if benchmark_name == "LSQB" and not data_file.startswith("q"):
                continue

            q_path = path + data_file
            with open(q_path, 'r') as f:
                query = f.read()
                try:
                    num_tables = getNumberTables(query)
                    num_group = getNumberGroupAttributes(query)
                    num_agg = getNumberAggAttributes(query)
                    agg_guarded = isAggregateGuarded(query)
                    group_guarded = isGroupGuarded(query)

                    query_info[idx] = (data_file, num_tables, num_group, num_agg, agg_guarded, group_guarded)
                except sqlglot.errors.ParseError as e:
                    skipped += 1
                    print(benchmark_name + ": " + data_file)
                    print(f"Error parsing query: {e}")

        idx += 1
    df = pd.DataFrame(query_info, index=['qID', 'c_tables', 'c_groupingAttributes', 'c_aggAttributes', 'agg_guarded',
                                         'group_guarded'])
    df = df.T  # transpose matrix

    f = open("../output/Benchmark_analyis.txt", "a")
    f.write(benchmark_name + "\n")
    f.write("Queries analyzed: " + str(len(query_info)) + "\n")
    f.write("Queries skipped: " + str(skipped) + "\n")
    f.write("Average number of tables per query: " + str(df["c_tables"].mean()) + '\n')
    f.write("Average number of grouped attributes per query: " + str(df["c_groupingAttributes"].mean()) + '\n')
    f.write("Average number of aggregated attributes per query: " + str(df["c_aggAttributes"].mean()) + '\n')
    f.write("Number of aggregate-guarded queries: " + str(df['agg_guarded'].sum()) + '\n')
    f.write("Number of group-guarded queries: " + str(df['group_guarded'].sum()) + '\n')
    f.write("Number of queries that are group-guarded, but not aggregate-guarded: " + str(c_partly_guarded) + '\n')
    f.close()
    df.to_csv("../output/" + benchmark_name + '_query_data.csv')


def main():
    #path_query = "../queries/1a.sql"
    path_query = "../../../spark-eval/benchmark/job/1a.sql"
    path_JOB_external = "../../../spark-eval/benchmark/job/"
    path_LSQB_external = "../../../spark-eval/benchmark/lsqb/"
    path_TPCH_external = "../../../spark-eval/benchmark/tpch-queries/"  #error in 11-hint.sql
    path_TPCDS_external = "../../../spark-eval/benchmark/tpcds-queries/"
    path_JOB = "../queries/job/"
    path_TPCDS = "../queries/tpcds-queries/"
    path_TPCH = "../queries/tpch-queries/"
    path_LSQB = "../queries/lsqb/sql/"
    path_tpcds_syn = "../queries/tpcds-syn/"
    paths = [path_JOB, path_TPCDS, path_TPCH, path_LSQB, path_tpcds_syn]

    for path in paths:
        analyse_queries(path)


if __name__ == "__main__":
    main()
