import sqlglot
import pandas as pd
import os

path = "../queries/1a.sql"

def readFile(path):
    with open(path, 'r') as f:
        return f.read()

# query: query to be parsed
# returns: list of attributes of different relations that are part of the query
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
    for agg_func in parsed_query.find_all(sqlglot.expressions.Group):
        # Get the column being grouped
        for column in agg_func.find_all(sqlglot.expressions.Column):
            table_name = column.table
            column_name = column.name
            grouped_columns.append((table_name, column_name))

    return grouped_columns

# query: query to be parsed
# returns: number of attributes that appear in aggregate functions of the query
def getNumberGroupAttributes(query):
    parsed_query = sqlglot.parse_one(query)
    count_group = 0

    for agg_func in parsed_query.find_all(sqlglot.expressions.Group):
        # Get the column being grouped
        for column in agg_func.find_all(sqlglot.expressions.Column):
            count_group += 1

    return count_group

# query: query to be parsed
# returns: number of tables that are part of the query
def getNumberTables(query):
    parsed_query = sqlglot.parse_one(query)
    table_names = [table.name for table in parsed_query.find_all(sqlglot.expressions.Table)]
    return len(table_names)


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
    else:
        print("Unknown benchmark type.")
        exit(0)

    query_info = {}  # qID, #tables, #groupingAttributes
    idx = 0
    skipped = 0
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
                    query_info[idx] = (data_file, num_tables, num_group)
                except sqlglot.errors.ParseError as e:
                    skipped += 1
                    print(benchmark_name + ": " + data_file)
                    print(f"Error parsing query: {e}")

        idx += 1
    df = pd.DataFrame(query_info, index=['qID', 'c_tables', 'c_groupingAttributes'])
    df = df.T  # transpose matrix

    f = open("../output/Benchmark_analyis.txt", "a")
    f.write(benchmark_name+"\n")
    f.write("Queries analyzed: " + str(len(query_info)) + "\n")
    f.write("Queries skipped: " + str(skipped) + "\n")
    f.write("Average number of tables per query: " + str(df["c_tables"].mean()) + '\n')
    f.write("Average number of grouped attributes per query: " + str(df["c_groupingAttributes"].mean()) + '\n')
    f.close()
    df.to_csv("../output/"+ benchmark_name +'_query_data.csv')

def main():
    #path_query = "../queries/1a.sql"
    path_query = "../../../spark-eval/benchmark/job/1a.sql"
    path_JOB_external = "../../../spark-eval/benchmark/job/"
    path_LSQB_external = "../../../spark-eval/benchmark/lsqb/"
    path_TPCH_external = "../../../spark-eval/benchmark/tpch-queries/" #error in 11-hint.sql
    path_TPCDS_external = "../../../spark-eval/benchmark/tpcds-queries/"
    path_JOB = "../queries/job/"
    path_TPCDS = "../queries/tpcds-queries/"
    path_TPCH = "../queries/tpch-queries/"
    path_LSQB = "../queries/lsqb/sql/"
    paths = [path_JOB, path_TPCDS, path_TPCH,path_LSQB]

    for path in paths:

        analyse_queries(path)





if __name__ == "__main__":
    main()
