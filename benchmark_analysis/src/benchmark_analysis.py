import sqlglot

path = "../queries/1a.sql"

def readFile(path):
    with open(path, 'r') as f:
        return f.read()

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

def main():
    #path_query = "../queries/1a.sql"
    path_query = "../../../spark-eval/benchmark/job/1a.sql"
    path_job = "../../../spark-eval/benchmark/job"
    query = readFile(path_query)
    table_columns = parseQueryTablesAttributes(query)

    # Print the table names and their corresponding attributes
    for table, columns in table_columns.items():
        print(f"Table: {table}, Columns: {columns}")

    aggregated_columns = parseQueryAggregates(query)

    # Print the aggregated columns
    for table, column in aggregated_columns:
        print(f"Table: {table}, Column in Aggregation: {column}")

    grouped_colums = parseQueryGroupBy(query)

    # Print the aggregated columns
    for table, column in grouped_colums:
        print(f"Table: {table}, Column in Group-by: {column}")

if __name__ == "__main__":
    main()
