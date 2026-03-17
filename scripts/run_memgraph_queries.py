from neo4j import GraphDatabase
import sys

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "")

def run_query(query_name, query_file):
    driver = GraphDatabase.driver(URI, auth=AUTH)
    
    with open(query_file, "r") as f:
        query = f.read()
    
    print(f"\n{'='*60}")
    print(f"Running {query_name}...")
    print(f"{'='*60}")
    
    try:
        with driver.session() as session:
            result = session.run(query)
            records = list(result)
            
            if records:
                print(f"Found {len(records)} rows\n")
                for i, record in enumerate(records[:5]):  # Показываем первые 5
                    print(f"Row {i+1}: {dict(record)}")
                if len(records) > 5:
                    print(f"... and {len(records) - 5} more rows")
            else:
                print("No results returned")
                
    except Exception as e:
        print(f"Error: {e}")
    
    driver.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        query_name = sys.argv[1]
        run_query(query_name, f"scripts/{query_name}.cypher")
    else:
        run_query("q1", "scripts/q1.cypher")
        run_query("q2", "scripts/q2.cypher")
        run_query("q3", "scripts/q3.cypher")
