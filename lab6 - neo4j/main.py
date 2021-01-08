import random
import datetime as dt
from neo4j import GraphDatabase


# DATA
key = ["1", "2", "3", "4", "5"]
brand = ["Nike", "Adidas", "New Balance", "Oakley", "Burton"]
address = ["Barcelona", "Madrid", "Paris", "Tokio", "Chicago"]
nation = ["Spain", "France", "Japan", "United States"]
region = ["Europe", "Asia", "America"]
date = ["2020-02-01", "2020-12-23", "2003-10-02", "2021-04-24", "2020-02-02"]
priority = ["High", "Low", "Medium", "Medium-Low", "High-Medium"]
mktsegment = ["Product", "Comunication", "Distribution", "Price", "Digital mkt", "RRSS"]
flag = ["True", "False"]


# DATABASE
def create_database(session):
    session.run("MATCH (n) DETACH DELETE n") # drop all data in the database

    create_part_nodes(session)
    create_supp_nodes(session)
    create_partsupp_nodes(session)
    create_nation_nodes(session)
    create_region_nodes(session)
    create_order_nodes(session)
    create_customer_nodes(session)
    create_lineitem_nodes(session)
    create_relationships(session)


# NODES
def create_part_nodes(session):
    for i in [0, 1, 2, 3, 4]:
        session.run("CREATE (part" + key[i] + ": Part{p_partkey: " + key[i] + ", p_name: 'Partkey" + key[i] + "'"
                    ", p_mfgr: 'ABCDEFG', p_brand: '" + brand[i] + "', p_type: 'Running'" +
                    ", p_size: " + str(random.randint(38, 45)) + ", p_container: 'Containter" + key[i] + "'"
                    ", p_retailprice: " + str(float(random.randint(1000, 5000) / 100)) +
                    ", p_comment: 'OK'})")


def create_supp_nodes(session):
    for i in [0, 1, 2, 3, 4]:
        session.run("CREATE (supp" + key[i] + ": Supplier{s_suppkey: " + key[i] + ", s_name: 'Supplier" + key[i] + "'"
                    ", s_address: '" + address[i] + "', s_phone: " + str(random.randint(600000000, 699999999)) +
                    ", s_acctbal: " + str(random.random()) +
                    ", s_comment: 'OK'})")


def create_partsupp_nodes(session):
    for i in [0, 1, 2, 3, 4]:
        session.run("CREATE (partsupp" + key[i] + ": PartSupp{ps_partkey: " + key[i] + ", ps_suppkey: " + key[i] +
                    ", ps_availqty: " + str(random.randint(100, 500)) +
                    ", ps_supplycost: " + str(float(random.randint(100, 500) / 100)) + ", ps_comment: 'OK'})")

    session.run("CREATE INDEX ON: PartSupp(ps_supplycost)")


def create_nation_nodes(session):
    for i in [0, 1, 2, 3]:
        session.run("CREATE (nation" + key[i] + ": Nation{n_nationkey: " + key[i] + ", n_name: '" + nation[i] + "'"
                    ", n_comment: 'OK'})")


def create_region_nodes(session):
    for i in [0, 1, 2]:
        session.run("CREATE (region" + key[i] + ": Region{r_regionkey: " + key[i] + ", r_name: '" + region[i] + "'"
                    ", r_comment: 'OK'})")


def create_order_nodes(session):
    for i in [0, 1, 2, 3, 4]:
        session.run("CREATE (order" + key[i] + ": Order{o_orderkey: " + key[i] + ", o_orderstatus: 'OK" + "'"
                    ", o_totalprice: " + str(random.randint(0, 1000)) + ", o_orderdate: '" + random.choice(date) +
                    "', o_orderpriority: '" + random.choice(priority) +
                    "', o_clerk: '" + "Louis" +
                    "', o_shippriority: '" + random.choice(priority) +
                    "', o_comment: 'OK'})")

    session.run("CREATE INDEX ON: Order(o_orderdate)")


def create_customer_nodes(session):
    for i in [0, 1, 2, 3, 4]:
        session.run("CREATE (customer" + key[i] + ": Customer{c_custkey: " + key[i] + ", c_name: 'Supplier" + key[i] +
                    "', c_address: '" + address[i] + "', c_phone: " + str(random.randint(600000000, 699999999)) +
                    ", c_acctbal: " + str(random.random()) +
                    ", c_mktsegment: '" + random.choice(mktsegment) +
                    "', s_comment: 'OK'})")


def create_lineitem_nodes(session):
    for i in [0, 1, 2, 3, 4]:
        session.run("CREATE (lineitem" + key[i] + ": Lineitem{l_linenumber: " + key[i] + ", l_quantity: " + str(random.randint(0, 100)) +
                    ", l_extendedprice: " + str(random.randint(0, 200)) + ", l_discount: " + str(random.randint(0, 99)) +
                    ", l_tax: " + str(random.randint(0, 20)) +
                    ", l_returnflag: '" + random.choice(flag) +
                    "', l_linestatus: '" + random.choice(flag) +
                    "', l_shipdate: '" + random.choice(date) +
                    "', l_commitdate: '" + random.choice(date) +
                    "', l_receiptdate: '" + random.choice(date) +
                    "', l_shipinstruct: 'Ok" + key[i] + 
                    "', l_shipmode: 'Ok" + key[i] + 
                    "', l_comment: 'OK'})")

    session.run("CREATE INDEX ON: Lineitem(l_shipdate)")


# RELATIONSHIPS BETWEEN NODES
def create_relationships(session):
    # PART --> PARTSUPP
    session.run("MATCH (part1: Part{p_partkey: 1}), (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 1}) "
                "CREATE (part1) -[:BELONGS_TO]-> (partsupp1)")
    session.run("MATCH (part2: Part{p_partkey: 2}), (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 2}) "
                "CREATE (part2) -[:BELONGS_TO]-> (partsupp2)")
    session.run("MATCH (part3: Part{p_partkey: 3}), (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 3}) "
                "CREATE (part3) -[:BELONGS_TO]-> (partsupp3)")
    session.run("MATCH (part4: Part{p_partkey: 4}), (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 4}) "
                "CREATE (part4) -[:BELONGS_TO]-> (partsupp4)")
    session.run("MATCH (part5: Part{p_partkey: 5}), (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 5}) "
                "CREATE (part5) -[:BELONGS_TO]-> (partsupp5)")

    # PARTSUPP --> SUPPLIER
    session.run("MATCH (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 1}), (supp1: Supplier{s_suppkey: 1}) "
                "CREATE (partsupp1) -[:BELONGS_TO]-> (supp1)")
    session.run("MATCH (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 2}), (supp2: Supplier{s_suppkey: 2}) "
                "CREATE (partsupp2) -[:BELONGS_TO]-> (supp2)")
    session.run("MATCH (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 3}), (supp3: Supplier{s_suppkey: 3}) "
                "CREATE (partsupp3) -[:BELONGS_TO]-> (supp3)")
    session.run("MATCH (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 4}), (supp4: Supplier{s_suppkey: 4}) "
                "CREATE (partsupp4) -[:BELONGS_TO]-> (supp4)")
    session.run("MATCH (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 5}), (supp5: Supplier{s_suppkey: 5}) "
                "CREATE (partsupp5) -[:BELONGS_TO]-> (supp5)")

    # SUPPLIER --> NATION
    session.run("MATCH (supp1: Supplier{s_suppkey: 1}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (supp1) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (supp2: Supplier{s_suppkey: 2}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (supp2) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (supp3: Supplier{s_suppkey: 3}), (nation2: Nation{n_nationkey: 2}) "
                "CREATE (supp3) -[:BELONGS_TO]-> (nation2)")
    session.run("MATCH (supp4: Supplier{s_suppkey: 4}), (nation3: Nation{n_nationkey: 3}) "
                "CREATE (supp4) -[:BELONGS_TO]-> (nation3)")
    session.run("MATCH (supp5: Supplier{s_suppkey: 5}), (nation4: Nation{n_nationkey: 4}) "
                "CREATE (supp5) -[:BELONGS_TO]-> (nation4)")

    # NATION --> REGION
    session.run("MATCH (nation1: Nation{n_nationkey: 1}), (region1: Region{r_regionkey: 1}) "
                "CREATE (nation1) -[:BELONGS_TO]-> (region1)")
    session.run("MATCH (nation2: Nation{n_nationkey: 2}), (region1: Region{r_regionkey: 1}) "
                "CREATE (nation2) -[:BELONGS_TO]-> (region1)")
    session.run("MATCH (nation3: Nation{n_nationkey: 3}), (region2: Region{r_regionkey: 2}) "
                "CREATE (nation3) -[:BELONGS_TO]-> (region2)")
    session.run("MATCH (nation4: Nation{n_nationkey: 4}), (region3: Region{r_regionkey: 3}) "
                "CREATE (nation4) -[:BELONGS_TO]-> (region3)")

    # CUSTOMER --> NATION
    session.run("MATCH (customer1: Customer{c_custkey: 1}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (customer1) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (customer2: Customer{c_custkey: 2}), (nation2: Nation{n_nationkey: 2}) "
                "CREATE (customer2) -[:BELONGS_TO]-> (nation2)")
    session.run("MATCH (customer3: Customer{c_custkey: 3}), (nation3: Nation{n_nationkey: 3}) "
                "CREATE (customer3) -[:BELONGS_TO]-> (nation3)")
    session.run("MATCH (customer4: Customer{c_custkey: 4}), (nation4: Nation{n_nationkey: 4}) "
                "CREATE (customer4) -[:BELONGS_TO]-> (nation4)")
    session.run("MATCH (customer5: Customer{c_custkey: 5}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (customer5) -[:BELONGS_TO]-> (nation1)")

    # CUSTOMER --> ORDER
    session.run("MATCH (customer1: Customer{c_custkey: 1}), (order1: Order{o_orderkey: 1}) "
                "CREATE (customer1) -[:BELONGS_TO]-> (order1)")
    session.run("MATCH (customer2: Customer{c_custkey: 2}), (order2: Order{o_orderkey: 2}) "
                "CREATE (customer2) -[:BELONGS_TO]-> (order2)")
    session.run("MATCH (customer3: Customer{c_custkey: 3}), (order3: Order{o_orderkey: 3}) "
                "CREATE (customer3) -[:BELONGS_TO]-> (order3)")
    session.run("MATCH (customer4: Customer{c_custkey: 4}), (order4: Order{o_orderkey: 4}) "
                "CREATE (customer4) -[:BELONGS_TO]-> (order4)")
    session.run("MATCH (customer5: Customer{c_custkey: 5}), (order5: Order{o_orderkey: 5}) "
                "CREATE (customer5) -[:BELONGS_TO]-> (order5)")

    # ORDER --> LINEITEM
    session.run("MATCH (order1: Order{o_orderkey: 1}), (lineitem1: Lineitem{l_linenumber: 1}) "
                "CREATE (order1) -[:BELONGS_TO]-> (lineitem1)")
    session.run("MATCH (order2: Order{o_orderkey: 2}), (lineitem2: Lineitem{l_linenumber: 2}) "
                "CREATE (order2) -[:BELONGS_TO]-> (lineitem2)")
    session.run("MATCH (order3: Order{o_orderkey: 3}), (lineitem3: Lineitem{l_linenumber: 3}) "
                "CREATE (order3) -[:BELONGS_TO]-> (lineitem3)")
    session.run("MATCH (order4: Order{o_orderkey: 4}), (lineitem4: Lineitem{l_linenumber: 4}) "
                "CREATE (order4) -[:BELONGS_TO]-> (lineitem4)")
    session.run("MATCH (order5: Order{o_orderkey: 5}), (lineitem5: Lineitem{l_linenumber: 5}) "
                "CREATE (order5) -[:BELONGS_TO]-> (lineitem5)")

    # LINEITEM --> PARTSUPP
    session.run("MATCH (lineitem1: Lineitem{l_linenumber: 1}), (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 1}) "
                "CREATE (lineitem1) -[:BELONGS_TO]-> (partsupp1)")
    session.run("MATCH (lineitem2: Lineitem{l_linenumber: 2}), (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 2}) "
                "CREATE (lineitem2) -[:BELONGS_TO]-> (partsupp2)")
    session.run("MATCH (lineitem3: Lineitem{l_linenumber: 3}), (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 3}) "
                "CREATE (lineitem3) -[:BELONGS_TO]-> (partsupp3)")
    session.run("MATCH (lineitem4: Lineitem{l_linenumber: 4}), (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 4}) "
                "CREATE (lineitem4) -[:BELONGS_TO]-> (partsupp4)")
    session.run("MATCH (lineitem5: Lineitem{l_linenumber: 5}), (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 5}) "
                "CREATE (lineitem5) -[:BELONGS_TO]-> (partsupp5)")


# QUERIES
def query1(session, date):
    return  \
        session.run("MATCH (li: Lineitem) "
                    "WHERE li.l_shipdate <= $date "
                    "RETURN li.l_returnflag AS l_returnflag, li.l_linestatus AS l_linestatus, sum(li.l_quantity) AS sum_qty, "
                    "sum(li.l_extendedprice) AS sum_base_price, sum(li.l_extendedprice * (1 - li.l_discount)) AS sum_disc_price, "
                    "sum(li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax)) AS sum_charge, avg(li.l_quantity) AS avg_qty, "
                    "avg(li.l_extendedprice) AS avg_price, AVG(li.l_discount) AS avg_disc, COUNT(*) AS count_order "
                    "ORDER BY li.l_returnflag, li.l_linestatus",
                    {"date": date})


def query2(session, size, type, region):
    min_supplycost = 1.0
    for row in sub_query2(session, region):
        min_supplycost = float(row["min_supplycost"])

    return \
        session.run("MATCH (p: Part)-[:BELONGS_TO]->(ps: PartSupp)-[:BELONGS_TO]->(s: Supplier)-[:BELONGS_TO]->"
                    "(n: Nation)-[:BELONGS_TO]->(r: Region) "
                    "WHERE p.p_size = $size and p.p_type = $type and r.r_name = $region and ps.ps_supplycost = $min_supplycost "
                    "RETURN s.s_acctbal AS s_acctbal, s.s_name AS s_name, n.n_name AS n_name, p.p_partkey AS p_partkey, "
                    "p.p_mfgr AS p_mfgr, s.s_address AS s_address, s.s_phone AS s_phone, s.s_comment AS s_comment "
                    "ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey",
                    {"size": size,
                     "type": type,
                     "region": region,
                     "min_supplycost": min_supplycost})


def sub_query2(session, region):
    return \
        session.run("MATCH "
                    "(ps: PartSupp)-[:BELONGS_TO]->(s: Supplier)-[:BELONGS_TO]->(n: Nation)-[:BELONGS_TO]->(r: Region) "
                    "WHERE r.r_name = $region "
                    "RETURN min(ps.ps_supplycost) AS min_supplycost",
                    {"region": region})


def query3(session, mkt_segment, date1, date2):
    return  \
        session.run("MATCH (c: Customer)-[:BELONGS_TO]->(o: Order)-[:BELONGS_TO]->(li: Lineitem) "
                    "WHERE li.l_shipdate > $date2 and o.o_orderdate < $date1 and c.c_mktsegment = $mkt_segment "
                    "RETURN o.o_orderkey AS l_orderkey, o.o_orderdate AS o_orderdate, o.o_shippriority AS o_shippriority, "
                    "sum(li.l_extendedprice * (1 - li.l_discount)) AS revenue "
                    "ORDER BY revenue DESC, o.o_orderdate",
                    {"date1": date1,
                    "date2": date2,
                    "mkt_segment": mkt_segment})


def query4(session, date1, date2, region):
    return \
        session.run("MATCH (c: Customer)-[:BELONGS_TO]->(o: Order)-[:BELONGS_TO]->(li: Lineitem)-[:BELONGS_TO]->"
                    "(ps: PartSupp)-[:BELONGS_TO]->(s: Supplier)-[:BELONGS_TO]->(n: Nation)-[:BELONGS_TO]->(r: Region) "
                    "WHERE o.o_orderdate >= $date1 and o.o_orderdate < $date2 and r.r_name = $region "
                    "RETURN n.n_name AS n_name, sum(li.l_extendedprice * (1 - li.l_discount)) AS revenue "
                    "ORDER BY revenue DESC",
                    {"date1": date1,
                     "date2": date2,
                     "region": region})


# AUX
def valid_date(date):
    year, month, day = date.split('-')
    try:
        dt.datetime(int(year), int(month), int(day))
    except ValueError:
        return False
    return True


# MAIN
def main():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test"))
    session = driver.session()
    create_database(session)

    print("What action do you want to do?\n",
          "[1] Execute query 1\n",
          "[2] Execute query 2\n",
          "[3] Execute query 3\n",
          "[4] Execute query 4\n",
          "[0] See the graph database\n",
          "[-1] Exit")

    op = input("Enter the corresponding number.\n")
    op = int(op)

    while op != -1:
        if op == 0:
            for item in session.run('MATCH (n)-[r]->(m) RETURN n, r, m'):
                print(item)

        elif op == 1:
            date_param = input("Enter a lineitem shipdate in the format YYYY-mm-dd: ")
            while not valid_date(date_param):
                date_param = input("Enter a lineitem shipdate in the format YYYY-mm-dd: ")

            q1 = query1(session,
                        str(dt.datetime.strptime(date_param, "%Y-%m-%d")))

            print("Query 1 results:")
            for row in q1:
                print(row)

        elif op == 2:
            size_param = input("Enter a part size: ")
            while not size_param.isdigit():
                size_param = input("Enter a part size: ")

            type_param = input("Enter a part type: ")

            region_param = input("Enter a region name: ")

            q2 = query2(session,
                        int(size_param),
                        str(type_param),
                        str(region_param))

            print("Query 2 results:")
            for row in q2:
                print(row)

        elif op == 3:
            mkt_segment_param = input("Enter a customer mkt_segment: ")

            date1_param = input("Enter an order orderdate in the format YYYY-mm-dd: ")
            while not valid_date(date1_param):
                date1_param = input("Enter an order orderdate in the format YYYY-mm-dd: ")

            date2_param = input("Enter a lineitem shipdate in the format YYYY-mm-dd: ")
            while not valid_date(date2_param):
                date2_param = input("Enter a lineitem shipdate in the format YYYY-mm-dd: ")

            q3 = query3(session,
                        str(mkt_segment_param),
                        str(dt.datetime.strptime(date1_param, "%Y-%m-%d")),
                        str(dt.datetime.strptime(date2_param, "%Y-%m-%d")))

            print("Query 3 results:")
            for row in q3:
                print(row)

        elif op == 4:
            date = input("Enter an order orderdate in the format YYYY-mm-dd: ")
            while not valid_date(date):
                date = input("Enter an order orderdate in the format YYYY-mm-dd: ")

            date_param = dt.datetime.strptime(date, "%Y-%m-%d")
            date2_param = date_param.replace(date_param.year + 1)

            region_param = input("Enter a region name: ")

            q4 = query4(session,
                        str(date_param),
                        str(date2_param),
                        str(region_param))

            print("Query 4 results:")
            for row in q4:
                print(row)

        elif op == -1:
            break

        print("\nWhat action do you want to do?\n",
              "[1] Execute query 1\n",
              "[2] Execute query 2\n",
              "[3] Execute query 3\n",
              "[4] Execute query 4\n",
              "[0] See the graph database\n",
              "[-1] Exit")

        op = input("Enter the corresponding number.\n")
        op = int(op)


if __name__ == "__main__":
    main()
